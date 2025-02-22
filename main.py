from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel, validator
from celery import Celery
import requests
import redis
import json
import time
import re
from unidecode import unidecode
from typing import Dict, List
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Celery configuration for background tasks
celery = Celery(
    "tasks", broker="redis://localhost:6379/0", backend="redis://localhost:6379/0"
)

# Redis instance for task tracking
redis_client = redis.Redis(host='localhost', port=6379, db=1, decode_responses=True)

# Weather API configuration: Providers and API keys
class WeatherAPIConfig:
    PROVIDERS = {
        "weatherapi": {
            "url": "http://api.weatherapi.com/v1/current.json",
            "keys": ["b2d52432ba9b4a608d9151307252202", "backup_key_1"],
            "params": lambda key, city: {"key": key, "q": city}
        },
        "openweathermap": {
            "url": "http://api.openweathermap.org/data/2.5/weather",
            "keys": ["4e401a07b786a9aab36032d09b05994c"],
            "params": lambda key, city: {"appid": key, "q": city, "units": "metric"}
        }
    }

# Geographic region boundaries
REGION_BOUNDS = {
    "Europe": {
        "lat": (35, 70),
        "lon": (-25, 40)
    },
    "North America": {
        "lat": (25, 70),
        "lon": (-170, -50)
    },
    "South America": {
        "lat": (-60, 15),
        "lon": (-80, -35)
    },
    "Asia": {
        "lat": (0, 70),
        "lon": (40, 180)
    },
    "Africa": {
        "lat": (-35, 37),
        "lon": (-20, 50)
    },
    "Australia": {
        "lat": (-45, -10),
        "lon": (110, 155)
    }
}

# Request model to receive city names
class CityRequest(BaseModel):
    cities: List[str]

    @validator('cities')
    def validate_cities(cls, cities):
        if not cities:
            raise ValueError("Cities list cannot be empty")
        if len(cities) > 50:  # Reasonable limit
            raise ValueError("Too many cities requested")

        # Basic validation for city names using a regex pattern
        city_pattern = re.compile(r'^[A-Za-zА-Яа-я\s\-\.\']{2,50}$')
        for city in cities:
            if not city_pattern.match(city):
                raise ValueError(f"Invalid city name format: {city}")
        return cities

# Function to normalize city names by removing special characters and formatting
def normalize_city(city: str) -> str:
    city = city.strip().lower()  # Remove extra spaces and convert to lowercase
    city = unidecode(city)  # Convert unicode characters to ASCII
    city = ' '.join(word.capitalize() for word in city.split())  # Capitalize each word
    return city

# Determine region by latitude and longitude coordinates
def determine_region(coords: Dict[str, float]) -> str:
    lat = coords["lat"]
    lon = coords["lon"]
    for region, bounds in REGION_BOUNDS.items():
        if (bounds["lat"][0] <= lat <= bounds["lat"][1] and
                bounds["lon"][0] <= lon <= bounds["lon"][1]):
            return region
    return "Other"  # Default region if no match

# Celery task to fetch weather data for cities in the background
@celery.task(bind=True, max_retries=3)
def fetch_weather_data(self, task_id: str, cities: List[str]):
    results = {region: [] for region in REGION_BOUNDS.keys() | {"Other"}}
    Path("weather_data").mkdir(exist_ok=True)  # Create directory if not exists

    for city in cities:
        normalized_city = normalize_city(city)
        weather_data = None

        # Attempt to fetch data from different weather API providers
        for provider, config in WeatherAPIConfig.PROVIDERS.items():
            if weather_data:
                break

            api_key = config["keys"][int(time.time()) % len(config["keys"])]
            url = config["url"]
            params = config["params"](api_key, normalized_city)

            try:
                logger.info(f"Fetching weather data for {normalized_city} from {provider}")
                response = requests.get(url, params=params, timeout=5)
                if response.status_code == 200:
                    data = response.json()

                    # Extract weather data depending on the provider
                    if provider == "weatherapi":
                        temp_c = data["current"].get("temp_c")
                        description = data["current"].get("condition", {}).get("text", "Unknown")
                        coords = {
                            "lat": float(data["location"]["lat"]),
                            "lon": float(data["location"]["lon"])
                        }
                    else:  # openweathermap
                        temp_c = data["main"].get("temp")
                        description = data["weather"][0].get("description", "Unknown")
                        coords = {
                            "lat": float(data["coord"]["lat"]),
                            "lon": float(data["coord"]["lon"])
                        }

                    if temp_c is not None and -50 <= temp_c <= 50:
                        weather_data = {
                            "city": normalized_city,
                            "temperature": round(temp_c, 1),
                            "description": description,
                            "coordinates": coords
                        }
                else:
                    logger.error(f"API {provider} error: {response.status_code} for {normalized_city}")
                    redis_client.hset(f"{task_id}:errors", city,
                                      f"API {provider} error: {response.status_code}")

            except requests.exceptions.RequestException as e:
                logger.error(f"Request to {provider} failed for {normalized_city}: {str(e)}")
                redis_client.hset(f"{task_id}:errors", city,
                                  f"Request to {provider} failed: {str(e)}")
                continue

        if weather_data:
            region = determine_region(weather_data["coordinates"])
            results[region].append(weather_data)
        else:
            logger.warning(f"Failed to get weather data for {normalized_city} from all providers")
            redis_client.hset(f"{task_id}:errors", city, "Failed to get weather data from all providers")

    # Save weather data results by region
    for region, data in results.items():
        if data:
            file_path = f"weather_data/task_{task_id}_{region}.json"
            logger.info(f"Saving weather data for {region} to {file_path}")
            with open(file_path, "w") as f:
                json.dump({
                    "region": region,
                    "data": data,
                    "timestamp": time.time()
                }, f, indent=4)

    redis_client.set(task_id, "completed")  # Task is completed
    return results


# FastAPI endpoint to process weather data requests
@app.post("/weather")
async def process_weather(cities_request: CityRequest, background_tasks: BackgroundTasks):
    task_id = str(int(time.time()))
    redis_client.set(task_id, "running")  # Set task status to running
    fetch_weather_data.apply_async(args=(task_id, cities_request.cities))  # Trigger background task
    logger.info(f"Weather data request accepted for task_id {task_id}")
    return {
        "task_id": task_id,
        "status": "accepted",
        "message": "Processing weather data for cities"
    }


# FastAPI endpoint to get task status
@app.get("/tasks/{task_id}")
async def get_task_status(task_id: str):
    status = redis_client.get(task_id)
    if not status:
        raise HTTPException(status_code=404, detail="Task not found")

    response = {"status": status}

    if status == "completed":
        # Retrieve all result files for the task_id
        base_path = Path("weather_data")
        result_files = list(base_path.glob(f"task_{task_id}_*.json"))

        response["results"] = {
            file.stem.split('_')[-1]: f"/results/{file.name}"
            for file in result_files
        }

        # Include errors if there are any
        errors = redis_client.hgetall(f"{task_id}:errors")
        if errors:
            response["errors"] = errors

    logger.info(f"Returning task status for task_id {task_id}")
    return response


# FastAPI endpoint to get the results of a specific region
@app.get("/results/{region}")
async def get_results(region: str):
    try:
        base_path = Path("weather_data")
        result_files = list(base_path.glob(f"*_{region}.json"))

        if not result_files:
            raise HTTPException(
                status_code=404,
                detail=f"No results found for region: {region}"
            )

        # Return the latest result file
        latest_file = max(result_files, key=lambda p: p.stat().st_mtime)
        logger.info(f"Returning latest weather data for region {region} from {latest_file}")
        with open(latest_file) as f:
            return json.load(f)

    except Exception as e:
        logger.error(f"Error retrieving results for region {region}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving results: {str(e)}"
        )

