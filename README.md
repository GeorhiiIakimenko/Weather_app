# Weather API Integration with FastAPI and Celery

This project integrates multiple weather APIs to fetch weather data for cities and categorizes them based on geographic regions using FastAPI and Celery.

## Overview

This application provides an endpoint to fetch weather data for multiple cities. It uses background tasks to fetch data asynchronously via Celery, and stores the results in region-based JSON files.

- Fetches weather data using APIs like WeatherAPI and OpenWeatherMap.
- Organizes cities' weather data into geographic regions.
- Supports task status tracking and retrieving results for specific regions.

## Requirements

To run this project, you'll need:

- Python 3.7 or higher
- Redis server for task queue management
- Celery for background task processing
- FastAPI for the API framework

Install the required dependencies:

```bash
pip install -r requirements.txt
Start Redis server: Ensure Redis is installed and running on your machine. You can start Redis locally by running:

```bash
redis-server
Run Celery worker: In a separate terminal, run the Celery worker to process background tasks:

```bash
celery -A app.celery worker --loglevel=info
Run the FastAPI server: In another terminal, start the FastAPI server:

```bash
uvicorn app:app --reload
This will start the FastAPI application at http://127.0.0.1:8000.

