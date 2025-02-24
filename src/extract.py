import requests
import json
import os
import threading
from abc import ABC, abstractmethod

# Creational: Factory Pattern
class HttpClient:
    @staticmethod
    def create_client():
        return requests.Session()

# Behavioral: Strategy Pattern
class WeatherDataFetcher(ABC):
    @abstractmethod
    def fetch(self, latitude, longitude):
        pass

class OpenMeteoFetcher(WeatherDataFetcher):
    def fetch(self, latitude, longitude):
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": "temperature_2m,precipitation"
        }
        response = HttpClient.create_client().get(url, params=params)
        return response

# Structural: Repository Pattern
class WeatherDataRepository:
    def __init__(self, storage_path='data/weather_data.json'):
        self.storage_path = storage_path
    
    def save(self, data):
        os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)
        with open(self.storage_path, "w") as f:
            json.dump(data, f, indent=4)

# Concurrency: Using threading to fetch data
class WeatherService:
    def __init__(self, fetcher: WeatherDataFetcher, repository: WeatherDataRepository):
        self.fetcher = fetcher
        self.repository = repository

    def fetch_and_save_weather_data(self, latitude=40.71, longitude=-74.01):
        def task():
            response = self.fetcher.fetch(latitude, longitude)
            if response.status_code == 200:
                data = response.json()
                self.repository.save(data)
                print("✅ Weather data successfully fetched and saved!")
            else:
                print(f"❌ Failed to fetch data. Status code: {response.status_code}")

        thread = threading.Thread(target=task)
        thread.start()
        thread.join()

# Run the service when executed directly (not imported as a module).
if __name__ == "__main__":
    fetcher = OpenMeteoFetcher()
    repository = WeatherDataRepository()
    service = WeatherService(fetcher, repository)
    service.fetch_and_save_weather_data()