import json
import os
import pandas as pd
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor

# Structural: Repository Pattern
class WeatherDataRepository:
    def __init__(self, storage_path='data/weather_data.json'):
        self.storage_path = storage_path

    def load(self):
        """Load raw JSON weather data from a file."""
        if not os.path.exists(self.storage_path):
            raise FileNotFoundError(f"❌ File not found: {self.storage_path}")
        
        with open(self.storage_path, "r") as f:
            return json.load(f)

    def save(self, df, output_path="data/processed_weather.csv"):
        """Save processed DataFrame to a CSV file."""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        print(f"✅ Processed data saved to {output_path}")

# Behavioral: Strategy Pattern for data transformation
class WeatherDataTransformer(ABC):
    @abstractmethod
    def transform(self, raw_data):
        pass

class OpenMeteoTransformer(WeatherDataTransformer):
    def transform(self, raw_data):
        """Extract and transform relevant weather data."""
        try:
            timestamps = raw_data["hourly"]["time"]
            temperatures = raw_data["hourly"]["temperature_2m"]
            precipitation = raw_data["hourly"]["precipitation"]

            # Convert to DataFrame
            df = pd.DataFrame({
                "Timestamp": timestamps,
                "Temperature (°C)": temperatures,
                "Precipitation (mm)": precipitation
            })

            print("✅ Data transformation successful!")
            return df
        except KeyError as e:
            print(f"❌ Missing key in data: {e}")
            return None

# Behavioral: Command Pattern for transformation process
class WeatherTransformationCommand:
    def __init__(self, repository: WeatherDataRepository, transformer: WeatherDataTransformer):
        self.repository = repository
        self.transformer = transformer

    def execute(self):
        """Load raw data, transform it, and save as CSV."""
        try:
            raw_data = self.repository.load()
            df = self.transformer.transform(raw_data)
            if df is not None:
                self.repository.save(df)
        except Exception as e:
            print(f"❌ Error during transformation: {e}")

# Concurrency: Using a ThreadPoolExecutor for the transformation
class WeatherTransformationService:
    def __init__(self, repository: WeatherDataRepository, transformer: WeatherDataTransformer):
        self.repository = repository
        self.transformer = transformer

    def transform_and_save(self):
        """Use concurrency to load, transform, and save weather data."""
        command = WeatherTransformationCommand(self.repository, self.transformer)
        with ThreadPoolExecutor() as executor:
            future = executor.submit(command.execute)
            future.result()  # Wait for the transformation to complete

# Run the transformation process
if __name__ == "__main__":
    repository = WeatherDataRepository()
    transformer = OpenMeteoTransformer()
    service = WeatherTransformationService(repository, transformer)
    service.transform_and_save()