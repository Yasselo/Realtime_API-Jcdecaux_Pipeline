import time
import requests
from datetime import datetime
from utils.kafka_utils import create_kafka_producer, send_message
from config import settings
from logger import get_logger

logger = get_logger(__name__)

producer = create_kafka_producer()

def fetch_station_data():
    """Fetch data from the Velib API."""
    try:
        response = requests.get(f"{settings.api_url}?country_code=FR&apiKey={settings.api_key}")
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Error fetching data: {e}")
        return []

def transform_data(line: dict) -> dict:
    """Transforms data from the API to the desired format."""
    return {
        'numbers': line.get('number'),
        'contract_name': line.get('contract_name'),
        'banking': line.get('banking'),
        'bike_stands': line.get('bike_stands'),
        'available_bike_stands': line.get('available_bike_stands'),
        'available_bikes': line.get('available_bikes'),
        'address': line.get('address'),
        'status': line.get('status'),
        'position': line.get('position'),
        'last_update': datetime.utcfromtimestamp(line['last_update'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
    }

def main():
    while True:
        data = fetch_station_data()
        for line in data:
            transformed_data = transform_data(line)
            send_message(producer, settings.kafka_topic, transformed_data)
            time.sleep(2)
        time.sleep(10)

if __name__ == "__main__":
    main()
