import json
import time
import requests
from kafka import KafkaProducer
from datetime import datetime

# Constants

KAFKA_TOPIC = "stations"
KAFKA_SERVER = 'kafka:9092'
API_URL = f"https://api.jcdecaux.com/vls/v1/stations?country_code=FR&apiKey={venv.API_KEY}"

# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    max_request_size=2000000
)

def fetch_station_data():
    """Fetches data from the Velib API."""
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def transform_data(line):
    """Transforms data from the API to the desired format."""
    return {
        'numbers': line['number'],
        'contract_name': line['contract_name'],
        'banking': line['banking'],
        'bike_stands': line['bike_stands'],
        'available_bike_stands': line['available_bike_stands'],
        'available_bikes': line['available_bikes'],
        'address': line['address'],
        'status': line['status'],
        'position': line['position'],
        'last_update': datetime.utcfromtimestamp(line['last_update'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
    }

def send_to_kafka(data):
    """Sends transformed data to the Kafka topic."""
    producer.send(KAFKA_TOPIC, value=data)
    print(f"Data sent to Kafka: {data}")



def main():
    """Main function to continuously fetch, transform, and send data."""
    while True:
        data = fetch_station_data()
        if data:
            for line in data:
                transformed_data = transform_data(line)
                send_to_kafka(transformed_data)
                time.sleep(2)
        time.sleep(10)

if __name__ == "__main__":
    main()
