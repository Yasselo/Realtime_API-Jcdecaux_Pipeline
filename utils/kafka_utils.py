import json
from kafka import KafkaProducer
from config import settings
from logger import get_logger

logger = get_logger(__name__)

def create_kafka_producer() -> KafkaProducer:
    """Create a Kafka producer instance."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[settings.kafka_server],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            max_request_size=2000000
        )
        logger.info("Kafka producer created successfully")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        raise

def send_message(producer: KafkaProducer, topic: str, data: dict) -> None:
    """Send a message to the specified Kafka topic."""
    try:
        producer.send(topic, value=data)
        logger.info(f"Data sent to Kafka topic '{topic}': {data}")
    except Exception as e:
        logger.error(f"Failed to send data to Kafka: {e}")
