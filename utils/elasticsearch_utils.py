from elasticsearch import Elasticsearch, exceptions
from config import settings
from logger import get_logger

logger = get_logger(__name__)

def create_elasticsearch_client() -> Elasticsearch:
    """Create an Elasticsearch client instance."""
    try:
        es = Elasticsearch([{"host": settings.elasticsearch_host, "port": settings.elasticsearch_port}])
        if es.ping():
            logger.info("Connected to Elasticsearch")
        else:
            logger.warning("Could not connect to Elasticsearch")
        return es
    except exceptions.ConnectionError as e:
        logger.error(f"Elasticsearch connection error: {e}")
        raise

def create_index(es: Elasticsearch, index_name: str, mapping: dict) -> None:
    """Create an Elasticsearch index with the specified mapping."""
    try:
        if not es.indices.exists(index=index_name):
            es.indices.create(index=index_name, body=mapping)
            logger.info(f"Index '{index_name}' created successfully")
        else:
            logger.info(f"Index '{index_name}' already exists")
    except exceptions.ElasticsearchException as e:
        logger.error(f"Error creating Elasticsearch index '{index_name}': {e}")
