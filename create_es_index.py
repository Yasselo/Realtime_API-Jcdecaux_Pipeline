from utils.elasticsearch_utils import create_elasticsearch_client, create_index
from config import settings

# Elasticsearch index mapping
mapping = {
    "mappings": {
        "properties": {
            "numbers": {"type": "integer"},
            "contract_name": {"type": "text"},
            "banking": {"type": "boolean"},
            "bike_stands": {"type": "integer"},
            "available_bike_stands": {"type": "integer"},
            "available_bikes": {"type": "integer"},
            "address": {"type": "text"},
            "status": {"type": "text"},
            "position": {"type": "geo_point"},
            "last_update": {"type": "date"}
        }
    }
}

def main():
    es = create_elasticsearch_client()
    create_index(es, settings.elasticsearch_index, mapping)

if __name__ == "__main__":
    main()
