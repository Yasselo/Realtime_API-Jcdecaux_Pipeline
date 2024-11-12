
from elasticsearch import Elasticsearch
from config import settings


def create_index():
    es = Elasticsearch([{"host": settings.elasticsearch_host, "port": settings.elasticsearch_port}])
    index_name = "stations"
    
    # Define your index mapping (update as needed)
    # Define Elasticsearch mapping
    mapping = {
        "mappings": {
            "properties": {
                "numbers": { "type": "integer" },
                "contract_name": { "type": "text" },
                "banking": { "type": "text" },
                "bike_stands": { "type": "integer" },
                "available_bike_stands": { "type": "integer" },
                "available_bikes": { "type": "integer" },
                "address": { "type": "text" },
                "status": { "type": "text" },
                "position": {"type": "geo_point"},
                "last_update": { "type": "date" }
            }
        }
    }

    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=mapping)
        print(f"Index '{index_name}' created successfully.")
    else:
        print(f"Index '{index_name}' already exists.")

if __name__ == "__main__":
    create_index()
