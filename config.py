from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    kafka_topic: str = Field("stations", env="KAFKA_TOPIC")
    kafka_server: str = Field("kafka:9092", env="KAFKA_SERVER")
    api_key: str = Field(..., env="API_KEY")
    api_url: str = "https://api.jcdecaux.com/vls/v1/stations"
    elasticsearch_host: str = Field("elasticsearch", env="ELASTICSEARCH_HOST")
    elasticsearch_port: int = Field(9200, env="ELASTICSEARCH_PORT")
    elasticsearch_index: str = Field("stations", env="ELASTICSEARCH_INDEX")
    log_level: str = Field("INFO", env="LOG_LEVEL")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()
