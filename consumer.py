# consumer.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from elasticsearch import Elasticsearch
from config import settings
from logger import get_logger

logger = get_logger(__name__)

# Initialize Elasticsearch
es = Elasticsearch([{"host": settings.elasticsearch_host, "port": settings.elasticsearch_port}])

spark = SparkSession.builder \
    .appName("VelibConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,org.elasticsearch:elasticsearch-spark-30_2.12:8.8.2") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("numbers", IntegerType()),
    StructField("contract_name", StringType()),
    StructField("banking", StringType()),
    StructField("bike_stands", IntegerType()),
    StructField("available_bike_stands", IntegerType()),
    StructField("available_bikes", IntegerType()),
    StructField("address", StringType()),
    StructField("status", StringType()),
    StructField("position", StructType([
        StructField("lat", DoubleType()),
        StructField("lng", DoubleType())
    ])),
    StructField("last_update", StringType())
])

logger.info("Reading data from Kafka...")
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", settings.kafka_server) \
    .option("subscribe", settings.kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

json_df = kafka_df.selectExpr("CAST(value AS STRING)").select(F.from_json("value", schema).alias("data")).select("data.*")

logger.info("Writing data to Elasticsearch...")
es_query = json_df.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", settings.elasticsearch_host) \
    .option("es.port", settings.elasticsearch_port) \
    .option("es.resource", settings.elasticsearch_index) \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .start()

es_query.awaitTermination()
