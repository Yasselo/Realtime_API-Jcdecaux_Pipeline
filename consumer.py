#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from elasticsearch import Elasticsearch

# Stop existing Spark session if any
sessions = SparkSession._instantiatedSession
if sessions is not None:
    sessions.stop()

# Initialize Elasticsearch
try:
    es = Elasticsearch("http://elasticsearch:9200")
    if not es.ping():
        print("Failed to connect to Elasticsearch")
except Exception as e:
    print(f"Error connecting to Elasticsearch: {e}")

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

# Create Elasticsearch index if it doesn't exist
def create_index_station(client, index, mapping):
    try:
        if not client.indices.exists(index=index):
            client.indices.create(index=index, body=mapping)
            print(f"Index '{index}' created successfully")
        else:
            print(f"Index '{index}' already exists")
    except Exception as e:
        print(f"Error creating index: {e}")

create_index_station(client=es, index="stations", mapping=mapping)

# Create Spark Session
spark = SparkSession.builder \
    .appName("VelibConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,org.elasticsearch:elasticsearch-spark-30_2.12:8.8.2") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Define schema of data retrieved from Kafka
schema = StructType([
    StructField("numbers", IntegerType(), True),
    StructField("contract_name", StringType(), True),
    StructField("banking", StringType(), True),
    StructField("bike_stands", IntegerType(), True),
    StructField("available_bike_stands", IntegerType(), True),
    StructField("available_bikes", IntegerType(), True),
    StructField("address", StringType(), True),
    StructField("status", StringType(), True),
    StructField("position", StructType([
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True)
    ]), True),
    StructField("last_update", StringType(), True)
])

# Step 1: Read Data from Kafka
print("Attempting to read data from Kafka...")
try:
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "stations") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    print("Successfully connected to Kafka")
except Exception as e:
    print(f"Error reading from Kafka: {e}")

# Debug: Show Kafka schema
print("Kafka DataFrame Schema:")
kafka_df.printSchema()

# Step 2: Extract JSON Data
print("Extracting JSON data from Kafka messages...")
json_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# Debug: Show parsed JSON schema and a sample
json_df.printSchema()

# Step 3: Debug Streaming Query (Console Output)
print("Starting console query to debug data ingestion...")
console_query = json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Wait for a few seconds to collect some data
console_query.awaitTermination(10)

# Step 4: Write to Elasticsearch
print("Writing data to Elasticsearch...")
try:
    es_query = json_df.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .outputMode("append") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .option("es.resource", "stations") \
        .option("es.nodes.wan.only", "false") \
        .option("checkpointLocation", "/tmp/spark-checkpoint") \
        .start()
    print("Elasticsearch streaming started successfully")
except Exception as e:
    print(f"Error writing to Elasticsearch: {e}")

# Monitor Elasticsearch stream
import time
while es_query.isActive:
    print("Elasticsearch Stream Progress:")
    print(es_query.lastProgress)
    time.sleep(10)

# Keep both streams running
console_query.awaitTermination()
es_query.awaitTermination()
