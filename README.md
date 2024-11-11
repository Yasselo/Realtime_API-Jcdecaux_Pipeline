
# 🚴‍♂️ _Jcdecaux Bike Real-time Data Pipeline with Docker_

### 📊 Overview
This pipeline captures real-time bike station data from the Jcdecaux public API and processes it using Dockerized services for messaging, real-time processing, storage, and visualization. Kafka handles messaging, Spark Streaming processes data, Elasticsearch stores the information, and Kibana  provide visualization.

### 🛠 Architecture
![Jcdecaux Bike Real-time Data Pipeline](files/Architecture.png)

The pipeline consists of the following components:
1. **Jcdecaux API**: Source of real-time bike station data.
2. **Kafka**: Distributed messaging system for data distribution.
3. **Spark Streaming**: Processes data in real-time.
4. **Elasticsearch**: Stores data for easy querying.
5. **Kibana**: Visualize data stored in Elasticsearch.

### 🧰 Prerequisites
All components run in Docker containers. Ensure Docker and Docker Compose are installed on your system.

### ⚙️ Installation and Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/Yasselo/Realtime_jcdecaux_pipeline
   cd Realtime_jcdecaux_pipeline
   ```

2. **Build and Start Services**:
   Run the following command to start all services, including Kafka, Spark, Elasticsearch, and Kibana, in Docker containers:
   ```bash
   docker-compose up --build
   ```

   This command pulls the necessary Docker images, builds the custom bike pipeline container, and starts all services defined in \`docker-compose.yml\`.

### 🔧 Project Configuration

* **Kafka Topic**: A Kafka topic named \`velib_stations\` will be created automatically for bike station data.
* **Environment Variables**:
  - \`KAFKA_BROKER\`: Kafka broker address (\`kafka:9092\`).
  - \`ELASTICSEARCH_HOST\`: Elasticsearch host (\`elasticsearch\`).
  - \`SPARK_MASTER\`: Spark master URL (\`spark://spark:7077\`).

### 🚀 Running the Pipeline

1. Start the Docker containers:
   ```bash
   docker-compose up
   ```
2. Start the pipeline:
   ```bash
   docker exec -it bike_pipeline bash
   ```
Wait for the various services to initialize. Once they're running, enter the following commands in the bash:
``` python3 producer & python3 consumer```
3. **Kibana Access**: Open [http://localhost:5601](http://localhost:5601) to view Kibana and check Elasticsearch data under "Index Management".


### 🛠 Docker Images and Setup Details

The following Docker images are used:

- **Kafka**: \`wurstmeister/kafka:2.13-2.8.1\`
- **Spark**: \`bitnami/spark:3.2.4\`
- **Elasticsearch**: \`docker.elastic.co/elasticsearch/elasticsearch:8.8.2\`
- **Kibana**: \`docker.elastic.co/kibana/kibana:8.8.2\`
- **Custom Pipeline**: Defined in \`Dockerfile\`, containing OpenJDK 11, Spark, Kafka, and Elasticsearch dependencies.

### 📌 Conclusion
With the setup complete, real-time bike station data should now be visible in Kibana. This project demonstrates the use of a streaming data pipeline for processing and visualizing real-time data using Docker.


