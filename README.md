# Kappa Architecture Data Pipeline

## Overview
This project implements a **Kappa Architecture**-based data pipeline using the following technologies:

- **Apache Kafka**: Event streaming platform for real-time data ingestion.
- **Apache Spark**: Batch and stream processing engine.
- **Apache Airflow**: Workflow orchestration and scheduling.
- **Apache Cassandra**: NoSQL database for storing processed data.

## System Architecture

<img src="./images/architecture.png">

### Key Principles:
1. **Single Stream Processing**: All data is ingested and processed as a continuous stream.
2. **Immutable Data Log**: Data is stored in an append-only log (Kafka) to ensure consistency and replayability.
3. **Scalability & Fault Tolerance**: Distributed computing systems ensure high availability.

## Technologies Used

### 1. Apache Kafka
- Serves as the central message broker.
- Handles real-time data ingestion and buffering.
- Allows replaying data streams for reprocessing.

### 2. Apache Spark
- Processes real-time and historical data using **Spark Streaming**.
- Transforms, aggregates, and analyzes streaming data.
- Stores results in Cassandra for fast querying.

### 3. Apache Airflow
- Manages and schedules data workflows.
- Orchestrates Spark jobs and Kafka data pipelines.

### 4. Apache Cassandra
- Distributed NoSQL database for storing processed data.
- Optimized for write-heavy workloads and real-time querying.

## Data Flow
1. **Data Ingestion**: Events are ingested into Kafka from multiple sources.
2. **Stream Processing**: Spark Streaming consumes Kafka topics, processes data, and stores results in Cassandra.
3. **Workflow Orchestration**: Airflow schedules and monitors Spark jobs.
4. **Data Storage**: Processed data is persisted in Cassandra for efficient retrieval.

## Setup & Installation
### Prerequisites
- **Java 8**
- **Python 3.9**
- **Docker & Docker Compose**

### Running the Pipeline
   ```bash
   pip install -r requirements.txt
   docker-compose up -d 
   spark-submit --master spark://localhost:7077 ./main.py
   ```

### Access UI
1. Control Center
   <a href="http://localhost:9021">http://localhost:9021</a>
2. Airflow UI
   <a href="http://localhost:8080">http://localhost:8080</a>
   - Account: airflow
   - Password: airflow
3. SparkUI
   <a href="http://localhost:9090">http://localhost:9090</a>