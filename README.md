# Kappa Architecture Data Pipeline

## Overview
This project implements a **Kappa Architecture**-based data pipeline using the following technologies:

- **Apache Kafka**: Event streaming platform for real-time data ingestion.
- **Apache Spark**: Batch and stream processing engine.
- **Apache Airflow**: Workflow orchestration and scheduling.
- **Apache Cassandra**: NoSQL database for storing processed data.

## System Architecture

<img src="./images/architecture.png">

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
