# Azure Data Lakehouse Pipeline (Bronze to Silver)

This repository contains a localized data pipeline utilizing the Medallion Architecture. It extracts live API data, transforms it using distributed processing, and orchestrates the workflow via a localized Directed Acyclic Graph.

### Architecture & Tech Stack
* **Extraction:** Azure Functions (Python) pulling live metric data (Celsius, km/h).
* **Storage:** Localized Azure Blob Storage (Azurite emulator).
* **Transformation:** Apache Spark (PySpark) flattening nested JSON and converting to columnar Parquet format.
* **Orchestration:** Apache Airflow running in isolated Docker containers.

### Pipeline Workflow
1. **Airflow DAG:** Triggers daily on a cron schedule to initiate the pipeline.
2. **Bronze Layer:** A serverless Azure Function requests live weather data from Open-Meteo, appends ingestion timestamps, and writes raw JSON to a Bronze storage container.
3. **Silver Layer:** A PySpark job reads the Bronze JSON, applies schema transformations, and writes compressed Parquet files to a Silver storage container to optimize future OLAP queries.

### How to Run Locally

**1. Start the Storage Emulator**
docker run -p 10000:10000 -d mcr.microsoft.com/azure-storage/azurite azurite --skipApiVersionCheck --blobHost 0.0.0.0

**2. Start the Airflow Orchestrator**
cd orchestration_engine
docker compose up -d

**3. Monitor the Pipeline**
Navigate to `http://localhost:8080` to access the Airflow UI (Credentials: airflow/airflow) and trigger the `bronze_to_silver_weather_pipeline` DAG.