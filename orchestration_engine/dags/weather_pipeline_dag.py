from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define the default rules for the pipeline
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 11), # Set to yesterday so it can run immediately
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
with DAG(
    'bronze_to_silver_weather_pipeline',
    default_args=default_args,
    description='A daily pipeline to extract weather metrics (Celsius, km/h) and transform via Spark',
    schedule_interval='0 8 * * *', # Runs daily at 8:00 AM
    catchup=False,
    tags=['weather', 'medallion_architecture'],
) as dag:

    # Tasks
    
    # Simulate the signal to the Azure Function
    trigger_ingestion_api = BashOperator(
        task_id='extract_live_weather_data',
        bash_command='echo "Sending HTTP trigger to Azure Function to fetch Open-Meteo API data..." && sleep 2'
    )

    # Simulate the signal to the Databricks/Spark cluster
    trigger_spark_transformation = BashOperator(
        task_id='transform_json_to_parquet_silver',
        bash_command='echo "Sending API signal to Apache Spark to flatten JSON and save to Parquet..." && sleep 3'
    )
    
    # A simple quality check step
    data_quality_check = BashOperator(
        task_id='verify_silver_layer',
        bash_command='echo "Querying Parquet file to ensure temperature_celsius is not null..." && sleep 1'
    )

    # Define the Execution Order 
    trigger_ingestion_api >> trigger_spark_transformation >> data_quality_check