import os
import shutil
import glob
from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def run_transformation():
    # Initialize the Apache Spark engine
    print("Initializing Apache Spark...")
    spark = SparkSession.builder \
        .appName("WeatherTransformation") \
        .getOrCreate()

    # Connect to local azurite
    connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
    if not connect_str:
        print("ERROR: Connection string not found. Did you run the export command?")
        return
        
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    
    # Define our data lake layers
    bronze_container = blob_service_client.get_container_client("bronze-raw-data")
    silver_container_name = "silver-transformed-data"
    
    # Create the silver container if it doesn't exist yet
    try:
        blob_service_client.create_container(silver_container_name)
    except Exception:
        pass 
    
    silver_container = blob_service_client.get_container_client(silver_container_name)

    # Find the latest raw JSON file
    print("Locating latest raw data in Bronze layer...")
    blobs = list(bronze_container.list_blobs())
    json_blobs = [b for b in blobs if b.name.endswith(".json")]
    
    if not json_blobs:
        print("No JSON data found in Bronze.")
        return
        
    # Sort to grab the most recent file
    latest_blob = sorted(json_blobs, key=lambda x: x.name)[-1]
    
    # Download locally for Spark processing
    temp_json_path = "temp_raw.json"
    print(f"Downloading {latest_blob.name}...")
    with open(temp_json_path, "wb") as f:
        f.write(bronze_container.download_blob(latest_blob.name).readall())

    # Transform the data with Spark DataFrames
    print("\nTransforming data with Spark...")
    df = spark.read.json(temp_json_path, multiLine=True)
    
    # Flatten the nested JSON structure and rename columns for clarity
    
    clean_df = df.select(
        col("latitude"),
        col("longitude"),
        col("ingestion_timestamp"),
        col("current_weather.time").alias("observation_time"),
        col("current_weather.temperature").alias("temperature_celsius"),
        col("current_weather.windspeed").alias("windspeed_kmh")
    )
    
    # Show the cleaned table in the terminal so we can verify it
    clean_df.show()

    # Save as parquet format
    temp_parquet_dir = "temp_silver_parquet"
    print("Compressing into Parquet format...")
    clean_df.write.mode("overwrite").parquet(temp_parquet_dir)

    # Upload to the silver layer in Azurite
    print("Uploading to Azurite Silver Layer...")
    
    # Find the actual data file
    parquet_files = glob.glob(f"{temp_parquet_dir}/*.parquet")
    
    for file_path in parquet_files:
        file_name = os.path.basename(file_path)
        destination_name = f"weather_data/processed/{file_name}"
        
        with open(file_path, "rb") as data:
            silver_container.upload_blob(name=destination_name, data=data, overwrite=True)
            
    # Clean up temporary files
    os.remove(temp_json_path)
    shutil.rmtree(temp_parquet_dir)
    print("\nSUCCESS: Data transformed and loaded into Silver layer!")

if __name__ == "__main__":
    run_transformation()