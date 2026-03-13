import azure.functions as func
import datetime
import json
import logging
import requests
import os
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()

# The cron schedule "0 0 8 * * *" means it runs at 8:00 AM daily.
# run_on_startup=True forces it to run immediately when we test it locally.
@app.timer_trigger(schedule="0 0 8 * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False) 
def FetchWeatherData(myTimer: func.TimerRequest) -> None:
    
    logging.info('Python timer trigger function started execution.')

    try:
        # 1. Extract: Pull live metric data (Celsius, km/h) from Open-Meteo API
        # We are using coordinates for Berlin as an example
        url = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current_weather=true"
        response = requests.get(url)
        response.raise_for_status() # Fails the pipeline immediately if the API is down
        raw_data = response.json()
        
        # Add an ingestion timestamp so we know exactly when this ran
        timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
        raw_data['ingestion_timestamp'] = timestamp
        
        # 2. Connect to the local Azurite emulator
        connect_str = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        container_client = blob_service_client.get_container_client("bronze-raw-data")
        
        # 3. Load: Save the raw JSON file into the bronze container
        file_name = f"weather_data_{timestamp}.json"
        blob_client = container_client.get_blob_client(file_name)
        
        json_payload = json.dumps(raw_data)
        blob_client.upload_blob(json_payload, overwrite=True)
        
        logging.info(f"SUCCESS: Uploaded {file_name} to local Azurite!")

    except Exception as e:
        logging.error(f"PIPELINE FAILURE: {e}")