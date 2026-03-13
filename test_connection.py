import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# Load the connection string from the .env file
load_dotenv()
connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

def test_azurite_connection():
    try:
        # Connect to the local Azurite emulator
        print("Connecting to local Azure Storage...")
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        # Create a unique container 
        container_name = "bronze-raw-data"
        print(f"Creating container: {container_name}")
        container_client = blob_service_client.create_container(container_name)

        # Create some dummy data and upload it
        file_name = "test_payload.txt"
        data_to_upload = b"Hello! This is my first payload landing in the local data lake."
        
        print(f"Uploading file: {file_name}")
        blob_client = container_client.get_blob_client(file_name)
        blob_client.upload_blob(data_to_upload)

        print("\nSUCCESS! The pipeline foundation is working.")

    except Exception as e:
        print(f"\nERROR: Something went wrong. \n{e}")

if __name__ == "__main__":
    test_azurite_connection()