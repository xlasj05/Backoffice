import streamlit as st
import pandas as pd
import requests as rq
import json
import base64
import os
import time
from azure.storage.blob import BlobServiceClient
from io import StringIO  # For in-memory CSV handling

# Azure Blob Storage configuration
AZURE_BLOB_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=xlasj05;AccountKey=hylOvvY15TxY9bBvN0DZib7PdEjHRwDfwA5yVYdwgilMjPS9W9MCHsXsZywh8DA3YMC5WH6ecqsq+AStwMr2tA==;EndpointSuffix=core.windows.net"  # Replace with your Azure Blob Storage connection string
BLOB_CONTAINER_NAME = "csv"
BLOB_FILE_NAME = "idealista_data.csv"

# Idealista API Authentication
def get_oauth_token():
    url = "https://api.idealista.com/oauth/token"
    apikey = '82bzidhyikkdonhpcdr6eoel1beljbgx'  # Replace with your actual API key
    secret = 'IeO2OmRLxf9Z'  # Replace with your actual secret
    auth = base64.b64encode(f'{apikey}:{secret}'.encode('utf-8')).decode('utf-8')
    headers = {'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8', 'Authorization': 'Basic ' + auth}
    params = {'grant_type': 'client_credentials'}
    content = rq.post(url, headers=headers, data=params)
    if content.status_code == 200:
        return json.loads(content.text)['access_token']
    else:
        raise Exception(f"Failed to obtain access token: {content.status_code}, {content.text}")

# Fetch properties from Idealista
def fetch_properties(center, distance, max_requests):
    token = get_oauth_token()
    df_tot = pd.DataFrame()

    for i in range(1, max_requests + 1):
        url = (f'https://api.idealista.com/3.5/es/search?operation=sale&'
               f'maxItems=500&order=publicationDate&center={center}&distance={distance}&'
               f'propertyType=homes&sort=desc&numPage={i}&language=en')
        
        headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token}
        content = rq.post(url, headers=headers)

        if content.status_code == 200:
            result = json.loads(content.text)
            if 'elementList' in result:
                df = pd.DataFrame.from_dict(result['elementList'])
                df_tot = pd.concat([df_tot, df], ignore_index=True)

                if i >= result.get('totalPages', 1):
                    break
            else:
                st.warning("No data found for this request.")
                break
            time.sleep(2)
        else:
            st.error(f"Failed to fetch properties: {content.status_code}, {content.text}")
            break

    return df_tot

# Save data to Azure Blob Storage
def save_to_blob(data):
    try:
        # Initialize Azure Blob Service Client
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_BLOB_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)

        # Check if the container exists; create if it doesn't
        if not container_client.exists():
            container_client.create_container()
            st.info("Azure Blob Storage container created successfully.")

        # Check if the blob file exists
        blob_client = container_client.get_blob_client(BLOB_FILE_NAME)
        if blob_client.exists():
            # If the blob exists, read its content
            existing_blob = blob_client.download_blob().readall().decode('utf-8')
            existing_df = pd.read_csv(StringIO(existing_blob))
            combined_df = pd.concat([existing_df, data], ignore_index=True).drop_duplicates(subset='propertyCode', keep='last')
        else:
            # If the blob does not exist, use the new data
            combined_df = data

        # Save the updated data to a CSV in-memory
        output = StringIO()
        combined_df.to_csv(output, index=False, encoding='utf-8-sig')
        output.seek(0)

        # Upload the in-memory CSV to Azure Blob Storage
        blob_client.upload_blob(output.getvalue(), overwrite=True)
        st.success("Data successfully saved to Azure Blob Storage!")
        return len(data)
    except Exception as e:
        st.error(f"Failed to save data to Azure Blob Storage: {e}")
        return 0

# Streamlit App
def main():
    st.title("Idealista API Fetcher")
    st.markdown("A tool to fetch property data from Idealista API and store it in Azure Blob Storage.")

    # Input Fields
    max_requests_per_run = st.number_input("Max Requests Per Run", min_value=1, value=1, step=1)
    center_estepona = '36.4277,-5.1459'  # Default coordinates
    distance = 20000  # Distance in meters

    # Start Button
    if st.button("Start"):
        try:
            st.info("Fetching data...")
            df = fetch_properties(center=center_estepona, distance=distance, max_requests=max_requests_per_run)
            
            if not df.empty:
                st.write("Fetched Data:", df.head())
                count = save_to_blob(df)
                st.success(f"Successfully stored {count} records in Azure Blob Storage.")
            else:
                st.warning("No data was fetched.")
        except Exception as e:
            st.error(f"An error occurred: {e}")

# Run the Streamlit App
if __name__ == "__main__":
    main()
