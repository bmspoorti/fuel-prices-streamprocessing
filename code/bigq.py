import requests
import json
import time
import pandas as pd
from google.cloud import bigquery
import os

cred_path = "C:/Semester-2/DE-2/DE2-Project\KEY.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = cred_path

# API endpoint for fetching petrol prices
url = "https://live-petrol-diesel-price-india.p.rapidapi.com/api/petrol-city-all"
querystring = {"stateID": "uttar-pradesh"}
headers = {
    "X-RapidAPI-Key": "d684616ec8msh3743bff1b4ac2bap1a6edajsn2808d7f22cde",
    "X-RapidAPI-Host": "live-petrol-diesel-price-india.p.rapidapi.com"
}

# BigQuery setup
client = bigquery.Client()
project_id = 'data-engineering2-425417' 
dataset_id = 'FuelPrice' 
table_id = 'FuelTable' 
table_ref = client.dataset(dataset_id).table(table_id)

# Define the schema
schema = [
    bigquery.SchemaField("City", "STRING"),
    bigquery.SchemaField("Price", "FLOAT"),
    bigquery.SchemaField("Change", "FLOAT"),
    bigquery.SchemaField("Unit", "STRING"),
]

def create_table_if_not_exists():
    """Create the BigQuery table if it doesn't exist."""
    try:
        client.get_table(table_ref)
        print(f"Table {table_id} already exists.")
    except Exception as e:
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f"Created table {table_id}.")

def fetch_petrol_prices(url, headers, querystring):
    """Fetch petrol prices from the API."""
    response = requests.get(url, headers=headers, params=querystring)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data from API. Status code: {response.status_code}")

def format_to_table(data):
    """Format JSON data to a table."""
    petrol_price_list = data.get('PetrolPrice', [])
    df = pd.DataFrame(petrol_price_list)
    df.columns = ['City', 'Price', 'Change', 'Unit']
    df['Price'] = df['Price'].astype(float)
    df['Change'] = df['Change'].astype(float)
    return df

def upload_to_bigquery(df):
    """Upload DataFrame to BigQuery."""
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete
    print(f"Loaded {job.output_rows} rows into {dataset_id}.{table_id}.")

# Main loop to fetch and upload data
try:
    create_table_if_not_exists()
    while True:
        data = fetch_petrol_prices(url, headers, querystring)
        df = format_to_table(data)
        print("Fetched data:")
        print(df)
        upload_to_bigquery(df)
        time.sleep(10800) 
except Exception as e:
    print(f"An error occurred while streaming data: {e}")
