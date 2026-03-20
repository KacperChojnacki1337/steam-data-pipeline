import boto3
import json
import os
import hashlib
import base64
from google.cloud import bigquery
from google.oauth2 import service_account

# --- Configuration ---
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BQ_DATASET = os.environ.get('BQ_DATASET')
GCP_KEY_PARAM = os.environ.get('GCP_KEY_PARAM')

ssm = boto3.client('ssm')

def get_gcp_credentials():
    """Retrieves GCP Service Account JSON from AWS SSM."""
    parameter = ssm.get_parameter(Name=GCP_KEY_PARAM, WithDecryption=True)
    credentials_json = json.loads(parameter['Parameter']['Value'])
    return service_account.Credentials.from_service_account_info(credentials_json)

def generate_asset_id(item_id, buy_date):
    """Generates the surrogate key to match BigQuery schema."""
    unique_str = f"{item_id}_{buy_date}"
    return hashlib.md5(unique_str.encode()).hexdigest()

def lambda_handler(event, context):
    credentials = get_gcp_credentials()
    client = bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID)
    
    inventory_rows = []
    price_rows = []

# Iterate through records received from Redpanda
    for topic_partition, records in event['records'].items():
        
        for record in records:
            # Decode payload from Base64
            raw_payload = base64.b64decode(record['value']).decode('utf-8')
            payload = json.loads(raw_payload)
            redpanda_time = payload.get('timestamp')
            
            if 'db-inventory-events' in topic_partition:
                asset_id = payload.get('asset_id')
                if not asset_id:
                    asset_id = generate_asset_id(payload.get('item_id'), payload.get('buy_date','unknown'))
                    inventory_rows.append({
                    'asset_id': asset_id,
                    'item_id': payload.get('item_id'),
                    'quantity': payload.get('quantity'),
                    'buy_price': payload.get('buy_price'),
                    'buy_currency': payload.get('buy_currency'),
                    'buy_date': payload.get('buy_date'),
                    'category': payload.get('category'),
                    'purchase_channel': payload.get('purchase_channel'),
                    'last_updated': redpanda_time  
                })
                
            elif 'market-price-events' in topic_partition:
                # Budujemy czysty wiersz dla prices_history
                price_rows.append({
                    'item_id': payload.get('item_id'),
                    'price_usd': payload.get('price_usd'),
                    'timestamp': redpanda_time  
                })
        
    # Load data to BigQuery
    results = {}
    if inventory_rows:
        table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.assets_history"
        # We filter keys to match only what BQ table expects
        errors = client.insert_rows_json(table_id, inventory_rows)
        results['inventory'] = "Success" if not errors else f"Errors: {errors}"

    if price_rows:
        table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.prices_history"
        errors = client.insert_rows_json(table_id, price_rows)
        results['prices'] = "Success" if not errors else f"Errors: {errors}"

    print(f"📊 Consumer Summary: {results}")
    return results