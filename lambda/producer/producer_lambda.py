import boto3
import json
import requests
import os
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

# --- AWS Configuration ---
ssm = boto3.client('ssm', region_name='eu-central-1')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('steam_inventory_metadata')

def get_gcp_credentials():
    """Retrieves GCP Service Account JSON from AWS SSM Parameter Store"""
    parameter = ssm.get_parameter(Name='/steam-tracker/gcp-key', WithDecryption=True)
    credentials_json = json.loads(parameter['Parameter']['Value'])
    return service_account.Credentials.from_service_account_info(credentials_json)

def get_steam_price(item_id):
    """Simple web scraping for Steam Market prices using Steam's priceoverview API"""
    # Currency=1 represents USD. Note: market_hash_name must be URL-encoded in production if items have special chars
    url = f"https://steamcommunity.com/market/priceoverview/?appid=730&currency=1&market_hash_name={item_id}"
    try:
        response = requests.get(url)
        data = response.json()
        if data.get("success"):
            # Price usually comes as "$1.50" -> convert to float 1.50
            price_str = data["lowest_price"].replace("$", "").replace(",", "")
            return float(price_str)
    except Exception as e:
        print(f"Error fetching price for {item_id}: {e}")
    return None

def lambda_handler(event, context):
    # A. Fetch the list of items from DynamoDB (Our Dimension Table)
    items = table.scan().get('Items', [])
    
    # B. Initialize BigQuery client with credentials from SSM
    credentials = get_gcp_credentials()
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    table_id = f"{credentials.project_id}.steam_analytics.raw_price_history"

    rows_to_insert = []
    
    for item in items:
        item_name = item['item_id']
        print(f"Processing: {item_name}")
        
        price = get_steam_price(item_name)
        
        if price:
            rows_to_insert.append({
                "item_id": item_name,
                "price_usd": price,
                "price_pln": None, # Currency conversion to be added later
                "timestamp": datetime.utcnow().isoformat()
            })

    # C. Stream data to BigQuery
    if rows_to_insert:
        errors = client.insert_rows_json(table_id, rows_to_insert)
        if errors == []:
            print(f"✅ Success! Sent {len(rows_to_insert)} records to BigQuery.")
        else:
            print(f"❌ Errors during insert: {errors}")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Price ingestion completed successfully!')
    }