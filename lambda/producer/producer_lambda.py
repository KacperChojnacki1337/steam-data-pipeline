import boto3
import json
import requests
import os
import hashlib
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

# --- Configuration from Environment Variables (managed by Terraform) ---
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE')
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BQ_DATASET = os.environ.get('BQ_DATASET')  # Now points to 'steam_raw'
GCP_KEY_PARAM = os.environ.get('GCP_KEY_PARAM')

# Initialize AWS Clients
ssm = boto3.client('ssm')
dynamodb = boto3.resource('dynamodb')
inventory_table = dynamodb.Table(DYNAMODB_TABLE)

def get_gcp_credentials():
    """Retrieves GCP Service Account JSON from AWS SSM Parameter Store."""
    parameter = ssm.get_parameter(Name=GCP_KEY_PARAM, WithDecryption=True)
    credentials_json = json.loads(parameter['Parameter']['Value'])
    return service_account.Credentials.from_service_account_info(credentials_json)

def generate_asset_id(item_id, buy_date):
    """Generates a unique surrogate key (hash) for each purchase."""
    unique_str = f"{item_id}_{buy_date}"
    return hashlib.md5(unique_str.encode()).hexdigest()

def get_steam_price(market_hash_name):
    """Fetches the latest lowest price from Steam Market API."""
    encoded_name = requests.utils.quote(market_hash_name)
    url = f"https://steamcommunity.com/market/priceoverview/?appid=730&currency=1&market_hash_name={encoded_name}"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get("success") and "lowest_price" in data:
                # Convert "$1.50" or "$1,200.00" to float 1.50
                price_str = data["lowest_price"].replace("$", "").replace(",", "")
                return float(price_str)
        else:
            print(f"⚠️ Steam API returned status {response.status_code} for {market_hash_name}")
    except Exception as e:
        print(f"❌ Error fetching price for {market_hash_name}: {e}")
    return None

def lambda_handler(event, context):
    # 1. Extract: Get items from DynamoDB (Portfolio Source)
    print(f"🔍 Scanning DynamoDB table: {DYNAMODB_TABLE}")
    items = inventory_table.scan().get('Items', [])
    
    if not items:
        print("ℹ️ No items found in DynamoDB.")
        return {'statusCode': 200, 'body': 'No items to process.'}

    # 2. Setup BigQuery Client
    credentials = get_gcp_credentials()
    client = bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID)
    
    # Updated Table IDs to match the new Bronze Layer (steam_raw) structure
    assets_raw_table = f"{GCP_PROJECT_ID}.{BQ_DATASET}.assets_history"
    prices_raw_table = f"{GCP_PROJECT_ID}.{BQ_DATASET}.prices_history"

    assets_rows = []
    prices_rows = []
    current_ts = datetime.utcnow().isoformat()

    # 3. Transform: Prepare data for Bronze Layer ingestion
    for item in items:
        item_id = item['item_id']
        buy_date = item['buy_date']
        asset_id = generate_asset_id(item_id, buy_date)
        
        print(f"📦 Processing: {item_id} (Asset ID: {asset_id})")
        
        # Mapping for assets_history table
        assets_rows.append({
            "asset_id": asset_id,
            "item_id": item_id,
            "buy_date": buy_date,
            "buy_price": float(item.get('buy_price', 0)),
            "buy_currency": item.get('buy_currency', 'PLN'),
            "quantity": int(item.get('quantity', 1)),
            "category": item.get('category', 'Skin'),
            "purchase_channel": item.get('purchase_channel', 'Unknown'),
            "last_updated": current_ts
        })

        # Mapping for prices_history table
        market_price = get_steam_price(item_id)
        if market_price is not None:
            prices_rows.append({
                "item_id": item_id,
                "price_usd": market_price,
                "timestamp": current_ts
            })
        else:
            print(f"⚠️ Skipping price fact for {item_id} due to API error.")

    # 4. Load: Stream data into BigQuery Bronze Layer
    report = {}

    if assets_rows:
        errors = client.insert_rows_json(assets_raw_table, assets_rows)
        report['assets_ingestion'] = "✅ Success" if not errors else f"❌ Errors: {errors}"

    if prices_rows:
        errors = client.insert_rows_json(prices_raw_table, prices_rows)
        report['prices_ingestion'] = "✅ Success" if not errors else f"❌ Errors: {errors}"

    print(f"📊 Summary: {json.dumps(report)}")

    return {
        'statusCode': 200,
        'body': json.dumps(report)
    }