import boto3
import json
import requests
import os
import hashlib
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

# --- Konfiguracja pobierana ze zmiennych środowiskowych (z main.tf) ---
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE')
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BQ_DATASET = os.environ.get('BQ_DATASET')
GCP_KEY_PARAM = os.environ.get('GCP_KEY_PARAM')

# Inicjalizacja klientów AWS
ssm = boto3.client('ssm')
dynamodb = boto3.resource('dynamodb')
inventory_table = dynamodb.Table(DYNAMODB_TABLE)

def get_gcp_credentials():
    """Pobiera klucz Service Account z AWS SSM Parameter Store."""
    parameter = ssm.get_parameter(Name=GCP_KEY_PARAM, WithDecryption=True)
    credentials_json = json.loads(parameter['Parameter']['Value'])
    return service_account.Credentials.from_service_account_info(credentials_json)

def generate_asset_id(item_id, buy_date):
    """Tworzy unikalny identyfikator (hash) dla każdego zakupu."""
    unique_str = f"{item_id}_{buy_date}"
    return hashlib.md5(unique_str.encode()).hexdigest()

def get_steam_price(market_hash_name):
    """Pobiera aktualną najniższą cenę z Steam Market API."""
    # market_hash_name musi być zakodowany w URL (np. spacje -> %20)
    encoded_name = requests.utils.quote(market_hash_name)
    url = f"https://steamcommunity.com/market/priceoverview/?appid=730&currency=1&market_hash_name={encoded_name}"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get("success") and "lowest_price" in data:
                # Zamiana "$1.50" lub "$1,200.00" na float 1.50
                price_str = data["lowest_price"].replace("$", "").replace(",", "")
                return float(price_str)
        else:
            print(f"⚠️ Steam API returned status {response.status_code} for {market_hash_name}")
    except Exception as e:
        print(f"❌ Error fetching price for {market_hash_name}: {e}")
    return None

def lambda_handler(event, context):
    # 1. Pobierz przedmioty z DynamoDB (Twoje Portfolio)
    print(f"🔍 Scanning DynamoDB table: {DYNAMODB_TABLE}")
    items = inventory_table.scan().get('Items', [])
    
    if not items:
        print("ℹ️ No items found in DynamoDB.")
        return {'statusCode': 200, 'body': 'No items to process.'}

    # 2. Ustawienia BigQuery
    credentials = get_gcp_credentials()
    client = bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID)
    
    fact_table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.fact_price_history"
    dim_table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.dim_assets"

    fact_rows = []
    dim_rows = []
    current_ts = datetime.utcnow().isoformat()

    # 3. Przetwarzanie każdego przedmiotu
    for item in items:
        item_id = item['item_id']
        buy_date = item['buy_date']
        asset_id = generate_asset_id(item_id, buy_date)
        
        print(f"📦 Processing: {item_id} (Asset ID: {asset_id})")
        
        # A. Dane do DIM_ASSETS (wymiary - portfolio)
        dim_rows.append({
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

        # B. Dane do FACT_PRICE_HISTORY (fakty - historia cen rynkowych)
        market_price = get_steam_price(item_id)
        if market_price is not None:
            fact_rows.append({
                "item_id": item_id,
                "price_usd": market_price,
                "timestamp": current_ts
            })
        else:
            print(f"⚠️ Skipping price fact for {item_id} due to API error.")

    # 4. Wstawianie danych do BigQuery
    report = {}

    if dim_rows:
        dim_errors = client.insert_rows_json(dim_table_id, dim_rows)
        report['dim_assets_status'] = "✅ Success" if not dim_errors else f"❌ Errors: {dim_errors}"

    if fact_rows:
        fact_errors = client.insert_rows_json(fact_table_id, fact_rows)
        report['fact_prices_status'] = "✅ Success" if not fact_errors else f"❌ Errors: {fact_errors}"

    print(f"📊 Summary: {json.dumps(report)}")

    return {
        'statusCode': 200,
        'body': json.dumps(report)
    }