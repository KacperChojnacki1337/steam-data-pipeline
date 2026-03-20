import boto3
import json
import requests
import os
import hashlib
from datetime import datetime
from confluent_kafka import Producer

# --- Configuration from Environment Variables ---
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE')
GCP_KEY_PARAM = os.environ.get('GCP_KEY_PARAM') 
RP_BOOTSTRAP_PARAM = os.environ.get('RP_BOOTSTRAP_PARAM')
RP_USER_PARAM = os.environ.get('RP_USER_PARAM')
RP_PASS_PARAM = os.environ.get('RP_PASS_PARAM')

# Initialize AWS Clients
ssm = boto3.client('ssm')
dynamodb = boto3.resource('dynamodb')
inventory_table = dynamodb.Table(DYNAMODB_TABLE)

def get_ssm_param(param_name):
    """Retrieves secure parameters from AWS SSM."""
    parameter = ssm.get_parameter(Name=param_name, WithDecryption=True)
    return parameter['Parameter']['Value']

def get_redpanda_producer():
    """Configures Kafka Producer for Redpanda Serverless."""
    conf = {
        'bootstrap.servers': get_ssm_param(RP_BOOTSTRAP_PARAM),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'SCRAM-SHA-256',
        'sasl.username': get_ssm_param(RP_USER_PARAM),
        'sasl.password': get_ssm_param(RP_PASS_PARAM),
        'client.id': 'steam-producer-lambda'
    }
    return Producer(conf)

def get_steam_price(market_hash_name):
    """Fetches the latest lowest price from Steam Market API."""
    encoded_name = requests.utils.quote(market_hash_name)
    url = f"https://steamcommunity.com/market/priceoverview/?appid=730&currency=1&market_hash_name={encoded_name}"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get("success") and "lowest_price" in data:
                price_str = data["lowest_price"].replace("$", "").replace(",", "")
                return float(price_str)
    except Exception as e:
        print(f"❌ Error fetching price for {market_hash_name}: {e}")
    return None

def lambda_handler(event, context):
    print(f"🔍 Scanning DynamoDB: {DYNAMODB_TABLE}")
    items = inventory_table.scan().get('Items', [])
    
    if not items:
        return {'statusCode': 200, 'body': 'No items found.'}

    producer = get_redpanda_producer()
    current_ts = datetime.utcnow().isoformat()
    inventory_count = 0
    price_count = 0

    for item in items:
        item_id = item['item_id']
        
        # 1. Send Inventory Data to 'db-inventory-events'
        inventory_payload = {
            "item_id": item_id,
            "buy_date": item.get('buy_date'),
            "buy_price": float(item.get('buy_price', 0)),
            "buy_currency": item.get('buy_currency', 'PLN'),
            "quantity": int(item.get('quantity', 1)),
            "category": item.get('category', 'Skin'),
            "purchase_channel": item.get('purchase_channel', 'Unknown'),
            "timestamp": current_ts
        }
        producer.produce(
            'db-inventory-events', 
            key=item_id, 
            value=json.dumps(inventory_payload)
        )
        inventory_count += 1

        # 2. Fetch and Send Price Data to 'market-price-events'
        market_price = get_steam_price(item_id)
        if market_price is not None:
            price_payload = {
                "item_id": item_id,
                "price_usd": market_price,
                "timestamp": current_ts
            }
            producer.produce(
                'market-price-events', 
                key=item_id, 
                value=json.dumps(price_payload)
            )
            price_count += 1
        else:
            print(f"⚠️ Could not fetch price for {item_id}, skipping price event.")

    # Ensure all messages are delivered before Lambda finishes
    producer.flush() 
    
    summary = {
        "status": "success", 
        "inventory_events_sent": inventory_count,
        "price_events_sent": price_count
    }
    print(f"📊 Summary: {summary}")
    
    return {
        'statusCode': 200,
        'body': json.dumps(summary)
    }