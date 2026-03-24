import boto3
import json
import requests
import os
from datetime import datetime, timezone
from confluent_kafka import Producer
import time

# --- Configuration from Environment Variables ---
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE')
RP_BOOTSTRAP_PARAM = os.environ.get('RP_BOOTSTRAP_PARAM')
RP_USER_PARAM = os.environ.get('RP_USER_PARAM')
RP_PASS_PARAM = os.environ.get('RP_PASS_PARAM')

# --- Initialize AWS Clients ---
ssm = boto3.client('ssm')
dynamodb = boto3.resource('dynamodb')
inventory_table = dynamodb.Table(DYNAMODB_TABLE)

def _load_redpanda_config():
    response = ssm.get_parameters(
        Names=[RP_BOOTSTRAP_PARAM, RP_USER_PARAM, RP_PASS_PARAM],
        WithDecryption=True
    )
    params = {p['Name']: p['Value'] for p in response['Parameters']}
    return {
        'bootstrap.servers': params[RP_BOOTSTRAP_PARAM],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'SCRAM-SHA-256',
        'sasl.username': params[RP_USER_PARAM],
        'sasl.password': params[RP_PASS_PARAM],
        'client.id': 'steam-producer-lambda'
    }

_REDPANDA_CONF = _load_redpanda_config()

def get_redpanda_producer():
    """Configures Kafka Producer for Redpanda Serverless."""
    return Producer(_REDPANDA_CONF)

def get_steam_price(market_hash_name, retries=3, backoff=2):
    """Fetches the latest lowest price from Steam Market API."""
    encoded_name = requests.utils.quote(market_hash_name)
    url = f"https://steamcommunity.com/market/priceoverview/?appid=730&currency=1&market_hash_name={encoded_name}"
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get("success") and "lowest_price" in data:
                    price_str = data["lowest_price"].replace("$", "").replace(",", "")
                    return float(price_str)
        except Exception as e:
            print(f"⚠️ Attempt {attempt + 1}/{retries} failed for {market_hash_name}: {e}")
            if attempt < retries - 1:
                time.sleep(backoff ** attempt)
    print(f"❌ All {retries} attempts failed for {market_hash_name}, skipping.")
    return None

def get_nbp_rate(currency='USD', retries=3, backoff=2):
    """Fetches current exchange rate from NBP API (PLN per 1 unit of currency)."""
    url = f"https://api.nbp.pl/api/exchangerates/rates/a/{currency.lower()}/today/?format=json"
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                return float(data['rates'][0]['mid'])
            elif response.status_code == 404:
                # NBP returns 404 when rate not yet published (weekends, holidays)
                # Fallback: fetch last available rate
                url_last = f"https://api.nbp.pl/api/exchangerates/rates/a/{currency.lower()}/last/1/?format=json"
                response = requests.get(url_last, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    return float(data['rates'][0]['mid'])
        except Exception as e:
            print(f"⚠️ Attempt {attempt + 1}/{retries} failed for NBP rate {currency}: {e}")
            if attempt < retries - 1:
                time.sleep(backoff ** attempt)
    print(f"❌ All {retries} attempts failed for NBP rate {currency}, skipping.")
    return None

def lambda_handler(event, context):
    print(f"🔍 Scanning DynamoDB: {DYNAMODB_TABLE}")
    items = inventory_table.scan().get('Items', [])

    if not items:
        return {'statusCode': 200, 'body': 'No items found.'}

    producer = get_redpanda_producer()
    current_ts = datetime.now(timezone.utc).isoformat()
    inventory_count = 0
    price_count = 0

    # 1. Fetch NBP rate once per invocation (not per skin)
    usd_pln_rate = get_nbp_rate('USD')
    if usd_pln_rate is not None:
        exchange_payload = {
            "from_currency": "USD",
            "to_currency": "PLN",
            "rate": usd_pln_rate,
            "source": "NBP",
            "timestamp": current_ts
        }
        producer.produce(
            'exchange-rate-events',
            key='USD_PLN',
            value=json.dumps(exchange_payload)
        )
        print(f"💱 NBP USD/PLN rate: {usd_pln_rate}")
    else:
        print("⚠️ Could not fetch NBP rate, skipping exchange rate event.")

    for item in items:
        item_id = item['item_id']

        # 2. Send inventory data to 'db-inventory-events'
        inventory_payload = {
            "asset_id": item.get('asset_id'),
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

        # 3. Fetch and send price data to 'market-price-events'
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

    producer.flush()

    summary = {
        "status": "success",
        "inventory_events_sent": inventory_count,
        "price_events_sent": price_count,
        "exchange_rate_usd_pln": usd_pln_rate
    }
    print(f"📊 Summary: {summary}")

    return {
        'statusCode': 200,
        'body': json.dumps(summary)
    }