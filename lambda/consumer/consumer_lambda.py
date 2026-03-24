import boto3
import json
import os
import base64
from google.cloud import bigquery
from google.oauth2 import service_account

# --- Configuration ---
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BQ_DATASET = os.environ.get('BQ_DATASET')
GCP_KEY_PARAM = os.environ.get('GCP_KEY_PARAM')

ssm = boto3.client('ssm')

def _load_gcp_credentials():
    """Retrieves GCP Service Account JSON from AWS SSM. Cached at module level."""
    parameter = ssm.get_parameter(Name=GCP_KEY_PARAM, WithDecryption=True)
    credentials_json = json.loads(parameter['Parameter']['Value'])
    return service_account.Credentials.from_service_account_info(credentials_json)

_GCP_CREDENTIALS = _load_gcp_credentials()

def lambda_handler(event, context):
    client = bigquery.Client(credentials=_GCP_CREDENTIALS, project=GCP_PROJECT_ID)

    inventory_rows = []
    price_rows = []
    exchange_rate_rows = []

    # Iterate through records received from Redpanda
    for topic_partition, records in event.get('records', {}).items():

        for record in records:
            # Decode payload from Base64
            raw_payload = base64.b64decode(record['value']).decode('utf-8')
            payload = json.loads(raw_payload)
            redpanda_time = payload.get('timestamp')

            if 'db-inventory-events' in topic_partition:
                asset_id = payload.get('asset_id')
                if not asset_id:
                    print(f"⚠️ Missing asset_id in payload, skipping: {payload}")
                    continue
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
                price_rows.append({
                    'item_id': payload.get('item_id'),
                    'price_usd': payload.get('price_usd'),
                    'timestamp': redpanda_time
                })

            elif 'exchange-rate-events' in topic_partition:
                exchange_rate_rows.append({
                    'from_currency': payload.get('from_currency'),
                    'to_currency': payload.get('to_currency'),
                    'rate': payload.get('rate'),
                    'source': payload.get('source'),
                    'timestamp': redpanda_time
                })

    # Load data to BigQuery
    results = {}

    if inventory_rows:
        table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.assets_history"
        errors = client.insert_rows_json(table_id, inventory_rows)
        results['inventory'] = "Success" if not errors else f"Errors: {errors}"

    if price_rows:
        table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.prices_history"
        errors = client.insert_rows_json(table_id, price_rows)
        results['prices'] = "Success" if not errors else f"Errors: {errors}"

    if exchange_rate_rows:
        table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.exchange_rates"
        errors = client.insert_rows_json(table_id, exchange_rate_rows)
        results['exchange_rates'] = "Success" if not errors else f"Errors: {errors}"

    print(f"📊 Consumer Summary: {results}")
    return results