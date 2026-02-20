import boto3
from decimal import Decimal
from datetime import datetime

# Connection Configuration
dynamodb = boto3.resource('dynamodb', region_name='eu-central-1')
table = dynamodb.Table('steam_inventory_metadata')

# Data with Composite Key (item_id + buy_date)
assets_data = [
    # --- Purchase from CSFloat ---
    {
        "item_id": "AWP | Printstream (Well-Worn)", 
        "buy_date": "2026-02-06",  # Approx 13 days ago from Feb 19
        "quantity": 1,
        "buy_price": Decimal('164.81'),
        "buy_currency": "PLN",
        "purchase_channel": "CSFloat",
        "steam_url": "https://steamcommunity.com/market/listings/730/AWP%20%7C%20Printstream%20%28Well-Worn%29",
        "category": "Skin",
        "updated_at": datetime.utcnow().isoformat()
    },
    # --- Purchase from CSFloat (2 years ago) ---
    {
        "item_id": "Little Kev | The Professionals",
        "buy_date": "2024-02-19",  # Approx 2 years ago
        "quantity": 1,
        "buy_price": Decimal('25.00'), # Adjusted - check your history for exact PLN
        "buy_currency": "PLN",
        "purchase_channel": "CSFloat",
        "steam_url": "https://steamcommunity.com/market/listings/730/Little%20Kev%20%7C%20The%20Professionals",
        "category": "Agent",
        "updated_at": datetime.utcnow().isoformat()
    }
]

def seed_data():
    print(f"🚀 Seeding table with Composite Key: {table.table_name}")
    try:
        with table.batch_writer() as batch:
            for asset in assets_data:
                batch.put_item(Item=asset)
                print(f"✅ Added: {asset['item_id']} bought on {asset['buy_date']}")
        print("\n✨ Dimension table updated successfully!")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    seed_data()