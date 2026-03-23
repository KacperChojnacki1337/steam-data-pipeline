# 🎮 Steam CS2 Portfolio Tracker — Data Engineering Pipeline

A production-grade data engineering pipeline that tracks CS2 skin inventory, fetches real-time market prices from Steam, and calculates portfolio value and unrealized PnL with live USD/PLN exchange rates from the National Bank of Poland (NBP).

---

## 📐 Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          AWS (eu-central-1)                                 │
│                                                                             │
│  ┌─────────────────┐     ┌──────────────────────────────────────────────┐  │
│  │    DynamoDB     │     │           Producer Lambda                    │  │
│  │                 │────►│  1. Scan inventory                           │  │
│  │  steam_         │     │  2. Fetch prices  ──► Steam Market API       │  │
│  │  inventory_     │     │  3. Fetch FX rate ──► NBP API (USD/PLN)      │  │
│  │  metadata       │     │  4. Publish to Redpanda                      │  │
│  │                 │     └──────────────────┬───────────────────────────┘  │
│  │  PK: asset_id   │                        │                              │
│  │  PITR: enabled  │                        ▼                              │
│  └─────────────────┘     ┌──────────────────────────────────────────────┐  │
│                          │            Redpanda Serverless                │  │
│                          │                                               │  │
│                          │  ├── db-inventory-events                      │  │
│                          │  ├── market-price-events                      │  │
│                          │  └── exchange-rate-events                     │  │
│                          └──────────────────┬───────────────────────────┘  │
│                                             │                              │
│                          ┌──────────────────▼───────────────────────────┐  │
│                          │           Consumer Lambda                    │  │
│                          │  Routes events to BigQuery by topic          │  │
│                          └──────────────────┬───────────────────────────┘  │
│                                             │                              │
└─────────────────────────────────────────────┼───────────────────────────────┘
                                              │
                                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        GCP — BigQuery                                       │
│                                                                             │
│  steam_raw (Bronze)                  steam_marts (Gold)                     │
│  ├── assets_history                  ├── stg_assets                         │
│  ├── prices_history                  ├── stg_prices                         │
│  └── exchange_rates                  ├── stg_exchange_rates                 │
│                                      ├── int_latest_prices                  │
│                                      ├── int_latest_exchange_rate           │
│                                      ├── dim_assets                         │
│                                      └── fct_portfolio ◄── PnL + FX        │
└─────────────────────────────────────────────────────────────────────────────┘
                                              ▲
                          ┌───────────────────┴──────────────────────────────┐
                          │         GitHub Actions — dbt Pipeline            │
                          │  Trigger: push to main (dbt/**) + daily 08:00   │
                          │  Steps:  dbt deps → run → test → docs generate  │
                          └──────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack & Design Decisions

| Layer | Technology | Why |
|---|---|---|
| **Source of Truth** | AWS DynamoDB | Schemaless, serverless, scales with inventory size. PITR enabled for audit safety. |
| **Message Broker** | Redpanda Serverless | Kafka-compatible, no cluster management, free tier sufficient for this workload. Decouples ingestion from storage. |
| **Compute** | AWS Lambda | Event-driven, zero idle cost. Producer runs on schedule, consumer triggers on Redpanda events. |
| **Data Warehouse** | Google BigQuery | Serverless, columnar, native dbt support. EU region for GDPR compliance. |
| **Transformations** | dbt | Version-controlled SQL, lineage tracking, built-in testing. Surrogate keys generated in marts, not in application code. |
| **IaC** | Terraform | Full infrastructure as code — DynamoDB, Lambda, IAM, BigQuery datasets and tables, Redpanda event source mappings. |
| **CI/CD** | GitHub Actions | dbt pipeline runs on push to main and daily schedule. No self-hosted runners needed. |
| **Secrets** | AWS SSM Parameter Store | Encrypted parameters for Redpanda credentials and GCP service account key. Cached at Lambda module level to minimize API calls. |
| **FX Rates** | NBP API | Free, official Polish National Bank rates. Fetched once per Lambda invocation, not per item. |

---

## 📊 dbt Models

### Medallion Architecture

```
Bronze (raw)  →  Staging  →  Intermediate  →  Gold (marts)
```

### Staging
Clean and type-cast raw data. One model per source table. No business logic.

| Model | Source | Description |
|---|---|---|
| `stg_assets` | `raw.assets_history` | Casts types, normalises currency to uppercase |
| `stg_prices` | `raw.prices_history` | Casts price and timestamp |
| `stg_exchange_rates` | `raw.exchange_rates` | Renames `source` to `rate_source` to avoid SQL reserved word |

### Intermediate
Reusable logic, not exposed to end users.

| Model | Description |
|---|---|
| `int_latest_prices` | Latest Steam price per `item_id` using `ROW_NUMBER()` |
| `int_latest_exchange_rate` | Latest NBP USD/PLN rate |

### Marts
Business-facing tables. Materialised as tables in BigQuery.

| Model | Description |
|---|---|
| `dim_assets` | Asset dimension — deduplicated, one row per asset. Surrogate key via `dbt_utils.generate_surrogate_key`. |
| `fct_portfolio` | Portfolio fact — current value and unrealized PnL in both USD and PLN |

### Key metrics in `fct_portfolio`

```sql
current_value_pln    = price_usd × usd_pln_rate × quantity
pnl_per_unit_pln     = (price_usd × usd_pln_rate) - buy_price_pln
pnl_total_pln        = pnl_per_unit_pln × quantity
pnl_pct              = (pnl_per_unit_pln / buy_price_pln) × 100
```

---

## 🚀 Setup Guide

### Prerequisites

- AWS CLI configured (`aws configure`)
- Terraform >= 1.5
- GCP project with BigQuery API enabled
- GCP Service Account with BigQuery Admin role
- Redpanda Serverless account

### 1. Clone the repository

```bash
git clone https://github.com/KacperChojnacki1337/steam-data-pipeline.git
cd steam-data-pipeline
```

### 2. Configure Terraform variables

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars`:

### 3. Deploy infrastructure

```bash
terraform init
terraform apply
```

This provisions:
- DynamoDB table with PITR
- SSM parameters for Redpanda credentials
- IAM roles and policies for Lambda
- BigQuery datasets (`steam_raw`, `steam_marts`) and tables
- Lambda functions (producer + consumer) with layers
- Redpanda event source mappings (3 topics)

### 4. Store GCP service account key in SSM

```bash
aws ssm put-parameter \
  --name "/steam-tracker/gcp-key" \
  --type "SecureString" \
  --value "$(cat your-gcp-key.json)"
```

### 5. Add inventory items to DynamoDB

```bash
aws dynamodb put-item \
  --table-name steam_inventory_metadata \
  --item '{
    "asset_id": {"S": "YOUR-UUID"},
    "item_id": {"S": "AWP | Printstream (Well-Worn)"},
    "buy_price": {"N": "164.81"},
    "buy_currency": {"S": "PLN"},
    "buy_date": {"S": "2026-02-06"},
    "category": {"S": "Skin"},
    "purchase_channel": {"S": "CSFloat"},
    "quantity": {"N": "1"},
    "updated_at": {"S": "2026-02-06T00:00:00Z"}
  }'
```

### 6. Configure GitHub Actions

Add the following secret to your repository (`Settings → Secrets → Actions`):

| Secret | Value |
|---|---|
| `GCP_SA_KEY` | Contents of your GCP service account JSON key |

### 7. Run the pipeline

Trigger the producer Lambda manually from AWS Console or wait for the EventBridge schedule. dbt will run automatically on push to `main` or at 08:00 UTC daily.

---

## 📁 Project Structure

```
steam-data-pipeline/
├── .github/
│   └── workflows/
│       └── dbt.yml              # GitHub Actions — dbt pipeline
├── dbt/
│   └── steam_tracker/
│       ├── dbt_project.yml
│       ├── packages.yml
│       └── models/
│           ├── staging/
│           │   ├── sources.yml
│           │   ├── stg_assets.sql
│           │   ├── stg_prices.sql
│           │   └── stg_exchange_rates.sql
│           ├── intermediate/
│           │   ├── int_latest_prices.sql
│           │   └── int_latest_exchange_rate.sql
│           └── marts/
│               ├── schema.yml
│               ├── dim_assets.sql
│               └── fct_portfolio.sql
├── lambda/
│   ├── producer/
│   │   └── producer_lambda.py
│   └── consumer/
│       └── consumer_lambda.py
└── terraform/
    └── main.tf
```

---

## 🔐 Security

- All secrets stored in AWS SSM Parameter Store (SecureString)
- GCP service account key never committed to repository
- IAM roles follow least-privilege principle — producer has DynamoDB read + SSM read only, consumer has SSM read only
- Lambda execution roles are separate for producer and consumer
- DynamoDB Point-in-Time Recovery enabled

---

## 📝 License

MIT
