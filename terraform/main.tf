# --- AWS: DynamoDB ---
resource "aws_dynamodb_table" "inventory_metadata" {
  name           = "steam_inventory_metadata"
  billing_mode   = "PAY_PER_REQUEST"
  
  # Partition Key
  hash_key       = "item_id"
  # Sort Key ()
  range_key      = "buy_date"

  attribute {
    name = "item_id"
    type = "S"
  }

  attribute {
    name = "buy_date"
    type = "S" # String w formacie ISO 
  }
  tags = {
    Environment = "Dev"
    Project     = "steam-tracker"
  }
}

# --- GCP: BigQuery  ---
resource "google_bigquery_dataset" "steam_dataset" {
  dataset_id                  = "steam_analytics"
  friendly_name               = "Steam Price Analytics"
  description                 = "A place for raw and processed data on item prices"
  location                    = "EU" # Multi-region EU 
  delete_contents_on_destroy  = false
}

# --- GCP: BigQuery  ---
resource "google_bigquery_table" "raw_prices" {
  dataset_id = google_bigquery_dataset.steam_dataset.dataset_id
  table_id   = "raw_price_history"

  time_partitioning {
    type  = "DAY"
    field = "timestamp" 
  }

  schema = <<EOF
[
  {"name": "item_id", "type": "STRING", "mode": "REQUIRED"},
  {"name": "price_usd", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "price_pln", "type": "FLOAT", "mode": "NULLABLE"},
  {"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"}
]
EOF
}


# --- IAM: Role for Lambda ---
resource "aws_iam_role" "lambda_exec_role" {
  name = "steam_tracker_lambda_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

# --- IAM: Policies for Lambda ---
resource "aws_iam_role_policy" "lambda_policy" {
  name = "steam_tracker_lambda_policy"
  role = aws_iam_role.lambda_exec_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Access to DynamoDB
        Action = [
          "dynamodb:Scan",
          "dynamodb:Query",
          "dynamodb:GetItem"
        ]
        Effect   = "Allow"
        Resource = aws_dynamodb_table.inventory_metadata.arn
      },
      {
        # Access to SSM Parameter Store (Your GCP Key)
        Action = "ssm:GetParameter"
        Effect = "Allow"
        Resource = "arn:aws:ssm:eu-central-1:*:parameter/steam-tracker/gcp-key"
      },
      {
        # Logging to CloudWatch
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Effect   = "Allow"
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# --- ZIP: Packaging the Code ---
data "archive_file" "lambda_code_zip" {
  type        = "zip"
  source_file = "../lambda/producer/producer_lambda.py"
  output_path = "producer_lambda.zip"
}

# --- ZIP: Packaging the Layer (Dependencies) ---
data "archive_file" "lambda_layer_zip" {
  type        = "zip"
  source_dir  = "../lambda/producer/layer"
  output_path = "lambda_layer.zip"
}

# --- Lambda Layer ---
resource "aws_lambda_layer_version" "python_libs" {
  filename            = data.archive_file.lambda_layer_zip.output_path
  layer_name          = "steam_tracker_libs"
  compatible_runtimes = ["python3.11"]
}

# --- Lambda Function ---
resource "aws_lambda_function" "steam_producer" {
  filename         = data.archive_file.lambda_code_zip.output_path
  function_name    = "steam_price_producer"
  role             = aws_iam_role.lambda_exec_role.arn
  handler          = "producer_lambda.lambda_handler" # File name . Function name
  runtime          = "python3.11"
  timeout          = 60 # Increased because scraping and BQ inserts take time
  memory_size      = 256

layers = [aws_lambda_layer_version.python_libs.arn]

  source_code_hash = data.archive_file.lambda_code_zip.output_base64sha256
}

