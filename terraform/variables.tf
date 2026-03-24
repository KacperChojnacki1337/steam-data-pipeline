variable "aws_region" {
  description = "AWS region for Lambda and DynamoDB"
  type        = string
  default     = "eu-central-1"
}

variable "gcp_region" {
  description = "GCP region for BigQuery"
  type        = string
  default     = "europe-west3"
}

variable "gcp_project_id" {
  description = "Your unique Project ID from the GCP console"
  type        = string
}

variable "gcp_credentials_file" {
  description = "Path to the JSON file with the service account key"
  type        = string
}

variable "redpanda_password" {
  description = "Password for Redpanda lambda-producer user"
  type        = string
  sensitive   = true
}

variable "alert_email" {
  description = "Email address for CloudWatch alarm notifications"
  type        = string
}