variable "aws_region" {
  description = "AWS region for lambda and dynamo"
  type        = string
  default     = "eu-central-1" # Frankfurt
}

variable "gcp_region" {
  description = "Region for GCP"
  type        = string
  default     = "europe-west3" # Frankfurt
}

variable "gcp_project_id" {
  description = "Your unique Project ID from the GCP console"
  type        = string
}

variable "gcp_credentials_file" {
  description = "Path to the JSON file with the service account key"
  type        = string
}