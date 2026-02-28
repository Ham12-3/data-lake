variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name used as prefix for resource naming"
  type        = string
  default     = "data-lake"
}

variable "bucket_name" {
  description = "S3 bucket for the data lake"
  type        = string
  default     = "my-data-lake-bucket"
}

variable "alert_email" {
  description = "Email address for SNS failure alerts"
  type        = string
  default     = "your-email@example.com"
}

variable "enable_dynamodb" {
  description = "Create DynamoDB table for lineage tracking"
  type        = bool
  default     = true
}
