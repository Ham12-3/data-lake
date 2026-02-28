terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ── S3 Bucket ────────────────────────────────────────────────────────────────

resource "aws_s3_bucket" "data_lake" {
  bucket = var.bucket_name

  tags = {
    Project = var.project_name
  }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "data_lake" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Create the zone prefixes so they show up in the console
resource "aws_s3_object" "zones" {
  for_each = toset(["raw/", "curated/", "lineage/", "quality_reports/", "dvc-store/"])
  bucket   = aws_s3_bucket.data_lake.id
  key      = each.value
  content  = ""
}

# ── SNS Topic for Failure Alerts ─────────────────────────────────────────────

resource "aws_sns_topic" "pipeline_alerts" {
  name = "${var.project_name}-pipeline-alerts"

  tags = {
    Project = var.project_name
  }
}

resource "aws_sns_topic_subscription" "email_alert" {
  topic_arn = aws_sns_topic.pipeline_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ── DynamoDB Table for Lineage (optional) ────────────────────────────────────

resource "aws_dynamodb_table" "lineage" {
  count        = var.enable_dynamodb ? 1 : 0
  name         = "${var.project_name}-lineage"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "lineage_id"

  attribute {
    name = "lineage_id"
    type = "S"
  }

  attribute {
    name = "destination_key"
    type = "S"
  }

  global_secondary_index {
    name            = "destination-index"
    hash_key        = "destination_key"
    projection_type = "ALL"
  }

  tags = {
    Project = var.project_name
  }
}
