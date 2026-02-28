output "s3_bucket_name" {
  description = "Data lake S3 bucket name"
  value       = aws_s3_bucket.data_lake.id
}

output "s3_bucket_arn" {
  description = "Data lake S3 bucket ARN"
  value       = aws_s3_bucket.data_lake.arn
}

output "sns_topic_arn" {
  description = "SNS topic ARN for pipeline failure alerts"
  value       = aws_sns_topic.pipeline_alerts.arn
}

output "dynamodb_table_name" {
  description = "DynamoDB table name for lineage tracking"
  value       = var.enable_dynamodb ? aws_dynamodb_table.lineage[0].name : "none"
}

output "aws_region" {
  description = "AWS region"
  value       = var.aws_region
}
