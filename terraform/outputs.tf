output "data_lake_bucket" {
  description = "Nombre del bucket S3 del data lake"
  value       = aws_s3_bucket.data_lake.id
}

output "data_lake_bucket_arn" {
  description = "ARN del bucket S3"
  value       = aws_s3_bucket.data_lake.arn
}

output "sns_topic_arn" {
  description = "ARN del topic SNS para alertas de calidad"
  value       = aws_sns_topic.data_quality_alerts.arn
}

output "glue_role_arn" {
  description = "ARN del IAM role para jobs de Glue"
  value       = aws_iam_role.glue_role.arn
}