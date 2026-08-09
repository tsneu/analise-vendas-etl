output "bucket_bronze_name" {
  value       = aws_s3_bucket.datalake_bronze.bucket
  description = "Nome do Bucket S3 da camada Bronze"
}

output "bucket_silver_name" {
  value       = aws_s3_bucket.datalake_silver.bucket
  description = "Nome do Bucket S3 da camada Prata"
}

output "bucket_gold_name" {
  value       = aws_s3_bucket.datalake_gold.bucket
  description = "Nome do Bucket S3 da camada Ouro"
}

output "glue_job_name" {
  value       = aws_glue_job.etl_ecommerce.name
  description = "Nome do Job do AWS Glue criado"
}