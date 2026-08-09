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

# ------------------------------------------------------------------------------
# 1. Buckets S3 (Camadas Bronze, Prata e Ouro + Athena Queries)
# ------------------------------------------------------------------------------

resource "aws_s3_bucket" "datalake_bronze" {
  bucket        = "${var.project_name}-datalake-bronze-${var.environment}"
  force_destroy = true
}

resource "aws_s3_bucket" "datalake_silver" {
  bucket        = "${var.project_name}-datalake-silver-${var.environment}"
  force_destroy = true
}

resource "aws_s3_bucket" "datalake_gold" {
  bucket        = "${var.project_name}-datalake-gold-${var.environment}"
  force_destroy = true
}

resource "aws_s3_bucket" "athena_results" {
  bucket        = "${var.project_name}-athena-results-${var.environment}"
  force_destroy = true
}

# ------------------------------------------------------------------------------
# 2. AWS Glue Data Catalog Database
# ------------------------------------------------------------------------------

resource "aws_glue_catalog_database" "ecommerce_db" {
  name        = "db_ecommerce_${var.environment}"
  description = "Base de dados para o pipeline de e-commerce (Medalhão)"
}

# ------------------------------------------------------------------------------
# 3. IAM Role & Policies para o AWS Glue Job
# ------------------------------------------------------------------------------

resource "aws_iam_role" "glue_service_role" {
  name = "glue_ecommerce_service_role_${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

# Anexa a politica padrão gerenciada da AWS para o Glue
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Política customizada para acesso aos Buckets do Data Lake
resource "aws_iam_policy" "glue_s3_access" {
  name = "glue_ecommerce_s3_access_${var.environment}"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.datalake_bronze.arn,
          "${aws_s3_bucket.datalake_bronze.arn}/*",
          aws_s3_bucket.datalake_silver.arn,
          "${aws_s3_bucket.datalake_silver.arn}/*",
          aws_s3_bucket.datalake_gold.arn,
          "${aws_s3_bucket.datalake_gold.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3_attach" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = aws_iam_policy.glue_s3_access.arn
}

# ------------------------------------------------------------------------------
# 4. AWS Glue Job (PySpark)
# ------------------------------------------------------------------------------

# Bucket para armazenar os scripts do Glue
resource "aws_s3_bucket" "glue_scripts" {
  bucket        = "${var.project_name}-glue-scripts-${var.environment}"
  force_destroy = true
}

# Upload do script PySpark para o S3
resource "aws_s3_object" "etl_script" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "scripts/etl_ecommerce.py"
  source = "../src/jobs/etl_ecommerce.py"
  etag   = filemd5("../src/jobs/etl_ecommerce.py")
}

resource "aws_glue_job" "etl_ecommerce" {
  name     = "job_etl_ecommerce_medallion"
  role_arn = aws_iam_role.glue_service_role.arn

  glue_version      = "${var.glue_version}"
  worker_type       = "${var.glue_worker_type}"
  number_of_workers = "${var.glue_number_of_workers}"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/${aws_s3_object.etl_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                    = "python"
    "--enable-metrics"                  = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--datalake-bronze"                 = "s3://${aws_s3_bucket.datalake_bronze.bucket}/"
    "--datalake-silver"                 = "s3://${aws_s3_bucket.datalake_silver.bucket}/"
    "--datalake-gold"                   = "s3://${aws_s3_bucket.datalake_gold.bucket}/"
    "--database-name"                   = aws_glue_catalog_database.ecommerce_db.name
    # Suporte nativo ao Apache Iceberg no Glue 4.0
    "--datalake-formats"                = "iceberg"
    "--user-jars-first"                 = "true"
    "--enable-spark-ui"                 = "true"
    "--spark-event-logs-path"           = "s3://${aws_s3_bucket.glue_scripts.bucket}/spark-ui-logs/"
    "--enable-continuous-cloudwatch-log" = "true"
  }
}

# ------------------------------------------------------------------------------
# 5. AWS Athena Workgroup
# ------------------------------------------------------------------------------

resource "aws_athena_workgroup" "ecommerce_workgroup" {
  name = "wg_ecommerce_${var.environment}"

  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/output/"
    }
  }
}