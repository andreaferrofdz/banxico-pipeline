terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

# ─── S3 ───────────────────────────────────────────────────────────────────────

resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.project_name}-${var.environment}-datalake"
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

# Carpetas lógicas de la arquitectura medallion
resource "aws_s3_object" "bronze_prefix" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "bronze/"
  content = ""
}

resource "aws_s3_object" "silver_prefix" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "silver/"
  content = ""
}

resource "aws_s3_object" "gold_prefix" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "gold/"
  content = ""
}

resource "aws_s3_object" "scripts_prefix" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "scripts/"
  content = ""
}

# ─── SNS ──────────────────────────────────────────────────────────────────────

resource "aws_sns_topic" "data_quality_alerts" {
  name = "${var.project_name}-${var.environment}-dq-alerts"
}

resource "aws_sns_topic_subscription" "email_alert" {
  topic_arn = aws_sns_topic.data_quality_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ─── IAM ──────────────────────────────────────────────────────────────────────

resource "aws_iam_role" "glue_role" {
  name = "${var.project_name}-${var.environment}-glue-role"

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

resource "aws_iam_role_policy" "glue_passrole" {
  name = "${var.project_name}-${var.environment}-glue-passrole"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["iam:PassRole"]
        Resource = [aws_iam_role.glue_role.arn]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_sns" {
  name = "${var.project_name}-${var.environment}-glue-s3-sns"
  role = aws_iam_role.glue_role.id

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
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = [aws_sns_topic.data_quality_alerts.arn]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
          "glue:GetTable",
          "glue:GetDatabase",
          "glue:GetPartition",
        ]
        Resource = ["*"]
      }
    ]
  })
}



# ─── Glue Data Catalog ────────────────────────────────────────────────────────

resource "aws_glue_catalog_database" "banxico" {
  name        = "${var.project_name}-${var.environment}"
  description = "Banxico financial data pipeline — medallion architecture"
}

# ─── Silver Tables ────────────────────────────────────────────────────────────

locals {
  silver_tables = {
    tipo_de_cambio = "SF43718"
    tiie_28        = "SF60648"
    inpc           = "SP1"
  }
}

resource "aws_glue_catalog_table" "silver" {
  for_each      = local.silver_tables
  database_name = aws_glue_catalog_database.banxico.name
  name          = "silver_${each.key}"
  description   = "Silver layer — ${each.key} (${each.value})"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification"      = "parquet"
    "compressionType"     = "snappy"
    "EXTERNAL"            = "TRUE"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data_lake.id}/silver/source=banxico/dataset=${each.key}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    columns {
      name = "date"
      type = "date"
    }
    columns {
      name = "source"
      type = "string"
    }
    columns {
      name = "dataset"
      type = "string"
    }
    columns {
      name = "serie_id"
      type = "string"
    }
    columns {
      name = "processed_at"
      type = "string"
    }
    columns {
      name = "value"
      type = "double"
    }
    columns {
      name = "title"
      type = "string"
    }
  }

  partition_keys {
    name = "year"
    type = "string"
  }

  partition_keys {
    name = "month"
    type = "string"
  }
}

# ─── Athena ───────────────────────────────────────────────────────────────────

resource "aws_s3_object" "athena_results_prefix" {
  bucket  = aws_s3_bucket.data_lake.id
  key     = "athena-results/"
  content = ""
}

resource "aws_athena_workgroup" "banxico" {
  name        = "${var.project_name}-${var.environment}"
  description = "Athena workgroup for Banxico pipeline queries"

  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.data_lake.id}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }

    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
  }
}

resource "aws_glue_catalog_table" "gold" {
  database_name = aws_glue_catalog_database.banxico.name
  name          = "gold_macro_indicators"
  description   = "Gold layer — monthly macro indicators (FX, TIIE 28, INPC)"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "classification"      = "parquet"
    "compressionType"     = "snappy"
    "EXTERNAL"            = "TRUE"
    "parquet.compression" = "SNAPPY"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data_lake.id}/gold/source=banxico/dataset=macro_indicators/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters = {
        "serialization.format" = "1"
      }
    }

    columns {
      name = "date"
      type = "date"
    }
    columns {
      name = "fx_rate"
      type = "double"
    }
    columns {
      name = "fx_volatility"
      type = "double"
    }
    columns {
      name = "fx_mom_pct"
      type = "double"
    }
    columns {
      name = "tiie_28"
      type = "double"
    }
    columns {
      name = "tiie_mom_change"
      type = "double"
    }
    columns {
      name = "inpc"
      type = "double"
    }
    columns {
      name = "inpc_mom_pct"
      type = "double"
    }
    columns {
      name = "inpc_yoy_pct"
      type = "double"
    }
    columns {
      name = "processed_at"
      type = "string"
    }
  }

  partition_keys {
    name = "execution_date"
    type = "string"
  }
}

# ─── Glue Scripts ─────────────────────────────────────────────────────────────

resource "aws_s3_object" "script_pipeline" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/pipeline.py"
  source = "${path.module}/../src/pipeline.py"
  etag   = filemd5("${path.module}/../src/pipeline.py")
}

resource "aws_s3_object" "script_extract" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/extract/banxico_api.py"
  source = "${path.module}/../src/extract/banxico_api.py"
  etag   = filemd5("${path.module}/../src/extract/banxico_api.py")
}

resource "aws_s3_object" "script_silver" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/transform/silver.py"
  source = "${path.module}/../src/transform/silver.py"
  etag   = filemd5("${path.module}/../src/transform/silver.py")
}

resource "aws_s3_object" "script_gold" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/transform/gold.py"
  source = "${path.module}/../src/transform/gold.py"
  etag   = filemd5("${path.module}/../src/transform/gold.py")
}

resource "aws_s3_object" "script_checkpoints" {
  bucket = aws_s3_bucket.data_lake.id
  key    = "scripts/checkpoints/checkpoints.py"
  source = "${path.module}/../src/checkpoints/checkpoints.py"
  etag   = filemd5("${path.module}/../src/checkpoints/checkpoints.py")
}

# ─── Glue Jobs ────────────────────────────────────────────────────────────────

resource "aws_glue_job" "extract" {
  name        = "${var.project_name}-${var.environment}-extract"
  role_arn    = aws_iam_role.glue_role.arn
  description = "Bronze layer — extract Banxico API series to S3"

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${aws_s3_bucket.data_lake.id}/scripts/pipeline.py"
  }

  default_arguments = {
    "--mode"                     = "daily"
    "--BUCKET_NAME"              = aws_s3_bucket.data_lake.id
    "--AWS_REGION"               = var.aws_region
    "--GLUE_DATABASE"            = aws_glue_catalog_database.banxico.name
    "--SNS_TOPIC_ARN"            = aws_sns_topic.data_quality_alerts.arn
    "--extra-py-files"           = "s3://${aws_s3_bucket.data_lake.id}/scripts/extract/banxico_api.py,s3://${aws_s3_bucket.data_lake.id}/scripts/transform/silver.py,s3://${aws_s3_bucket.data_lake.id}/scripts/transform/gold.py,s3://${aws_s3_bucket.data_lake.id}/scripts/checkpoints/checkpoints.py"
    "--additional-python-modules" = "requests,tenacity,pandas,pyarrow,python-dotenv"
    "--enable-job-insights"      = "true"
  }

  max_capacity = 0.0625
  timeout      = 60

  tags = {
    Layer = "extract"
  }
}

resource "aws_glue_job" "silver" {
  name        = "${var.project_name}-${var.environment}-silver"
  role_arn    = aws_iam_role.glue_role.arn
  description = "Silver layer — transform Bronze JSON to Parquet"

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${aws_s3_bucket.data_lake.id}/scripts/transform/silver.py"
  }

  default_arguments = {
    "--BUCKET_NAME"              = aws_s3_bucket.data_lake.id
    "--AWS_REGION"               = var.aws_region
    "--GLUE_DATABASE"            = aws_glue_catalog_database.banxico.name
    "--extra-py-files"           = "s3://${aws_s3_bucket.data_lake.id}/scripts/checkpoints/checkpoints.py"
    "--additional-python-modules" = "pandas,pyarrow,python-dotenv"
    "--enable-job-insights"      = "true"
  }

  max_capacity = 0.0625
  timeout      = 30
}

resource "aws_glue_job" "gold" {
  name        = "${var.project_name}-${var.environment}-gold"
  role_arn    = aws_iam_role.glue_role.arn
  description = "Gold layer — aggregate Silver to monthly macro indicators"

  command {
    name            = "pythonshell"
    python_version  = "3.9"
    script_location = "s3://${aws_s3_bucket.data_lake.id}/scripts/transform/gold.py"
  }

  default_arguments = {
    "--BUCKET_NAME"              = aws_s3_bucket.data_lake.id
    "--AWS_REGION"               = var.aws_region
    "--GLUE_DATABASE"            = aws_glue_catalog_database.banxico.name
    "--additional-python-modules" = "pandas,pyarrow,python-dotenv"
    "--enable-job-insights"      = "true"
  }

  max_capacity = 0.0625
  timeout      = 30
}

# ─── Glue Workflow ────────────────────────────────────────────────────────────

resource "aws_glue_workflow" "pipeline" {
  name        = "${var.project_name}-${var.environment}-pipeline"
  description = "Orchestrates extract → silver → gold in sequence"
}

resource "aws_glue_trigger" "start_extract" {
  name          = "${var.project_name}-${var.environment}-trigger-extract"
  type          = "SCHEDULED"
  schedule      = "cron(0 8 * * ? *)"
  workflow_name = aws_glue_workflow.pipeline.name
  description   = "Triggers extract job daily at 8am UTC"

  actions {
    job_name = aws_glue_job.extract.name
    arguments = {
      "--mode" = "daily"
    }
  }
}

resource "aws_glue_trigger" "start_silver" {
  name          = "${var.project_name}-${var.environment}-trigger-silver"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.pipeline.name
  description   = "Triggers silver job after extract succeeds"

  predicate {
    conditions {
      job_name         = aws_glue_job.extract.name
      state            = "SUCCEEDED"
      logical_operator = "EQUALS"
    }
  }

  actions {
    job_name = aws_glue_job.silver.name
  }
}

resource "aws_glue_trigger" "start_gold" {
  name          = "${var.project_name}-${var.environment}-trigger-gold"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.pipeline.name
  description   = "Triggers gold job after silver succeeds"

  predicate {
    conditions {
      job_name         = aws_glue_job.silver.name
      state            = "SUCCEEDED"
      logical_operator = "EQUALS"
    }
  }

  actions {
    job_name = aws_glue_job.gold.name
  }
}