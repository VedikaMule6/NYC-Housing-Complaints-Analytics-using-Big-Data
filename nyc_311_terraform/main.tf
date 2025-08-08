# -------------------------------
# S3 Bucket for Project Data
# -------------------------------
resource "aws_s3_bucket" "project_data" {
  bucket = var.bucket_name
}

# Folder Structure (creates empty objects to simulate folders)
locals {
  folders = [
    "Bronze-level/311_nyc_dataset/",
    "Bronze-level/HPD_dataset/",
    "Bronze-level/Master_dataset/",
    "Bronze-level/PLUTO_dataset/",
    "Silver-level/transformed_data/",
    "Gold-level/aggregated_data/",
    "ETL_script_for_glue/",
    "glue_temp/"
  ]
}

resource "aws_s3_object" "folders" {
  for_each = toset(local.folders)
  bucket   = aws_s3_bucket.project_data.bucket
  key      = each.key
  content = ""
}

# # # Athena results bucket
# resource "aws_s3_bucket" "athena_results" {
#   bucket = var.bucket_name
# }

# -------------------------------
# Glue Database
# -------------------------------
resource "aws_glue_catalog_database" "nyc_db" {
  name = "nyc_db"
}

# -------------------------------
# Glue Job
# -------------------------------
resource "aws_glue_job" "etl_job" {
  name     = "daily-etl-job"
  role_arn = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/LabRole"

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/ETL_script_for_glue/${var.etl_script_filename}"
    python_version  = "3"
  }

  glue_version         = "5.0"
  worker_type          = "G.1X"
  number_of_workers    = 5
  max_retries          = 1
  timeout              = 120
}

# -------------------------------
# Glue Crawler
# -------------------------------
resource "aws_glue_crawler" "etl_output_crawler" {
  name        = "crawler-etl-output"
  role        = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/LabRole"
  database_name = aws_glue_catalog_database.nyc_db.name
  description = "This crawler will run after etl job."
  table_prefix = "transformed_"

  s3_target {
    path = "s3://${var.bucket_name}/Silver-level/transformed_data/"
  }

  schedule = null  # On demand
}

# -------------------------------
# Athena Workgroup
# -------------------------------
resource "aws_athena_workgroup" "primary" {
  name = "primary"

  configuration {
    result_configuration {
      output_location = "s3://${var.bucket_name}/Gold-level/aggregated_data/"
    }
  }
}

# Get current AWS account info
data "aws_caller_identity" "current" {}
