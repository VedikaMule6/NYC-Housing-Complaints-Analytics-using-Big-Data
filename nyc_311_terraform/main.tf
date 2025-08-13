# Terraform script for creating AWS Infrastructure

# Setting up bucket to save terraform state for versioning
terraform {
  backend "s3" {
    bucket         = "terraform-state-bucket-nyc"
    key            = "terraform-state/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
  }
}


# Get current AWS account info from aws credentials
data "aws_caller_identity" "current" {}


# S3 Bucket for storing Project Data
resource "aws_s3_bucket" "project_data" {
  bucket = var.bucket_name
}


# Medallion Architecture Folder Structure
locals {
  folders = [
    "Bronze-level/311_nyc_dataset/",
    "Bronze-level/HPD_dataset/",
    "Bronze-level/Master_dataset/",
    "Bronze-level/PLUTO_dataset/",
    "Bronze-level/Affordable_Housing_dataset/",
    "Silver-level/transformed_data/",
    "Gold-level/aggregated_data/",
    "ETL_script_for_glue/",
    "glue_temp/"
  ]
}


# creates empty objects to simulate folders
resource "aws_s3_object" "folders" {
  for_each = toset(local.folders)
  bucket   = aws_s3_bucket.project_data.bucket
  key      = each.key
  content = ""
}


# Glue Database to store tables
resource "aws_glue_catalog_database" "nyc_db" {
  name = "nyc_db"
}


# Glue Job with 3 DPU to run ETL script
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
  number_of_workers    = 3
  max_retries          = 0
  timeout              = 120
}


# Glue Crawler to crawl transform dataset
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


# Athena Workgroup to query data
resource "aws_athena_workgroup" "nyc_group" {
  name = "nyc_workgroup"

  configuration {
    result_configuration {
      output_location = "s3://${var.bucket_name}/Gold-level/aggregated_data/"
    }
  }
}
