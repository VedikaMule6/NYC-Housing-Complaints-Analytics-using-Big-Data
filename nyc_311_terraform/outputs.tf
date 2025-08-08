output "s3_project_bucket" {
  value = aws_s3_bucket.project_data.bucket
}

# output "s3_athena_bucket" {
#   value = aws_s3_bucket.athena_results.bucket
# }

output "glue_database_name" {
  value = aws_glue_catalog_database.nyc_311_db.name
}

output "glue_job_name" {
  value = aws_glue_job.etl_job.name
}

output "glue_crawler_name" {
  value = aws_glue_crawler.etl_output_crawler.name
}
