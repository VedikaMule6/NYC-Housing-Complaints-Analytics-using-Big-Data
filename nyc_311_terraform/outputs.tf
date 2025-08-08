output "s3_bucket" {
  value = aws_s3_bucket.nyc_etl.bucket
}

output "glue_job" {
  value = aws_glue_job.nyc_etl_job.name
}
