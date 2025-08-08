variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "aws_access_key" {}
variable "aws_secret_key" {}
variable "aws_session_token" {}


variable "bucket_name" {
  type    = string
  default = "cdac-final-project-data"
}


variable "etl_script_filename" {
  type    = string
  default = "etl.py"
}
