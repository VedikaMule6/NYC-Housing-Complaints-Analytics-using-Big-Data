variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "aws_access_key" {
  type = string
  default = ""
}

variable "aws_secret_key" {
  type = string
  default = ""
}


variable "aws_session_token" {
  type = string
  default = ""
}

variable "bucket_name" {
  type    = string
  default = "cdac-final-project-data"
}

variable "athena_bucket" {
  type    = string
  default = "masterdataforathena"
}

variable "etl_script_filename" {
  type    = string
  default = "etl.py"
}
