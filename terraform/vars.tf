variable "state_bucket_name" {
    type = string
    default = "gb-ttotes-remote-state-bucket"
}

variable "ingestion_lambda_name" {
    type = string
    default = "gb-ttotes-ingestion-lambda"
}

variable "code_bucket_prefix" {
    type = string
    default = "code-bucket-"
}

variable "python_runtime" {
  type    = string
  default = "python3.12"
}

variable "ingestion_bucket_prefix" {
    type = string
    default = "green-bean-ingestion-bucket-"
}

variable "error_tag" {
  type = string
  default = "ERROR"
}

variable "metric_transformation_name" {
  type = string
  default = "PublishValue1"
}

variable "ingestion_metric_namespace" {
  type = string
  default = "lambda-ingestion-function"
}

variable "ingestion_lambda_filename" {
  type = string
  default = "ingestion_lambda"
}

variable "state_machine_name" {
    type = string
    default = "gb-ttotes-state-machine"
}

variable "scheduler_name" {
  type = string
  default = "gb_ttotes_etl_scheduler"
}