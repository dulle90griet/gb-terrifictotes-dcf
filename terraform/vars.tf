variable "project_prefix" {
  type    = string
  default = "df2-ttotes-" 
}

variable "ingestion_lambda_name" {
  type    = string
  default = "ingestion-lambda"
}

variable "code_bucket_prefix" {
  type    = string
  default = "code-bucket-"
}

variable "python_runtime" {
  type    = string
  default = "python3.9"
}

variable "ingestion_bucket_prefix" {
  type    = string
  default = "ingestion-bucket-"
}

variable "processing_bucket_prefix" {
  type    = string
  default = "processing-bucket-"
}

variable "error_tag" {
  type    = string
  default = "ERROR"
}

variable "metric_transformation_name" {
  type    = string
  default = "PublishValue1"
}

variable "ingestion_metric_namespace" {
  type    = string
  default = "lambda-ingestion-function"
}

variable "metric_transformation_name_2" {
  type    = string
  default = "PublishValue2"
}

variable "processing_metric_namespace" {
  type    = string
  default = "lambda-processing-function"
}

variable "metric_transformation_name_3" {
  type    = string
  default = "PublishValue3"
}

variable "uploading_metric_namespace" {
  type    = string
  default = "lambda-uploading-function"
}

variable "ingestion_lambda_filename" {
  type    = string
  default = "ingestion_lambda"
}

variable "processing_lambda_filename" {
  type    = string
  default = "processing_lambda"
}

variable "uploading_lambda_filename" {
  type    = string
  default = "uploading_lambda"
}

variable "state_machine_name" {
  type    = string
  default = "state-machine"
}

variable "scheduler_name" {
  type    = string
  default = "etl-scheduler"
}

variable "processing_lambda_name" {
  type    = string
  default = "processing-lambda"
}

variable "uploading_lambda_name" {
  type    = string
  default = "uploading-lambda"
}