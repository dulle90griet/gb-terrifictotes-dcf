resource "aws_cloudwatch_log_group" "ingestion_lambda_log_group" {
  name              = "/aws/lambda/${var.ingestion_lambda_name}"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_group" "processing_lambda_log_group" {
  name              = "/aws/lambda/${var.processing_lambda_name}"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_group" "uploading_lambda_log_group" {
  name              = "/aws/lambda/${var.uploading_lambda_name}"
  retention_in_days = 7
}