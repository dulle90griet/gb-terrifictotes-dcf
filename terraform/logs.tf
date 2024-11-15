resource "aws_cloudwatch_log_group" "ingestion_lambda_log_group"{
  name = "/aws/lambda/${var.ingestion_lambda_name}"
  retention_in_days = 7
}

