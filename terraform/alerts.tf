# INGESTION LAMBDA - METRIC FILTER AND ALARM

resource "aws_cloudwatch_log_metric_filter" "lambda_ingestion_error_filter" {
  name           = "lambda-ingestion-error-filter"
  pattern        = var.error_tag
  log_group_name = aws_cloudwatch_log_group.ingestion_lambda_log_group.name

  metric_transformation {
    name      = var.metric_transformation_name
    namespace = var.ingestion_metric_namespace
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_ingestion_error_alarm" {
  alarm_name                = "lambda-ingestion-error-alarm"
  evaluation_periods        = 1
  period                    = 60
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  statistic                 = "Sum"
  threshold                 = 1
  metric_name               = aws_cloudwatch_log_metric_filter.lambda_ingestion_error_filter.name
  namespace                 = var.ingestion_metric_namespace
  alarm_description         = "This metric monitors the lambda ingestion function log for error messages"
  insufficient_data_actions = []
  alarm_actions             = [aws_sns_topic.error_email_topic.arn]
}

# PROCESSING LAMBDA - METRIC FILTER AND ALARM

resource "aws_cloudwatch_log_metric_filter" "lambda_processing_error_filter" {
  name           = "lambda-processing-error-filter"
  pattern        = var.error_tag
  log_group_name = aws_cloudwatch_log_group.processing_lambda_log_group.name

  metric_transformation {
    name      = var.metric_transformation_name_2
    namespace = var.processing_metric_namespace
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_processing_error_alarm" {
  alarm_name                = "lambda-processing-error-alarm"
  evaluation_periods        = 1
  period                    = 60
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  statistic                 = "Sum"
  threshold                 = 1
  metric_name               = aws_cloudwatch_log_metric_filter.lambda_processing_error_filter.name
  namespace                 = var.processing_metric_namespace
  alarm_description         = "This metric monitors the processing lambda function log for error messages"
  insufficient_data_actions = []
  alarm_actions             = [aws_sns_topic.error_email_topic.arn]
}

# UPLOADING LAMBDA - METRIC FILTER AND ALARM

resource "aws_cloudwatch_log_metric_filter" "lambda_uploading_error_filter" {
  name           = "lambda-uploading-error-filter"
  pattern        = var.error_tag
  log_group_name = aws_cloudwatch_log_group.uploading_lambda_log_group.name

  metric_transformation {
    name      = var.metric_transformation_name_3
    namespace = var.uploading_metric_namespace
    value     = "1"
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_uploading_error_alarm" {
  alarm_name                = "lambda-uploading-error-alarm"
  evaluation_periods        = 1
  period                    = 60
  comparison_operator       = "GreaterThanOrEqualToThreshold"
  statistic                 = "Sum"
  threshold                 = 1
  metric_name               = aws_cloudwatch_log_metric_filter.lambda_uploading_error_filter.name
  namespace                 = var.uploading_metric_namespace
  alarm_description         = "This metric monitors the uploading lambda function log for error messages"
  insufficient_data_actions = []
  alarm_actions             = [aws_sns_topic.error_email_topic.arn]
}