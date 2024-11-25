resource "aws_cloudwatch_log_metric_filter" "lambda_ingestion_error_filter" {
  name = "lambda-ingestion-error-filter"     ## gives a name to the filter
  pattern = var.error_tag       ## pattern we want to search for (should be a var also used by lambda error logging)
  log_group_name = aws_cloudwatch_log_group.ingestion_lambda_log_group.name  ## name of log group to watch (possible var?)
  
  metric_transformation {
      name = var.metric_transformation_name  ## gives a name to the metric (possible var?)
      namespace = var.ingestion_metric_namespace  ## name of the metric namespace (possible var?)
      value = "1" ## the value published to the metric each time the pattern is found  
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_ingestion_error_alarm" {
    alarm_name = "lambda-ingestion-error-alarm"    ## gives a name to the alarm
    evaluation_periods = 1      ## number of periods to include in the comparison
    period = 60
    comparison_operator = "GreaterThanOrEqualToThreshold"
    statistic = "Sum"
    threshold = 1       ## alarm should be triggered by presence of 1 or more errors
    metric_name = aws_cloudwatch_log_metric_filter.lambda_ingestion_error_filter.name
    namespace = var.ingestion_metric_namespace
    alarm_description = "This metric monitors the lambda ingestion function log for error messages"
    alarm_actions = []  ## this list must contain the arn of the sns topic once created
    insufficient_data_actions = []
}

resource "aws_cloudwatch_log_metric_filter" "lambda_processing_error_filter" {
  name = "lambda-processing-error-filter"     ## gives a name to the filter
  pattern = var.error_tag       ## pattern we want to search for (should be a var also used by lambda error logging)
  log_group_name = aws_cloudwatch_log_group.processing_lambda_log_group.name  ## name of log group to watch (possible var?)
  
  metric_transformation {
      name = var.metric_transformation_name_2 ## gives a name to the metric (possible var?)
      namespace = var.processing_metric_namespace  ## name of the metric namespace (possible var?)
      value = "1" ## the value published to the metric each time the pattern is found  
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_processing_error_alarm" {
    alarm_name = "lambda-ingestion-processing-alarm"    ## gives a name to the alarm
    evaluation_periods = 1      ## number of periods to include in the comparison
    period = 60
    comparison_operator = "GreaterThanOrEqualToThreshold"
    statistic = "Sum"
    threshold = 1       ## alarm should be triggered by presence of 1 or more errors
    metric_name = aws_cloudwatch_log_metric_filter.lambda_processing_error_filter.name
    namespace = var.processing_metric_namespace
    alarm_description = "This metric monitors the processing lambda function log for error messages"
    alarm_actions = []  ## this list must contain the arn of the sns topic once created
    insufficient_data_actions = []
}