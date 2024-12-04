# resource "aws_sns_topic" "error_email_topic" {
#   name = "error_email_topic"
# }

# resource "aws_sns_topic_subscription" "error_email_alert_target" {
#   topic_arn = aws_sns_topic.error_email_topic.arn
#   protocol  = "email"
#   endpoint  = "donald.futerss@gmail.com"
# }