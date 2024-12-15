resource "aws_scheduler_schedule" "state_machine_scheduler" {
  name       = "${var.project_prefix}${var.scheduler_name}"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = "rate(15 minutes)"

  target {
    arn      = aws_sfn_state_machine.sfn_state_machine.arn
    role_arn = aws_iam_role.scheduler_role.arn
  }
}
