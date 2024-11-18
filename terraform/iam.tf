# LAMBDA POLICY AND ROLE
data "aws_iam_policy_document" "trust_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
  
}

resource "aws_iam_role" "ingestion_lambda_role" {
  name_prefix        = "role-${var.ingestion_lambda_name}"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}

# LAMBDA UPDATE AND RETRIEVE SECRETS ATTACHMENT

data "aws_iam_policy_document" "secrets_manager_data_policy_doc" {
  statement {
    actions = ["secretsmanager:GetSecretValue", "secretsmanager:CreateSecret", "secretsmanager:UpdateSecret"]
    resources = ["arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:gb-ttotes/*"]
  }
  statement {
    actions = ["secretsmanager:ListSecrets"]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "secrets_manager_read_write_policy" {
  name_prefix = "secrets-manager-policy-${var.ingestion_lambda_name}"
  policy = data.aws_iam_policy_document.secrets_manager_data_policy_doc.json
}

resource "aws_iam_role_policy_attachment" "lambda_secrets_manager_read_write_policy_attachment" {
  role = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.secrets_manager_read_write_policy.arn
}

# LAMBDA s3 WRITE PERMISSIONS AND ATTACHMENT

data "aws_iam_policy_document" "s3_data_policy_doc" {
  statement {
    actions = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.ingestion_bucket.arn}/*"]
  }
}
# we have given permission to write into any s3 bucket, 

resource "aws_iam_policy" "s3_write_policy" {
  name_prefix = "s3-policy-${var.ingestion_lambda_name}-write"
  policy      = data.aws_iam_policy_document.s3_data_policy_doc.json
}


resource "aws_iam_role_policy_attachment" "lambda_s3_write_policy_attachment" {
    role = aws_iam_role.ingestion_lambda_role.name
    policy_arn = aws_iam_policy.s3_write_policy.arn
}


# CLOUDWATCH LOGS POLICY AND ATTACHMENT TO LAMBDA ROLE


data "aws_iam_policy_document" "cloudwatch_logs_policy_document" {
  statement {
    

    actions = ["logs:CreateLogGroup"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }
  statement {
    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.ingestion_lambda_name}:*"]
  }
  }


resource "aws_iam_policy" "cloudwatch_logs_policy" {
  name   = "cloudwatch_logs_policy"
  policy = data.aws_iam_policy_document.cloudwatch_logs_policy_document.json
}


resource "aws_iam_role_policy_attachment" "lambda_cloudwatch_logs_policy_attachment" {
    role = aws_iam_role.ingestion_lambda_role.name
    policy_arn = aws_iam_policy.cloudwatch_logs_policy.arn
}



