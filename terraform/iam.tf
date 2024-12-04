# LAMBDA POLICY AND ROLE
data "aws_iam_policy_document" "lambda_trust_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }

}

## INGESTION LAMBDA ## 

resource "aws_iam_role" "ingestion_lambda_role" {
  name_prefix        = "role-${var.ingestion_lambda_name}"
  assume_role_policy = data.aws_iam_policy_document.lambda_trust_policy.json
}

# PROCESSING LAMBDA #

resource "aws_iam_role" "processing_lambda_role" {
  name_prefix        = "role-${var.processing_lambda_name}"
  assume_role_policy = data.aws_iam_policy_document.lambda_trust_policy.json
}

# UPLOADING LAMBDA #

resource "aws_iam_role" "uploading_lambda_role" {
  name_prefix        = "role-${var.uploading_lambda_name}"
  assume_role_policy = data.aws_iam_policy_document.lambda_trust_policy.json
}

# LAMBDA UPDATE AND RETRIEVE SECRETS ATTACHMENT

data "aws_iam_policy_document" "secrets_manager_data_policy_doc" {
  statement {
    actions   = ["secretsmanager:GetSecretValue", "secretsmanager:CreateSecret", "secretsmanager:UpdateSecret"]
    resources = ["arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:gb-ttotes/*"]
  }
  statement {
    actions   = ["secretsmanager:ListSecrets"]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "secrets_manager_read_write_policy" {
  name_prefix = "secrets-manager-policy-${var.ingestion_lambda_name}"
  policy      = data.aws_iam_policy_document.secrets_manager_data_policy_doc.json
}

resource "aws_iam_role_policy_attachment" "lambda_secrets_manager_read_write_policy_attachment" {
  role       = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.secrets_manager_read_write_policy.arn
}

resource "aws_iam_role_policy_attachment" "uploading_lambda_secrets_manager_read_write_policy_attachment" {
  role       = aws_iam_role.uploading_lambda_role.name
  policy_arn = aws_iam_policy.secrets_manager_read_write_policy.arn
}


# INGESTION LAMBDA s3 WRITE PERMISSIONS AND ATTACHMENT

data "aws_iam_policy_document" "ingestion_s3_data_policy_doc" {
  statement {
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.ingestion_bucket.arn}/*"]
  }
}

resource "aws_iam_policy" "ingestion_s3_write_policy" {
  name_prefix = "s3-policy-${var.ingestion_lambda_name}-write"
  policy      = data.aws_iam_policy_document.ingestion_s3_data_policy_doc.json
}


resource "aws_iam_role_policy_attachment" "ingestion_lambda_s3_write_policy_attachment" {
  role       = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.ingestion_s3_write_policy.arn
}

# PROCESSING LAMBDA s3 WRITE PERMISSIONS AND ATTACHMENT

data "aws_iam_policy_document" "processing_s3_data_policy_doc" {
  statement {
    actions   = ["s3:ListBucket"]
    resources = ["${aws_s3_bucket.ingestion_bucket.arn}"]
  }
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.ingestion_bucket.arn}/*"]
  }
  statement {
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.processing_bucket.arn}/*"]
  }
}

resource "aws_iam_policy" "processing_s3_write_policy" {
  name_prefix = "s3-policy-${var.processing_lambda_name}-write"
  policy      = data.aws_iam_policy_document.processing_s3_data_policy_doc.json
}


resource "aws_iam_role_policy_attachment" "processing_lambda_s3_write_policy_attachment" {
  role       = aws_iam_role.processing_lambda_role.name
  policy_arn = aws_iam_policy.processing_s3_write_policy.arn
}

# UPLOADING LAMBDA S3 PERMISSIONS AND ATTACHMENT

data "aws_iam_policy_document" "retrieving_data_from_s3_processing_bucket_policy_doc" {
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.processing_bucket.arn}/*"]
  }
}

resource "aws_iam_policy" "uploading_s3_read_policy" {
  name_prefix = "s3-policy-${var.uploading_lambda_name}-read"
  policy      = data.aws_iam_policy_document.retrieving_data_from_s3_processing_bucket_policy_doc.json
}

resource "aws_iam_role_policy_attachment" "uploading_lambda_s3_read_policy_attachment" {
  role       = aws_iam_role.uploading_lambda_role.name
  policy_arn = aws_iam_policy.uploading_s3_read_policy.arn
}

# INGESTION CLOUDWATCH LOGS POLICY AND ATTACHMENT TO LAMBDA ROLE


data "aws_iam_policy_document" "ingestion_cloudwatch_logs_policy_document" {
  statement {
    actions = ["logs:CreateLogGroup"]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.ingestion_lambda_name}:*"]
  }
}


resource "aws_iam_policy" "ingestion_cloudwatch_logs_policy" {
  name   = "ingestion_cloudwatch_logs_policy"
  policy = data.aws_iam_policy_document.ingestion_cloudwatch_logs_policy_document.json
}


resource "aws_iam_role_policy_attachment" "ingestion_lambda_cloudwatch_logs_policy_attachment" {
  role       = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.ingestion_cloudwatch_logs_policy.arn
}

##PROCESSING CLOUDWATCH LOGS POLICY AND ATTACHMENT TO LAMBDA ROLE##

data "aws_iam_policy_document" "processing_cloudwatch_logs_policy_document" {
  statement {
    actions = ["logs:CreateLogGroup"]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.processing_lambda_name}:*"]
  }
}


resource "aws_iam_policy" "processing_cloudwatch_logs_policy" {
  name   = "processing_cloudwatch_logs_policy"
  policy = data.aws_iam_policy_document.processing_cloudwatch_logs_policy_document.json
}


resource "aws_iam_role_policy_attachment" "processing_lambda_cloudwatch_logs_policy_attachment" {
  role       = aws_iam_role.processing_lambda_role.name
  policy_arn = aws_iam_policy.processing_cloudwatch_logs_policy.arn
}

# UPLOADING CLOUDWATCH LOGS POLICY AND ATTACHMENT TO LAMBDA ROLE

data "aws_iam_policy_document" "uploading_cloudwatch_logs_policy_document" {
  statement {


    actions = ["logs:CreateLogGroup"]

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.uploading_lambda_name}:*"]
  }
}

resource "aws_iam_policy" "uploading_cloudwatch_logs_policy" {
  name   = "uploading_cloudwatch_logs_policy"
  policy = data.aws_iam_policy_document.uploading_cloudwatch_logs_policy_document.json
}


resource "aws_iam_role_policy_attachment" "uploading_lambda_cloudwatch_logs_policy_attachment" {
  role       = aws_iam_role.uploading_lambda_role.name
  policy_arn = aws_iam_policy.uploading_cloudwatch_logs_policy.arn
}

########################################################################## 

## STATE MACHINE ##


# STATE MACHINE POLICY AND ROLE

data "aws_iam_policy_document" "state_machine_trust_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "state_machine_role" {
  name_prefix        = "role-${var.state_machine_name}"
  assume_role_policy = data.aws_iam_policy_document.state_machine_trust_policy.json
}


# STATE MACHINE POLICY AND ATTACHMENT TO STATE MACHINE ROLE

data "aws_iam_policy_document" "state_machine_policy_document" {

  statement {
    actions = ["lambda:InvokeFunction"]
    resources = [
      "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.ingestion_lambda_name}:*", "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.processing_lambda_name}:*",
      "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.uploading_lambda_name}:*"
    ]
  }
}


resource "aws_iam_policy" "state_machine_policy" {

  name_prefix = "state-machine-police-${var.state_machine_name}"
  policy      = data.aws_iam_policy_document.state_machine_policy_document.json

}


resource "aws_iam_role_policy_attachment" "state_machine_policy_attachment" {

  role       = aws_iam_role.state_machine_role.name
  policy_arn = aws_iam_policy.state_machine_policy.arn

}


# FOR FUTURE REFERENCE: we may need to add a policy for monitoring the state machine (either cloudwatch logs or amazon xray)

# SCHEDULER POLICY DOC, IAM ROLE, POLICY AND POLICY ROLE ATTACHMENT

data "aws_iam_policy_document" "scheduler_trust_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}


resource "aws_iam_role" "scheduler_role" {
  name               = "scheduler-iam-role-gg-ttotes"
  assume_role_policy = data.aws_iam_policy_document.scheduler_trust_policy.json
}

data "aws_iam_policy_document" "scheduler_policy_document" {

  statement {
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.sfn_state_machine.arn]
  }

}

resource "aws_iam_policy" "scheduler_policy" {

  name_prefix = "scheduler-policy-"
  policy      = data.aws_iam_policy_document.scheduler_policy_document.json

}

resource "aws_iam_role_policy_attachment" "scheduler_policy_attachment" {

  role       = aws_iam_role.scheduler_role.name
  policy_arn = aws_iam_policy.scheduler_policy.arn

}


