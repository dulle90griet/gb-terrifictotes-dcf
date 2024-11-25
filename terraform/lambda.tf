resource "aws_lambda_function" "ingestion_lambda" {

  function_name    = var.ingestion_lambda_name
  role             = aws_iam_role.ingestion_lambda_role.arn
  s3_bucket        = aws_s3_bucket.code_bucket.id
  s3_key           = aws_s3_object.ingestion_lambda_code.key
  handler          = "${var.ingestion_lambda_filename}.ingestion_lambda_handler"
  runtime          = var.python_runtime
  timeout          = 180
  memory_size      = 512
  source_code_hash = filebase64sha256("${path.module}/../src/${var.ingestion_lambda_filename}.py")
  publish          = true
  layers           = [aws_lambda_layer_version.dependencies.arn]

  depends_on = [
    aws_s3_object.ingestion_lambda_code,
    aws_s3_object.lambda_layer
  ]

  environment {
    variables = {
      INGESTION_BUCKET_NAME = aws_s3_bucket.ingestion_bucket.id
    }
  }
}

resource "aws_lambda_function" "processing_lambda" {

  function_name    = var.processing_lambda_name
  role             = aws_iam_role.processing_lambda_role.arn
  s3_bucket        = aws_s3_bucket.code_bucket.id
  s3_key           = aws_s3_object.processing_lambda_code.key
  handler          = "${var.processing_lambda_filename}.processing_lambda_handler"
  runtime          = var.python_runtime
  timeout          = 180
  memory_size      = 512
  source_code_hash = filebase64sha256("${path.module}/../src/${var.processing_lambda_filename}.py")
  publish          = true
  layers           = [aws_lambda_layer_version.dependencies.arn]

  depends_on = [
    aws_s3_object.processing_lambda_code,
    aws_s3_object.lambda_layer
  ]

  environment {
    variables = {
      PROCESSING_BUCKET_NAME = aws_s3_bucket.processing_bucket.id
      INGESTION_BUCKET_NAME = aws_s3_bucket.ingestion_bucket.id
    }
  }
}


