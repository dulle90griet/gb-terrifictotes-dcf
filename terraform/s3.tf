##BUCKETS##

resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "${var.project_prefix}${var.code_bucket_prefix}"
  tags = {
    Name = "code_bucket"
  }
}

resource "aws_s3_bucket" "ingestion_bucket" {
  bucket_prefix = "${var.project_prefix}${var.ingestion_bucket_prefix}"
}

resource "aws_s3_bucket" "processing_bucket" {
  bucket_prefix = "${var.project_prefix}${var.processing_bucket_prefix}"
}

##LAMBDA CODE##

resource "aws_s3_object" "ingestion_lambda_code" {
  bucket = aws_s3_bucket.code_bucket.id
  key    = "ingestion/function.zip"
  source = "${path.module}/../packages/ingestion/function.zip"
  etag   = filemd5(data.archive_file.ingestion_lambda_zip.output_path)
}

resource "aws_s3_object" "processing_lambda_code" {
  bucket = aws_s3_bucket.code_bucket.id
  key    = "processing/function.zip"
  source = "${path.module}/../packages/processing/function.zip"
  etag   = filemd5(data.archive_file.processing_lambda_zip.output_path)
}

resource "aws_s3_object" "uploading_lambda_code" {
  bucket = aws_s3_bucket.code_bucket.id
  key    = "load/function.zip"
  source = "${path.module}/../packages/load/function.zip"
  etag   = filemd5(data.archive_file.uploading_lambda_zip.output_path)
}

##LAYERS##

resource "aws_s3_object" "lambda_layer" {
  bucket     = aws_s3_bucket.code_bucket.id
  key        = "layer/layer.zip"
  source     = data.archive_file.layer_code.output_path
  etag       = filemd5(data.archive_file.layer_code.output_path)
  depends_on = [data.archive_file.layer_code]
}

resource "aws_s3_object" "processing_lambda_layer" {
  bucket = aws_s3_bucket.code_bucket.id
  key    = "layer/processing-layer.zip"
  source = "${path.module}/../packages/layer/processing_layer.zip"
  etag   = filemd5("${path.module}/../packages/layer/processing_layer.zip")
}