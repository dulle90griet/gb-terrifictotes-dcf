resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = var.code_bucket_prefix
  tags  = {
    Name = "code_bucket"
  }
}


resource "aws_s3_bucket" "ingestion_bucket" {
    bucket_prefix = var.ingestion_bucket_prefix
}


resource "aws_s3_object" "ingestion_lambda_code" {
  bucket = aws_s3_bucket.code_bucket.id
  key    = "ingestion/function.zip"
  source = "${path.module}/../packages/ingestion/function.zip" 
  etag = filemd5(data.archive_file.ingestion_lambda_zip.output_path)
}


resource "aws_s3_object" "lambda_layer" {
  bucket = aws_s3_bucket.code_bucket.id
  key = "layer/layer.zip"
  source = data.archive_file.layer_code.output_path
  etag = filemd5(data.archive_file.layer_code.output_path)
  depends_on = [ data.archive_file.layer_code ]
}