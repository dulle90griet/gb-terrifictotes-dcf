data "archive_file" "ingestion_lambda_zip" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../src/${var.ingestion_lambda_filename}.py"
  output_path      = "${path.module}/../packages/ingestion/function.zip"
}

data "archive_file" "processing_lambda_zip" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../src/${var.processing_lambda_filename}.py"
  # source_dir       = "${path.module}/../src/processing-lambda"
  output_path = "${path.module}/../packages/processing/function.zip"
}

data "archive_file" "uploading_lambda_zip" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../src/${var.uploading_lambda_filename}.py"
  output_path      = "${path.module}/../packages/load/function.zip"
}