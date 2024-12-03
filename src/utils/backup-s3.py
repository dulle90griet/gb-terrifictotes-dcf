import os, datetime
import boto3

from src.ingestion_lambda import update_secret


s3 = boto3.client('s3')

def bucket_backup(s3_client, output_directory, bucket, prefix=None):
    paginator = s3.get_paginator('list_objects_v2')

    if prefix is None:
        pages = paginator.paginate(Bucket=bucket)
    else:
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in pages:
        for obj in page['Contents']:
            try:
                output_path = f"{output_directory}/{obj['Key']}"
                directories = output_path.split("/")
                for i in range(len(directories) - 1):
                    path_so_far = (os.getcwd() + "/" + 
                                   "/".join([directories[j] for j in range(i + 1)]))
                    if not os.path.isdir(path_so_far):
                        os.mkdir(path_so_far)

                s3.download_file(
                    bucket,
                    obj['Key'],
                    f"{os.getcwd()}/{output_directory}/{obj['Key']}"
                )

                print(f"{obj['Key']} > {output_path}")
            except Exception as e:
                raise Exception(e)


if __name__ == "__main__":
    date_and_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    bucket_backup(s3, f"s3_backups/ingestion/{date_and_time}",
                  "green-bean-ingestion-bucket-20241120145518347200000003")
    bucket_backup(s3, f"s3_backups/processing/{date_and_time}",
                  "green-bean-processing-bucket-20241121160032242000000001")