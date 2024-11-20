import boto3, os


def json_to_s3(client, json_string, bucket_name, folder, file_name):

    with open(f"/tmp/{file_name}", "w", encoding="UTF-8") as file:
        file.write(json_string)

    client.upload_file(
        f"/tmp/{file_name}", bucket_name, f"{folder}/{file_name}"
    )

    os.remove(f"/tmp/{file_name}")


# (file name for s3 in main function will need date time ingested)
    """takes json string, writes as local file and then uploads to s3 bucket;
    name of table data has come from,
    bucket name data to be uploaded to,
    folder - path in bucket
    file_name - name of data when stored in bucket"""