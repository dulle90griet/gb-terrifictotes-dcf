from src.ingestion_lambda import json_to_s3
from moto import mock_aws
import pytest, os, boto3


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto"""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture
def s3_client(aws_credentials):
    with mock_aws():
        s3 = boto3.client("s3")
        yield s3


def test_json_to_s3_uploads_json(s3_client):
    #  test_file_directory = f"{os.getcwd()}/test"
    s3_client.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={
            "LocationConstraint": os.environ["AWS_DEFAULT_REGION"]
        },
    )
    test_json = '{"name":"John", "age":30, "car":null}'
    test_folder = "test-folder"
    test_file_name = "test-file-name.json"

    json_to_s3(s3_client, test_json, "test-bucket", test_folder, test_file_name)
    # print(s3_client.list_objects(Bucket="test-bucket"))
    object = s3_client.list_objects(Bucket="test-bucket")

    assert object["Contents"][0]["Key"] == f"{test_folder}/{test_file_name}"
    response = s3_client.get_object(
        Bucket="test-bucket", Key=f"{test_folder}/{test_file_name}"
    )
    # print(response["Body"].read().decode("UTF-8"))
    assert (
        response["Body"].read().decode("UTF-8")
        == '{"name":"John", "age":30, "car":null}'
    )
