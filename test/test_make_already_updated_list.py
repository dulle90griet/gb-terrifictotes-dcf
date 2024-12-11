from src.processing_lambda import make_already_updated_list
import pytest, os, boto3
from moto import mock_aws


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


@pytest.fixture
def s3_bucket(s3_client):
    s3_client.create_bucket(
        Bucket="test_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    yield s3_client


def test_make_already_updated_list_fetches_updated_ids_from_update_packet(s3_bucket):
    s3_bucket.upload_file(
        Bucket="test_bucket",
        Filename="test/test_data/staff/2024-11-20 15_22_10.531518.json",
        Key="staff/2024-11-20 15_22_10.531518.json",
    )
    s3_bucket.upload_file(
        Bucket="test_bucket",
        Filename="test/test_data/staff/2024-11-21 09_38_15.221234.json",
        Key="staff/2024-11-21 09_38_15.221234.json",
    )

    test_already_updated_list = make_already_updated_list(
        s3_bucket, "test_bucket", "staff", "2024-11-21 09_38_15.221234"
    )

    assert isinstance(test_already_updated_list, list)
    assert test_already_updated_list == [1, 16]
