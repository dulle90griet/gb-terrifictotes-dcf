import os, pytest, boto3
from moto import mock_aws

from src.ingestion_lambda import fetch_and_update_last_update_time


@pytest.fixture
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

    os.environ["INGESTION_BUCKET_NAME"] = "test-bucket"


@pytest.fixture
def sm_client(aws_credentials):
    with mock_aws():
        client = boto3.client("secretsmanager")
        yield client
        client.close()


def test_fetch_and_update_creates_secret_if_not_found(sm_client):
    pass


def test_fetch_and_update_updates_secret_if_found(sm_client):
    pass


def test_fetch_and_update_returns_plausible_datetime(sm_client):
    pass
