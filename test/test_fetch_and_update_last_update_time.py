import os, pytest, boto3
from datetime import datetime, timedelta
from moto import mock_aws

from src.ingestion_lambda import (
    fetch_and_update_last_update_time,
    store_secret,
    retrieve_secret,
)


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


def test_fetch_and_update_creates_secret_if_not_found(sm_client):
    secret_id_to_check = "df2-ttotes/last-update-test-bucket"

    sm_response = sm_client.list_secrets(MaxResults=99, IncludePlannedDeletion=False)
    secrets_list = sm_response["SecretList"]
    assert secret_id_to_check not in [secret["Name"] for secret in secrets_list]

    fetch_and_update_last_update_time(sm_client, "test-bucket")

    sm_response = sm_client.list_secrets(MaxResults=99, IncludePlannedDeletion=False)
    secrets_list = sm_response["SecretList"]
    assert secret_id_to_check in [secret["Name"] for secret in secrets_list]


def test_fetch_and_update_updates_secret_if_found(sm_client):
    secret_id = "df2-ttotes/last-update-test-bucket"
    date_and_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    store_secret(sm_client, secret_id, ["last_update", date_and_time])

    fetch_and_update_last_update_time(sm_client, "test-bucket")

    last_update_secret = retrieve_secret(sm_client, secret_id)
    last_update = last_update_secret["last_update"]
    former_datetime = datetime.strptime(date_and_time, "%Y-%m-%d %H:%M:%S.%f")
    latest_datetime = datetime.strptime(last_update, "%Y-%m-%d %H:%M:%S.%f")

    assert latest_datetime > former_datetime


def test_fetch_and_update_returns_dict(sm_client):
    result = fetch_and_update_last_update_time(sm_client, "test-bucket")
    assert isinstance(result, dict)


def test_fetch_and_update_returns_plausible_last_update_string(sm_client):
    response = fetch_and_update_last_update_time(sm_client, "test-bucket")
    last_update_1 = response["last_update"]
    assert isinstance(last_update_1, str)
    assert last_update_1 == "2020-01-01 00:00:00.000000"

    date_and_time = datetime.now()
    response = fetch_and_update_last_update_time(sm_client, "test-bucket")
    last_update_2 = response["last_update"]
    response = fetch_and_update_last_update_time(sm_client, "test-bucket")
    last_update_3 = response["last_update"]
    assert isinstance(last_update_2, str)
    assert isinstance(last_update_3, str)

    utc_acceptable_variation = timedelta(seconds=30)
    last_update_2_time = datetime.strptime(last_update_2, "%Y-%m-%d %H:%M:%S.%f")
    last_update_3_time = datetime.strptime(last_update_3, "%Y-%m-%d %H:%M:%S.%f")

    if last_update_2_time > date_and_time:
        variation = last_update_2_time - date_and_time
    else:
        variation = date_and_time - last_update_2_time
    assert variation <= utc_acceptable_variation

    assert last_update_3_time > last_update_2_time
    assert last_update_3_time - last_update_2_time < timedelta(seconds=1)


def test_fetch_and_update_returns_plausible_current_update_string(sm_client):
    date_and_time = datetime.now()

    response = fetch_and_update_last_update_time(sm_client, "test-bucket")
    current_update = response["current_update"]
    assert isinstance(current_update, str)

    current_update_time = datetime.strptime(current_update, "%Y-%m-%d %H:%M:%S.%f")
    assert current_update_time > date_and_time
    assert current_update_time - date_and_time < timedelta(seconds=1)
