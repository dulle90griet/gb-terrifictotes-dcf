from unittest.mock import patch, Mock
from moto import mock_aws
import pytest, os, boto3, io, random, string
import pandas as pd

from src.processing_lambda import df_to_parquet_in_s3


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
def s3_with_bucket(s3_client):
    s3_client.create_bucket(
        Bucket="test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    yield s3_client


def test_df_to_parquet_in_s3_creates_parquet_and_uploads_to_s3(s3_with_bucket):
    test_dict = [
        {
            "currency_id": 1,
            "currency_code": "GBP",
            "created_at": "2022-11-03 14:20:49.962000",
            "last_updated": "2022-11-03 14:20:49.962000",
        },
        {
            "currency_id": 2,
            "currency_code": "USD",
            "created_at": "2022-11-03 14:20:49.962000",
            "last_updated": "2022-11-03 14:20:49.962000",
        },
        {
            "currency_id": 3,
            "currency_code": "EUR",
            "created_at": "2022-11-03 14:20:49.962000",
            "last_updated": "2022-11-03 14:20:49.962000",
        },
    ]

    test_df = pd.DataFrame.from_dict(test_dict)
    test_folder = "test-folder"
    test_file_name = "test-file-name"
    df_to_parquet_in_s3(
        s3_with_bucket, test_df, "test-bucket", "test-folder", "test-file-name"
    )

    object = s3_with_bucket.list_objects(Bucket="test-bucket")
    assert object["Contents"][0]["Key"] == f"{test_folder}/{test_file_name}.parquet"

    response = s3_with_bucket.get_object(
        Bucket="test-bucket", Key=f"{test_folder}/{test_file_name}.parquet"
    )
    buff = io.BytesIO(response["Body"].read())
    df = pd.read_parquet(buff)
    assert isinstance(df, pd.DataFrame)
    assert all(
        ["currency_id", "currency_code", "created_at", "last_updated"] == df.columns
    )
    assert all(df["currency_code"].isin(["GBP", "EUR", "USD"]).values)
    assert len(df.index) == 3


def test_df_to_parquet_in_s3_makes_root_tmp_dir_if_none_exists(s3_with_bucket):
    # Create patch for os.path.exists
    path_exists_patcher = patch(
        "src.processing_lambda.os.path.exists", return_value=False
    )

    # Create patch for os.mkdir
    while True:
        rand_dir = "".join(
            random.choice(string.ascii_uppercase + string.digits) for _ in range(3)
        )
        if not os.path.exists(rand_dir):
            break

    def prepend_path(func):
        def function_invoker(path):
            func(rand_dir)
            func(rand_dir + path)

        return function_invoker

    mkdir_patcher = patch(
        "src.processing_lambda.os.mkdir", side_effect=prepend_path(os.mkdir)
    )

    # Begin testing
    path_exists_patcher.start()
    mkdir_patcher.start()

    test_dict = [{"column": "value"}]
    test_df = pd.DataFrame.from_dict(test_dict)
    df_to_parquet_in_s3(
        s3_with_bucket, test_df, "test-bucket", "test-folder", "test-file-name"
    )

    path_exists_patcher.stop()
    mkdir_patcher.stop()

    assert os.path.exists(f"{rand_dir}/tmp")

    os.rmdir(f"{rand_dir}/tmp")
    os.rmdir(rand_dir)
