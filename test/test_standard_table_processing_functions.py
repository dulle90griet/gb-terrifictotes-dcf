from moto import mock_aws
import pytest, boto3, os


from src.processing_lambda import process_counterparty_updates, process_currency_updates, process_design_updates, process_staff_updates, process_sales_order_updates


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
        Bucket="test_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    yield s3_client


def test_process_counterparty_updates_returns_expected_dataframe(s3_with_bucket):
    # upload test/test_data/counterparty/2024-11-20 15_22_10.531518.json
    # upload test/test_data/address/2024-11-20 15_22_10.531518.json

    # test with those files

    # upload test/test_data/counterparty/2024-11-21 09_38_15.221234.json
    # upload test/test_data/address/2024-11-21 09_38_15.221234.json

    # test with those files


    ## PROCESS_COUNTERPARTY_UPDATES
    # TAKES ARGS: s3_client, bucket_name, current_check_time
    # RETURNS: dim_counterparty_df
    pass


@pytest.mark.skip
def test_process_currency_updates_returns_expected_dataframe(s3_with_bucket):
    # upload test/test_data/currency/2024-11-20 15_22_10.531518.json

    # test output
    pass


@pytest.mark.skip
def test_process_design_updates_returns_expected_dataframe(s3_with_bucket):
    # upload test/test_data/design/2024-11-20 15_22_10.531518.json

    # test output
    pass


@pytest.mark.skip
def test_process_staff_updates_returns_expected_dataframe(s3_with_bucket):
    # upload test/test_data/staff/2024-11-20 15_22_10.531518.json
    # upload test/test_data/department/2024-11-20 15_22_10.531518.json

    # test with those files

    # upload test/test_data/staff/2024-11-21 09_38_15.221234.json
    # upload test/test_data/department/2024-11-21 09_38_15.221234.json

    # test with those files
    pass


@pytest.mark.skip
def test_process_sales_order_updates_returns_expected_dataframe(s3_with_bucket):
    # upload test/test_data/sales_order/2024-11-21 13_32_38.364280.json

    # test output
    pass