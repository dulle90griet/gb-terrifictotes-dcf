from unittest.mock import patch, Mock
from moto import mock_aws
import pytest, os, boto3, io, random, string, json
import pandas as pd

from src.processing_lambda import save_processed_tables


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


def test_save_processed_tables_creates_parquet_and_uploads_to_s3_for_single_table(
    s3_with_bucket,
):
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

    test_table_dict = {"test_table": pd.DataFrame.from_dict(test_dict)}
    test_check_time = "2022-11-03 14:32:22.906020"
    save_processed_tables(
        s3_with_bucket, "test-bucket", test_table_dict, test_check_time
    )

    object = s3_with_bucket.list_objects(Bucket="test-bucket")
    assert (
        object["Contents"][0]["Key"] == "test_table/2022-11-03 14:32:22.906020.parquet"
    )

    response = s3_with_bucket.get_object(
        Bucket="test-bucket", Key="test_table/2022-11-03 14:32:22.906020.parquet"
    )
    buff = io.BytesIO(response["Body"].read())
    df = pd.read_parquet(buff)
    assert df.columns.tolist() == [
        "currency_id",
        "currency_code",
        "created_at",
        "last_updated",
    ]
    assert all(df["currency_code"].isin(["GBP", "EUR", "USD"]).values)
    assert len(df.index) == 3


def test_save_processed_tables_creates_parquet_and_uploads_to_s3_for_multiple_tables(
    s3_with_bucket,
):
    test_table_dict = {"dim_address": None}
    with open("test/test_data/currency/2024-11-20 15_22_10.531518.json", "r") as f:
        test_table_dict["dim_currency"] = pd.DataFrame.from_dict(json.load(f))
    with open("test/test_data/counterparty/2024-11-21 09_38_15.221234.json") as f:
        test_table_dict["dim_counterparty"] = pd.DataFrame.from_dict(json.load(f))
    with open("test/test_data/sales_order/2024-11-21 16_02_38.340563.json") as f:
        test_table_dict["fact_sales_order"] = pd.DataFrame.from_dict(json.load(f))
    test_check_time = "2024-11-22 08_01_13.193846"

    save_processed_tables(
        s3_with_bucket, "test-bucket", test_table_dict, test_check_time
    )

    objects = s3_with_bucket.list_objects(Bucket="test-bucket")
    s3_file_paths = [object["Key"] for object in objects["Contents"]]
    assert len(s3_file_paths) == 3
    assert "dim_currency/2024-11-22 08_01_13.193846.parquet" in s3_file_paths
    assert "dim_counterparty/2024-11-22 08_01_13.193846.parquet" in s3_file_paths
    assert "fact_sales_order/2024-11-22 08_01_13.193846.parquet" in s3_file_paths

    response_1 = s3_with_bucket.get_object(
        Bucket="test-bucket", Key="dim_currency/2024-11-22 08_01_13.193846.parquet"
    )
    buff_1 = io.BytesIO(response_1["Body"].read())
    df_1 = pd.read_parquet(buff_1)
    assert df_1.columns.tolist() == [
        "currency_id",
        "currency_code",
        "created_at",
        "last_updated",
    ]
    assert df_1.loc[0, "currency_id"] == 1
    assert df_1.loc[0, "currency_code"] == "GBP"
    assert df_1.loc[1, "currency_id"] == 2
    assert df_1.loc[1, "currency_code"] == "USD"
    assert df_1.loc[2, "currency_id"] == 3
    assert df_1.loc[2, "currency_code"] == "EUR"
    assert len(df_1.index) == 3

    response_2 = s3_with_bucket.get_object(
        Bucket="test-bucket", Key="dim_counterparty/2024-11-22 08_01_13.193846.parquet"
    )
    buff_2 = io.BytesIO(response_2["Body"].read())
    df_2 = pd.read_parquet(buff_2)
    assert df_2.columns.tolist() == [
        "counterparty_id",
        "counterparty_legal_name",
        "legal_address_id",
        "commercial_contact",
        "delivery_contact",
        "created_at",
        "last_updated",
    ]
    assert df_2.loc[0, "counterparty_id"] == 1
    assert df_2.loc[0, "legal_address_id"] == 15
    assert df_2.loc[1, "counterparty_legal_name"] == "Kohler Inc"
    assert df_2.loc[1, "commercial_contact"] == "Taylor Haag"
    assert df_2.loc[2, "delivery_contact"] == "Laurie Hermiston"
    assert df_2.loc[2, "last_updated"] == "2024-11-20 16:25:45.036000"
    assert len(df_2.index) == 3

    response_3 = s3_with_bucket.get_object(
        Bucket="test-bucket", Key="fact_sales_order/2024-11-22 08_01_13.193846.parquet"
    )
    buff_3 = io.BytesIO(response_3["Body"].read())
    df_3 = pd.read_parquet(buff_3)
    assert df_3.columns.tolist() == [
        "sales_order_id",
        "created_at",
        "last_updated",
        "design_id",
        "staff_id",
        "counterparty_id",
        "units_sold",
        "unit_price",
        "currency_id",
        "agreed_delivery_date",
        "agreed_payment_date",
        "agreed_delivery_location_id",
    ]
    assert df_3.loc[0, "sales_order_id"] == 11283
    assert df_3.loc[0, "design_id"] == 10
    assert df_3.loc[0, "staff_id"] == 8
    assert df_3.loc[0, "units_sold"] == 53443
    assert df_3.loc[0, "agreed_delivery_date"] == "2024-11-22"
    assert df_3.loc[0, "agreed_delivery_location_id"] == 8
    assert len(df_3.index) == 1


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

    test_df = pd.DataFrame.from_dict([{"column": "value"}])
    test_table_dict = {"test_table": test_df}
    test_check_time = "2022-11-03 14:32:22.906020"
    save_processed_tables(
        s3_with_bucket, "test-bucket", test_table_dict, test_check_time
    )

    path_exists_patcher.stop()
    mkdir_patcher.stop()

    assert os.path.exists(f"{rand_dir}/tmp")

    os.rmdir(f"{rand_dir}/tmp")
    os.rmdir(rand_dir)
