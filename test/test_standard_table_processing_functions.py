from moto import mock_aws
from unittest.mock import Mock, patch
import pandas as pd
import pytest, boto3, os


from src.processing_lambda import (
    process_counterparty_updates,
    process_currency_updates,
    process_design_updates,
    process_staff_updates,
    process_sales_order_updates,
)


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


def test_process_counterparty_updates_returns_expected_dataframe(s3_with_bucket):
    s3_with_bucket.upload_file(
        Bucket="test-bucket",
        Filename="test/test_data/counterparty/2024-11-20 15_22_10.531518.json",
        Key="counterparty/2024-11-20 15_22_10.531518.json",
    )
    s3_with_bucket.upload_file(
        Bucket="test-bucket",
        Filename="test/test_data/address/2024-11-20 15_22_10.531518.json",
        Key="address/2024-11-20 15_22_10.531518.json",
    )
    current_check_time = "2024-11-20 15_22_10.531518"

    dim_counterparty_df = process_counterparty_updates(
        s3_with_bucket, "test-bucket", current_check_time
    )

    counterparty_id_1_df = dim_counterparty_df[
        dim_counterparty_df["counterparty_id"] == 1
    ]
    assert (
        counterparty_id_1_df.loc[
            counterparty_id_1_df.index[0], "counterparty_legal_name"
        ]
        == "Fahey and Sons"
    )

    counterparty_id_4_df = dim_counterparty_df[
        dim_counterparty_df["counterparty_id"] == 4
    ]
    assert (
        counterparty_id_4_df.loc[
            counterparty_id_4_df.index[0], "counterparty_legal_address_line_1"
        ]
        == "37736 Heathcote Lock"
    )

    counterparty_id_6_df = dim_counterparty_df[
        dim_counterparty_df["counterparty_id"] == 6
    ]
    assert (
        counterparty_id_6_df.loc[
            counterparty_id_6_df.index[0], "counterparty_legal_address_line_2"
        ]
        is None
    )

    counterparty_id_9_df = dim_counterparty_df[
        dim_counterparty_df["counterparty_id"] == 9
    ]
    assert (
        counterparty_id_9_df.loc[
            counterparty_id_9_df.index[0], "counterparty_legal_district"
        ]
        is None
    )

    counterparty_id_11_df = dim_counterparty_df[
        dim_counterparty_df["counterparty_id"] == 11
    ]
    assert (
        counterparty_id_11_df.loc[
            counterparty_id_11_df.index[0], "counterparty_legal_city"
        ]
        == "Hackensack"
    )

    counterparty_id_14_df = dim_counterparty_df[
        dim_counterparty_df["counterparty_id"] == 14
    ]
    assert (
        counterparty_id_14_df.loc[
            counterparty_id_14_df.index[0], "counterparty_legal_postal_code"
        ]
        == "94545-4284"
    )

    counterparty_id_16_df = dim_counterparty_df[
        dim_counterparty_df["counterparty_id"] == 16
    ]
    assert (
        counterparty_id_16_df.loc[
            counterparty_id_16_df.index[0], "counterparty_legal_country"
        ]
        == "Iraq"
    )

    counterparty_id_18_df = dim_counterparty_df[
        dim_counterparty_df["counterparty_id"] == 18
    ]
    assert (
        counterparty_id_18_df.loc[
            counterparty_id_18_df.index[0], "counterparty_legal_phone_number"
        ]
        == "5507 549583"
    )

    assert len(dim_counterparty_df.index) == 20

    s3_with_bucket.upload_file(
        Bucket="test-bucket",
        Filename="test/test_data/counterparty/2024-11-21 09_38_15.221234.json",
        Key="counterparty/2024-11-21 09_38_15.221234.json",
    )
    s3_with_bucket.upload_file(
        Bucket="test-bucket",
        Filename="test/test_data/address/2024-11-21 09_38_15.221234.json",
        Key="address/2024-11-21 09_38_15.221234.json",
    )
    current_check_time = "2024-11-21 09_38_15.221234"

    dim_counterparty_df_2 = process_counterparty_updates(
        s3_with_bucket, "test-bucket", current_check_time
    )

    counterparty_id_1_df = dim_counterparty_df_2[
        dim_counterparty_df_2["counterparty_id"] == 1
    ]
    assert (
        counterparty_id_1_df.loc[
            counterparty_id_1_df.index[0], "counterparty_legal_name"
        ]
        == "Fahey and Sons"
    )

    counterparty_id_1_df = dim_counterparty_df_2[
        dim_counterparty_df_2["counterparty_id"] == 1
    ]
    assert (
        counterparty_id_1_df.loc[
            counterparty_id_1_df.index[0], "counterparty_legal_address_line_1"
        ]
        == "605 Haskell Trafficway"
    )

    counterparty_id_1_df = dim_counterparty_df_2[
        dim_counterparty_df_2["counterparty_id"] == 1
    ]
    assert (
        counterparty_id_1_df.loc[
            counterparty_id_1_df.index[0], "counterparty_legal_address_line_2"
        ]
        == "Axel Freeway"
    )

    counterparty_id_4_df = dim_counterparty_df_2[
        dim_counterparty_df_2["counterparty_id"] == 4
    ]
    assert (
        counterparty_id_4_df.loc[
            counterparty_id_4_df.index[0], "counterparty_legal_district"
        ]
        is None
    )

    counterparty_id_4_df = dim_counterparty_df_2[
        dim_counterparty_df_2["counterparty_id"] == 4
    ]
    assert (
        counterparty_id_4_df.loc[
            counterparty_id_4_df.index[0], "counterparty_legal_city"
        ]
        == "Suffolk"
    )

    counterparty_id_4_df = dim_counterparty_df_2[
        dim_counterparty_df_2["counterparty_id"] == 4
    ]
    assert (
        counterparty_id_4_df.loc[
            counterparty_id_4_df.index[0], "counterparty_legal_postal_code"
        ]
        == "56693-0660"
    )

    counterparty_id_11_df = dim_counterparty_df_2[
        dim_counterparty_df_2["counterparty_id"] == 11
    ]
    assert (
        counterparty_id_11_df.loc[
            counterparty_id_11_df.index[0], "counterparty_legal_country"
        ]
        == "Indonesia"
    )

    counterparty_id_11_df = dim_counterparty_df_2[
        dim_counterparty_df_2["counterparty_id"] == 11
    ]
    assert (
        counterparty_id_11_df.loc[
            counterparty_id_11_df.index[0], "counterparty_legal_phone_number"
        ]
        == "5507 549583"
    )

    assert len(dim_counterparty_df_2.index) == 3


def test_process_currency_updates_returns_expected_dataframe(s3_with_bucket):
    s3_with_bucket.upload_file(
        Bucket="test-bucket",
        Filename="test/test_data/currency/2024-11-20 15_22_10.531518.json",
        Key="currency/2024-11-20 15_22_10.531518.json",
    )
    current_check_time = "2024-11-20 15_22_10.531518"

    dim_currency_df = process_currency_updates(
        s3_with_bucket, "test-bucket", current_check_time
    )

    currency_id_1_df = dim_currency_df[dim_currency_df["currency_id"] == 1]
    assert currency_id_1_df.loc[currency_id_1_df.index[0], "currency_code"] == "GBP"

    currency_id_2_df = dim_currency_df[dim_currency_df["currency_id"] == 2]
    assert currency_id_2_df.loc[currency_id_2_df.index[0], "currency_code"] == "USD"

    currency_id_3_df = dim_currency_df[dim_currency_df["currency_id"] == 3]
    assert currency_id_3_df.loc[currency_id_3_df.index[0], "currency_code"] == "EUR"

    assert dim_currency_df.columns.tolist() == [
        "currency_id",
        "currency_code",
        "currency_name",
    ]

    assert len(dim_currency_df.index) == 3


def test_process_currency_updates_invokes_Currency_for_each_row(
    s3_with_bucket,
):
    s3_with_bucket.upload_file(
        Bucket="test-bucket",
        Filename="test/test_data/currency/2024-11-20 15_22_10.531518.json",
        Key="currency/2024-11-20 15_22_10.531518.json",
    )
    current_check_time = "2024-11-20 15_22_10.531518"

    def currency_intercepter(currency_code):
        currency_obj = Mock()
        currency_obj.currency_name = f"{currency_code} name"
        return currency_obj

    currency_patch = patch(
        "src.processing_lambda.Currency", side_effect=currency_intercepter
    )
    currency_patch.start()

    dim_currency_df = process_currency_updates(
        s3_with_bucket, "test-bucket", current_check_time
    )

    currency_patch.stop()

    currency_id_1_df = dim_currency_df[dim_currency_df["currency_id"] == 1]
    assert (
        currency_id_1_df.loc[currency_id_1_df.index[0], "currency_name"] == "GBP name"
    )

    currency_id_2_df = dim_currency_df[dim_currency_df["currency_id"] == 2]
    assert (
        currency_id_2_df.loc[currency_id_2_df.index[0], "currency_name"] == "USD name"
    )

    currency_id_3_df = dim_currency_df[dim_currency_df["currency_id"] == 3]
    assert (
        currency_id_3_df.loc[currency_id_3_df.index[0], "currency_name"] == "EUR name"
    )


def test_process_design_updates_returns_expected_dataframe(s3_with_bucket):
    s3_with_bucket.upload_file(
        Bucket="test-bucket",
        Filename="test/test_data/design/2024-11-20 15_22_10.531518.json",
        Key="design/2024-11-20 15_22_10.531518.json",
    )
    current_check_time = "2024-11-20 15_22_10.531518"

    dim_design_df = process_design_updates(
        s3_with_bucket, "test-bucket", current_check_time
    )

    assert dim_design_df.columns.tolist() == [
        "design_id",
        "design_name",
        "file_location",
        "file_name",
    ]

    design_id_8_df = dim_design_df[dim_design_df["design_id"] == 8]
    assert design_id_8_df.loc[design_id_8_df.index[0], "design_name"] == "Wooden"

    design_id_51_df = dim_design_df[dim_design_df["design_id"] == 51]
    assert design_id_51_df.loc[design_id_51_df.index[0], "file_location"] == "/private"

    design_id_69_df = dim_design_df[dim_design_df["design_id"] == 69]
    assert (
        design_id_69_df.loc[design_id_69_df.index[0], "file_name"]
        == "bronze-20230102-r904.json"
    )

    design_id_16_df = dim_design_df[dim_design_df["design_id"] == 16]
    assert design_id_16_df.loc[design_id_16_df.index[0], "design_name"] == "Soft"

    design_id_54_df = dim_design_df[dim_design_df["design_id"] == 54]
    assert (
        design_id_54_df.loc[design_id_54_df.index[0], "file_location"] == "/usr/ports"
    )

    design_id_10_df = dim_design_df[dim_design_df["design_id"] == 10]
    assert (
        design_id_10_df.loc[design_id_10_df.index[0], "file_name"]
        == "soft-20220201-hzz1.json"
    )

    assert len(dim_design_df.index) == 6


def test_process_staff_updates_returns_expected_dataframe(s3_with_bucket):
    s3_with_bucket.upload_file(
        Bucket="test-bucket",
        Filename="test/test_data/staff/2024-11-20 15_22_10.531518.json",
        Key="staff/2024-11-20 15_22_10.531518.json",
    )
    s3_with_bucket.upload_file(
        Bucket="test-bucket",
        Filename="test/test_data/department/2024-11-20 15_22_10.531518.json",
        Key="department/2024-11-20 15_22_10.531518.json",
    )
    current_check_time = "2024-11-20 15_22_10.531518"

    dim_staff_df = process_staff_updates(
        s3_with_bucket, "test-bucket", current_check_time
    )

    assert dim_staff_df.columns.tolist() == [
        "staff_id",
        "first_name",
        "last_name",
        "department_name",
        "location",
        "email_address",
    ]

    staff_id_1_df = dim_staff_df[dim_staff_df["staff_id"] == 1]
    assert staff_id_1_df.loc[staff_id_1_df.index[0], "first_name"] == "Jeremie"

    staff_id_6_df = dim_staff_df[dim_staff_df["staff_id"] == 6]
    assert staff_id_6_df.loc[staff_id_6_df.index[0], "last_name"] == "Kreiger"

    staff_id_11_df = dim_staff_df[dim_staff_df["staff_id"] == 11]
    assert staff_id_11_df.loc[staff_id_11_df.index[0], "department_name"] == "Finance"

    staff_id_16_df = dim_staff_df[dim_staff_df["staff_id"] == 16]
    assert staff_id_16_df.loc[staff_id_16_df.index[0], "location"] == "Manchester"

    staff_id_20_df = dim_staff_df[dim_staff_df["staff_id"] == 20]
    assert (
        staff_id_20_df.loc[staff_id_20_df.index[0], "email_address"]
        == "flavio.kulas@terrifictotes.com"
    )

    assert len(dim_staff_df.index) == 20

    for i in range(1, 21):
        assert any(dim_staff_df["staff_id"].isin([i]).values)

    s3_with_bucket.upload_file(
        Bucket="test-bucket",
        Filename="test/test_data/staff/2024-11-21 09_38_15.221234.json",
        Key="staff/2024-11-21 09_38_15.221234.json",
    )
    s3_with_bucket.upload_file(
        Bucket="test-bucket",
        Filename="test/test_data/department/2024-11-21 09_38_15.221234.json",
        Key="department/2024-11-21 09_38_15.221234.json",
    )
    current_check_time = "2024-11-21 09_38_15.221234"

    dim_staff_df_2 = process_staff_updates(
        s3_with_bucket, "test-bucket", current_check_time
    )

    assert dim_staff_df_2.columns.tolist() == [
        "staff_id",
        "first_name",
        "last_name",
        "department_name",
        "location",
        "email_address",
    ]

    staff_id_1_df_2 = dim_staff_df_2[dim_staff_df_2["staff_id"] == 1]
    assert staff_id_1_df_2.loc[staff_id_1_df_2.index[0], "last_name"] == "Franey-Pitts"
    assert (
        staff_id_1_df_2.loc[staff_id_1_df_2.index[0], "department_name"] == "Purchasing"
    )
    assert staff_id_1_df_2.loc[staff_id_1_df_2.index[0], "location"] == "Manchester"
    assert (
        staff_id_1_df_2.loc[staff_id_1_df_2.index[0], "email_address"]
        == "jeremie.franeypitts@terrifictotes.com"
    )

    staff_id_16_df_2 = dim_staff_df_2[dim_staff_df_2["staff_id"] == 16]
    assert staff_id_16_df_2.loc[staff_id_16_df_2.index[0], "first_name"] == "Jessica"
    assert (
        staff_id_16_df_2.loc[staff_id_16_df_2.index[0], "department_name"]
        == "Facilities"
    )
    assert staff_id_16_df_2.loc[staff_id_16_df_2.index[0], "location"] == "Liverpool"
    assert (
        staff_id_16_df_2.loc[staff_id_16_df_2.index[0], "email_address"]
        == "jessica.parisian@terrifictotes.com"
    )

    assert len(dim_staff_df_2.index) == 2


def test_process_sales_order_updates_returns_expected_dataframe(s3_with_bucket):
    s3_with_bucket.upload_file(
        Bucket="test-bucket",
        Filename="test/test_data/sales_order/2024-11-21 13_32_38.364280.json",
        Key="sales_order/2024-11-21 13_32_38.364280.json",
    )
    current_check_time = "2024-11-21 13_32_38.364280"

    fact_sales_order_df = process_sales_order_updates(
        s3_with_bucket, "test-bucket", current_check_time
    )

    assert fact_sales_order_df.columns.tolist() == [
        "sales_order_id",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time",
        "sales_staff_id",
        "counterparty_id",
        "units_sold",
        "unit_price",
        "currency_id",
        "design_id",
        "agreed_payment_date",
        "agreed_delivery_date",
        "agreed_delivery_location_id",
    ]

    sales_order_id_11283_df = fact_sales_order_df[
        fact_sales_order_df["sales_order_id"] == 11283
    ]
    assert (
        sales_order_id_11283_df.loc[sales_order_id_11283_df.index[0], "design_id"] == 8
    )

    sales_order_id_11283_df = fact_sales_order_df[
        fact_sales_order_df["sales_order_id"] == 11283
    ]
    assert (
        sales_order_id_11283_df.loc[sales_order_id_11283_df.index[0], "sales_staff_id"]
        == 1
    )

    sales_order_id_11283_df = fact_sales_order_df[
        fact_sales_order_df["sales_order_id"] == 11283
    ]
    assert (
        sales_order_id_11283_df.loc[sales_order_id_11283_df.index[0], "counterparty_id"]
        == 5
    )

    sales_order_id_11283_df = fact_sales_order_df[
        fact_sales_order_df["sales_order_id"] == 11283
    ]
    assert (
        sales_order_id_11283_df.loc[sales_order_id_11283_df.index[0], "created_date"]
        == "2024-11-21"
    )

    sales_order_id_11283_df = fact_sales_order_df[
        fact_sales_order_df["sales_order_id"] == 11283
    ]
    assert (
        sales_order_id_11283_df.loc[sales_order_id_11283_df.index[0], "unit_price"]
        == 2.29
    )

    sales_order_id_11284_df = fact_sales_order_df[
        fact_sales_order_df["sales_order_id"] == 11284
    ]
    assert (
        sales_order_id_11284_df.loc[sales_order_id_11284_df.index[0], "units_sold"]
        == 17909
    )

    sales_order_id_11284_df = fact_sales_order_df[
        fact_sales_order_df["sales_order_id"] == 11284
    ]
    assert (
        sales_order_id_11284_df.loc[sales_order_id_11284_df.index[0], "currency_id"]
        == 2
    )

    sales_order_id_11284_df = fact_sales_order_df[
        fact_sales_order_df["sales_order_id"] == 11284
    ]
    assert (
        sales_order_id_11284_df.loc[sales_order_id_11284_df.index[0], "created_time"]
        == "13:31:10.326000"
    )

    sales_order_id_11284_df = fact_sales_order_df[
        fact_sales_order_df["sales_order_id"] == 11284
    ]
    assert (
        sales_order_id_11284_df.loc[
            sales_order_id_11284_df.index[0], "last_updated_date"
        ]
        == "2024-11-21"
    )

    sales_order_id_11284_df = fact_sales_order_df[
        fact_sales_order_df["sales_order_id"] == 11284
    ]
    assert (
        sales_order_id_11284_df.loc[sales_order_id_11284_df.index[0], "unit_price"]
        == "2.03"
    )

    sales_order_id_11285_df = fact_sales_order_df[
        fact_sales_order_df["sales_order_id"] == 11285
    ]
    assert (
        sales_order_id_11285_df.loc[
            sales_order_id_11285_df.index[0], "agreed_delivery_date"
        ]
        == "2024-11-23"
    )

    sales_order_id_11285_df = fact_sales_order_df[
        fact_sales_order_df["sales_order_id"] == 11285
    ]
    assert (
        sales_order_id_11285_df.loc[
            sales_order_id_11285_df.index[0], "agreed_payment_date"
        ]
        == "2024-11-23"
    )

    sales_order_id_11285_df = fact_sales_order_df[
        fact_sales_order_df["sales_order_id"] == 11285
    ]
    assert (
        sales_order_id_11285_df.loc[
            sales_order_id_11285_df.index[0], "agreed_delivery_location_id"
        ]
        == 24
    )

    sales_order_id_11285_df = fact_sales_order_df[
        fact_sales_order_df["sales_order_id"] == 11285
    ]
    assert (
        sales_order_id_11285_df.loc[
            sales_order_id_11285_df.index[0], "agreed_delivery_date"
        ]
        == "2024-11-23"
    )

    sales_order_id_11285_df = fact_sales_order_df[
        fact_sales_order_df["sales_order_id"] == 11285
    ]
    assert (
        sales_order_id_11285_df.loc[
            sales_order_id_11285_df.index[0], "last_updated_time"
        ]
        == "13:32:09.809000"
    )

    sales_order_id_11285_df = fact_sales_order_df[
        fact_sales_order_df["sales_order_id"] == 11285
    ]
    assert (
        sales_order_id_11285_df.loc[sales_order_id_11285_df.index[0], "unit_price"]
        == 2.42
    )

    assert len(fact_sales_order_df.index) == 3
