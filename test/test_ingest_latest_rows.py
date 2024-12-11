from unittest.mock import patch, Mock
from decimal import Decimal
from datetime import datetime, timedelta
from moto import mock_aws
import pytest, os, boto3


from src.ingestion_lambda import ingest_latest_rows


##########################################
####                                  ####
####     FIXTURES FOR MOCKING AWS     ####
####                                  ####
##########################################


@pytest.fixture
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

    os.environ["INGESTION_BUCKET_NAME"] = "test-bucket"


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


###########################################
####                                   ####
####     FIXTURES FOR MOCKING DATA     ####
####                                   ####
###########################################


@pytest.fixture
def mock_empty_data():
    return {
        "counterparty": (
            [],
            [
                "counterparty_id",
                "counterparty_legal_name",
                "legal_address_id",
                "commercial_contact",
                "delivery_contact",
                "created_at",
                "last_updated",
            ],
        ),
        "currency": (
            [],
            ["currency_id", "currency_code", "created_at", "last_updated"],
        ),
        "department": (
            [],
            [
                "department_id",
                "department_name",
                "location",
                "manager",
                "created_at",
                "last_updated",
            ],
        ),
        "design": (
            [],
            [
                "design_id",
                "created_at",
                "design_name",
                "file_location",
                "file_name",
                "last_updated",
            ],
        ),
        "staff": (
            [],
            [
                "staff_id",
                "first_name",
                "last_name",
                "department_id",
                "email_address",
                "created_at",
                "last_updated",
            ],
        ),
        "sales_order": (
            [],
            [
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
            ],
        ),
        "address": (
            [],
            [
                "address_id",
                "address_line_1",
                "address_line_2",
                "district",
                "city",
                "postal_code",
                "country",
                "phone",
                "created_at",
                "last_updated",
            ],
        ),
        "payment": (
            [],
            [
                "payment_id",
                "created_at",
                "last_updated",
                "transaction_id",
                "counterparty_id",
                "payment_amount",
                "currency_id",
                "payment_type_id",
                "paid",
                "payment_date",
                "company_ac_number",
                "counterparty_ac_number",
            ],
        ),
        "purchase_order": (
            [],
            [
                "purchase_order_id",
                "created_at",
                "last_updated",
                "staff_id",
                "counterparty_id",
                "item_code",
                "item_quantity",
                "item_unit_price",
                "currency_id",
                "agreed_delivery_date",
                "agreed_payment_date",
                "agreed_delivery_location_id",
            ],
        ),
        "payment_type": (
            [],
            ["payment_type_id", "payment_type_name", "created_at", "last_updated"],
        ),
        "transaction": (
            [],
            [
                "transaction_id",
                "transaction_type",
                "sales_order_id",
                "purchase_order_id",
                "created_at",
                "last_updated",
            ],
        ),
    }


@pytest.fixture
def mock_sales_order_data():
    return {
        "sales_order": (
            [
                [
                    11599,
                    datetime(2024, 12, 11, 8, 5, 9, 817000),
                    datetime(2024, 12, 11, 8, 5, 9, 817000),
                    274,
                    12,
                    13,
                    52286,
                    Decimal("2.93"),
                    2,
                    "2024-12-16",
                    "2024-12-16",
                    7,
                ]
            ],
            [
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
            ],
        )
    }


@pytest.fixture
def mock_payment_data():
    return {
        "payment": (
            [
                [
                    16248,
                    datetime(2024, 12, 5, 8, 4, 10, 88000),
                    datetime(2024, 12, 11, 7, 55, 11, 303000),
                    16248,
                    17,
                    Decimal("2146.56"),
                    2,
                    3,
                    True,
                    "2024-12-11",
                    73507572,
                    35379467,
                ]
            ],
            [
                "payment_id",
                "created_at",
                "last_updated",
                "transaction_id",
                "counterparty_id",
                "payment_amount",
                "currency_id",
                "payment_type_id",
                "paid",
                "payment_date",
                "company_ac_number",
                "counterparty_ac_number",
            ],
        )
    }


@pytest.fixture
def connection_patcher():
    mock_conn = Mock()
    mock_conn.close.return_value = None
    return patch("src.ingestion_lambda.connect_to_db", return_value=mock_conn)


@pytest.fixture
def patch_data():
    def data_handler(mock_data):
        return patch("src.ingestion_lambda.get_data", return_value=mock_data)

    return data_handler


# @pytest.mark.skip
# def test_db_patching_test(connection_patcher, patch_data, mock_sales_order_data):
#     connection_patcher.start()
#     data_patcher = patch_data(mock_sales_order_data)
#     data_patcher.start()

#     result = db_patching_test("2024-12-10 18:37:18.000000")

#     connection_patcher.stop()
#     data_patcher.stop()

#     assert result == {
#         "sales_order": (
#             [
#                 [
#                     11599,
#                     datetime(2024, 12, 11, 8, 5, 9, 817000),
#                     datetime(2024, 12, 11, 8, 5, 9, 817000),
#                     274,
#                     12,
#                     13,
#                     52286,
#                     Decimal("2.93"),
#                     2,
#                     "2024-12-16",
#                     "2024-12-16",
#                     7,
#                 ]
#             ],
#             [
#                 "sales_order_id",
#                 "created_at",
#                 "last_updated",
#                 "design_id",
#                 "staff_id",
#                 "counterparty_id",
#                 "units_sold",
#                 "unit_price",
#                 "currency_id",
#                 "agreed_delivery_date",
#                 "agreed_payment_date",
#                 "agreed_delivery_location_id",
#             ],
#         )
#     }


def test_ingest_latest_returns_expected_dict_for_no_new_rows(
    s3_with_bucket, connection_patcher, patch_data, mock_empty_data
):
    last_update = "2024-12-11 16:51:37.123092"
    current_update = "2024-12-11 17:06:41.003418"

    connection_patcher.start()
    data_patcher = patch_data(mock_empty_data)
    data_patcher.start()

    output = ingest_latest_rows(
        s3_with_bucket, "test-bucket", last_update, current_update
    )

    connection_patcher.stop()
    data_patcher.stop()

    assert output == {
        "HasNewRows": {
            "counterparty": False,
            "currency": False,
            "department": False,
            "design": False,
            "staff": False,
            "sales_order": False,
            "address": False,
            "payment": False,
            "purchase_order": False,
            "payment_type": False,
            "transaction": False,
        },
        "LastCheckedTime": "2024-12-11 17:06:41.003418",
    }


def test_ingest_latest_returns_expected_dict_for_new_rows(
    s3_with_bucket,
    connection_patcher,
    patch_data,
    mock_empty_data,
    mock_sales_order_data,
    mock_payment_data,
):
    last_update = "2024-12-11 16:51:37.123092"
    current_update = "2024-12-11 17:06:41.003418"

    connection_patcher.start()

    mock_data = mock_empty_data
    mock_data["sales_order"] = mock_sales_order_data["sales_order"]
    data_patcher = patch_data(mock_data)
    data_patcher.start()

    output = ingest_latest_rows(
        s3_with_bucket, "test-bucket", last_update, current_update
    )

    data_patcher.stop()

    assert output == {
        "HasNewRows": {
            "counterparty": False,
            "currency": False,
            "department": False,
            "design": False,
            "staff": False,
            "sales_order": True,
            "address": False,
            "payment": False,
            "purchase_order": False,
            "payment_type": False,
            "transaction": False,
        },
        "LastCheckedTime": "2024-12-11 17:06:41.003418",
    }

    mock_data["payment"] = mock_payment_data["payment"]
    data_patcher = patch_data(mock_data)
    data_patcher.start()

    output = ingest_latest_rows(
        s3_with_bucket, "test-bucket", last_update, current_update
    )

    data_patcher.stop()

    assert output == {
        "HasNewRows": {
            "counterparty": False,
            "currency": False,
            "department": False,
            "design": False,
            "staff": False,
            "sales_order": True,
            "address": False,
            "payment": True,
            "purchase_order": False,
            "payment_type": False,
            "transaction": False,
        },
        "LastCheckedTime": "2024-12-11 17:06:41.003418",
    }
