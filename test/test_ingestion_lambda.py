from src.ingestion_lambda import *
from unittest import TestCase
from unittest.mock import Mock, patch
import datetime
import pytest
from moto import mock_aws
import os


@pytest.fixture
def conn_fixture():
    conn = connect_to_db()
    yield conn


@pytest.fixture
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


def test_gets_data_returns_updated_rows_from_each_table(conn_fixture):
    current_time = datetime.datetime.now()
    tables = [
        "counterparty",
        "currency",
        "department",
        "design",
        "staff",
        "sales_order",
        "address",
        "payment",
        "purchase_order",
        "payment_type",
        "transaction",
    ]
    date_1 = (datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)).strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )
    date_2 = (datetime.datetime(2024, 11, 3, 14, 20, 51, 563000)).strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )
    date_3 = (current_time + datetime.timedelta(weeks=104)).strftime(
        "%Y-%m-%d %H:%M:%S.%f"
    )
    dates = [date_1, date_2, date_3]

    results = []
    for i in range(len(dates)):
        output = get_data(conn_fixture, dates[i])

        for table in tables:
            formatted_table_data = [
                dict(zip(output[table][1], row)) for row in output[table][0]
            ]
            output[table] = formatted_table_data

        results.append(output)

    for i in range(len(results)):
        for table in results[i]:
            for row in table:
                if "last_updated" in row:
                    assert row["last_updated"] >= dates[i]

    close_connection(conn_fixture)


def test_get_data_ouputs_the_correct_data_intact():
    mocked_connection = Mock()
    dummy_counterparty_table = [
        [
            1,
            "Fahey and Sons",
            15,
            "Micheal Toy",
            "Mrs. Lucy Runolfsdottir",
            datetime.datetime(2022, 11, 3, 14, 20, 51, 563000),
            datetime.datetime(2022, 11, 3, 14, 20, 51, 563000),
        ],
        [
            2,
            "Dummy LPR",
            28,
            "Melba Sanford",
            "Jean Hane III",
            datetime.datetime(2024, 11, 3, 14, 20, 51, 563000),
            datetime.datetime(2024, 11, 3, 14, 20, 51, 563000),
        ],
        [
            3,
            "Armstrong Inc",
            2,
            "Jane Wiza",
            "Myra Kovacek",
            datetime.datetime(2022, 11, 3, 14, 20, 51, 563000),
            datetime.datetime(2022, 11, 3, 14, 20, 51, 563000),
        ],
    ]
    mocked_connection.run.return_value = dummy_counterparty_table
    mocked_connection.columns = [{"name": "fake_column_1"}, {"name": "fake_column_2"}]
    test_datetime = datetime.datetime(2023, 11, 3, 14, 20, 51, 563000)
    mocked_output = get_data(mocked_connection, test_datetime)
    tables = [
        "counterparty",
        "currency",
        "department",
        "design",
        "staff",
        "sales_order",
        "address",
        "payment",
        "purchase_order",
        "payment_type",
        "transaction",
    ]
    for table in tables:
        assert mocked_output[table] == (
            dummy_counterparty_table,
            ["fake_column_1", "fake_column_2"],
        )


def test_ingestion_lambda_handler_logs_errors():
    with TestCase.assertLogs("logger", level="ERROR") as log:
        # Check whether ingestion bucket env variable exists
        # so that we can restore it later
        # (this is so that we are able to keep it set locally
        #  for the purposes of testing other parts of our code)
        try:
            ingestion_env_keeper = os.environ["INGESTION_BUCKET_NAME"]
            ingestion_env_var_existed = True

            # Ditch the env var so the error will be raised
            os.environ.pop("INGESTION_BUCKET_NAME")
        except:
            ingestion_env_var_existed = False

        ingestion_lambda_handler({}, {})

        if ingestion_env_var_existed:
            os.environ["INGESTION_BUCKET_NAME"] = ingestion_env_keeper

        assert (
            log.output[0]
            == "ERROR:logger:{'Error found': KeyError('INGESTION_BUCKET_NAME')}"
        )
