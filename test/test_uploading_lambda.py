from src.uploading_lambda import *
import pg8000.native
from moto import mock_aws
import boto3
from src.processing_lambda import df_to_parquet_in_s3
import ssl


def connect_to_db():
    sm_client = boto3.client("secretsmanager", "eu-west-2")
    credentials = retrieve_secret(sm_client, "df2-ttotes/test-db-credentials")
    ssl_context = ssl.SSLContext()

    return pg8000.connect(
        user=credentials["TEST_USER"],
        password=credentials["TEST_PASSWORD"],
        database=credentials["TEST_DB"],
        host=credentials["TEST_HOST"],
        port=credentials["TEST_PORT"],
        ssl_context=ssl_context,
    )


@mock_aws
def test_util_read_parquet_from_s3():
    s3_client = boto3.client("s3")
    s3_client.create_bucket(
        Bucket="processing-test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    test_json = '[{"currency_id": 1, "currency_code": "GBP", "currency_name": "British pound sterling"}]'
    test_df = pd.read_json(test_json)
    test_folder = "test-folder"
    test_file_name = "test-file-name"
    df_to_parquet_in_s3(
        s3_client, test_df, "processing-test-bucket", test_folder, test_file_name
    )
    output = read_parquet_from_s3(
        s3_client, "processing-test-bucket", f"{test_folder}/{test_file_name}.parquet"
    )
    # print(output)
    assert isinstance(output, pd.DataFrame)


def test_util_insert_row_into_data_warehouse():

    test_dict = {
        "currency_id": [1],
        "currency_code": ["GBP"],
        "currency_name": ["British pound sterling"],
    }
    test_df = pd.DataFrame(test_dict)
    db = connect_to_db()
    db.run("DROP TABLE IF EXISTS dim_currency;")
    db.run(
        "CREATE TABLE IF NOT EXISTS dim_currency (currency_id INT PRIMARY KEY, currency_code VARCHAR, currency_name VARCHAR);"
    )
    insert_into_dw(test_df, db, "dim_currency")
    assert db.run("SELECT * FROM dim_currency;") == (
        [1, "GBP", "British pound sterling"],
    )
    db.close()


def test_util_insert_multiple_rows_into_data_warehouse():

    test_dict = {
        "currency_id": [1, 2],
        "currency_code": ["GBP", "USD"],
        "currency_name": ["British pound sterling", "US dollar"],
    }
    test_df = pd.DataFrame(test_dict)
    db = connect_to_db()
    db.run("DROP TABLE IF EXISTS dim_currency;")
    db.run(
        "CREATE TABLE IF NOT EXISTS dim_currency (currency_id INT PRIMARY KEY, currency_code VARCHAR, currency_name VARCHAR);"
    )
    insert_into_dw(test_df, db, "dim_currency")
    assert db.run("SELECT * FROM dim_currency;") == (
        [1, "GBP", "British pound sterling"],
        [2, "USD", "US dollar"],
    )
    db.close()


def test_util_updates_row_in_dim_table_if_exists():

    test_dict = {
        "currency_id": [1],
        "currency_code": ["GBP"],
        "currency_name": ["British pound sterling"],
    }
    test_df = pd.DataFrame(test_dict)
    db = connect_to_db()
    db.run("DROP TABLE IF EXISTS dim_currency;")
    db.run(
        "CREATE TABLE IF NOT EXISTS dim_currency (currency_id INT PRIMARY KEY, currency_code VARCHAR, currency_name VARCHAR);"
    )
    insert_into_dw(test_df, db, "dim_currency")
    assert db.run("SELECT * FROM dim_currency;") == (
        [1, "GBP", "British pound sterling"],
    )
    test_dict2 = {
        "currency_id": [1],
        "currency_code": ["USD"],
        "currency_name": ["US Dollar"],
    }
    test_df2 = pd.DataFrame(test_dict2)
    insert_into_dw(test_df2, db, "dim_currency")
    assert db.run("SELECT * FROM dim_currency;") == ([1, "USD", "US Dollar"],)
    db.close()


"""test below was updating the actual datawarehouse, DO NOT re-run otherwise will duplicate rows in fact_sales_order table"""
# def test_uploading_lambda_insert():

#     test_event = {
#   "HasNewRows": {
#     "dim_counterparty": True,
#     "dim_currency": True,
#     "dim_design": True,
#     "dim_location": True,
#     "dim_staff": True,
#     "fact_sales_order": True
#   },
#   "LastCheckedTime": "2024-11-20 15:22:10.531518"
# }
#     uploading_lambda_handler(test_event,{})
