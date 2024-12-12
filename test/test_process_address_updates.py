from src.processing_lambda import process_address_updates
from src.utils.fetch_latest_row_versions import fetch_latest_row_versions
import pytest, os, boto3
import pandas as pd
from moto import mock_aws
import json


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


def test_process_address_updates_updates_counterparty_df_and_makes_location_df(s3_bucket):
    s3_bucket.upload_file(
        Bucket="test_bucket",
        Filename="test/test_data/counterparty/2024-11-20 15_22_10.531518.json",
        Key="counterparty/2024-11-20 15_22_10.531518.json",
    )
    s3_bucket.upload_file(
        Bucket="test_bucket",
        Filename="test/test_data/counterparty/2024-11-21 09_38_15.221234.json",
        Key="counterparty/2024-11-21 09_38_15.221234.json",
    )
    s3_bucket.upload_file(
        Bucket="test_bucket",
        Filename="test/test_data/address/2024-11-20 15_22_10.531518.json",
        Key="address/2024-11-20 15_22_10.531518.json",
    )
    s3_bucket.upload_file(
        Bucket="test_bucket",
        Filename="test/test_data/address/2024-11-21 09_38_15.221234.json",
        Key="address/2024-11-21 09_38_15.221234.json",
    )
    last_checked_time = "2024-11-21 09_38_15.221234"

    # Simulates previous generation of counterparty df
    file_name = f"counterparty/{last_checked_time}.json"
    json_string = (
        s3_bucket.get_object(Bucket="test_bucket", Key=file_name)["Body"]
        .read()
        .decode("utf-8")
    )
    counterparty_df = pd.DataFrame.from_dict(json.loads(json_string))

    address_ids_to_fetch = counterparty_df["legal_address_id"].tolist()
    addresses_df = fetch_latest_row_versions(
        s3_bucket, "test_bucket", "address", address_ids_to_fetch
    )
    dim_counterparty_df = pd.merge(
        counterparty_df,
        addresses_df,
        how="left",
        left_on="legal_address_id",
        right_on="address_id",
    )
    dim_counterparty_df = dim_counterparty_df.drop(
        columns=[
            "legal_address_id",
            "commercial_contact",
            "delivery_contact",
            "created_at_x",
            "last_updated_x",
            "address_id",
            "created_at_y",
            "last_updated_y",
        ]
    )
    dim_counterparty_df = dim_counterparty_df.rename(
        columns={
            "address_line_1": "counterparty_legal_address_line_1",
            "address_line_2": "counterparty_legal_address_line_2",
            "district": "counterparty_legal_district",
            "city": "counterparty_legal_city",
            "postal_code": "counterparty_legal_postal_code",
            "country": "counterparty_legal_country",
            "phone": "counterparty_legal_phone_number",
        }
    )

    # Begin testing address update function
    output = process_address_updates(
        s3_bucket, "test_bucket", last_checked_time, dim_counterparty_df
    )

    test_counterparty_df, test_location_df = output

    # already updated counterparty ids: 1, 4, 11
    # updated address data changes address ids 15, 17
    # counterparty_ids with address_id 15 are 1
    # counterparty_ids with address_id 17 are 11, 15, 17, 18
    # final df should have counterparty_ids 1, 4, 11, 15, 17, 18
    # counterparty_ids with address_id 17 NOT ALREADY UPDATED are 15, 17, 18

    counterparty_id_1_df = test_counterparty_df[
        test_counterparty_df["counterparty_id"] == 1
    ]
    assert (
        counterparty_id_1_df.loc[
            counterparty_id_1_df.index[0], "counterparty_legal_phone_number"
        ]
        == "2242 809035"
    )
    assert len(counterparty_id_1_df.index) == 1

    counterparty_id_15_df = test_counterparty_df[
        test_counterparty_df["counterparty_id"] == 15
    ]
    assert (
        counterparty_id_15_df.loc[
            counterparty_id_15_df.index[0], "counterparty_legal_address_line_1"
        ]
        == "27 Maisel Underpass"
    )
    assert len(counterparty_id_15_df.index) == 1

    counterparty_id_17_df = test_counterparty_df[
        test_counterparty_df["counterparty_id"] == 17
    ]
    assert (
        counterparty_id_17_df.loc[
            counterparty_id_17_df.index[0], "counterparty_legal_city"
        ]
        == "Blotsworth"
    )
    assert len(counterparty_id_17_df.index) == 1

    counterparty_id_18_df = test_counterparty_df[
        test_counterparty_df["counterparty_id"] == 18
    ]
    assert (
        counterparty_id_18_df.loc[
            counterparty_id_18_df.index[0], "counterparty_legal_postal_code"
        ]
        == "22567-7329"
    )
    assert len(counterparty_id_18_df.index) == 1

    assert any(test_counterparty_df["counterparty_id"].isin([4, 11]).values)

    assert len(test_counterparty_df.index) == 6

    # print(test_location_df)
    location_id_15_df = test_location_df[test_location_df["location_id"] == 15]
    assert location_id_15_df.loc[location_id_15_df.index[0], "phone"] == "2242 809035"
    assert len(location_id_15_df.index) == 1

    location_id_17_df = test_location_df[test_location_df["location_id"] == 17]
    assert (
        location_id_17_df.loc[location_id_17_df.index[0], "address_line_1"]
        == "27 Maisel Underpass"
    )
    assert len(location_id_17_df.index) == 1

    assert len(test_location_df.index) == 2


def test_process_address_updates_with_empty_counterparty_df(s3_bucket):
    s3_bucket.upload_file(
        Bucket="test_bucket",
        Filename="test/test_data/counterparty/2024-11-20 15_22_10.531518.json",
        Key="counterparty/2024-11-20 15_22_10.531518.json",
    )
    s3_bucket.upload_file(
        Bucket="test_bucket",
        Filename="test/test_data/address/2024-11-21 09_38_15.221234.json",
        Key="address/2024-11-21 09_38_15.221234.json",
    )
    last_checked_time = "2024-11-21 09_38_15.221234"


    # Begin testing address update function
    output = process_address_updates(
        s3_bucket, "test_bucket", last_checked_time
    )

    test_counterparty_df, test_location_df = output

    # updated address data changes address ids 15, 17
    # the counterparty_id with address_id 15 is 1
    # the counterparty_ids with address_id 17 are 11, 15, 17, 18

    counterparty_id_1_df = test_counterparty_df[
        test_counterparty_df["counterparty_id"] == 1
    ]
    assert (
        counterparty_id_1_df.loc[
            counterparty_id_1_df.index[0], "counterparty_legal_phone_number"
        ]
        == "2242 809035"
    )
    assert (
        counterparty_id_1_df.loc[
            counterparty_id_1_df.index[0], "counterparty_legal_city"
        ]
        == "East Bobbie"
    )
    assert len(counterparty_id_1_df.index) == 1

    counterparty_id_11_df = test_counterparty_df[
        test_counterparty_df["counterparty_id"] == 11
    ]
    assert (
        counterparty_id_11_df.loc[
            counterparty_id_11_df.index[0], "counterparty_legal_phone_number"
        ]
        == "5507 549583"
    )
    assert len(counterparty_id_11_df.index) == 1

    counterparty_id_15_df = test_counterparty_df[
        test_counterparty_df["counterparty_id"] == 15
    ]
    assert (
        counterparty_id_15_df.loc[
            counterparty_id_15_df.index[0], "counterparty_legal_address_line_1"
        ]
        == "27 Maisel Underpass"
    )
    assert len(counterparty_id_15_df.index) == 1

    counterparty_id_17_df = test_counterparty_df[
        test_counterparty_df["counterparty_id"] == 17
    ]
    assert (
        counterparty_id_17_df.loc[
            counterparty_id_17_df.index[0], "counterparty_legal_city"
        ]
        == "Blotsworth"
    )
    assert len(counterparty_id_17_df.index) == 1

    counterparty_id_18_df = test_counterparty_df[
        test_counterparty_df["counterparty_id"] == 18
    ]
    assert (
        counterparty_id_18_df.loc[
            counterparty_id_18_df.index[0], "counterparty_legal_postal_code"
        ]
        == "22567-7329"
    )
    assert len(counterparty_id_18_df.index) == 1

    assert len(test_counterparty_df.index) == 5

    # location df should contain just the updated rows for address_ids 15 and 17

    location_id_15_df = test_location_df[test_location_df["location_id"] == 15]
    assert location_id_15_df.loc[location_id_15_df.index[0], "phone"] == "2242 809035"
    assert len(location_id_15_df.index) == 1

    location_id_17_df = test_location_df[test_location_df["location_id"] == 17]
    assert (
        location_id_17_df.loc[location_id_17_df.index[0], "address_line_1"]
        == "27 Maisel Underpass"
    )
    assert len(location_id_17_df.index) == 1

    assert len(test_location_df.index) == 2
