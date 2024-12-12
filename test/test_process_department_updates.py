from src.processing_lambda import process_department_updates
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
def s3_with_bucket(s3_client):
    s3_client.create_bucket(
        Bucket="test_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    yield s3_client


def test_process_department_updates_updates_staff_df(s3_with_bucket):
    s3_with_bucket.upload_file(
        Bucket="test_bucket",
        Filename="test/test_data/staff/2024-11-20 15_22_10.531518.json",
        Key="staff/2024-11-20 15_22_10.531518.json",
    )
    s3_with_bucket.upload_file(
        Bucket="test_bucket",
        Filename="test/test_data/staff/2024-11-21 09_38_15.221234.json",
        Key="staff/2024-11-21 09_38_15.221234.json",
    )
    s3_with_bucket.upload_file(
        Bucket="test_bucket",
        Filename="test/test_data/department/2024-11-20 15_22_10.531518.json",
        Key="department/2024-11-20 15_22_10.531518.json",
    )
    s3_with_bucket.upload_file(
        Bucket="test_bucket",
        Filename="test/test_data/department/2024-11-21 09_38_15.221234.json",
        Key="department/2024-11-21 09_38_15.221234.json",
    )
    last_checked_time = "2024-11-21 09_38_15.221234"

    # Simulates previous generation of staff df
    file_name = f"staff/{last_checked_time}.json"
    json_string = (
        s3_with_bucket.get_object(Bucket="test_bucket", Key=file_name)["Body"]
        .read()
        .decode("utf-8")
    )
    staff_df = pd.DataFrame.from_dict(json.loads(json_string))

    department_ids_to_fetch = staff_df["department_id"].tolist()
    department_df = fetch_latest_row_versions(
        s3_with_bucket, "test_bucket", "department", department_ids_to_fetch
    )
    dim_staff_df = pd.merge(staff_df, department_df, how="left", on="department_id")
    dim_staff_df = dim_staff_df.drop(
        columns=[
            "department_id",
            "created_at_x",
            "last_updated_x",
            "manager",
            "created_at_y",
            "last_updated_y",
        ]
    )
    dim_staff_df = dim_staff_df[
        [
            "staff_id",
            "first_name",
            "last_name",
            "department_name",
            "location",
            "email_address",
        ]
    ]
    dim_staff_df["location"] = dim_staff_df["location"].fillna("Undefined")

    # Begin testing departments update function
    test_output_df = process_department_updates(
        s3_with_bucket, "test_bucket", last_checked_time, dim_staff_df
    )

    # already updated staff ids: 1, 16
    # 2024-11-21 09:38:15 packet updates only dept id 6
    # staff ids 2, 3 and 16 have dept id 6
    staff_id_2_df = test_output_df[test_output_df["staff_id"] == 2]
    assert staff_id_2_df.loc[staff_id_2_df.index[0], "location"] == "Liverpool"
    assert len(staff_id_2_df.index) == 1

    staff_id_3_df = test_output_df[test_output_df["staff_id"] == 3]
    assert staff_id_3_df.loc[staff_id_3_df.index[0], "location"] == "Liverpool"
    assert len(staff_id_3_df.index) == 1

    staff_id_16_df = test_output_df[test_output_df["staff_id"] == 16]
    assert staff_id_16_df.loc[staff_id_16_df.index[0], "location"] == "Liverpool"
    assert len(staff_id_16_df.index) == 1

    assert any(test_output_df["staff_id"].isin([1]).values)

    assert len(test_output_df.index) == 4


def test_process_department_updates_with_empty_staff_df(s3_with_bucket):
    s3_with_bucket.upload_file(
        Bucket="test_bucket",
        Filename="test/test_data/staff/2024-11-20 15_22_10.531518.json",
        Key="staff/2024-11-20 15_22_10.531518.json",
    )
    s3_with_bucket.upload_file(
        Bucket="test_bucket",
        Filename="test/test_data/department/2024-11-21 09_38_15.221234.json",
        Key="department/2024-11-21 09_38_15.221234.json",
    )
    last_checked_time = "2024-11-21 09_38_15.221234"

    test_output_df = process_department_updates(
        s3_with_bucket, "test_bucket", last_checked_time
    )

    # 2024-11-21 09:38:15 packet updates only dept id 6
    # staff ids 2, 3 and 16 have dept id 6
    staff_id_2_df = test_output_df[test_output_df["staff_id"] == 2]
    assert staff_id_2_df.loc[staff_id_2_df.index[0], "first_name"] == "Deron"
    assert staff_id_2_df.loc[staff_id_2_df.index[0], "department_name"] == "Facilities"
    assert staff_id_2_df.loc[staff_id_2_df.index[0], "location"] == "Liverpool"
    assert len(staff_id_2_df.index) == 1

    staff_id_3_df = test_output_df[test_output_df["staff_id"] == 3]
    assert staff_id_3_df.loc[staff_id_3_df.index[0], "last_name"] == "Erdman"
    assert staff_id_3_df.loc[staff_id_3_df.index[0], "department_name"] == "Facilities"
    assert staff_id_3_df.loc[staff_id_3_df.index[0], "location"] == "Liverpool"
    assert len(staff_id_3_df.index) == 1

    staff_id_16_df = test_output_df[test_output_df["staff_id"] == 16]
    assert (
        staff_id_16_df.loc[staff_id_16_df.index[0], "email_address"]
        == "jett.parisian@terrifictotes.com"
    )
    assert (
        staff_id_16_df.loc[staff_id_16_df.index[0], "department_name"] == "Facilities"
    )
    assert staff_id_16_df.loc[staff_id_16_df.index[0], "location"] == "Liverpool"
    assert len(staff_id_16_df.index) == 1

    assert len(test_output_df.index) == 3
