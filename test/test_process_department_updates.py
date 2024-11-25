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
def s3_bucket(s3_client):
    s3_client.create_bucket(
        Bucket="test_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    yield s3_client

def test_process_department_updates_updates_staff_df(s3_bucket):
    s3_bucket.upload_file(
        Bucket="test_bucket",
        Filename="test/test_data/staff/2024-11-20 15_22_10.531518.json",
        Key="staff/2024-11-20 15_22_10.531518.json",
    )
    s3_bucket.upload_file(
        Bucket="test_bucket",
        Filename="test/test_data/staff/2024-11-21 09_38_15.221234.json",
        Key="staff/2024-11-21 09_38_15.221234.json",
    )
    s3_bucket.upload_file(
        Bucket="test_bucket",
        Filename="test/test_data/department/2024-11-20 15_22_10.531518.json",
        Key="department/2024-11-20 15_22_10.531518.json",
    )
    s3_bucket.upload_file(
        Bucket="test_bucket",
        Filename="test/test_data/department/2024-11-21 09_38_15.221234.json",
        Key="department/2024-11-21 09_38_15.221234.json",
    )
    last_checked_time = '2024-11-21 09_38_15.221234'

    #Simulates previous generation of staff df
    file_name = f'staff/{last_checked_time}.json'
    json_string = s3_client.get_object(
        Bucket='test_bucket', 
        Key=file_name
        )['Body'].read().decode('utf-8')
    staff_df = pd.DataFrame.from_dict(json.loads(json_string))

    department_ids_to_fetch = staff_df['department_id'].tolist()
    departments_df = fetch_latest_row_versions(
        s3_client, 'test_bucket',
        'department', department_ids_to_fetch)
    dim_staff_df = pd.merge(
        staff_df, departments_df,
        how='left', on='department_id')
    dim_staff_df = dim_staff_df.drop(columns=[
        'department_id', 
        'created_at_x', 
        'last_updated_x', 
        'manager', 
        'created_at_y', 
        'last_updated_y'])
    dim_staff_df = dim_staff_df[[
        "staff_id",
        "first_name",
        "last_name",
        "department_name",
        "location",
        "email_address"
    ]]
    dim_staff_df['location'] = dim_staff_df['location'].fillna("Undefined")

    # Begin testing departments update function
    test_ouput = process_department_updates(s3_bucket, 'test_bucket', last_checked_time, dim_staff_df)

    # already updated staff ids: 1, 16
    # updated department data changes dep id 6
    # staff ids, 2 and 3 have dep id 6
    


