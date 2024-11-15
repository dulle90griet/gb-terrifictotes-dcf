from src.secret_utils import store_secret, retrieve_secret
import pytest, boto3
from moto import mock_aws


@pytest.fixture
def sm_client():
    client = boto3.client('secretsmanager')
    yield client
    client.close()


@mock_aws
def test_secrets_are_stored_successfully(sm_client):
    store_secret(sm_client, "my_secret", [["user", "keen_green_bean_1"], ["password", "pa55word"]])
    list_secrets = sm_client.list_secrets()
    assert list_secrets['SecretList'][0]['Name'] == "my_secret"

    store_secret(sm_client, "second_secret", ["user", "keen_green_bean_1"])
    list_secrets = sm_client.list_secrets()
    assert list_secrets['SecretList'][1]['Name'] == "second_secret"

    store_secret(sm_client, "our_secret", [["user", "keen_green_bean_2"], ["password", "pa55word"]])
    store_secret(sm_client, "your_secret", [["user", "keen_green_bean_3"], ["password", "pa55word"]])

    list_secrets = sm_client.list_secrets()
    assert len(list_secrets['SecretList']) == 4
    secret_names = [secret['Name'] for secret in list_secrets['SecretList']]
    assert "our_secret" in secret_names
    assert "your_secret" in secret_names


@mock_aws
def test_secret_can_be_retrieved(sm_client):
    store_secret(sm_client, "my_secret", [["user", "keen_green_bean_1"], ["password", "pa55word"]])
    store_secret(sm_client, "our_secret", [["user", "keen_green_bean_2"], ["password", "pa55word"]])
    store_secret(sm_client, "your_secret", [["user", "keen_green_bean_3"], ["password", "pa55word"]])

    output = retrieve_secret(sm_client, "my_secret")
    assert output['user'] == "keen_green_bean_1"
    assert output['password'] == "pa55word"

    output = retrieve_secret(sm_client, "our_secret")
    assert output['user'] == "keen_green_bean_2"
    assert output['password'] == "pa55word"

    output = retrieve_secret(sm_client, "your_secret")
    assert output['user'] == "keen_green_bean_3"
    assert output['password'] == "pa55word"
