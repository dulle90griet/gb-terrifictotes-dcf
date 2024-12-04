from src.ingestion_lambda import store_secret, retrieve_secret, update_secret
import pytest, boto3, os
from moto import mock_aws
import json


@pytest.fixture(autouse=True, scope="class")
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture
def sm_client():
    client = boto3.client("secretsmanager")
    yield client
    client.close()


class TestSecretUtils:
    @mock_aws
    def test_secrets_are_stored_successfully(self, sm_client):
        store_secret(
            sm_client,
            "my_secret",
            [["user", "keen_green_bean_1"], ["password", "pa55word"]],
        )
        list_secrets = sm_client.list_secrets()
        assert list_secrets["SecretList"][0]["Name"] == "my_secret"

        store_secret(sm_client, "second_secret", ["user", "keen_green_bean_1"])
        list_secrets = sm_client.list_secrets()
        assert list_secrets["SecretList"][1]["Name"] == "second_secret"

        store_secret(
            sm_client,
            "our_secret",
            [["user", "keen_green_bean_2"], ["password", "pa55word"]],
        )
        store_secret(
            sm_client,
            "your_secret",
            [["user", "keen_green_bean_3"], ["password", "pa55word"]],
        )

        list_secrets = sm_client.list_secrets()
        assert len(list_secrets["SecretList"]) == 4
        secret_names = [secret["Name"] for secret in list_secrets["SecretList"]]
        assert "our_secret" in secret_names
        assert "your_secret" in secret_names

    @mock_aws
    def test_secret_can_be_retrieved(self, sm_client):
        store_secret(
            sm_client,
            "my_secret",
            [["user", "keen_green_bean_1"], ["password", "pa55word"]],
        )
        store_secret(
            sm_client,
            "our_secret",
            [["user", "keen_green_bean_2"], ["password", "pa55word"]],
        )
        store_secret(
            sm_client,
            "your_secret",
            [["user", "keen_green_bean_3"], ["password", "pa55word"]],
        )

        output = retrieve_secret(sm_client, "my_secret")
        assert output["user"] == "keen_green_bean_1"
        assert output["password"] == "pa55word"

        output = retrieve_secret(sm_client, "our_secret")
        assert output["user"] == "keen_green_bean_2"
        assert output["password"] == "pa55word"

        output = retrieve_secret(sm_client, "your_secret")
        assert output["user"] == "keen_green_bean_3"
        assert output["password"] == "pa55word"

    @mock_aws
    def test_update_secret_updates_keys_and_values_correctly(self, sm_client):
        store_secret(
            sm_client,
            "my_secret",
            [["user", "keen_green_bean_1"], ["password", "pa55word"]],
        )
        update_secret(
            sm_client,
            "my_secret",
            [["user", "keen_green_bean_2"], ["password", "pa55word2"]],
        )
        test_secret_json = sm_client.get_secret_value(SecretId="my_secret")[
            "SecretString"
        ]
        test_secret_value = json.loads(test_secret_json)

        assert test_secret_value["user"] == "keen_green_bean_2"
        assert test_secret_value["password"] == "pa55word2"
