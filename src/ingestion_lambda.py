import json
import datetime
import boto3
import os
from pg8000.native import Connection, identifier
import logging
from collections.abc import Mapping, Iterable
from decimal import Decimal, Context, MAX_PREC


logger = logging.getLogger("logger")
logger.setLevel(logging.INFO)


class CustomEncoder(json.JSONEncoder):
    def encode(self, obj):
        if isinstance(obj, Mapping):
            return (
                "{"
                + ", ".join(
                    f"{self.encode(k)}: {self.encode(v)}" for (k, v) in obj.items()
                )
                + "}"
            )
        if isinstance(obj, Iterable) and (not isinstance(obj, str)):
            return "[" + ", ".join(map(self.encode, obj)) + "]"
        if isinstance(obj, Decimal):
            return f"{obj.normalize():f}"
        if isinstance(obj, datetime.datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S.%f")
        return super().encode(obj)


logger = logging.getLogger("logger")
logger.setLevel(logging.INFO)


# RETRIEVE SECRET UTIL
def retrieve_secret(sm_client, secret_id):
    logger.info(f"retrieving secret {secret_id}")
    secret_json = sm_client.get_secret_value(SecretId=secret_id)["SecretString"]
    secret_value = json.loads(secret_json)
    return secret_value


# UPDATE SECRET
def update_secret(sm_client, secret_id, keys_and_values):
    logger.info(f"updating secret {secret_id}")
    if isinstance(keys_and_values[0], list):
        key_value_dict = {}
        for key_value_pair in keys_and_values:
            key_value_dict[key_value_pair[0]] = key_value_pair[1]
    else:
        key_value_dict = {keys_and_values[0]: keys_and_values[1]}

    secret_string = json.dumps(key_value_dict)
    response = sm_client.update_secret(SecretId=secret_id, SecretString=secret_string)
    return response


# GET DATA
def get_data(db, last_update):

    data = {}

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
        logger.info(f"querying table {table}, with last update {last_update}")
        query = db.run(
            f"SELECT * FROM {identifier(table)} WHERE last_updated >= :last_update;",
            last_update=last_update,
        )
        data[table] = (query, [col["name"] for col in db.columns])

    return data


# ZIP DICTIONARY
def zip_dictionary(new_rows, columns):
    zipped_dict = [dict(zip(columns, row)) for row in new_rows]

    return zipped_dict


# FORMAT TO JSON
def format_to_json(list_of_dicts):
    formatted_data = json.dumps(list_of_dicts, cls=CustomEncoder)
    return formatted_data


# JSON TO s3
def json_to_s3(client, json_string, bucket_name, folder, file_name):

    with open(f"/tmp/{file_name}", "w", encoding="UTF-8") as file:
        file.write(json_string)

    client.upload_file(f"/tmp/{file_name}", bucket_name, f"{folder}/{file_name}")

    os.remove(f"/tmp/{file_name}")


# CONNECTION
def connect_to_db():
    sm_client = boto3.client("secretsmanager", "eu-west-2")
    credentials = retrieve_secret(sm_client, "df2-ttotes/totesys-oltp-credentials")

    return Connection(
        user=credentials["PG_USER"],
        password=credentials["PG_PASSWORD"],
        database=credentials["PG_DATABASE"],
        host=credentials["PG_HOST"],
        port=credentials["PG_PORT"],
    )


def close_connection(conn):
    conn.close()


# STORE SECRET (create)
def store_secret(sm_client, secret_id, keys_and_values):

    if isinstance(keys_and_values[0], list):
        key_value_dict = {}
        for key_value_pair in keys_and_values:
            key_value_dict[key_value_pair[0]] = key_value_pair[1]
    else:
        key_value_dict = {keys_and_values[0]: keys_and_values[1]}

    secret_string = json.dumps(key_value_dict)
    response = sm_client.create_secret(Name=secret_id, SecretString=secret_string)
    return response


# LAMBDA HANDLER
def ingestion_lambda_handler(event, context):

    try:

        # to test for error logs in the console uncomment line below:
        # logger.error({"Error found": "Test error"})

        BUCKET_NAME = os.environ["INGESTION_BUCKET_NAME"]

        sm_client = boto3.client("secretsmanager")
        secret_request = sm_client.list_secrets(
            MaxResults=99, IncludePlannedDeletion=False
        )
        list_of_secrets = secret_request["SecretList"]
        secret_names = [secret["Name"] for secret in list_of_secrets]
        last_update_secret_id = f"df2-ttotes/last-update-{BUCKET_NAME}"

        if last_update_secret_id not in secret_names:
            date_and_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            last_update = (datetime.datetime(2020, 1, 1, 00, 00, 00, 000000)).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            )
            store_secret(
                sm_client, last_update_secret_id, ["last_update", date_and_time]
            )
        else:
            last_updated_secret = retrieve_secret(sm_client, last_update_secret_id)
            last_update = last_updated_secret["last_update"]
            date_and_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            update_secret(
                sm_client, last_update_secret_id, ["last_update", date_and_time]
            )

        db = connect_to_db()
        data = get_data(db, last_update)
        close_connection(db)

        s3_client = boto3.client("s3")

        output = {"HasNewRows": {}, "LastCheckedTime": date_and_time}

        for table in data:
            rows = data[table][0]
            columns = data[table][1]

            if len(rows) > 0:
                output["HasNewRows"][table] = True
                # new_rows = [datetime_to_strftime(row) for row in rows]
                new_rows = rows
                logger.info(f"zipping table {table} to dictionary")
                zipped_dict = zip_dictionary(new_rows, columns)
                json_data = format_to_json(zipped_dict)
                file_name = f"{date_and_time}.json"
                folder_name = table
                logger.info(f"saving table {table} to file")
                json_to_s3(s3_client, json_data, BUCKET_NAME, folder_name, file_name)
            else:
                output["HasNewRows"][table] = False

        logger.info(output)
        return output

    except Exception as e:

        logger.error({"Error found": e})
        return {"Error found": e}


if __name__ == "__main__":
    ingestion_lambda_handler({}, {})
