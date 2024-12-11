import json
import datetime
import boto3
import os
from pg8000.native import Connection, identifier
import logging


logger = logging.getLogger("logger")
logger.setLevel(logging.INFO)


############################################
####                                    ####
####     UTILITY FUNCTIONS: SECRETS     ####
####                                    ####
############################################


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


def retrieve_secret(sm_client, secret_id):
    logger.info(f"retrieving secret {secret_id}")
    secret_json = sm_client.get_secret_value(SecretId=secret_id)["SecretString"]
    secret_value = json.loads(secret_json)
    return secret_value


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


#############################################
####                                     ####
####     UTILITY FUNCTIONS: DATABASE     ####
####                                     ####
#############################################


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


####################################
####                            ####
####     UTILITY FUNCTIONS:     ####
####        JSON AND S3         ####
####                            ####
####################################


def zip_dictionary(new_rows, columns):
    zipped_dict = [dict(zip(columns, row)) for row in new_rows]

    return zipped_dict


def format_to_json(list_of_dicts):
    formatted_data = json.dumps(list_of_dicts, default=str)
    return formatted_data


def json_to_s3(client, json_string, bucket_name, folder, file_name):

    with open(f"/tmp/{file_name}", "w", encoding="UTF-8") as file:
        file.write(json_string)

    client.upload_file(f"/tmp/{file_name}", bucket_name, f"{folder}/{file_name}")

    os.remove(f"/tmp/{file_name}")


#######################
####               ####
####     LOGIC     ####
####               ####
#######################


def fetch_and_update_last_update_time(sm_client, s3_bucket_name):
    last_update_secret_id = f"df2-ttotes/last-update-{s3_bucket_name}"

    sm_response = sm_client.list_secrets(MaxResults=99, IncludePlannedDeletion=False)
    secrets_list = sm_response["SecretList"]
    secret_names = [secret["Name"] for secret in secrets_list]

    if last_update_secret_id not in secret_names:
        last_update = (datetime.datetime(2020, 1, 1, 00, 00, 00, 000000)).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )
        current_update = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        store_secret(sm_client, last_update_secret_id, ["last_update", current_update])
    else:
        last_update_secret = retrieve_secret(sm_client, last_update_secret_id)
        last_update = last_update_secret["last_update"]
        current_update = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        update_secret(sm_client, last_update_secret_id, ["last_update", current_update])

    return {"last_update": last_update, "current_update": current_update}


def ingest_latest_rows(s3_client, s3_bucket_name, last_update, current_update):
    db = connect_to_db()
    data = get_data(db, last_update)
    close_connection(db)

    output = {"HasNewRows": {}, "LastCheckedTime": current_update}

    for table in data:
        rows = data[table][0]
        columns = data[table][1]

        if rows:
            output["HasNewRows"][table] = True

            logger.info("zipping table {table} to dictionary")
            zipped_dict = zip_dictionary(rows, columns)

            json_data = format_to_json(zipped_dict)
            dir_name = table
            file_name = f"{current_update}.json"

            logger.info(f"saving table {table} to file")
            json_to_s3(s3_client, json_data, s3_bucket_name, dir_name, file_name)
        else:
            output["HasNewRows"][table] = False

    logger.info(output)
    return output


###################################
####                           ####
####      LAMBDA  HANDLER      ####
####                           ####
###################################


def ingestion_lambda_handler(event, context):
    try:
        BUCKET_NAME = os.environ["INGESTION_BUCKET_NAME"]

        sm_client = boto3.client("secretsmanager")
        updates = fetch_and_update_last_update_time(sm_client, BUCKET_NAME)

        s3_client = boto3.client("s3")
        output = ingest_latest_rows(
            s3_client, BUCKET_NAME, updates["last_update"], updates["current_update"]
        )

        logger.info(output)
        return output

    except Exception as e:
        logger.error({"Error found": e})
        return {"Error found": e}


if __name__ == "__main__":
    ingestion_lambda_handler({}, {})
