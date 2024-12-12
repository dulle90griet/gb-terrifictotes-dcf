import logging, os, json
import boto3
import pandas as pd
from iso4217 import Currency


logger = logging.getLogger("logger")
logger.setLevel(logging.INFO)


###################################
####                           ####
####     UTILITY FUNCTIONS     ####
####                           ####
###################################


def get_currency_name(currency_code):
    return Currency(currency_code).currency_name


def fetch_latest_row_versions(s3_client, bucket_name, table_name, list_of_ids):
    id_col_name = f"{table_name}_id"

    # eliminating repeats from list of ids
    set_of_ids = set(list_of_ids)
    list_of_ids = list(set_of_ids)

    # look in folder of S3 bucket
    file_list = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{table_name}/")[
        "Contents"
    ]

    latest_row_dicts = []

    for i in range(len(file_list), 0, -1):
        cur_filename = file_list[i - 1]["Key"]

        json_object = s3_client.get_object(Bucket=bucket_name, Key=cur_filename)
        json_string = json_object["Body"].read().decode("utf-8")

        row_dicts = json.loads(json_string)
        for j in range(len(row_dicts), 0, -1):
            if row_dicts[j - 1][id_col_name] in list_of_ids:
                latest_row_dicts.append(row_dicts[j - 1])
                list_of_ids.remove(row_dicts[j - 1][id_col_name])

    return pd.DataFrame(latest_row_dicts)


def make_already_updated_list(s3_client, bucket_name, table_name, last_checked_time):
    json_object = s3_client.get_object(
        Bucket=bucket_name, Key=f"{table_name}/{last_checked_time}.json"
    )
    json_string = json_object["Body"].read().decode("utf-8")

    updated_rows = json.loads(json_string)

    return [row[f"{table_name}_id"] for row in updated_rows]


def df_to_parquet_in_s3(client, df, bucket_name, folder, file_name):
    if not os.path.exists("/tmp"):
        os.mkdir("/tmp")
    df.to_parquet(f"/tmp/{file_name}.parquet")

    client.upload_file(
        f"/tmp/{file_name}.parquet", bucket_name, f"{folder}/{file_name}.parquet"
    )
    logger.info(f"{folder}/{file_name}.parquet uploaded to processing")

    os.remove(f"/tmp/{file_name}.parquet")


#######################
####               ####
####     LOGIC     ####
####               ####
#######################


def process_counterparty_updates(s3_client, bucket_name, current_checK_time):
    logger.info("Processing new rows for table 'counterparty'.")

    file_name = f"counterparty/{current_checK_time}.json"
    json_object = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    json_string = json_object["Body"].read().decode("utf-8")
    counterparty_df = pd.DataFrame.from_dict(json.loads(json_string))

    address_ids_to_fetch = counterparty_df["legal_address_id"].tolist()
    addresses_df = fetch_latest_row_versions(
        s3_client, bucket_name, "address", address_ids_to_fetch
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

    logger.info(
        "dim_counterparty_df DataFrame created with "
        + f"{len(dim_counterparty_df.index)} rows."
    )

    return dim_counterparty_df


def process_department_updates(
    s3_client, bucket_name, last_checked_time, dim_staff_df=None
):
    # Fetch updated department table rows
    file_name = f"department/{last_checked_time}.json"
    json_string = (
        s3_client.get_object(Bucket=bucket_name, Key=file_name)["Body"]
        .read()
        .decode("utf-8")
    )
    department_df = pd.DataFrame.from_dict(json.loads(json_string))
    updated_department_ids = department_df["department_id"].tolist()

    if dim_staff_df is None:
        dim_staff_df = pd.DataFrame()

    # Establish staff table rows to be updated
    try:
        already_updated_list = dim_staff_df["staff_id"].tolist()
    except KeyError:
        already_updated_list = []

    file_list = s3_client.list_objects(Bucket=bucket_name, Prefix="staff/")["Contents"]
    new_row_count = 0

    # Iterate over files in the staff/ dir from newest to oldest
    for i in range(len(file_list) - 1, -1, -1):
        cur_filename = file_list[i]["Key"]

        json_object = s3_client.get_object(Bucket=bucket_name, Key=cur_filename)
        json_string = json_object["Body"].read().decode("utf-8")

        working_df = pd.DataFrame.from_dict(json.loads(json_string))

        # Iterate over rows in the current file from newest to oldest
        for j in range(len(working_df.index) - 1, -1, -1):
            # Find and update all staff rows that reference an updated department row
            # and aren't already present in dim_staff_df
            if working_df.loc[j, "staff_id"] not in already_updated_list:
                if working_df.loc[j, "department_id"] in updated_department_ids:
                    current_row = working_df.loc[[j]]
                    current_row = current_row.merge(
                        department_df, left_on="department_id", right_on="department_id"
                    )
                    current_row = current_row.drop(
                        columns=[
                            "department_id",
                            "created_at_x",
                            "last_updated_x",
                            "manager",
                            "created_at_y",
                            "last_updated_y",
                        ]
                    )
                    current_row = current_row[
                        [
                            "staff_id",
                            "first_name",
                            "last_name",
                            "department_name",
                            "location",
                            "email_address",
                        ]
                    ]
                    current_row["location"] = current_row["location"].fillna(
                        "Undefined"
                    )
                    dim_staff_df = pd.concat(
                        [dim_staff_df, current_row], ignore_index=True
                    )
                    already_updated_list.append(working_df.loc[j, "staff_id"])
                    new_row_count += 1

    logger.info(
        f"Added {new_row_count} rows with updated department info to dim_staff_df."
    )

    return dim_staff_df


def process_currency_updates(s3_client, bucket_name, current_check_time):
    logger.info("Processing new rows for table 'currency'.")
    file_name = f"currency/{current_check_time}.json"
    json_string = (
        s3_client.get_object(Bucket=bucket_name, Key=file_name)["Body"]
        .read()
        .decode("utf-8")
    )
    currency_df = pd.DataFrame.from_dict(json.loads(json_string))
    dim_currency_df = currency_df.drop(columns=["last_updated", "created_at"])

    dim_currency_df["currency_name"] = dim_currency_df["currency_code"].apply(
        lambda x: Currency(x).currency_name
    )

    logger.info(
        "dim_currency_df DataFrame created with "
        + f"{len(dim_currency_df.index)} rows."
    )

    return dim_currency_df


def process_address_updates(
    s3_client, bucket_name, last_checked_time, dim_counterparty_df=None
):
    # Fetch updated address table rows
    file_name = f"address/{last_checked_time}.json"
    json_string = (
        s3_client.get_object(Bucket=bucket_name, Key=file_name)["Body"]
        .read()
        .decode("utf-8")
    )
    address_df = pd.DataFrame.from_dict(json.loads(json_string))
    updated_address_ids = address_df["address_id"].tolist()

    # Create dim_location table
    dim_location_df = address_df.drop(columns=["created_at", "last_updated"])
    dim_location_df = dim_location_df.rename(columns={"address_id": "location_id"})

    logger.info(
        "dim_location_df DataFrame created with "
        + f"{len(dim_location_df.index)} rows."
    )

    if dim_counterparty_df is None:
        dim_counterparty_df = pd.DataFrame()

    # Establish counterparty table rows to be updated
    try:
        already_updated_list = dim_counterparty_df["counterparty_id"].tolist()
    except KeyError:
        already_updated_list = []

    file_list = s3_client.list_objects(Bucket=bucket_name, Prefix="counterparty/")[
        "Contents"
    ]
    new_row_count = 0

    # Iterate over files in the counterparty/ dir from newest to oldest
    for i in range(len(file_list) - 1, -1, -1):
        cur_filename = file_list[i]["Key"]

        json_object = s3_client.get_object(Bucket=bucket_name, Key=cur_filename)
        json_string = json_object["Body"].read().decode("utf-8")

        working_df = pd.DataFrame.from_dict(json.loads(json_string))

        # Iterate over rows in the current file from newest to oldest
        for j in range(len(working_df.index) - 1, -1, -1):
            # Find and update all counterparty rows that reference an updated
            # address id and aren't already present in dim_counterparty_df
            if working_df.loc[j, "counterparty_id"] not in already_updated_list:
                if working_df.loc[j, "legal_address_id"] in updated_address_ids:
                    current_row = working_df.loc[[j]]
                    current_row = current_row.merge(
                        address_df, left_on="legal_address_id", right_on="address_id"
                    )
                    current_row = current_row.drop(
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
                    current_row = current_row.rename(
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
                    dim_counterparty_df = pd.concat(
                        [dim_counterparty_df, current_row], ignore_index=True
                    )
                    already_updated_list.append(working_df.loc[j, "counterparty_id"])
                    new_row_count += 1

    logger.info(
        f"Added {new_row_count} rows with updated address info "
        + "to dim_counterparty_df."
    )

    return dim_counterparty_df, dim_location_df


###################################
####                           ####
####      LAMBDA  HANDLER      ####
####                           ####
###################################


def processing_lambda_handler(event, context):
    try:
        logger.info("Initializing processing lambda with input event:\n" + f"{event}")

        has_new_rows = event["HasNewRows"]
        last_checked_time = event["LastCheckedTime"]
        s3_client = boto3.client("s3")
        INGESTION_BUCKET_NAME = os.environ["INGESTION_BUCKET_NAME"]
        PROCESSING_BUCKET_NAME = os.environ["PROCESSING_BUCKET_NAME"]

        # assign DF variable names to None -- used in constructing output later
        dim_counterparty_df = None
        dim_currency_df = None
        dim_design_df = None
        dim_staff_df = None
        fact_sales_order_df = None
        dim_location_df = None

        logger.info(f"Ingestion bucket is {INGESTION_BUCKET_NAME}.")
        logger.info(f"Processing bucket is {PROCESSING_BUCKET_NAME}.")

        ####################################################
        ## PROCESS COUNTERPARTY and ADDRESS TABLE UPDATES ##
        ####################################################

        if has_new_rows["counterparty"]:
            logger.info("Processing new rows for table 'counterparty'.")

            file_name = f"counterparty/{last_checked_time}.json"
            json_object = s3_client.get_object(
                Bucket=INGESTION_BUCKET_NAME, Key=file_name
            )
            json_string = json_object["Body"].read().decode("utf-8")
            counterparty_df = pd.DataFrame.from_dict(json.loads(json_string))

            address_ids_to_fetch = counterparty_df["legal_address_id"].tolist()
            addresses_df = fetch_latest_row_versions(
                s3_client, INGESTION_BUCKET_NAME, "address", address_ids_to_fetch
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

            logger.info(
                "dim_counterparty_df DataFrame created with "
                + f"{len(dim_counterparty_df.index)} rows."
            )

        if has_new_rows["address"]:
            logger.info("Processing new rows for table 'address'.")
            if has_new_rows["counterparty"]:
                output = process_address_updates(
                    s3_client,
                    INGESTION_BUCKET_NAME,
                    last_checked_time,
                    dim_counterparty_df,
                )
            else:
                output = process_address_updates(
                    s3_client, INGESTION_BUCKET_NAME, last_checked_time
                )

            dim_counterparty_df, dim_location_df = output

            logger.info(
                "Saving dim_location_df DataFrame to "
                + f"dim_location/{last_checked_time}.parquet ..."
            )
            df_to_parquet_in_s3(
                s3_client,
                dim_location_df,
                PROCESSING_BUCKET_NAME,
                "dim_location",
                last_checked_time,
            )
            logger.info("Save successful.")

        if has_new_rows["counterparty"] or has_new_rows["address"]:
            logger.info(
                "Saving dim_counterparty_df DataFrame to "
                + f"dim_counterparty/{last_checked_time}.parquet ..."
            )
            df_to_parquet_in_s3(
                s3_client,
                dim_counterparty_df,
                PROCESSING_BUCKET_NAME,
                "dim_counterparty",
                last_checked_time,
            )
            logger.info("Save successful.")

        ####################################
        ## PROCESS CURRENCY TABLE UPDATES ##
        ####################################

        if has_new_rows["currency"]:
            logger.info("Processing new rows for table 'currency'.")
            file_name = f"currency/{last_checked_time}.json"
            json_string = (
                s3_client.get_object(Bucket=INGESTION_BUCKET_NAME, Key=file_name)[
                    "Body"
                ]
                .read()
                .decode("utf-8")
            )
            currency_df = pd.DataFrame.from_dict(json.loads(json_string))
            dim_currency_df = currency_df.drop(columns=["last_updated", "created_at"])

            dim_currency_df["currency_name"] = dim_currency_df["currency_code"].apply(
                lambda x: Currency(x).currency_name
            )

            logger.info(
                "dim_currency_df DataFrame created with "
                + f"{len(dim_currency_df.index)} rows."
            )

            logger.info(
                "Saving dim_currency_df DataFrame to "
                + f"dim_currency/{last_checked_time}.parquet ..."
            )

            df_to_parquet_in_s3(
                s3_client,
                dim_currency_df,
                PROCESSING_BUCKET_NAME,
                "dim_currency",
                last_checked_time,
            )
            logger.info("Save successful.")

        ##################################
        ## PROCESS DESIGN TABLE UPDATES ##
        ##################################

        if has_new_rows["design"]:
            logger.info("Processing new rows for table 'design'.")
            file_name = f"design/{last_checked_time}.json"
            json_string = (
                s3_client.get_object(Bucket=INGESTION_BUCKET_NAME, Key=file_name)[
                    "Body"
                ]
                .read()
                .decode("utf-8")
            )
            design_df = pd.DataFrame.from_dict(json.loads(json_string))
            dim_design_df = design_df.drop(columns=["last_updated", "created_at"])

            logger.info(
                "dim_design_df DataFrame created with "
                + f"{len(dim_design_df.index)} rows."
            )

            logger.info(
                "Saving dim_design_df DataFrame to "
                + f"dim_design/{last_checked_time}.parquet ..."
            )
            df_to_parquet_in_s3(
                s3_client,
                dim_design_df,
                PROCESSING_BUCKET_NAME,
                "dim_design",
                last_checked_time,
            )
            logger.info("Save successful.")

        ################################################
        ## PROCESS STAFF and DEPARTMENT TABLE UPDATES ##
        ################################################

        if has_new_rows["staff"]:
            logger.info("Processing new rows for table 'address'.")
            file_name = f"staff/{last_checked_time}.json"
            json_string = (
                s3_client.get_object(Bucket=INGESTION_BUCKET_NAME, Key=file_name)[
                    "Body"
                ]
                .read()
                .decode("utf-8")
            )
            staff_df = pd.DataFrame.from_dict(json.loads(json_string))

            department_ids_to_fetch = staff_df["department_id"].tolist()
            departments_df = fetch_latest_row_versions(
                s3_client, INGESTION_BUCKET_NAME, "department", department_ids_to_fetch
            )
            dim_staff_df = pd.merge(
                staff_df, departments_df, how="left", on="department_id"
            )
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

            logger.info(
                "dim_staff_df DataFrame created with "
                + f"{len(dim_staff_df.index)} rows."
            )

        if has_new_rows["department"]:
            logger.info("Processing new rows for table 'department'.")
            if has_new_rows["staff"]:
                dim_staff_df = process_department_updates(
                    s3_client, INGESTION_BUCKET_NAME, last_checked_time, dim_staff_df
                )
            else:
                dim_staff_df = process_department_updates(
                    s3_client, INGESTION_BUCKET_NAME, last_checked_time
                )

        if has_new_rows["staff"] or has_new_rows["department"]:
            logger.info(
                "Saving dim_staff_df DataFrame to "
                + f"dim_staff/{last_checked_time}.parquet ..."
            )
            df_to_parquet_in_s3(
                s3_client,
                dim_staff_df,
                PROCESSING_BUCKET_NAME,
                "dim_staff",
                last_checked_time,
            )
            logger.info("Save successful.")

        #######################################
        ## PROCESS SALES ORDER TABLE UPDATES ##
        #######################################

        if has_new_rows["sales_order"]:
            logger.info("Processing new rows for table 'sales_order'.")
            file_name = f"sales_order/{last_checked_time}.json"
            json_string = (
                s3_client.get_object(Bucket=INGESTION_BUCKET_NAME, Key=file_name)[
                    "Body"
                ]
                .read()
                .decode("utf-8")
            )
            sales_order_df = pd.DataFrame.from_dict(json.loads(json_string))

            # Split dates and times
            sales_order_df["created_date"] = (
                sales_order_df["created_at"].str.split(" ").str[0]
            )
            sales_order_df["created_time"] = (
                sales_order_df["created_at"].str.split(" ").str[1]
            )
            sales_order_df["last_updated_date"] = (
                sales_order_df["last_updated"].str.split(" ").str[0]
            )
            sales_order_df["last_updated_time"] = (
                sales_order_df["last_updated"].str.split(" ").str[1]
            )

            # Rename columns
            sales_order_df = sales_order_df.rename(
                columns={"staff_id": "sales_staff_id"}
            )

            # Drop columns and create the fact_sales_order df
            fact_sales_order_df = sales_order_df.drop(
                columns=["created_at", "last_updated"]
            )

            # Reorganise columns
            fact_sales_order_df = fact_sales_order_df[
                [
                    "sales_order_id",
                    "created_date",
                    "created_time",
                    "last_updated_date",
                    "last_updated_time",
                    "sales_staff_id",
                    "counterparty_id",
                    "units_sold",
                    "unit_price",
                    "currency_id",
                    "design_id",
                    "agreed_payment_date",
                    "agreed_delivery_date",
                    "agreed_delivery_location_id",
                ]
            ]

            logger.info(
                "fact_sales_order_df DataFrame created with "
                + f"{len(fact_sales_order_df.index)} rows."
            )

            logger.info(
                "Saving fact_sales_order_df DataFrame to "
                + f"fact_sales_order/{last_checked_time}.parquet ..."
            )
            df_to_parquet_in_s3(
                s3_client,
                fact_sales_order_df,
                PROCESSING_BUCKET_NAME,
                "fact_sales_order",
                last_checked_time,
            )
            logger.info("Save successful.")

        output = {
            "HasNewRows": {
                "dim_counterparty": dim_counterparty_df is not None,
                "dim_currency": dim_currency_df is not None,
                "dim_design": dim_design_df is not None,
                "dim_staff": dim_staff_df is not None,
                "dim_location": dim_location_df is not None,
                "fact_sales_order": fact_sales_order_df is not None,
            },
            "LastCheckedTime": last_checked_time,
        }

        logger.info(output)
        print(output)
        return output

    except Exception as e:

        logger.error({"Error found": e})
        return {"Error found": e}


if __name__ == "__main__":
    test_event = {
        "HasNewRows": {
            "counterparty": True,
            "currency": True,
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
        "LastCheckedTime": "2024-11-20 15:22:10.531518",
    }
    processing_lambda_handler(test_event, {})
