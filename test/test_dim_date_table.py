from src.utils.dim_date_table import insert_dim_date_table_into_data_warehouse
from datetime import date
from src.ingestion_lambda import retrieve_secret
import pg8000
import boto3
import ssl


def connect_to_test_dw():
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


def create_test_dim_date_table():
    conn = connect_to_test_dw()
    conn.run("DROP TABLE IF EXISTS dim_date;")
    print(conn)
    conn.run(
        """CREATE TABLE dim_date (
            date_id DATE PRIMARY KEY NOT NULL,
            year INT NOT NULL,
            month INT NOT NULL,
            day INT NOT NULL,
            day_of_week INT NOT NULL,
            day_name VARCHAR NOT NULL,
            month_name VARCHAR NOT NULL,
            quarter INT NOT NULL
            ); COMMIT;"""
    )
    conn.close()


def test_data_type_of_dim_date_columns():
    create_test_dim_date_table()
    conn = connect_to_test_dw()
    insert_dim_date_table_into_data_warehouse(conn)
    query = "SELECT * FROM dim_date;"
    result = conn.run(query)
    assert type(result[0][0]) == date
    assert type(result[0][1]) == int
    assert type(result[0][2]) == int
    assert type(result[0][3]) == int
    assert type(result[0][4]) == int
    assert type(result[0][5]) == str
    assert type(result[0][6]) == str
    assert type(result[0][7]) == int
    conn.close()


def test_check_start_and_end_date():
    conn = connect_to_test_dw()
    create_test_dim_date_table()
    insert_dim_date_table_into_data_warehouse(conn)
    start_query = "SELECT MIN(date_id) FROM dim_date;"
    end_query = "SELECT MAX(date_id) FROM dim_date;"

    assert conn.run(start_query) == ([date(2020, 1, 1)],)
    assert conn.run(end_query) == ([date(2030, 12, 31)],)
    conn.close()
