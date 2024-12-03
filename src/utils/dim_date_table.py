from pg8000.native import Connection
import boto3
from src.ingestion_lambda import retrieve_secret
import ssl

def connect_to_dw():
    sm_client = boto3.client("secretsmanager", "eu-west-2")
    credentials = retrieve_secret(sm_client, "dcf-ttotes/totesys-olap-credentials")
    ssl_context = ssl.SSLContext()
    return Connection(
        user=credentials["DW_USER"],
        password=credentials["DW_PASSWORD"],
        database=credentials["DW_DATABASE"],
        host=credentials["DW_HOST"],
        port=credentials["DW_PORT"],
        ssl_context=ssl_context
    )

def insert_dim_date_table_into_data_warehouse(conn):    
    conn.run('''INSERT INTO dim_date (date_id, year, month, day, day_of_week, day_name, month_name, quarter)
             SELECT
                date,
                EXTRACT(YEAR FROM date),
                EXTRACT(MONTH FROM date),
                EXTRACT(DAY FROM date),
                EXTRACT(isodow FROM date),
                TO_CHAR(date, 'Day'),
                TO_CHAR(date, 'Month'),
                EXTRACT(quarter FROM date)
             FROM generate_series(
                '2020-01-01',
                '2030-12-31', INTERVAL '1 day'
            ) AS date;
             ''')

if __name__ == '__main__':
    insert_dim_date_table_into_data_warehouse(connect_to_dw())
