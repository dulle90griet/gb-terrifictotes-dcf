from src.ingestion_lambda import *
from src.connection import connect_to_db, close_connection
import datetime
import pytest


@pytest.fixture
def conn_fixture():
    conn = connect_to_db()
    yield conn



def test_connection_can_access_table(conn_fixture):
    query = "SELECT * FROM counterparty LIMIT 3;"
    expect = conn_fixture.run(query)
    assert type(expect[0][0]) == int
    assert type(expect[0][1]) == str
    assert type(expect[0][2]) == int
    assert type(expect[0][3]) == str
    assert type(expect[0][4]) == str
    assert type(expect[0][5]) == datetime.datetime
    assert type(expect[0][6]) == datetime.datetime
    close_connection(conn_fixture)
    

def test_gets_data_returns_updated_rows_from_each_table(conn_fixture):
    current_time = datetime.datetime.now()
    tables = ['counterparty', 'currency', 'department', 'design', 'staff', 'sales_order', 'address', 'payment', 'purchase_order', 'payment_type', 'transaction']
    date_1 = (datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)).strftime('%Y-%m-%d %H:%M:%S.%f')
    date_2 = (datetime.datetime(2024, 11, 3, 14, 20, 51, 563000)).strftime('%Y-%m-%d %H:%M:%S.%f')
    date_3 = (current_time + timedelta(weeks= 104)).strftime('%Y-%m-%d %H:%M:%S.%f')
    dates = [date_1, date_2, date_3]

    results = []
    for i in range(len(dates)):
        output = get_data(conn_fixture, dates[i]) 
        
        for table in tables:
            formatted_table_data = [dict(zip(output[table][1],row)) for row in output[table][0]]
            output[table] = formatted_table_data

        results.append(output)
            

    for i in range(len(results)):
        for table in results[i]:
            for row in table:
                if 'last_updated' in row:
                    assert row['last_updated'] > dates[i]
    
    close_connection(conn_fixture)


## DELETE THE TEST WITH PATCH?
"""
dummy_counterparty_table = [[1, 'Fahey and Sons', 15, 'Micheal Toy', 'Mrs. Lucy Runolfsdottir', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)], [2, 'Dummy LPR', 28, 'Melba Sanford', 'Jean Hane III', datetime.datetime(2024, 11, 3, 14, 20, 51, 563000), datetime.datetime(2024, 11, 3, 14, 20, 51, 563000)], [3, 'Armstrong Inc', 2, 'Jane Wiza', 'Myra Kovacek', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)]]

dummy_tuple_table = [(1, 'Fahey and Sons', 15, 'Micheal Toy', 'Mrs. Lucy Runolfsdottir', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)), (2, 'Dummy LPR', 28, 'Melba Sanford', 'Jean Hane III', datetime.datetime(2024, 11, 3, 14, 20, 51, 563000), datetime.datetime(2024, 11, 3, 14, 20, 51, 563000)), (3, 'Armstrong Inc', 2, 'Jane Wiza', 'Myra Kovacek', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000))]

dummy_tuple_columns = ['counterparty_id', 'counterparty_legal_name', 'legal_address_id', 'commercial_contact', 'delivery_contact', 'created_at', 'last_updated']

connection = Mock()
connection.run.return_value = dummy_counterparty_table
connection.columns = [{"name": "id"}, {"name":"dd"}]

@patch("src.ingestion_lambda.connect_to_db", return_value=connection)
def test_ingests_updated_rows_of_counterparty_table(conn_fixture, pgmocker):
    get_counterparty()
    patcher = patch("src.ingestion_lambda", return_value=dummy_counterparty_table)
    patcher.start()
    pgmocker.patch(pgmock.table('counterparty'), dummy_tuple_table, dummy_tuple_columns)

    test_datetime = datetime.datetime(2023, 11, 3, 14, 20, 51, 563000)
    test_datetime = '2023-11-03'
    print(get_data(test_datetime)['counterparty'])

    test_datetime = datetime.datetime(2020, 11, 3, 14, 20, 51, 563000)
    test_datetime = test_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')

    assert get_data(conn_fixture, test_datetime)['counterparty'] == [[2, 'Dummy LPR', 28, 'Melba Sanford', 'Jean Hane III', datetime.datetime(2024, 11, 3, 14, 20, 51, 563000), datetime.datetime(2024, 11, 3, 14, 20, 51, 563000)]]
"""