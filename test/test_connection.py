from src.ingestion_lambda import connect_to_db, close_connection
import datetime


def test_connection_can_access_table():
    conn = connect_to_db()
    query = "SELECT * FROM counterparty LIMIT 3;"
    expect = conn.run(query)
    assert type(expect[0][0]) == int
    assert type(expect[0][1]) == str
    assert type(expect[0][2]) == int
    assert type(expect[0][3]) == str
    assert type(expect[0][4]) == str
    assert type(expect[0][5]) == datetime.datetime
    assert type(expect[0][6]) == datetime.datetime
    close_connection(conn)
