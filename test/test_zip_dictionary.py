from src.ingestion_lambda import zip_dictionary
from datetime import datetime


def test_zip_dictionary_empty_columns_empty_rows_returns_empty_list():
    rows_input = []
    columns_input = []
    assert zip_dictionary(rows_input, columns_input) == []


def test_zip_dictionary_works_for_non_empty_rows_and_columns():
    test_columns = ["name", "age", "dob"]
    test_rows = [["ruby", 31, "15-03-1993"], ["minal", 30, "23-07-1994"]]
    assert zip_dictionary(test_rows, test_columns) == [
        {"name": "ruby", "age": 31, "dob": "15-03-1993"},
        {"name": "minal", "age": 30, "dob": "23-07-1994"},
    ]


def test_zip_dictionary_works_for_nested_lists():
    test_columns = ["zoo_name", "animals", "date_opened", "region"]
    test_rows = [
        ["scunthorpe safari", ["elephant", "panda", "jaguar", "giraffe"], "13-03-1990"],
        ["london safari", ["dog", "panda", "leopard", "giraffe"], "13-03-1991"],
        ["liverpool zoo", ["tiger", "lion", "leopard", "giraffe"], "18-03-2000"],
    ]
    assert zip_dictionary(test_rows, test_columns) == [
        {
            "zoo_name": "scunthorpe safari",
            "animals": ["elephant", "panda", "jaguar", "giraffe"],
            "date_opened": "13-03-1990",
        },
        {
            "zoo_name": "london safari",
            "animals": ["dog", "panda", "leopard", "giraffe"],
            "date_opened": "13-03-1991",
        },
        {
            "zoo_name": "liverpool zoo",
            "animals": ["tiger", "lion", "leopard", "giraffe"],
            "date_opened": "18-03-2000",
        },
    ]
