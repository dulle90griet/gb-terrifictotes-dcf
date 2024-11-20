from src.ingestion_lambda import format_to_json
from datetime import datetime
from decimal import Decimal


def test_format_to_json_returns_empty_string_for_empty_list():
    test_input = []
    expected = "[]"
    assert format_to_json(test_input) == expected


def test_list_of_dicts_returns_json_string():
    test_input = [{"a": 1, "b": 2, "c": 3}, {"d": 4, "e": 5, "f": 6}]
    expected = '[{"a": 1, "b": 2, "c": 3}, {"d": 4, "e": 5, "f": 6}]'
    output = format_to_json(test_input)
    assert type(output) == str
    assert output == expected


def test_list_of_dicts_including_datetime():
    current_time = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S.%f")
    test_input = [
        {"a": 1, "b": 2, "c": current_time},
        {"d": 4, "e": 5, "f": current_time},
    ]
    output = format_to_json(test_input)
    assert current_time in output


def test_list_of_dicts_including_decimal():
    test_input = [{"a": 1, "b": 2, "c": Decimal("1.23")}]
    output = format_to_json(test_input)
    print(output)
