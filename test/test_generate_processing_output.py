from copy import deepcopy
import pandas as pd

from src.processing_lambda import generate_processing_output

def test_generate_processing_output_returns_expected_dict():
    simple_df = pd.DataFrame.from_dict([{"Column": "value"}])
    test_tables_to_report = {
        "table_1": None,
        "table_2": deepcopy(simple_df),
        "table_3": deepcopy(simple_df),
        "table_4": None,
        "table_5": None,
        "table_6": deepcopy(simple_df)
    }
    test_current_check_time = "2022-11-03 14:32:22.906020"

    output = generate_processing_output(
        test_tables_to_report, test_current_check_time
    )

    assert output == {
        "HasNewRows": {
            "table_1": False,
            "table_2": True,
            "table_3": True,
            "table_4": False,
            "table_5": False,
            "table_6": True
        },
        "LastCheckedTime": "2022-11-03 14:32:22.906020"
    }
