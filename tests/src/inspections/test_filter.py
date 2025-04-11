from toolbox_continu_inzicht.inspections.filter import Filter

from pathlib import Path
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter


def helper_create_data_adapter():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / "test_filter.yaml")
    config.lees_config()
    return DataAdapter(config=config)


def test_filter_query():
    """ "Tests the filter function with a simple query: measurement_location_id == 1"""
    data_adapter = helper_create_data_adapter()
    data_adapter.config.global_variables["Filter"]["query"] = (
        "measurement_location_id == 1"
    )
    filter = Filter(data_adapter=data_adapter)
    filter.run(input="locations_fews", output="filter_resultaten")
    output = filter.df_out["measurement_location_id"].unique()
    assert len(output) == 1
    assert output[0] == 1


def test_filter_drop_columns():
    """ "Tests the filter function with drop_columns option"""
    data_adapter = helper_create_data_adapter()
    data_adapter.config.global_variables["Filter"]["keep_columns"] = None
    data_adapter.config.global_variables["Filter"]["drop_columns"] = (
        "measurement_location_id"
    )

    filter = Filter(data_adapter=data_adapter)
    filter.run(input="locations_fews", output="filter_resultaten")
    output_cols = filter.df_out.columns
    assert len(output_cols) == 1
    assert output_cols[0] == "measurement_location_code"


def test_filter_keep_columns():
    """ "Tests the filter function with keep_columns option"""
    data_adapter = helper_create_data_adapter()
    data_adapter.config.global_variables["Filter"]["keep_columns"] = (
        "measurement_location_id"
    )
    filter = Filter(data_adapter=data_adapter)
    filter.run(input="locations_fews", output="filter_resultaten")
    output_cols = filter.df_out.columns
    assert len(output_cols) == 1
    assert output_cols[0] == "measurement_location_id"


def test_filter_keep_columns2():
    """
    Tests the filter function with keep_columns option
    data adapters sets it to:
    keep_columns:
    - "measurement_location_id"
    - "measurement_location_code"

    """
    data_adapter = helper_create_data_adapter()

    filter = Filter(data_adapter=data_adapter)
    filter.run(input="locations_fews", output="filter_resultaten")
    output_cols = filter.df_out.columns
    assert len(output_cols) == 2
    assert all(
        [
            col in ["measurement_location_id", "measurement_location_code"]
            for col in output_cols
        ]
    )
