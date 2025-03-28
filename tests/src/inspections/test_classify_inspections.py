import pytest
from toolbox_continu_inzicht.inspections.classify_inspections import ClassifyInspections

from pathlib import Path
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter


def helper_create_data_adapter(name):
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / name)
    config.lees_config()
    return DataAdapter(config=config)


def test_classify_inspections_fails_one_single_item_list():
    """of string, of list met minimaal 2 items"""
    data_adapter = helper_create_data_adapter("test_filter.yaml")
    filter = ClassifyInspections(data_adapter=data_adapter)
    with pytest.raises(AssertionError):
        filter.run(input=["locations_fews"], output="filter_resultaten")


def test_classify_inspections_minimal_styling():
    """test of de minimal styling werkt"""
    data_adapter = helper_create_data_adapter("test_filter.yaml")
    filter = ClassifyInspections(data_adapter=data_adapter)
    filter.run(input="locations_fews", output="filter_resultaten")


def test_classify_inspections_minimal_styling2():
    """test of de minimal styling werkt, andere input"""
    data_adapter = helper_create_data_adapter("test_inspection.yaml")
    filter = ClassifyInspections(data_adapter=data_adapter)
    filter.run(input="locations_inspections", output="filter_resultaten")


def test_classify_inspections_with_styling():
    """test werkt met styling"""
    data_adapter = helper_create_data_adapter("test_inspection.yaml")
    filter = ClassifyInspections(data_adapter=data_adapter)
    filter.run(
        input=["locations_inspections", "styling_example"], output="filter_resultaten"
    )
