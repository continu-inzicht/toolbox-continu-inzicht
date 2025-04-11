import pytest
from toolbox_continu_inzicht.inspections.inspections import InspectionsToDatabase

from pathlib import Path
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter


def helper_create_data_adapter(name):
    """Create a DataAdapter object with the given config file name, reducing code duplication."""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / name)
    config.lees_config()
    return DataAdapter(config=config)


def test_inspections_to_database_fails_one_row_max():
    """checkt of faalt met te veel rijen (boots na door max op 1 te zetten)"""
    data_adapter = helper_create_data_adapter("test_inspection_to_db.yaml")
    classify_inspection = InspectionsToDatabase(data_adapter=data_adapter)
    data_adapter.config.global_variables["InspectionsToDatabase"]["max_rows"] = 1
    with pytest.raises(UserWarning):
        classify_inspection.run(
            input=["test_inspection_to_db"], output="filter_resultaten"
        )


def test_inspections_to_database():
    """test of inspections goed wordt klaar gezet voor de database"""
    data_adapter = helper_create_data_adapter("test_inspection_to_db.yaml")
    inspections_to_database = InspectionsToDatabase(data_adapter=data_adapter)
    inspections_to_database.run(
        input=["classify_resultaten", "legenda"],
        output="example_to_database",
    )
    # check layer legend and data have been added
    assert (len(inspections_to_database.df_out.loc[0, "layer_legend"])) >= 472
    assert (len(inspections_to_database.df_out.loc[0, "layer_data"])) >= 699


def test_inspections_to_database_with_styling():
    """test of inspections goed wordt klaar gezet voor de database, met meer styling"""
    data_adapter = helper_create_data_adapter("test_inspection_to_db.yaml")
    inspections_to_database = InspectionsToDatabase(data_adapter=data_adapter)
    inspections_to_database.run(
        input=["classify_resultaten", "legenda", "layers"],
        output="example_to_database",
    )
    # check the layer file is appended to
    assert len(inspections_to_database.df_out) >= 3
    # check layer legend and data have been added
    assert (len(inspections_to_database.df_out.loc[0, "layer_legend"])) >= 472
    assert (len(inspections_to_database.df_out.loc[0, "layer_data"])) >= 699


def test_classify_simple_polygon_to_db():
    """test of het werkt om de polygoon naar database te zetten"""
    data_adapter = helper_create_data_adapter("test_inspection_polygon.yaml")
    inspections_to_database = InspectionsToDatabase(data_adapter=data_adapter)
    inspections_to_database.run(
        input=["filter_classify_resultaten_polygoon", "legenda"],
        output="test_to_db",
    )
    assert len(inspections_to_database.df_out) >= 1
    # check layer legend and data have been added
    assert (len(inspections_to_database.df_out.loc[0, "layer_legend"])) >= 234
    assert (len(inspections_to_database.df_out.loc[0, "layer_data"])) >= 699
