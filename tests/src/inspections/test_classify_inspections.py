import numpy as np
import pytest
from toolbox_continu_inzicht.inspections.inspections import ClassifyInspections

from pathlib import Path
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter


def helper_create_data_adapter(name):
    """Create a DataAdapter object with the given config file name, reducing code duplication."""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / name)
    config.lees_config()
    return DataAdapter(config=config)


def test_classify_inspections_fails_one_single_item_list():
    """of string, of list met minimaal 2 items"""
    classify_inspection = ClassifyInspections(
        data_adapter=helper_create_data_adapter("test_filter.yaml")
    )
    with pytest.raises(AssertionError):
        classify_inspection.run(input=["locations_fews"], output="filter_resultaten")


def test_classify_inspections_fails_missing_column():
    """test of missende kolom error werkt"""
    data_adapter = helper_create_data_adapter("test_inspection.yaml")
    data_adapter.config.global_variables["ClassifyInspections"] = {
        "classify_column": "NotExisting"
    }
    classify_inspection = ClassifyInspections(data_adapter=data_adapter)
    with pytest.raises(KeyError):
        classify_inspection.run(
            input=["locations_inspections", "styling_example"],
            output="classify_resultaten",
        )


def test_classify_inspections_minimal_styling():
    """test of de minimal styling werkt"""
    data_adapter = helper_create_data_adapter("test_inspection.yaml")
    classify_inspection = ClassifyInspections(data_adapter=data_adapter)
    classify_inspection.run(input="locations_inspections", output="classify_resultaten")
    result = classify_inspection.df_out.loc[:, "color"].to_list()
    assert result == ["#9e9e9e", "#9e9e9e", "#9e9e9e"]


def test_classify_inspections_minimal_styling2():
    """test of de minimal styling werkt, andere input"""
    data_adapter = helper_create_data_adapter("test_inspection.yaml")
    classify_inspection = ClassifyInspections(data_adapter=data_adapter)
    classify_inspection.run(input="locations_inspections", output="classify_resultaten")


def test_classify_inspections_with_styling():
    """test werkt met styling"""
    data_adapter = helper_create_data_adapter("test_inspection.yaml")
    classify_inspection = ClassifyInspections(data_adapter=data_adapter)
    classify_inspection.run(
        input=["locations_inspections", "styling_example"], output="classify_resultaten"
    )
    result = classify_inspection.df_out[
        [
            "priority",
            "color",
        ]
    ].to_numpy()
    expected = np.array([[1, "#a9070f"], [3, "#0760a9"], [11, "#9e9e9e"]], dtype=object)
    assert np.isclose(list(result[:, 0]), list(expected[:, 0])).all()
    assert all(result[:, 1] == expected[:, 1])


def test_classify_inspections_with_styling_only_lower_boundary():
    """test werkt met styling - alleen lower_boundary
    Let op is dus net anders dan vorige test en laatste waarde is dus ook anders want geen upper_boundary
    """
    data_adapter = helper_create_data_adapter("test_inspection.yaml")
    data_adapter.config.data_adapters["styling_example"]["path"] = (
        "styling_example_lower_boundary.csv"
    )
    classify_inspection = ClassifyInspections(data_adapter=data_adapter)
    classify_inspection.run(
        input=["locations_inspections", "styling_example"], output="classify_resultaten"
    )
    result = classify_inspection.df_out[
        [
            "priority",
            "color",
        ]
    ].to_numpy()
    expected = np.array([[1, "#a9070f"], [3, "#0760a9"], [11, "#5007a9"]], dtype=object)
    assert np.isclose(list(result[:, 0]), list(expected[:, 0])).all()
    assert all(result[:, 1] == expected[:, 1])


def test_classify_inspections_text():
    """test werkt met text, tot nu toe alleen met getallen"""
    data_adapter = helper_create_data_adapter("test_inspection.yaml")
    data_adapter.config.global_variables["ClassifyInspections"]["classify_column"] = (
        "opmerking"
    )
    data_adapter.config.data_adapters["styling_example"]["path"] = (
        "styling_example_text.csv"
    )
    classify_inspection = ClassifyInspections(data_adapter=data_adapter)
    classify_inspection.run(
        input=["locations_inspections", "styling_example"], output="classify_resultaten"
    )
    result = classify_inspection.df_out[
        [
            "priority",
            "color",
        ]
    ].to_numpy()
    expected = np.array([[1, "#a9070f"], [3, "#07a9a1"], [11, "#0760a9"]], dtype=object)
    assert np.isclose(list(result[:, 0]), list(expected[:, 0])).all()
    assert all(result[:, 1] == expected[:, 1])


def test_classify_inspections_text_contains():
    """test werkt met text, tot nu toe alleen met getallen"""
    data_adapter = helper_create_data_adapter("test_inspection.yaml")
    data_adapter.config.global_variables["ClassifyInspections"]["classify_column"] = (
        "opmerking"
    )
    data_adapter.config.data_adapters["styling_example"]["path"] = (
        "styling_example_text.csv"
    )
    data_adapter.config.global_variables["ClassifyInspections"][
        "classify_text_type"
    ] = "contains"
    classify_inspection = ClassifyInspections(data_adapter=data_adapter)
    classify_inspection.run(
        input=["locations_inspections", "styling_example"], output="classify_resultaten"
    )
    result = classify_inspection.df_out[
        [
            "priority",
            "color",
        ]
    ].to_numpy()
    expected = np.array([[1, "#a9070f"], [3, "#07a9a1"], [11, "#a9070f"]], dtype=object)
    assert np.isclose(list(result[:, 0]), list(expected[:, 0])).all()
    assert all(result[:, 1] == expected[:, 1])


def test_classify_inspections_edit_default_styling():
    """test of het werkt om de default styling aan te passen"""
    data_adapter = helper_create_data_adapter("test_inspection.yaml")
    classify_inspection = ClassifyInspections(data_adapter=data_adapter)
    df_default_style = classify_inspection.get_default_styling()
    df_changed_style = df_default_style.copy()
    df_changed_style.loc[0, "color"] = "#ffffff"
    classify_inspection.set_default_styling(df_changed_style)
    classify_inspection.run(
        input=["locations_inspections", "styling_example"], output="classify_resultaten"
    )
    result = classify_inspection.df_out[
        [
            "priority",
            "color",
        ]
    ].to_numpy()
    expected = np.array([[1, "#a9070f"], [3, "#0760a9"], [11, "#ffffff"]], dtype=object)
    assert np.isclose(list(result[:, 0]), list(expected[:, 0])).all()
    assert all(result[:, 1] == expected[:, 1])


def test_classify_inspections_with_styling_one_nan_value():
    """test werkt met styling, waarbij 1 nan value is"""
    data_adapter = helper_create_data_adapter("test_inspection.yaml")
    data_adapter.config.data_adapters["styling_example_one_nan"] = {
        "type": "csv",
        "path": "test_styling_one_nan.csv",
        "sep": ",",
    }
    classify_inspection = ClassifyInspections(data_adapter=data_adapter)
    classify_inspection.run(
        input=["locations_inspections", "styling_example_one_nan"],
        output="classify_resultaten",
    )
    result = classify_inspection.df_out[
        [
            "priority",
            "color",
        ]
    ].to_numpy()
    expected = np.array([[1, "#a9070f"], [3, "#0760a9"], [11, "#00ced1"]], dtype=object)
    assert np.isclose(list(result[:, 0]), list(expected[:, 0])).all()
    assert all(result[:, 1] == expected[:, 1])


def test_classify_inspections_with_styling_two_nan_value():
    """test werkt met styling, waarbij 2 nan values in de styling zitten"""
    data_adapter = helper_create_data_adapter("test_inspection.yaml")
    data_adapter.config.data_adapters["styling_example_two_nans"] = {
        "type": "csv",
        "path": "test_styling_two_nans.csv",
        "sep": ",",
    }
    classify_inspection = ClassifyInspections(data_adapter=data_adapter)
    classify_inspection.run(
        input=["locations_inspections", "styling_example_two_nans"],
        output="classify_resultaten",
    )
    result = classify_inspection.df_out[
        [
            "priority",
            "color",
        ]
    ].to_numpy()
    expected = np.array([[1, "#a9070f"], [3, "#0760a9"], [11, "#00ff00"]], dtype=object)
    assert np.isclose(list(result[:, 0]), list(expected[:, 0])).all()
    assert all(result[:, 1] == expected[:, 1])


def test_classify_inspections_with_styling_two_output():
    """test of het werkt om de default styling aan te passen"""
    data_adapter = helper_create_data_adapter("test_inspection.yaml")
    classify_inspection = ClassifyInspections(data_adapter=data_adapter)
    df_default_style = classify_inspection.get_default_styling()
    df_changed_style = df_default_style.copy()
    df_changed_style.loc[0, "color"] = "#ffffff"
    classify_inspection.set_default_styling(df_changed_style)
    classify_inspection.run(
        input=["locations_inspections", "styling_example"],
        output=["classify_resultaten", "legenda_example"],
    )
    result = classify_inspection.df_out[
        [
            "priority",
            "color",
        ]
    ].to_numpy()
    expected = np.array([[1, "#a9070f"], [3, "#0760a9"], [11, "#ffffff"]], dtype=object)
    assert np.isclose(list(result[:, 0]), list(expected[:, 0])).all()
    assert all(result[:, 1] == expected[:, 1])
    expected_col = classify_inspection.get_possible_styling(type="Marker")
    resulting_columns = classify_inspection.df_legend_out.columns
    assert set(expected_col).issubset(resulting_columns)


def test_get_default_styling_columns():
    """test of de default styling werkt"""
    data_adapter = helper_create_data_adapter("test_inspection.yaml")
    classify_inspection = ClassifyInspections(data_adapter=data_adapter)
    result = classify_inspection.get_possible_styling()
    print(result)


def test_classify_simple_polygon():
    """test of het werkt om de default styling aan te passen voor een polygoon"""
    data_adapter = helper_create_data_adapter("test_inspection_polygon.yaml")
    applied_format = ClassifyInspections(data_adapter=data_adapter)
    applied_format.run(
        input="classify_resultaten_polygoon",
        output=["filter_classify_resultaten_polygoon", "legenda"],
    )
    # check the styling is added correctly
    assert set(
        [
            "symbol",
            "weight",
            "color",
            "fillColor",
            "opacity",
            "fillOpacity",
            "dashArray",
            "radius",
        ]
    ).issubset(applied_format.df_out.columns)
