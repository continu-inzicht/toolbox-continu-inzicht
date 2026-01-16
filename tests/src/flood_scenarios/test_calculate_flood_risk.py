import os
from pathlib import Path
import pytest
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.flood_scenarios.calculate_flood_risk import (
    CalculateFloodRisk,
)


def helper_create_data_adapter(name):
    """Create a DataAdapter object with the given config file name, reducing code duplication."""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / name)
    config.lees_config()
    return DataAdapter(config=config)


def test_calculate_flood_risk():
    """test minimale werkting van functie"""
    data_adapter = helper_create_data_adapter("test_calculate_flood_risk.yaml")
    calculate_flood_risk = CalculateFloodRisk(data_adapter=data_adapter)
    calculate_flood_risk.run(
        input=[
            "segment_failure_probability",
            "flood_scenario_grids",
            "areas_to_average",
            "flood_risk_local_file",
        ],
        output="flood_risk_results",
    )
    df_out = calculate_flood_risk.df_out
    expected_cols = [
        "area_id",
        "casualties",
        "damage",
        "flooding",
        "affected_people",
        "name",
        "code",
        "zip",
        "people",
        "geometry",
    ]
    assert all([col in df_out.columns for col in expected_cols])


def test_calculate_flood_risk_one_grid():
    """test of functie ook 1 van de default grid namen aan kan"""
    data_adapter = helper_create_data_adapter("test_calculate_flood_risk.yaml")
    calculate_flood_risk = CalculateFloodRisk(data_adapter=data_adapter)
    calculate_flood_risk.run(
        input=[
            "segment_failure_probability",
            "flood_scenario_grids_one_gridfile",
            "areas_to_average",
            "flood_risk_local_file",
        ],
        output="flood_risk_results",
    )
    df_out = calculate_flood_risk.df_out
    expected_cols = [
        "area_id",
        "casualties",
        "name",
        "code",
        "zip",
        "people",
        "geometry",
    ]
    assert all([col in df_out.columns for col in expected_cols])


def test_calculate_flood_risk_different_name():
    """test of functie ook met andere namen kan"""
    data_adapter = helper_create_data_adapter("test_calculate_flood_risk.yaml")
    updated_averaging = {"custom_name": "sum"}
    data_adapter.config.global_variables["CalculateFloodRisk"][
        "aggregate_methods"
    ].update(updated_averaging)
    calculate_flood_risk = CalculateFloodRisk(data_adapter=data_adapter)
    calculate_flood_risk.run(
        input=[
            "segment_failure_probability",
            "flood_scenario_grids_one_gridfile_different_name",
            "areas_to_average",
            "flood_risk_local_file",
        ],
        output="flood_risk_results",
    )
    df_out = calculate_flood_risk.df_out
    expected_cols = [
        "area_id",
        "custom_name",
        "name",
        "code",
        "zip",
        "people",
        "geometry",
    ]
    assert all([col in df_out.columns for col in expected_cols])


def test_calculate_flood_risk_different_name_fail():
    """test of functie ook faalt als type averaging niet is opgegeven"""
    data_adapter = helper_create_data_adapter("test_calculate_flood_risk.yaml")
    calculate_flood_risk = CalculateFloodRisk(data_adapter=data_adapter)
    # correctly raises UserWarning if averaging method is not specified for a grid column
    with pytest.raises(
        UserWarning,
        match="Niet alle grid kolommen hebben een bijbehorende aggregate methode in de options.",
    ):
        calculate_flood_risk.run(
            input=[
                "segment_failure_probability",
                "flood_scenario_grids_one_gridfile_different_name",
                "areas_to_average",
                "flood_risk_local_file",
            ],
            output="flood_risk_results",
        )


def test_calculate_flood_risk_per_ha():
    """test of per ha ook werkt"""
    data_adapter = helper_create_data_adapter("test_calculate_flood_risk.yaml")
    calculate_flood_risk = CalculateFloodRisk(data_adapter=data_adapter)
    calculate_flood_risk.run(
        input=[
            "segment_failure_probability",
            "flood_scenario_grids",
            "areas_to_average",
            "flood_risk_local_file",
        ],
        output="flood_risk_results",
    )
    expected_cols = [
        "area_id",
        "casualties",
        "damage",
        "flooding",
        "affected_people",
        "name",
        "code",
        "zip",
        "people",
        "geometry",
    ]
    df_out = calculate_flood_risk.df_out
    assert all([col in df_out.columns for col in expected_cols])

    data_adapter_per_ha = helper_create_data_adapter(
        "test_calculate_flood_risk_per_ha.yaml"
    )
    calculate_flood_risk_per_ha = CalculateFloodRisk(data_adapter=data_adapter_per_ha)
    calculate_flood_risk_per_ha.run(
        input=[
            "segment_failure_probability",
            "flood_scenario_grids",
            "areas_to_average",
            "flood_risk_local_file",
        ],
        output="flood_risk_results",
    )
    df_out_per_ha = calculate_flood_risk_per_ha.df_out

    assert all([col in df_out_per_ha.columns for col in expected_cols])
    columns_per_hectare = [
        "casualties",
        "damage",
        "flooding",
        "affected_people",
    ]
    gdf_in_areas_to_average = data_adapter.input("areas_to_average")
    for column_per_hectare in columns_per_hectare:
        assert all(
            df_out_per_ha[f"{column_per_hectare}_per_ha"]
            == df_out[column_per_hectare]
            / (gdf_in_areas_to_average["geometry"].area / 10000)
        )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true",
    reason="Sloopt de python module integration test",
)
def test_calculate_flood_risk_per_ha_different_dir():
    """test of per ha ook werkt, maar dan de flood risk files in een andere dir staan"""
    data_adapter = helper_create_data_adapter("test_calculate_flood_risk.yaml")
    data_adapter.config.data_adapters["flood_risk_local_file"]["abs_path_user"] = str(
        Path(__file__).parent.parent.parent
        / "docs/examples/notebooks/data_sets/7.flood_scenarios/flood_scenarios"
    )

    calculate_flood_risk = CalculateFloodRisk(data_adapter=data_adapter)
    calculate_flood_risk.run(
        input=[
            "segment_failure_probability",
            "flood_scenario_grids",
            "areas_to_average",
            "flood_risk_local_file",
        ],
        output="flood_risk_results",
    )
    expected_cols = [
        "area_id",
        "casualties",
        "damage",
        "flooding",
        "affected_people",
        "name",
        "code",
        "zip",
        "people",
        "geometry",
    ]
    df_out = calculate_flood_risk.df_out
    assert all([col in df_out.columns for col in expected_cols])

    data_adapter_per_ha = helper_create_data_adapter(
        "test_calculate_flood_risk_per_ha.yaml"
    )
    calculate_flood_risk_per_ha = CalculateFloodRisk(data_adapter=data_adapter_per_ha)
    calculate_flood_risk_per_ha.run(
        input=[
            "segment_failure_probability",
            "flood_scenario_grids",
            "areas_to_average",
            "flood_risk_local_file",
        ],
        output="flood_risk_results",
    )
    df_out_per_ha = calculate_flood_risk_per_ha.df_out

    assert all([col in df_out_per_ha.columns for col in expected_cols])
    columns_per_hectare = [
        "casualties",
        "damage",
        "flooding",
        "affected_people",
    ]
    gdf_in_areas_to_average = data_adapter.input("areas_to_average")
    for column_per_hectare in columns_per_hectare:
        assert all(
            df_out_per_ha[f"{column_per_hectare}_per_ha"]
            == df_out[column_per_hectare]
            / (gdf_in_areas_to_average["geometry"].area / 10000)
        )


def test_calculate_flood_risk_per_ha_no_cols():
    """test fout afhaneling"""

    data_adapter_per_ha = helper_create_data_adapter(
        "test_calculate_flood_risk_per_ha.yaml"
    )
    # geen lijst
    data_adapter_per_ha.config.global_variables["CalculateFloodRisk"][
        "columns_per_hectare"
    ] = "test_string"
    calculate_flood_risk_per_ha = CalculateFloodRisk(data_adapter=data_adapter_per_ha)
    with pytest.raises(UserWarning):
        calculate_flood_risk_per_ha.run(
            input=[
                "segment_failure_probability",
                "flood_scenario_grids",
                "areas_to_average",
                "flood_risk_local_file",
            ],
            output="flood_risk_results",
        )

    # verkeerde kolomnamen - wel logging warning maar geen error
    data_adapter_per_ha.config.global_variables["CalculateFloodRisk"][
        "columns_per_hectare"
    ] = ["geen columns"]
    calculate_flood_risk_per_ha = CalculateFloodRisk(data_adapter=data_adapter_per_ha)
    calculate_flood_risk_per_ha.run(
        input=[
            "segment_failure_probability",
            "flood_scenario_grids",
            "areas_to_average",
            "flood_risk_local_file",
        ],
        output="flood_risk_results",
    )
    data_adapter_per_ha

    # helemaal niks
    data_adapter_per_ha.config.global_variables["CalculateFloodRisk"].pop(
        "columns_per_hectare"
    )
    calculate_flood_risk_per_ha = CalculateFloodRisk(data_adapter=data_adapter_per_ha)
    with pytest.raises(UserWarning):
        calculate_flood_risk_per_ha.run(
            input=[
                "segment_failure_probability",
                "flood_scenario_grids",
                "areas_to_average",
                "flood_risk_local_file",
            ],
            output="flood_risk_results",
        )
