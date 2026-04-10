from pathlib import Path

import pandas as pd
import pytest

from toolbox_continu_inzicht import Config, DataAdapter
from toolbox_continu_inzicht.base.adapters.input.pastas_models import (
    input_pastas_models,
)
from toolbox_continu_inzicht.gwdi import GwdiInference
from toolbox_continu_inzicht.gwdi.processing.gwdi_model import GwdiModel

DATA_SETS_PATH = Path(__file__).parent / "data_sets"
_SMOKE_SINGLE_PRECIPITATION_PATH = DATA_SETS_PATH / "precipitation_smoke_single_fid.csv"
_SMOKE_SINGLE_EVAPORATION_PATH = DATA_SETS_PATH / "evaporation_smoke_single_fid.csv"
_SMOKE_SINGLE_LOCATION_INFO_PATH = DATA_SETS_PATH / "location_info_smoke_single.csv"
_SMOKE_SINGLE_STATS_MINIMA_PATH = DATA_SETS_PATH / "df_stats_minima_smoke_single.csv"
_SMOKE_SINGLE_EXPECTED_PATH = DATA_SETS_PATH / "gwdi_expected_smoke_single_fid.csv"
_SMOKE_SINGLE_MODELS_DIR = DATA_SETS_PATH / "pastas_models_smoke_single"


@pytest.fixture
def gwdi_data_adapter() -> DataAdapter:
    config = Config(config_path=DATA_SETS_PATH / "test_gwdi_config.yaml")
    config.lees_config()
    return DataAdapter(config=config)


@pytest.fixture
def pastas_models_data_adapter() -> DataAdapter:
    config = Config(config_path=DATA_SETS_PATH / "test_pastas_models_adapter.yaml")
    config.lees_config()
    return DataAdapter(config=config)


def _make_location_info() -> pd.DataFrame:
    return pd.DataFrame({"location": ["loc_a"], "position": ["01"]})


def _make_stats_minima() -> pd.DataFrame:
    return pd.DataFrame({"loc_a": [1.0, 2.0, 3.0]}, index=[1, 3, 10])


def _set_non_model_inputs(data_adapter: DataAdapter) -> None:
    data_adapter.set_dataframe_adapter("GwdiInputStatsMinima", _make_stats_minima())


def _gwdi_inputs() -> list[str]:
    return [
        "GwdiInputPrecipitation",
        "GwdiInputEvaporation",
        "GwdiInputLocationInfo",
        "GwdiInputPastasModels",
        "GwdiInputStatsMinima",
    ]


def _load_model_test_inputs() -> tuple[
    pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame
]:
    df_precipitation = pd.read_csv(DATA_SETS_PATH / "precipitation.csv")
    df_evaporation = pd.read_csv(DATA_SETS_PATH / "evaporation.csv")
    info_locaties = pd.read_csv(
        DATA_SETS_PATH / "location_info.csv", dtype={"position": "string"}
    )
    df_stats_minima = pd.read_csv(DATA_SETS_PATH / "df_stats_minima.csv", index_col=0)
    return df_precipitation, df_evaporation, info_locaties, df_stats_minima


def _load_inference_climate_inputs(
    multi_fid: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    suffix = "_multi" if multi_fid else ""
    df_precipitation = pd.read_csv(DATA_SETS_PATH / f"precipitation{suffix}.csv")
    df_evaporation = pd.read_csv(DATA_SETS_PATH / f"evaporation{suffix}.csv")
    return df_precipitation, df_evaporation


def test_gwdi_inference_single_fid_run(gwdi_data_adapter: DataAdapter):
    _set_non_model_inputs(gwdi_data_adapter)
    module = GwdiInference(data_adapter=gwdi_data_adapter)
    module.run(input=_gwdi_inputs(), output="GwdiOutput")

    assert not module.df_out.empty
    assert module.df_out["fid"].astype(int).unique().tolist() == [100]
    assert set(module.df_out.columns) == {
        "fid",
        "parameterid",
        "methodid",
        "datetime",
        "value",
        "peilbuisid",
    }


@pytest.mark.parametrize(
    "invalid_input", ["precipitation", "evaporation", "location_info"]
)
def test_gwdi_inference_fails_early_on_invalid_schema(
    gwdi_data_adapter: DataAdapter, invalid_input: str
):
    _set_non_model_inputs(gwdi_data_adapter)
    df_precipitation, df_evaporation = _load_inference_climate_inputs()
    location_info_override = None

    if invalid_input == "precipitation":
        df_precipitation = df_precipitation.rename(columns={"P": "value"})
    if invalid_input == "evaporation":
        df_evaporation = df_evaporation.rename(columns={"evaporation": "value"})
    if invalid_input == "location_info":
        location_info_override = _make_location_info().drop(columns=["position"])

    module = GwdiInference(data_adapter=gwdi_data_adapter)
    adapter_overrides = {
        "GwdiInputPrecipitation": {
            "type": "python",
            "dataframe_from_python": df_precipitation,
        },
        "GwdiInputEvaporation": {
            "type": "python",
            "dataframe_from_python": df_evaporation,
        },
    }
    if location_info_override is not None:
        adapter_overrides["GwdiInputLocationInfo"] = {
            "type": "python",
            "dataframe_from_python": location_info_override,
        }

    with gwdi_data_adapter.temporary_adapters(adapter_overrides):
        with pytest.raises(UserWarning, match="Kolommen komen niet overeen"):
            module.run(input=_gwdi_inputs(), output="GwdiOutput")


def test_gwdi_inference_runs_for_multiple_fids(
    gwdi_data_adapter: DataAdapter,
):
    _set_non_model_inputs(gwdi_data_adapter)
    df_precipitation, df_evaporation = _load_inference_climate_inputs(multi_fid=True)
    module = GwdiInference(data_adapter=gwdi_data_adapter)

    with gwdi_data_adapter.temporary_adapters(
        {
            "GwdiInputPrecipitation": {
                "type": "python",
                "dataframe_from_python": df_precipitation,
            },
            "GwdiInputEvaporation": {
                "type": "python",
                "dataframe_from_python": df_evaporation,
            },
        }
    ):
        module.run(input=_gwdi_inputs(), output="GwdiOutput")

    assert sorted(module.df_out["fid"].astype(int).unique().tolist()) == [
        100,
        101,
    ]


def test_compute_gwdi_fails_on_missing_model_keys():
    (
        df_precipitation,
        df_evaporation,
        info_locaties,
        df_stats_minima,
    ) = _load_model_test_inputs()
    dict_models = {"other_model_key": object()}

    with pytest.raises(ValueError, match="mist verplichte model-sleutels"):
        GwdiModel.compute_gwdi(
            df_precipitation=df_precipitation,
            df_evaporation=df_evaporation,
            info_locaties=info_locaties,
            dict_models=dict_models,
            df_stats_minima=df_stats_minima,
        )


def test_compute_gwdi_fails_on_mismatched_climate_rows(
    pastas_models_data_adapter: DataAdapter,
):
    (
        df_precipitation,
        df_evaporation,
        info_locaties,
        df_stats_minima,
    ) = _load_model_test_inputs()
    dict_models = pastas_models_data_adapter.input("PastasModels")
    df_evaporation = df_evaporation.iloc[:-1].copy()

    with pytest.raises(ValueError, match="moeten identieke"):
        GwdiModel.compute_gwdi(
            df_precipitation=df_precipitation,
            df_evaporation=df_evaporation,
            info_locaties=info_locaties,
            dict_models=dict_models,
            df_stats_minima=df_stats_minima,
        )


def test_compute_gwdi_fails_on_missing_stats_columns(
    pastas_models_data_adapter: DataAdapter,
):
    (
        df_precipitation,
        df_evaporation,
        info_locaties,
        df_stats_minima,
    ) = _load_model_test_inputs()
    dict_models = pastas_models_data_adapter.input("PastasModels")
    df_stats_minima = df_stats_minima.drop(columns=["loc_a"])

    with pytest.raises(ValueError, match="mist verplichte locatiekolommen"):
        GwdiModel.compute_gwdi(
            df_precipitation=df_precipitation,
            df_evaporation=df_evaporation,
            info_locaties=info_locaties,
            dict_models=dict_models,
            df_stats_minima=df_stats_minima,
        )


def test_gwdi_inference_fails_on_duplicate_locations(
    gwdi_data_adapter: DataAdapter,
):
    _set_non_model_inputs(gwdi_data_adapter)
    df_precipitation, df_evaporation = _load_inference_climate_inputs()
    module = GwdiInference(data_adapter=gwdi_data_adapter)
    duplicate_location_info = pd.DataFrame(
        {"location": ["loc_a", "loc_a"], "position": ["01", "02"]}
    )

    with gwdi_data_adapter.temporary_adapters(
        {
            "GwdiInputPrecipitation": {
                "type": "python",
                "dataframe_from_python": df_precipitation,
            },
            "GwdiInputEvaporation": {
                "type": "python",
                "dataframe_from_python": df_evaporation,
            },
            "GwdiInputLocationInfo": {
                "type": "python",
                "dataframe_from_python": duplicate_location_info,
            },
        }
    ):
        with pytest.raises(UserWarning, match="dubbele `location`-waarden"):
            module.run(input=_gwdi_inputs(), output="GwdiOutput")


def test_input_pastas_models_loads_pas_directory(
    pastas_models_data_adapter: DataAdapter,
):
    models = pastas_models_data_adapter.input("PastasModels")

    assert set(models.keys()) == {"loc_a_01_tarso"}
    model = models["loc_a_01_tarso"]
    assert model.__class__.__name__ == "Model"
    assert model.__class__.__module__.startswith("pastas.")


def test_input_pastas_models_fails_on_missing_directory(tmp_path: Path):
    missing_dir = tmp_path / "missing_models_dir"

    with pytest.raises(UserWarning, match="bestaat niet"):
        input_pastas_models({"abs_path": missing_dir})


def test_input_pastas_models_fails_when_path_is_file(tmp_path: Path):
    file_path = tmp_path / "not_a_directory.pas"
    file_path.write_text("placeholder", encoding="utf-8")

    with pytest.raises(UserWarning, match="map verwijzen"):
        input_pastas_models({"abs_path": file_path})


def test_input_pastas_models_fails_on_empty_directory(tmp_path: Path):
    empty_dir = tmp_path / "empty_models_dir"
    empty_dir.mkdir()

    with pytest.raises(UserWarning, match="Geen `.pas` modellen gevonden"):
        input_pastas_models({"abs_path": empty_dir})


@pytest.mark.parametrize("position_value", [pd.NA, ""])
def test_gwdi_inference_fails_on_empty_position(
    gwdi_data_adapter: DataAdapter, position_value
):
    _set_non_model_inputs(gwdi_data_adapter)
    df_precipitation, df_evaporation = _load_inference_climate_inputs()
    module = GwdiInference(data_adapter=gwdi_data_adapter)
    df_location_info = pd.DataFrame(
        {"location": ["loc_a"], "position": [position_value]}
    )

    with gwdi_data_adapter.temporary_adapters(
        {
            "GwdiInputPrecipitation": {
                "type": "python",
                "dataframe_from_python": df_precipitation,
            },
            "GwdiInputEvaporation": {
                "type": "python",
                "dataframe_from_python": df_evaporation,
            },
            "GwdiInputLocationInfo": {
                "type": "python",
                "dataframe_from_python": df_location_info,
            },
        }
    ):
        with pytest.raises(UserWarning, match="`position`"):
            module.run(input=_gwdi_inputs(), output="GwdiOutput")


def test_gwdi_single_fid_matches_standalone_smoke_baseline():
    df_precipitation = pd.read_csv(_SMOKE_SINGLE_PRECIPITATION_PATH)
    df_evaporation = pd.read_csv(_SMOKE_SINGLE_EVAPORATION_PATH)
    info_locaties = pd.read_csv(_SMOKE_SINGLE_LOCATION_INFO_PATH)
    dict_models = input_pastas_models({"abs_path": _SMOKE_SINGLE_MODELS_DIR})
    df_stats_minima = pd.read_csv(_SMOKE_SINGLE_STATS_MINIMA_PATH, index_col=0)
    df_expected = pd.read_csv(_SMOKE_SINGLE_EXPECTED_PATH)

    df_tbci = GwdiModel.compute_gwdi(
        df_precipitation=df_precipitation,
        df_evaporation=df_evaporation,
        info_locaties=info_locaties,
        dict_models=dict_models,
        df_stats_minima=df_stats_minima,
    )

    pd.testing.assert_frame_equal(
        df_tbci,
        df_expected,
        check_dtype=False,
        atol=5e-8,
        rtol=5e-8,
    )
