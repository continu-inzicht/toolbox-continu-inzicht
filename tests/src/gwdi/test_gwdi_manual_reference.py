import os

import pandas as pd
import pytest
from pathlib import Path

from toolbox_continu_inzicht import Config, DataAdapter
from toolbox_continu_inzicht.gwdi import (
    GwdiInference,
    GwdiKnmiRetrieval,
    GwdiWiwbRetrieval,
)


EXPECTED_COLUMNS = [
    "fid",
    "parameterid",
    "methodid",
    "datetime",
    "value",
    "peilbuisid",
]
RETRIEVAL_COLUMNS = ["time", "fid"]


def _normalize_gwdi_output(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.loc[:, EXPECTED_COLUMNS].copy()
    normalized["fid"] = normalized["fid"].astype(int)
    normalized["parameterid"] = normalized["parameterid"].astype(int)
    normalized["methodid"] = normalized["methodid"].astype(int)
    normalized["datetime"] = normalized["datetime"].astype("int64")
    normalized["value"] = normalized["value"].astype(float)
    normalized["peilbuisid"] = normalized["peilbuisid"].astype(int)
    return normalized.sort_values(EXPECTED_COLUMNS).reset_index(drop=True)


def _normalize_climate_output(df: pd.DataFrame, value_column: str) -> pd.DataFrame:
    normalized = df.loc[:, [*RETRIEVAL_COLUMNS, value_column]].copy()
    normalized["time"] = pd.to_datetime(normalized["time"])
    normalized["fid"] = normalized["fid"].astype(int)
    normalized[value_column] = normalized[value_column].astype(float)
    return normalized.sort_values(RETRIEVAL_COLUMNS).reset_index(drop=True)


def _reference_data_adapter() -> DataAdapter:
    cfg_path = Path(__file__).parent / "data_sets/reference_original"
    config = Config(config_path=cfg_path / "test_gwdi_manual_reference_config.yaml")
    config.lees_config()
    return DataAdapter(config=config)


def _reference_retrieval_window(
    data_adapter: DataAdapter,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    window = data_adapter.config.global_variables["GwdiReferenceRetrievalTest"]
    return pd.Timestamp(window["start_date"]), pd.Timestamp(window["end_date"])


def _expected_reference_climate(
    data_adapter: DataAdapter,
    value_column: str,
) -> pd.DataFrame:
    adapter_name = (
        "gwdi_expected_precipitation"
        if value_column == "P"
        else "gwdi_expected_evaporation"
    )
    window_start, window_end = _reference_retrieval_window(data_adapter)
    expected = data_adapter.input(adapter_name)
    expected = _normalize_climate_output(expected, value_column)
    return expected[expected["time"].between(window_start, window_end)].reset_index(
        drop=True
    )


def test_gwdi_inference_matches_static_original_reference_fixture():
    data_adapter = _reference_data_adapter()
    df_expected = data_adapter.input("gwdi_expected_output")

    module = GwdiInference(data_adapter=data_adapter)
    module.run(
        input=[
            "gwdi_input_precipitation",
            "gwdi_input_evaporation",
            "gwdi_input_location_info",
            "gwdi_input_pastas_models",
            "gwdi_input_stats_minima",
        ],
        output="gwdi_output",
    )

    assert module.df_out is not None
    assert list(module.df_out.columns) == EXPECTED_COLUMNS

    pd.testing.assert_frame_equal(
        _normalize_gwdi_output(module.df_out),
        _normalize_gwdi_output(df_expected),
        check_dtype=False,
        atol=5e-8,
        rtol=5e-8,
    )


@pytest.mark.skipif(
    os.getenv("WIWB_KEY") is None,
    reason="Omgevingsvariabele `WIWB_KEY` ontbreekt.",
)
@pytest.mark.skipif(
    os.getenv("WIWB_CLIENT_ID") is None,
    reason="Omgevingsvariabele `WIWB_CLIENT_ID` ontbreekt.",
)
def test_gwdi_reference_positions_retrieve_precipitation():
    data_adapter = _reference_data_adapter()
    window_start, window_end = _reference_retrieval_window(data_adapter)
    module = GwdiWiwbRetrieval(data_adapter=data_adapter)
    options = module._combined_options()
    locations = module.normalize_locations_table(
        module.data_adapter.input("gwdi_input_locations", schema=module.input_schema)
    )

    # The static fixture stores the WIWB daily end labels directly. Retrieve one
    # source day before the comparison window so the first fixture label is
    # available, then compare only the intended three fixture dates.
    df_out = module._retrieve_dataset(
        df_locations=locations,
        publish_start=window_start - pd.Timedelta(days=1),
        publish_end=window_end,
        options=options,
    )
    df_out = df_out[df_out["time"].between(window_start, window_end)]
    df_out = df_out.reset_index(drop=True)

    assert locations["fid"].tolist() == [220748]
    assert df_out.columns.tolist() == [*RETRIEVAL_COLUMNS, "P"]
    assert len(df_out) == 10
    assert df_out["fid"].astype(int).unique().tolist() == [220748]
    assert df_out["time"].tolist() == list(pd.date_range(window_start, window_end))
    assert not df_out[RETRIEVAL_COLUMNS].duplicated().any()

    pd.testing.assert_frame_equal(
        _normalize_climate_output(df_out, "P"),
        _expected_reference_climate(data_adapter, "P"),
        check_dtype=False,
        rtol=0.0,
        atol=1e-6,
    )


@pytest.mark.skipif(
    os.getenv("KNMI_API_KEY") is None,
    reason="Omgevingsvariabele `KNMI_API_KEY` ontbreekt.",
)
def test_gwdi_reference_positions_retrieve_evaporation():
    data_adapter = _reference_data_adapter()
    window_start, window_end = _reference_retrieval_window(data_adapter)
    module = GwdiKnmiRetrieval(data_adapter=data_adapter)
    module.run(input="gwdi_input_locations", output="gwdi_output_evaporation")

    assert module.df_in is not None
    assert module.df_in["fid"].tolist() == [220748]
    assert module.df_out is not None
    assert module.df_out.columns.tolist() == [*RETRIEVAL_COLUMNS, "evaporation"]
    assert len(module.df_out) == 10
    assert module.df_out["fid"].astype(int).unique().tolist() == [220748]
    assert module.df_out["time"].tolist() == list(
        pd.date_range(window_start, window_end)
    )
    assert not module.df_out[RETRIEVAL_COLUMNS].duplicated().any()

    pd.testing.assert_frame_equal(
        _normalize_climate_output(module.df_out, "evaporation"),
        _expected_reference_climate(data_adapter, "evaporation"),
        check_dtype=False,
        rtol=0.0,
        atol=1e-6,
    )
