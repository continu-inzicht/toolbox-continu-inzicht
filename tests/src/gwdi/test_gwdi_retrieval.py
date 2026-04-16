import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from dotenv import load_dotenv

from toolbox_continu_inzicht import Config, DataAdapter
from toolbox_continu_inzicht.gwdi import GwdiKnmiRetrieval, GwdiWiwbRetrieval
from toolbox_continu_inzicht.gwdi.retrieval.gwdi_base_retrieval import (
    GwdiWiwbRetrievalBase,
)

load_dotenv()

DATA_SETS_PATH = Path(__file__).parent / "data_sets" / "retrieval"
CONFIG_PATH = DATA_SETS_PATH / "test_gwdi_retrieval_config.yaml"


def _load_data_adapter() -> DataAdapter:
    config = Config(config_path=CONFIG_PATH)
    config.lees_config()
    return DataAdapter(config=config)


def test_normalize_locations_duplicate_fid_raises():
    duplicate_locations = pd.DataFrame(
        {
            "fid": [100, 100],
            "name": ["a", "b"],
            "x": [4.9, 5.0],
            "y": [52.3, 52.4],
        }
    )

    with pytest.raises(UserWarning, match="dubbele `fid`"):
        GwdiWiwbRetrievalBase.normalize_locations_table(duplicate_locations)


def test_resolve_publish_window_defaults_to_calc_time_minus_one():
    publish_start, publish_end = GwdiWiwbRetrievalBase.resolve_publish_window(
        calc_time="2025-06-12 08:00:00",
        publish_days=3,
    )

    assert publish_end == pd.Timestamp("2025-06-11")
    assert publish_start == pd.Timestamp("2025-06-09")


def test_resolve_publish_window_target_date_overrides_calc_time():
    publish_start, publish_end = GwdiWiwbRetrievalBase.resolve_publish_window(
        calc_time="2025-06-12 08:00:00",
        publish_days=2,
        target_date="2025-06-01",
    )

    assert publish_end == pd.Timestamp("2025-06-01")
    assert publish_start == pd.Timestamp("2025-05-31")


def test_resolve_publish_window_invalid_publish_days_raise():
    with pytest.raises(UserWarning, match="publish_days"):
        GwdiWiwbRetrievalBase.resolve_publish_window(
            calc_time="2025-06-12 08:00:00",
            publish_days=0,
        )
    with pytest.raises(UserWarning, match="publish_days"):
        GwdiWiwbRetrievalBase.resolve_publish_window(
            calc_time="2025-06-12 08:00:00",
            publish_days=-1,
        )


def test_sample_points_from_dataset_subsets_requested_fids():
    locations = pd.DataFrame(
        {
            "fid": [101, 100],
            "name": ["loc_101", "loc_100"],
            "x": [4.91, 4.90],
            "y": [52.38, 52.37],
        }
    )
    dataset = xr.Dataset(
        data_vars={
            "P": (
                ("time", "y", "x"),
                np.array(
                    [
                        [[10.0, 20.0], [30.0, 40.0]],
                        [[11.0, 21.0], [31.0, 41.0]],
                    ],
                    dtype=float,
                ),
            )
        },
        coords={
            "time": pd.date_range("2025-06-01", periods=2, freq="D"),
            "x": [4.90, 4.91],
            "y": [52.37, 52.38],
        },
    )

    sampled = GwdiWiwbRetrievalBase.sample_points_from_dataset(
        dataset=dataset,
        variable_name="P",
        locations_table=locations,
        window_start=pd.Timestamp("2025-06-02"),
        window_end=pd.Timestamp("2025-06-02"),
        time_name="time",
        x_name="x",
        y_name="y",
    )

    assert sampled["P"].shape == (1, 2)
    assert sampled["fid"].to_numpy().tolist() == [101, 100]
    assert pd.to_datetime(sampled["time"].to_numpy()).tolist() == [
        pd.Timestamp("2025-06-02"),
    ]
    assert sampled["P"].to_numpy().tolist() == [[41.0, 11.0]]


def test_sample_points_from_dataset_missing_coordinates_raises():
    locations = pd.DataFrame(
        {
            "fid": [999, 998],
            "name": ["loc_999", "loc_998"],
            "x": [4.99, 4.98],
            "y": [52.39, 52.40],
        }
    )
    dataset = xr.Dataset(
        data_vars={"P": (("time", "fid"), np.array([[1.0], [2.0]], dtype=float))},
        coords={
            "time": pd.date_range("2025-06-01", periods=2, freq="D"),
            "fid": [100],
        },
    )

    with pytest.raises(ValueError, match="coördinaten"):
        GwdiWiwbRetrievalBase.sample_points_from_dataset(
            dataset=dataset,
            variable_name="P",
            locations_table=locations,
            window_start=pd.Timestamp("2025-06-01"),
            window_end=pd.Timestamp("2025-06-02"),
            time_name="time",
            x_name="x",
            y_name="y",
        )


def test_sample_points_from_dataset_uses_coordinate_grid():
    locations = pd.DataFrame(
        {
            "fid": [201, 200],
            "name": ["loc_201", "loc_200"],
            "x": [4.92, 4.90],
            "y": [52.39, 52.37],
        }
    )
    dataset = xr.Dataset(
        data_vars={
            "P": (
                ("time", "y", "x"),
                np.array(
                    [
                        [[10.0, 20.0], [30.0, 40.0]],
                        [[11.0, 21.0], [31.0, 41.0]],
                    ],
                    dtype=float,
                ),
            )
        },
        coords={
            "time": pd.date_range("2025-06-01", periods=2, freq="D"),
            "x": [4.90, 4.92],
            "y": [52.37, 52.39],
        },
    )

    sampled = GwdiWiwbRetrievalBase.sample_points_from_dataset(
        dataset=dataset,
        variable_name="P",
        locations_table=locations,
        window_start=pd.Timestamp("2025-06-01"),
        window_end=pd.Timestamp("2025-06-01"),
        time_name="time",
        x_name="x",
        y_name="y",
    )

    assert sampled["P"].shape == (1, 2)
    assert sampled["fid"].to_numpy().tolist() == [201, 200]
    assert sampled["P"].sel(fid=201).to_numpy().tolist() == [40.0]
    assert sampled["P"].sel(fid=200).to_numpy().tolist() == [10.0]


def test_infer_dataset_crs_from_proj4_grid_mapping():
    dataset = xr.Dataset(
        data_vars={
            "evaporation": (("time", "y", "x"), np.ones((1, 1, 1), dtype=float)),
            "projection": ((), 0),
        },
        coords={"time": [pd.Timestamp("2025-06-01")], "x": [155000.0], "y": [463000.0]},
    )
    dataset["evaporation"].attrs["grid_mapping"] = "projection"
    dataset["projection"].attrs["proj4_params"] = (
        "+proj=sterea +lat_0=52.15616055555555 +lon_0=5.38763888888889 "
        "+k=0.9999079 +x_0=155000 +y_0=463000 +ellps=bessel +units=m +no_defs"
    )

    inferred = GwdiWiwbRetrievalBase.infer_dataset_crs(
        dataset=dataset,
        data_array=dataset["evaporation"],
    )
    assert isinstance(inferred, str)
    assert "+proj=sterea" in inferred


def test_transform_locations_to_dataset_crs_knmi_rd():
    dataset = xr.Dataset(
        data_vars={
            "evaporation": (("time", "y", "x"), np.ones((1, 1, 1), dtype=float)),
            "projection": ((), 0),
        },
        coords={"time": [pd.Timestamp("2025-06-01")], "x": [155000.0], "y": [463000.0]},
    )
    dataset["evaporation"].attrs["grid_mapping"] = "projection"
    dataset["projection"].attrs["proj4_params"] = (
        "+proj=sterea +lat_0=52.15616055555555 +lon_0=5.38763888888889 "
        "+k=0.9999079 +x_0=155000 +y_0=463000 +ellps=bessel +units=m +no_defs"
    )
    locations = pd.DataFrame({"fid": [1], "name": ["loc"], "x": [4.9], "y": [52.37]})

    transformed = GwdiWiwbRetrievalBase.transform_locations_to_dataset_crs(
        locations_table=locations,
        dataset=dataset,
        data_array=dataset["evaporation"],
        input_crs="EPSG:4326",
        x_name="x",
        y_name="y",
    )

    assert transformed["x"].iloc[0] == pytest.approx(121819.0, rel=0.0, abs=500.0)
    assert transformed["y"].iloc[0] == pytest.approx(487014.0, rel=0.0, abs=500.0)


def test_wiwb_run_returns_sorted_grid_without_resampling(monkeypatch):
    data_adapter = _load_data_adapter()
    module = GwdiWiwbRetrieval(data_adapter=data_adapter)

    def _fake_retrieve_dataset(
        self, df_locations, publish_start, publish_end, options, session=None
    ):
        return pd.DataFrame(
            {
                "time": pd.to_datetime(
                    ["2025-06-12", "2025-06-11", "2025-06-10", "2025-06-10"]
                ),
                "fid": [100, 100, 100, 101],
                "P": [3.0, 2.0, 1.0, 10.0],
            }
        )

    monkeypatch.setattr(
        GwdiWiwbRetrieval,
        "_retrieve_dataset",
        _fake_retrieve_dataset,
    )

    module.run(input="gwdi_input_locations", output="gwdi_output_precipitation")

    assert module.df_out is not None
    assert module.df_out.columns.tolist() == ["time", "fid", "P"]
    assert not module.df_out[["time", "fid"]].duplicated().any()
    assert module.df_out["time"].tolist() == [
        pd.Timestamp("2025-06-10"),
        pd.Timestamp("2025-06-10"),
        pd.Timestamp("2025-06-11"),
        pd.Timestamp("2025-06-12"),
    ]
    assert module.df_out["fid"].tolist() == [100, 101, 100, 100]
    assert module.df_out["P"].tolist() == [1.0, 10.0, 2.0, 3.0]


def test_knmi_run_returns_sorted_grid_without_resampling(monkeypatch):
    data_adapter = _load_data_adapter()
    module = GwdiKnmiRetrieval(data_adapter=data_adapter)

    def _fake_retrieve_dataset(
        self, df_locations, publish_start, publish_end, options, session=None
    ):
        return pd.DataFrame(
            {
                "time": pd.to_datetime(
                    ["2025-06-12", "2025-06-11", "2025-06-10", "2025-06-10"]
                ),
                "fid": [100, 100, 100, 101],
                "evaporation": [0.3, 0.2, 0.1, 1.0],
            }
        )

    monkeypatch.setattr(
        GwdiKnmiRetrieval,
        "_retrieve_dataset",
        _fake_retrieve_dataset,
    )

    module.run(input="gwdi_input_locations", output="gwdi_output_evaporation")

    assert module.df_out is not None
    assert module.df_out.columns.tolist() == ["time", "fid", "evaporation"]
    assert not module.df_out[["time", "fid"]].duplicated().any()
    assert module.df_out["time"].tolist() == [
        pd.Timestamp("2025-06-10"),
        pd.Timestamp("2025-06-10"),
        pd.Timestamp("2025-06-11"),
        pd.Timestamp("2025-06-12"),
    ]
    assert module.df_out["fid"].tolist() == [100, 101, 100, 100]
    assert module.df_out["evaporation"].tolist() == pytest.approx([0.1, 1.0, 0.2, 0.3])


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true",
    reason="Real-world integration test draait niet in GitHub Actions.",
)
@pytest.mark.skipif(
    os.getenv("WIWB_KEY") is None,
    reason="Omgevingsvariabele `WIWB_KEY` ontbreekt.",
)
@pytest.mark.skipif(
    os.getenv("WIWB_CLIENT_ID") is None,
    reason="Omgevingsvariabele `WIWB_CLIENT_ID` ontbreekt.",
)
def test_wiwb_real_world_retrieval():
    module = GwdiWiwbRetrieval(data_adapter=_load_data_adapter())
    module.run(input="gwdi_input_locations", output="gwdi_output_precipitation")

    assert module.df_out is not None
    assert len(module.df_out) > 0
    assert {"time", "fid", "P"}.issubset(module.df_out.columns)
    assert not module.df_out[["time", "fid"]].duplicated().any()


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true",
    reason="Real-world integration test draait niet in GitHub Actions.",
)
@pytest.mark.skipif(
    os.getenv("KNMI_API_KEY") is None,
    reason="Omgevingsvariabele `KNMI_API_KEY` ontbreekt.",
)
def test_knmi_real_world_retrieval():
    module = GwdiKnmiRetrieval(data_adapter=_load_data_adapter())
    module.run(input="gwdi_input_locations", output="gwdi_output_evaporation")

    assert module.df_out is not None
    assert len(module.df_out) > 0
    assert {"time", "fid", "evaporation"}.issubset(module.df_out.columns)
    assert not module.df_out[["time", "fid"]].duplicated().any()
