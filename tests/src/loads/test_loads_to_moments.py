from pathlib import Path
import os

from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.loads import LoadsToMoments
from test_loads_matroos import test_LoadsMatroos_noos


def test_run_LoadsToMoments_no_tide():
    # zorg dat we een recent bestand hebben
    test_LoadsMatroos_noos()
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=test_data_sets_path / "test_loads_to_moments_no_tide_config.yaml"
    )
    config.lees_config()

    data_adapter = DataAdapter(config=config)

    maxima = LoadsToMoments(data_adapter=data_adapter)
    maxima.run(input="waterstanden", output="maxima")
    df_out = maxima.df_out

    output_file = config.data_adapters["maxima"]["abs_path"]
    assert os.path.exists(output_file)
    assert df_out is not None
    for id in df_out["measurement_location_id"]:
        location = df_out[df_out["measurement_location_id"] == id]
        assert len(location) == len(config.global_variables["moments"])


def test_run_LoadsToMoments_tide():
    # zorg dat we een recent bestand hebben
    test_LoadsMatroos_noos()
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=test_data_sets_path / "test_loads_to_moments_tide_config.yaml"
    )
    config.lees_config()

    data_adapter = DataAdapter(config=config)

    maxima = LoadsToMoments(data_adapter=data_adapter)
    maxima.run(input="waterstanden", output="maxima")
    df_out = maxima.df_out

    output_file = config.data_adapters["maxima"]["abs_path"]
    assert os.path.exists(output_file)
    assert df_out is not None
    for id in df_out["measurement_location_id"]:
        location = df_out[df_out["measurement_location_id"] == id]
        assert len(location) == len(config.global_variables["moments"])
