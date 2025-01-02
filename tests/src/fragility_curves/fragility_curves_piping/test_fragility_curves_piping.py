from pathlib import Path
import pandas as pd
import numpy as np
from toolbox_continu_inzicht.base.data_adapter import Config, DataAdapter
from toolbox_continu_inzicht.fragility_curves import (
    FragilityCurvePipingFixedWaterlevel,
    FragilityCurvePipingFixedWaterlevelCombined,
)


def test_fragility_curve_piping():
    path = Path(__file__).parent / "data_sets"
    config = Config(config_path=path / "test_fragility_curve_piping.yaml")
    config.lees_config()
    data_adapter = DataAdapter(config=config)

    fragility_curve_piping_fixed_waterlevel = FragilityCurvePipingFixedWaterlevel(
        data_adapter=data_adapter
    )
    fragility_curve_piping_fixed_waterlevel.run(
        input=["probabilistic_input", "waterlevels"], output="fragility_curve"
    )
    # compare to stored result
    df_results = pd.read_csv(path / "result_test_fragility_curve_piping.csv")
    assert np.isclose(
        fragility_curve_piping_fixed_waterlevel.df_out.failure_probability.to_list(),
        df_results.failure_probability.to_list(),
    ).all()


def test_fragility_curve_pipingCombined():
    path = Path(__file__).parent / "data_sets"
    config = Config(config_path=path / "test_fragility_curve_piping.yaml")
    config.lees_config()
    data_adapter = DataAdapter(config=config)

    fragility_curve_piping_fixed_waterlevel = (
        FragilityCurvePipingFixedWaterlevelCombined(data_adapter=data_adapter)
    )
    fragility_curve_piping_fixed_waterlevel.run(
        input=["probabilistic_input", "waterlevels"], output="fragility_curve"
    )
    # compare to stored result
    df_results = pd.read_csv(path / "results_test_fragility_curve_piping_combined.csv")
    for attr, col_result in zip(
        [
            "df_result_uplift",
            "df_result_heave",
            "df_result_sellmeijer",
            "df_result_combined",
        ],
        [
            "uplift",
            "heave",
            "sellmeijer",
            "combined",
        ],
    ):
        assert np.isclose(
            fragility_curve_piping_fixed_waterlevel.__getattribute__(
                attr
            ).failure_probability.to_list(),
            df_results[col_result].to_list(),
        ).all()
