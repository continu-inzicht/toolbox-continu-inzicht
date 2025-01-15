from pathlib import Path
import pandas as pd
import numpy as np
from toolbox_continu_inzicht.base.data_adapter import Config, DataAdapter
from toolbox_continu_inzicht.fragility_curves import (
    FragilityCurvePipingFixedWaterlevelSimple,
    FragilityCurvesPiping,
)


### unused for now
# def test_fragility_curve_piping():
#     path = Path(__file__).parent / "data_sets"
#     config = Config(config_path=path / "test_fragility_curve_piping.yaml")
#     config.lees_config()
#     data_adapter = DataAdapter(config=config)

#     fragility_curve_piping_fixed_waterlevel = FragilityCurvePipingFixedWaterlevel(
#         data_adapter=data_adapter
#     )
#     fragility_curve_piping_fixed_waterlevel.run(
#         input=["probabilistic_input", "waterlevels"], output="fragility_curve"
#     )
#     df_exp_results = pd.read_csv(path / "full_test_output.csv")
#     assert np.isclose(
#         fragility_curve_piping_fixed_waterlevel.df_out.failure_probability.to_list(),
#         df_exp_results.prob_cond.to_list(),
#         atol=1e-8,
#         rtol=1e-8,
#     ).all()


# in de probabilstic pipig module zelf zit al een hoop tests om met de oude versei te vergelijken
# deze herhalen we hier niet
def test_fragility_curve_piping_simple():
    path = Path(__file__).parent / "data_sets"
    config = Config(config_path=path / "test_fragility_curve_piping.yaml")
    config.lees_config()
    data_adapter = DataAdapter(config=config)

    fragility_curve_piping_fixed_waterlevel = FragilityCurvePipingFixedWaterlevelSimple(
        data_adapter=data_adapter
    )
    fragility_curve_piping_fixed_waterlevel.run(
        input=["probabilistic_input", "waterlevels"], output="fragility_curve"
    )
    df_exp_results = pd.read_csv(path / "full_test_output_simple.csv")
    for attr, col_result in zip(
        [
            "df_result_uplift",
            "df_result_heave",
            "df_result_sellmeijer",
            "df_result_combined",
        ],
        [
            "prob_uplift",
            "prob_heave",
            "prob_sellmeijer",
            "prob_cond",
        ],
    ):
        assert np.isclose(
            fragility_curve_piping_fixed_waterlevel.__getattribute__(
                attr
            ).failure_probability.to_list(),
            df_exp_results[col_result].to_list(),
            atol=1e-8,
            rtol=1e-8,
        ).all()


def test_fragility_curve_piping_multiple_sections():
    path = Path(__file__).parent / "data_sets"
    config = Config(config_path=path / "test_fragility_multiple_curves_piping.yaml")
    config.lees_config()
    data_adapter = DataAdapter(config=config)

    fragility_curve_piping = FragilityCurvesPiping(data_adapter=data_adapter)
    fragility_curve_piping.run(
        input=["probabilistic_input", "waterlevels"], output="fragility_curves"
    )
    # compare to stored result
    df_exp_results = pd.read_csv(path / "test_output_multiple.csv")
    assert np.isclose(
        fragility_curve_piping.df_out.failure_probability.to_list(),
        df_exp_results.failure_probability.to_list(),
        atol=1e-8,
        rtol=1e-8,
    ).all()
