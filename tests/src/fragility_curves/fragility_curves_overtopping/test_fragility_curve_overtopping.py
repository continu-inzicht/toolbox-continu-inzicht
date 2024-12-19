from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.fragility_curves import (
    FragilityCurveOvertopping,
)

# %%
slopes = {
    "profileid": 5,  # only for our reference
    "slopetypeid": [2, 2, 1, 1],
    "x": [-68.82, -12.59, -12.59, 0.00],
    "y": [10.00, 10.76, 10.76, 14.63],
    "r": 1,
    "damheight": 0,
}
bed_levels = {
    "sectionid": 11,  # only for our reference
    "direction": [
        22.5,
        45.0,
        67.5,
        90.0,
        112.5,
        135.0,
        157.5,
        180.0,
        202.5,
        225.0,
        247.5,
        270.0,
        292.5,
        315.0,
        337.5,
        360.0,
    ],
    "bedlevel": [
        10.3986,
        10.0646,
        9.52596,
        9.18148,
        8.87637,
        8.95625,
        9.52587,
        9.70184,
        9.60669,
        9.91175,
        9.88546,
        10.0923,
        10.3351,
        10.2347,
        10.3278,
        10.3542,
    ],
    "fetch": [
        83.2947,
        411.682,
        797.478,
        1078.28,
        1008.7,
        745.777,
        633.399,
        756.914,
        1153.69,
        1452.72,
        1115.41,
        654.088,
        237.057,
        77.6649,
        58.4134,
        59.5193,
    ],
}
default_options = {
    "windspeed": 20,
    "sectormin": 180.0,
    "sectorsize": 90.0,
    "effect": 0.0,
    "closing_situation": 0,
    "qcr": 10.0 / 1000,  # m3/s
    "orientation": 0,
    "crestlevel": 10,
    "dam": 0,
    "damheight": None,
}
parameters = [
    (
        20,
        180,
        90,
        0,
        True,
        10.0 / 1000,
        [
            2.57991838e-04,
            2.57991838e-04,
            2.93101369e-03,
            9.85472673e-03,
            1.73989878e-02,
            5.64815702e-02,
            6.71392857e-02,
            1.53670024e-01,
            1.81275845e-01,
            3.57981422e-01,
            5.33898361e-01,
            6.28391721e-01,
            7.85556006e-01,
            8.72086744e-01,
            9.24758408e-01,
            9.75677652e-01,
            9.83937876e-01,
            9.83937876e-01,
        ],
    ),
    (
        20,
        180,
        90,
        0,
        False,
        10.0 / 1000,
        [
            2.57991838e-04,
            2.57991838e-04,
            2.93101369e-03,
            2.93101369e-03,
            1.73989878e-02,
            3.69402790e-02,
            6.71392857e-02,
            1.53670024e-01,
            1.81275845e-01,
            3.57981422e-01,
            5.30126230e-01,
            7.06305608e-01,
            7.85556006e-01,
            8.72086744e-01,
            9.52364230e-01,
            9.75677652e-01,
            9.83937876e-01,
            9.83937876e-01,
        ],
    ),
    (
        20,
        180,
        90,
        0,
        False,
        "closed",
        [
            2.07342221e-06,
            5.55375711e-06,
            1.44122362e-05,
            3.62700743e-05,
            8.85746956e-05,
            2.09953363e-04,
            4.82974917e-04,
            1.07764668e-03,
            2.33005537e-03,
            4.87347422e-03,
            9.84793815e-03,
            1.91856314e-02,
            3.59373568e-02,
            6.44993070e-02,
            1.10452592e-01,
            1.79616436e-01,
            2.76015646e-01,
            3.98740732e-01,
        ],
    ),
    (
        20,
        180,
        90,
        0,
        True,
        "closed",
        [
            2.90283421e-06,
            7.36090664e-06,
            1.81758436e-05,
            4.37381132e-05,
            1.02614398e-04,
            2.34751485e-04,
            5.23520328e-04,
            1.13740659e-03,
            2.40424096e-03,
            4.93754656e-03,
            9.83677273e-03,
            1.89664645e-02,
            3.52850706e-02,
            6.30955630e-02,
            1.07936154e-01,
            1.75706696e-01,
            2.70687764e-01,
            3.92097266e-01,
        ],
    ),
]


# %%
def test_fragility_curves_wave_overtopping_csv():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=test_data_sets_path / "test_fragility_curve_overtopping.yaml"
    )
    config.lees_config()
    data_adapter = DataAdapter(config=config)
    wave_overtopping_fragility_curve = FragilityCurveOvertopping(
        data_adapter=data_adapter
    )
    wave_overtopping_fragility_curve.run(
        input=[
            "slopes",
            "profiles",
            "bedlevel_fetch",
        ],
        output="fragility_curves",
    )

    result = np.array(
        wave_overtopping_fragility_curve.df_out.failure_probability.to_list()
    )[41:59]
    expected = [
        2.90283421e-06,
        7.36090664e-06,
        1.81758436e-05,
        4.37381132e-05,
        1.02614398e-04,
        2.34751485e-04,
        5.23520328e-04,
        1.13740659e-03,
        2.40424096e-03,
        4.93754656e-03,
        9.83677273e-03,
        1.89664645e-02,
        3.52850706e-02,
        6.30955630e-02,
        1.07936154e-01,
        1.75706696e-01,
        2.70687764e-01,
        3.92097266e-01,
    ]
    assert np.isclose(result, expected).all()


def test_fragility_curves_wave_overtopping_vary_standard_values():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=test_data_sets_path / "test_fragility_curve_overtopping.yaml"
    )
    config.lees_config()
    data_adapter = DataAdapter(config=config)

    data_adapter.config.global_variables["FragilityCurveOvertopping"] = {}
    data_adapter.config.global_variables["FragilityCurveOvertopping"]["gh_onz_mu"] = (
        0.96
    )

    wave_overtopping_fragility_curve = FragilityCurveOvertopping(
        data_adapter=data_adapter
    )
    wave_overtopping_fragility_curve.run(
        input=[
            "slopes",
            "profiles",
            "bedlevel_fetch",
        ],
        output="fragility_curves",
    )

    result = np.array(
        wave_overtopping_fragility_curve.df_out.failure_probability.to_list()
    )[41:59]
    expected = [
        2.90283421e-06,
        7.36090664e-06,
        1.81758436e-05,
        4.37381132e-05,
        1.02614398e-04,
        2.34751485e-04,
        5.23520328e-04,
        1.13740659e-03,
        2.40424096e-03,
        4.93754656e-03,
        9.83677273e-03,
        1.89664645e-02,
        3.52850706e-02,
        6.30955630e-02,
        1.07936154e-01,
        1.75706696e-01,
        2.70687764e-01,
        3.92097266e-01,
    ]
    assert np.isclose(result, expected).all()


@pytest.mark.parametrize(
    "windspeed,sectormin,sectorsize,closing_situation,foreshore,qcr,expected",
    parameters,
)
def test_fragility_curves_wave_overtopping_parametric(
    windspeed, sectormin, sectorsize, closing_situation, foreshore, qcr, expected
):
    config = Config(config_path=Path.cwd())
    data_adapter = DataAdapter(config=config)
    data_adapter.config.global_variables["FragilityCurveOvertopping"] = {}

    df_slopes = pd.DataFrame(slopes)

    if not foreshore:
        df_slopes = df_slopes[df_slopes["slopetypeid"] == 1]

    profiles = {
        "sectionid": 11,  # only for our reference
        "crestlevel": 14.63,
        "orientation": 167,
        "dam": 0,
        "damheight": 0,
        "qcr": qcr,
        "windspeed": windspeed,
        "sectormin": sectormin,
        "sectorsize": sectorsize,
        "closing_situation": closing_situation,
    }
    df_profiles = pd.DataFrame(index=["values"], data=[profiles]).T
    df_bed_levels = pd.DataFrame(bed_levels)
    fragility_curves = pd.DataFrame()

    data_adapter.set_dataframe_adapter("slopes", df_slopes, if_not_exist="create")
    data_adapter.set_dataframe_adapter("profiles", df_profiles, if_not_exist="create")
    data_adapter.set_dataframe_adapter(
        "bed_levels", df_bed_levels, if_not_exist="create"
    )
    data_adapter.set_dataframe_adapter(
        "fragility_curves", fragility_curves, if_not_exist="create"
    )

    wave_overtopping_fragility_curve = FragilityCurveOvertopping(
        data_adapter=data_adapter
    )

    wave_overtopping_fragility_curve.run(
        input=["slopes", "profiles", "bed_levels"], output="fragility_curves"
    )
    result = wave_overtopping_fragility_curve.df_out.iloc[41:59][
        "failure_probability"
    ].to_list()
    assert np.isclose(result, expected).all()


# if __name__ == "__main__":
#     # test_fragility_curves_wave_overtopping_parametric(*parameters[0])
#     test_fragility_curves_wave_overtopping_csv()
