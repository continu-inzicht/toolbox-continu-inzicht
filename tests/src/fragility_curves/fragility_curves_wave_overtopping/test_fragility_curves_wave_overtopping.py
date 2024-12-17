from pathlib import Path

import pandas as pd
import pytest

from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.fragility_curves import (
    CreateFragilityCurvesWaveOvertopping,
)


def test_fragility_curves_wave_overtopping_start_to_end():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=test_data_sets_path / "test_fragility_curves_wave_overtopping.yaml"
    )
    config.lees_config()
    data_adapter = DataAdapter(config=config)
    wave_overtopping_fragility_curve = CreateFragilityCurvesWaveOvertopping(
        data_adapter=data_adapter
    )
    wave_overtopping_fragility_curve.run(
        input=["slopes", "bedlevel_fetch"], output="fragility_curves"
    )
    assert len(wave_overtopping_fragility_curve.df_out) == 405


@pytest.mark.parametrize(
    "windspeed,sectormin,sectorsize,closing_situation,foreshore,prob_qcr",
    [(20, 0, 90, 0, True, True)],
)
def test_fragility_curves_wave_overtopping_no_foreshore(
    windspeed, sectormin, sectorsize, closing_situation, foreshore, prob_qcr
):
    config = Config(config_path=Path.cwd())
    data_adapter = DataAdapter(config=config)

    slopes = {
        "profileid": 5,
        "slopetypeid": [1, 1, 2, 2],
        "x": [-12.59, 0.0, -68.82, -12.59],
        "y": [10.76, 14.63, 10.0, 10.76],
        "r": 1,
        "damheight": 0,
    }
    df_slopes = pd.DataFrame(slopes)

    if not foreshore:
        df_slopes = df_slopes[df_slopes["slopetypeid"] == 1]

    profiles = {
        "id": [5],
        "name": ["47-1_dp071+050-dp072+098"],
        "sectionid": [11],
        "crestlevel": [14.63],
        "orientation": [167],
        "dam": [0],
        "damheight": [0],
        "prob_qcr": [prob_qcr],
        "qcr_dist": ["closed"],
        "qcr": [10.0],
        "qcr_stbi": [0],
    }
    df_profiles = pd.DataFrame(profiles)

    bed_levels = {
        "sectionid": 11,
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

    data_adapter.config.global_variables = {
        "CreateFragilityCurvesWaveOvertopping": {
            "windspeed": windspeed,
            "sectormin": sectormin,
            "sectorsize": sectorsize,
            "closing_situation": closing_situation,
        }
    }

    wave_overtopping_fragility_curve = CreateFragilityCurvesWaveOvertopping(
        data_adapter=data_adapter
    )

    wave_overtopping_fragility_curve.run(
        input=["slopes", "bed_levels"], output="fragility_curves"
    )
    print(wave_overtopping_fragility_curve.df_out)


if __name__ == "__main__":
    test_fragility_curves_wave_overtopping_start_to_end()
