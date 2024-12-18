from toolbox_continu_inzicht.base.data_adapter import DataAdapter, Config
from pathlib import Path
import pandas as pd
from toolbox_continu_inzicht.fragility_curves import (
    ShiftFragilityCurveOvertopping,
    ChangeCrestHeightFragilityCurveOvertopping,
)

profiles = {
    "sectionid": 11,  # only for our reference
    "crestlevel": 14.63,
    "orientation": 167,
    "dam": 0,
    "damheight": 0,
    "qcr": 10 / 1000,
    "windspeed": 20,
    "sectormin": 180.0,
    "sectorsize": 90.0,
    "closing_situation": 0,
}
slopes = {
    "profileid": 5,  # only for our reference
    "slopetypeid": [1, 1, 2, 2],
    "x": [-12.59, 0.0, -68.82, -12.59],
    "y": [10.76, 14.63, 10.0, 10.76],
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


def setup_data_adapter():
    df_slopes = pd.DataFrame(slopes)
    df_profiles = pd.DataFrame(index=["values"], data=[profiles]).T
    df_bed_levels = pd.DataFrame(bed_levels)
    fragility_curves = pd.DataFrame()
    data_adapter = DataAdapter(config=Config(config_path=Path.cwd()))
    data_adapter.config.global_variables["FragilityCurveOvertopping"] = {}
    data_adapter.set_dataframe_adapter("slopes", df_slopes, if_not_exist="create")
    data_adapter.set_dataframe_adapter("profiles", df_profiles, if_not_exist="create")
    data_adapter.set_dataframe_adapter(
        "bed_levels", df_bed_levels, if_not_exist="create"
    )
    data_adapter.set_dataframe_adapter(
        "fragility_curves", fragility_curves, if_not_exist="create"
    )
    return data_adapter


def test_ShiftFragilityCurveOvertopping():
    data_adapter = setup_data_adapter()
    SFCC = ShiftFragilityCurveOvertopping(data_adapter=data_adapter)

    SFCC.run(
        input=["slopes", "profiles", "bed_levels"],
        output="fragility_curves",
        effect=0.5,
    )
    # TODO: assert the output


def test_ChangeCrestHeightFragilityCurveOvertopping():
    data_adapter = setup_data_adapter()
    CCHFCC = ChangeCrestHeightFragilityCurveOvertopping(data_adapter=data_adapter)

    CCHFCC.run(
        input=["slopes", "profiles", "bed_levels"],
        output="fragility_curves",
        effect=0.5,
    )
    # TODO: assert the output


if __name__ == "__main__":
    test_ChangeCrestHeightFragilityCurveOvertopping()
