from pathlib import Path

import numpy as np
import pandas as pd
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.fragility_curves import (
    ChangeCrestHeightFragilityCurveOvertoppingWaveData,
    FragilityCurveOvertoppingWaveData,
    ShiftFragilityCurveOvertoppingWaveData,
)


def test_fragility_curves_wavedata():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=test_data_sets_path
        / "test_fragility_curve_overtopping_wavedata.yaml"
    )
    config.lees_config()
    data_adapter = DataAdapter(config=config)
    fragility_curve_overtopping = FragilityCurveOvertoppingWaveData(
        data_adapter=data_adapter
    )
    fragility_curve_overtopping.run(
        input=[
            "slopes",
            "profiles",
            "waveval_uncert",
            "waveval_id",
            "waveval",
        ],
        output="fragility_curves",
    )
    df_out = data_adapter.input("fragility_curves")

    # @TODO proper assert
    assert isinstance(df_out, pd.DataFrame)


def test_fragility_curves_wavedata_effects():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=test_data_sets_path
        / "test_fragility_curve_overtopping_wavedata.yaml"
    )
    config.lees_config()
    data_adapter = DataAdapter(config=config)
    input_val = [
        "slopes",
        "profiles",
        "waveval_uncert",
        "waveval_id",
        "waveval",
    ]

    fragility_curve_overtopping = FragilityCurveOvertoppingWaveData(
        data_adapter=data_adapter
    )
    fragility_curve_overtopping.run(input=input_val, output="fragility_curves")
    base_fp = fragility_curve_overtopping.failure_probability[40:60]

    shift_curve = ShiftFragilityCurveOvertoppingWaveData(data_adapter=data_adapter)
    shift_curve.run(input=input_val, output="fragility_curves", effect=0.5)
    shifted_fp = shift_curve.failure_probability[40:60]

    assert not np.allclose(base_fp, shifted_fp)

    change_crest_curve = ChangeCrestHeightFragilityCurveOvertoppingWaveData(
        data_adapter=data_adapter
    )
    change_crest_curve.run(input=input_val, output="fragility_curves", effect=0.5)
    crest_fp = change_crest_curve.failure_probability[40:60]

    assert not np.allclose(base_fp, crest_fp)
