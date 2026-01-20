from pathlib import Path

import numpy as np
import pandas as pd
import pytest
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

    df_expected = pd.DataFrame(
        {
            "hydraulicload": {
                45: 13.900000000000018,
                46: 13.95000000000002,
                47: 14.00000000000002,
                48: 14.05000000000002,
                49: 14.10000000000002,
                50: 14.150000000000022,
                51: 14.200000000000022,
                52: 14.250000000000025,
                53: 14.300000000000024,
                54: 14.350000000000025,
                55: 14.400000000000023,
                56: 14.450000000000026,
                57: 14.500000000000028,
                58: 14.550000000000027,
                59: 14.600000000000028,
            },
            "failure_probability": {
                45: 0.9856298185333292,
                46: 0.98727710716003,
                47: 0.9887935175533384,
                48: 0.990199917728394,
                49: 0.9914985065751,
                50: 0.9926953244176774,
                51: 0.9937871163380972,
                52: 0.9947748288639996,
                53: 0.9956560922037287,
                54: 0.9964329002732534,
                55: 0.9971069700341127,
                56: 0.99768372250868,
                57: 0.9981693426946874,
                58: 0.9985717609805274,
                59: 0.9989002603490952,
            },
        }
    )
    df_actual = df_out.loc[df_expected.index, ["hydraulicload", "failure_probability"]]
    pd.testing.assert_frame_equal(df_actual, df_expected, rtol=1e-12)


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


def test_waveval_uncertainty_overrides_applied():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=test_data_sets_path
        / "test_fragility_curve_overtopping_wavedata.yaml"
    )
    config.lees_config()
    data_adapter = DataAdapter(config=config)
    data_adapter.config.global_variables["FragilityCurveOvertoppingWaveData"][
        "closing_situation"
    ] = 1

    fragility_curve_overtopping = FragilityCurveOvertoppingWaveData(
        data_adapter=data_adapter
    )
    fragility_curve_overtopping._load_inputs(
        [
            "slopes",
            "profiles",
            "waveval_uncert",
            "waveval_id",
            "waveval",
        ]
    )
    options = fragility_curve_overtopping._build_options()

    assert options["gh_onz_mu"] == pytest.approx(0.94)
    assert options["gh_onz_sigma"] == pytest.approx(0.15)
    assert options["gp_onz_mu_tspec"] == pytest.approx(0.89)
    assert options["gp_onz_sigma_tspec"] == pytest.approx(0.04)
