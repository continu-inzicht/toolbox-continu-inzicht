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


def test_fragility_curve_wavedata():
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
                45: 0.9986696272514936,
                46: 0.9990196494868108,
                47: 0.9992763514940018,
                48: 0.9994679225755082,
                49: 0.9996080479961704,
                50: 0.999712318122948,
                51: 0.9997884309706284,
                52: 0.9998449345619188,
                53: 0.9998861140972078,
                54: 0.9999166250771254,
                55: 0.9999388335723852,
                56: 0.9999552619255846,
                57: 0.9999672887559596,
                58: 0.9999760332954022,
                59: 0.999982487771726,
            },
        }
    )
    df_actual = df_out.loc[df_expected.index, ["hydraulicload", "failure_probability"]]
    pd.testing.assert_frame_equal(df_actual, df_expected, rtol=1e-12)


def test_fragility_curve_wavedata_effects():
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


def test_waveval_uncertainty_overrides_applied_with_globals():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=test_data_sets_path
        / "test_fragility_curve_overtopping_wavedata.yaml"
    )
    config.lees_config()
    data_adapter = DataAdapter(config=config)
    options = data_adapter.config.global_variables.setdefault(
        "FragilityCurveOvertoppingWaveData", {}
    )
    options.update(
        {
            "closing_situation": 1,
            "gh_onz_mu": 0.5,
            "gh_onz_sigma": 0.05,
            "gp_onz_mu_tspec": 0.8,
            "gp_onz_sigma_tspec": 0.08,
        }
    )

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


def test_waveval_uncertainty_overrides_applied_without_globals():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=test_data_sets_path
        / "test_fragility_curve_overtopping_wavedata.yaml"
    )
    config.lees_config()
    data_adapter = DataAdapter(config=config)
    data_adapter.config.global_variables["FragilityCurveOvertoppingWaveData"] = {
        "closing_situation": 1
    }

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
