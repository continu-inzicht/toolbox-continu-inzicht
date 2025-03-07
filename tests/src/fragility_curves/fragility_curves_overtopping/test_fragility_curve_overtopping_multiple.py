from pathlib import Path

import numpy as np

from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.fragility_curves import (
    FragilityCurveOvertoppingMultiple,
)


def test_fragility_curves_wave_overtopping_multiple():
    """Test de functie FragilityCurveOvertoppingMultiple met meerdere profielen"""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=test_data_sets_path / "test_fragility_curves_overtopping.yaml"
    )
    config.lees_config()
    data_adapter = DataAdapter(config=config)
    wave_overtopping_fragility_curve = FragilityCurveOvertoppingMultiple(
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

    result = wave_overtopping_fragility_curve.df_out.failure_probability[41:59]
    expected = [
        6.784028389491124e-06,
        1.619272152012685e-05,
        3.7615477997676235e-05,
        8.510181697582306e-05,
        0.00018761010726481227,
        0.0004031123993423229,
        0.0008441376765488336,
        0.0017219099317282926,
        0.0034184210373469953,
        0.006595602535647748,
        0.012342780111844892,
        0.02237558957562456,
        0.039220007811807206,
        0.06631714534147362,
        0.1078712617664665,
        0.16822136414448627,
        0.2505993808444639,
        0.3553930566770582,
    ]
    assert np.isclose(result, expected).all()
