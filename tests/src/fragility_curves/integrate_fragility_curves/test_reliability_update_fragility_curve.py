from pathlib import Path

import numpy as np
import pytest
from toolbox_continu_inzicht import Config, DataAdapter
from toolbox_continu_inzicht import FragilityCurve


def init():
    path = Path(__file__).parent / "data_sets"
    config = Config(config_path=path / "test_integrate_fragility_curves.yaml")
    config.lees_config()
    data_adapter = DataAdapter(config=config)
    return data_adapter


def tests_reliability_update_empty():
    """Tests of de reliability update werkt met een té lage update level en geen dvertrouwen, we verwachten geen aanpassingen"""
    data_adapter = init()
    # onrealistische update_level
    update_level = -10
    trust_factor = 1
    fragility_curve = FragilityCurve(data_adapter=data_adapter)
    fragility_curve.load("fragility_curve_csv")
    initial_value = fragility_curve.failure_probability.copy()
    with pytest.warns(UserWarning):
        fragility_curve.reliability_update(
            update_level=update_level, trust_factor=trust_factor
        )
    assert np.allclose(fragility_curve.failure_probability, initial_value)


def tests_reliability_update_no_trust():
    """Tests of de reliability update werkt met een hoge update level en geen vertrouwen, we verwachten geen aanpassingen"""
    data_adapter = init()
    # no trust is no update
    update_level = 14.3
    trust_factor = 0
    fragility_curve = FragilityCurve(data_adapter=data_adapter)
    fragility_curve.load("fragility_curve_csv")
    initial_value = fragility_curve.failure_probability.copy()
    fragility_curve.reliability_update(
        update_level=update_level, trust_factor=trust_factor
    )
    assert np.allclose(fragility_curve.failure_probability, initial_value)


def tests_reliability_update_full_trust_low_update():
    """Tests of de reliability update werkt met een lage update level en volledig vertrouwen, we verwachten geen aanpassingen"""
    data_adapter = init()
    # trust is update,
    update_level = 2.3
    trust_factor = 1
    fragility_curve = FragilityCurve(data_adapter=data_adapter)
    fragility_curve.load("fragility_curve_csv")
    initial_value = fragility_curve.failure_probability.copy()
    fragility_curve.reliability_update(
        update_level=update_level, trust_factor=trust_factor
    )
    assert np.allclose(fragility_curve.failure_probability, initial_value)


expected_result = np.array(
    [
        0.05132157,
        0.168546,
        0.27337265,
        0.36677762,
        0.44971413,
        0.52310313,
        0.58782577,
        0.64471779,
        0.69245017,
        0.73601305,
        0.77396061,
        0.80691906,
        0.83546169,
        0.86011036,
        0.88133755,
        0.89956884,
        0.91518572,
        0.92852857,
        0.93989972,
        0.9495666,
        0.95776478,
        0.96470098,
        0.97055588,
        0.97548687,
        0.97963056,
        0.98310512,
        0.98601241,
        0.98844002,
    ]
)


def tests_reliability_update_full_trust_high_update():
    data_adapter = init()
    # trust is update
    update_level = 10.3
    trust_factor = 1
    fragility_curve = FragilityCurve(data_adapter=data_adapter)
    fragility_curve.load("fragility_curve_csv")
    initial_value = fragility_curve.failure_probability.copy()
    fragility_curve.reliability_update(
        update_level=update_level, trust_factor=trust_factor
    )

    assert not (np.allclose(fragility_curve.failure_probability, initial_value))
    assert np.isclose(
        fragility_curve.failure_probability[
            (fragility_curve.failure_probability > 1e-10)
            & (fragility_curve.failure_probability < 0.99)
        ],
        expected_result,
    ).all()
