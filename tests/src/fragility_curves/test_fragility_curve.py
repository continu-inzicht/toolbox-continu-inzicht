from pathlib import Path

import numpy as np

from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.base.fragility_curve import FragilityCurve


def _setup_fragility_curve(hydraulicload, failure_probability, enforce_monotonic):
    test_data_path = Path(__file__).parent / "combine_fragility_curves" / "data_sets"
    config = Config(config_path=test_data_path / "test_combine_fragility_curve.yaml")
    config.lees_config()
    data_adapter = DataAdapter(config=config)

    fc = FragilityCurve(
        data_adapter=data_adapter,
        hydraulicload=hydraulicload,
        failure_probability=failure_probability,
        enforce_monotonic=enforce_monotonic,
    )

    return fc


def test_fragility_curve_ignore_step():
    hydraulicload = np.array([1, 2, 2, 3])
    failure_probability = np.array([0, 1e-8, 1e-6, 1e-6])
    new_hydraulicload = np.hstack([0, np.linspace(1.97, 2.01, 5)])

    fc = _setup_fragility_curve(hydraulicload, failure_probability, False)
    fc.refine(new_hydraulicload, add_steps=False)
    data = fc.as_array()
    check_data = np.array(
        [
            [0.00000000e00, 0.00000000e00],
            [1.97000000e00, 1.73780083e-14],
            [1.98000000e00, 1.44543977e-12],
            [1.99000000e00, 1.20226443e-10],
            [2.00000000e00, 1.00000000e-08],
            [2.01000000e00, 1.00000000e-06],
        ]
    )

    assert np.allclose(check_data, data, atol=0, rtol=1e-8)


def test_fragility_curve_include_step():
    hydraulicload = np.array([1, 2, 2, 3])
    failure_probability = np.array([0, 1e-8, 1e-6, 1e-6])
    new_hydraulicload = np.hstack([0, np.linspace(1.97, 2.01, 5)])

    fc = _setup_fragility_curve(hydraulicload, failure_probability, False)
    fc.refine(new_hydraulicload, add_steps=True)
    data = fc.as_array()
    check_data = np.array(
        [
            [0.00000000e00, 0.00000000e00],
            [1.97000000e00, 1.73780083e-14],
            [1.98000000e00, 1.44543977e-12],
            [1.99000000e00, 1.20226443e-10],
            [2.00000000e00, 1.00000000e-08],
            [2.00000000e00, 1.00000000e-08],
            [2.00000000e00, 1.00000000e-06],
            [2.01000000e00, 1.00000000e-06],
        ]
    )

    assert np.allclose(check_data, data, atol=0, rtol=1e-8)


def test_fragility_curve_not_monotonic():
    hydraulicload = np.array([1, 2, 2, 3, 4, 5])
    failure_probability = np.array([0, 1e-6, 1e-8, 1e-6, 1e-6, 1e-5])

    fc = _setup_fragility_curve(hydraulicload, failure_probability, False)
    fc.check_monotonic_curve()
    data = fc.as_array()

    assert (data[:, 0] == hydraulicload).all()
    assert (data[:, 1] == failure_probability).all()


def test_fragility_curve_monotonic():
    hydraulicload = np.array([1, 2, 2, 3, 4, 5])
    failure_probability = np.array([0, 1e-6, 1e-8, 1e-6, 1e-6, 1e-5])

    fc = _setup_fragility_curve(hydraulicload, failure_probability, True)
    fc.check_monotonic_curve()
    data = fc.as_array()
    check_data = np.array(
        [
            [1.0e00, 0.0e00],
            [2.0e00, 1.0e-08],
            [2.0e00, 1.0e-06],
            [3.0e00, 1.0e-06],
            [4.0e00, 1.0e-06],
            [5.0e00, 1.0e-05],
        ]
    )
    assert np.allclose(check_data, data, atol=0, rtol=1e-8)
