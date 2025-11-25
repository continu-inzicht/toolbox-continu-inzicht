from toolbox_continu_inzicht.utils.interpolate import (
    interpolate_1d,
    beta_x_interpolate_1d,
    log_x_interpolate_1d,
)
import numpy as np


def test_interpolate():
    xp = np.array([1, 2, 3])
    fp = np.array([0, 0.1, 1])
    x = np.array([1.0, 1.1])
    f = interpolate_1d(x, xp, fp)
    assert f[0] == 0
    assert np.isclose(f[1], 0.01, atol=0, rtol=1e-8)


def test_step_interpolate():
    xp = np.array([1, 2, 2, 3])
    fp = np.array([2, 2, 3, 3])
    x = np.array([1.99, 2.0, 2.01])
    assert (interpolate_1d(x, xp, fp) == np.array([2, 2, 3])).all()


def test_log_interpolate():
    xp = np.array([1, 2, 3])
    fp = np.array(([0, 0.1, 1]))
    x = np.array([1.0, 1.1])
    f = log_x_interpolate_1d(x, xp, fp)
    assert f[0] == 0
    assert np.isclose(f[1], 7.94328235e-181, atol=0, rtol=1e-8)


def test_beta_interpolate():
    xp = np.array([1, 2, 3])
    fp = np.array(([0, 0.1, 1]))
    x = np.array([1.0, 1.1])
    f = beta_x_interpolate_1d(x, xp, fp)
    assert f[0] == 0
    assert np.isclose(f[1], 1.47866437e-164, atol=0, rtol=1e-8)
