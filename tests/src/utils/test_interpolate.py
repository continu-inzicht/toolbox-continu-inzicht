from toolbox_continu_inzicht.utils.interpolate import (
    beta_y_interpolate_1d,
    interpolate_1d,
    beta_x_interpolate_1d,
    log_x_interpolate_1d,
    log_y_interpolate_1d,
    circular_interpolate_1d,
)
import numpy as np
from test_interpolate_data import fragility_curve_data


# 1D
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


# X
def test_log_x_interpolate():
    xp = np.array([1, 2, 3])
    fp = np.array(([0, 0.1, 1]))
    x = np.array([1.0, 1.1])
    f = log_x_interpolate_1d(x, xp, fp)
    assert f[0] == 0
    assert np.isclose(f[1], 7.94328235e-181, atol=0, rtol=1e-8)


def test_beta_x_interpolate():
    xp = np.array([1, 2, 3])
    fp = np.array(([0, 0.1, 1]))
    x = np.array([1.0, 1.1])
    f = beta_x_interpolate_1d(x, xp, fp)
    assert f[0] == 0
    assert np.isclose(f[1], 1.47866437e-164, atol=0, rtol=1e-8)


def test_log_x_interpolate_2():
    # expected results
    x_new = np.array([2, 3, 4])

    fp = np.array([1 / 1000, 1 / 10])
    x = np.array([2, 4])
    y_new = np.array([1 / 1000, 1 / 100, 1 / 10])
    y = log_x_interpolate_1d(x_new, x, fp)
    for i in range(len(y_new)):
        assert np.isclose(y[i], y_new[i], atol=0, rtol=1e-8)


# y
def test_log_y_interpolate():
    # expected results
    x_new = np.array([2, 3, 4])

    fp = np.array([1 / 1000, 1 / 10])
    x = np.array([2, 4])
    y_new = np.array([1 / 1000, 1 / 100, 1 / 10])
    x = log_y_interpolate_1d(y_new, x, fp)
    for i in range(len(x_new)):
        assert np.isclose(x[i], x_new[i], atol=0, rtol=1e-8)


def test_beta_y_interpolate():
    # expected results
    x_new = np.array([2.0, 2.84468686, 4.0])

    fp = np.array([1 / 1000, 1 / 10])
    x = np.array([2, 4])
    y_new = np.array([1 / 1000, 1 / 100, 1 / 10])
    x = beta_y_interpolate_1d(y_new, x, fp)
    for i in range(len(x_new)):
        assert np.isclose(x[i], x_new[i], atol=0, rtol=1e-8)


def test_interp_y_fc():
    x, fp = fragility_curve_data()
    y_new = np.array(sorted(list(fp) + [0.183]))
    new_x = log_y_interpolate_1d(y_new, x, fp, ll=1e-200)

    from test_interpolate_data import expected_x_new_0_183

    assert (np.isclose(new_x, expected_x_new_0_183, atol=0, rtol=1e-8)).all()

    ### debug plot
    # import matplotlib.pyplot as plt
    # plt.plot(x, fp, label="data points")
    # plt.plot(new_x, y, label="interpolated")
    # plt.yscale("log")
    # plt.show()
    # new_x


def test_interp_x_fc():
    x, fp = fragility_curve_data()
    x_new = np.array(sorted(list(x) + [15.075]))
    new_y = log_x_interpolate_1d(x_new, x, fp, ll=1e-200)
    new_y
    from test_interpolate_data import expected_y_new_15_075

    assert (np.isclose(new_y, expected_y_new_15_075, atol=0, rtol=1e-8)).all()

    ### debug plot
    # import matplotlib.pyplot as plt
    # plt.plot(x, fp, label="data points")
    # plt.plot(new_x, y, label="interpolated")
    # plt.yscale("log")
    # plt.show()
    # new_x


def test_circular_interpolate_wrap():
    wdv = np.array([10.0, 350.0])
    grid_wd = np.array([10.0, 350.0])
    wd_ext = np.concatenate([wdv - 360.0, wdv, wdv + 360.0])
    grid_wd_ext = np.concatenate([grid_wd, grid_wd, grid_wd])
    windrichtingen = np.array([0.0])

    angle = circular_interpolate_1d(windrichtingen, wd_ext, grid_wd_ext)

    assert np.isclose(angle[0], 0.0, atol=1e-6)


def test_circular_interpolate_non_boundary():
    wdv = np.array([90.0, 110.0])
    grid_wd = np.array([90.0, 110.0])
    wd_ext = np.concatenate([wdv - 360.0, wdv, wdv + 360.0])
    grid_wd_ext = np.concatenate([grid_wd, grid_wd, grid_wd])
    windrichtingen = np.array([100.0])

    angle = circular_interpolate_1d(windrichtingen, wd_ext, grid_wd_ext)

    assert np.isclose(angle[0], 100.0, atol=1e-6)
