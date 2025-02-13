import numpy as np
from scipy.stats import norm


def _interpolate_1d(x, xp, fp):
    # Determine lower bounds
    intidx = np.minimum(np.maximum(0, np.searchsorted(xp, x) - 1), len(xp) - 2)
    # Determine interpolation fractions
    fracs = (x - xp[intidx]) / (xp[intidx + 1] - xp[intidx])
    # Interpolate (1-frac) * f_low + frac * f_up
    f = (1 - fracs) * fp[intidx] + fp[intidx + 1] * fracs

    return f


def interpolate_1d(x, xp, fp, ll=1e-20, clip01=False):
    """
    Interpolate an array along the given axis.
    Similar to np.interp, but with extrapolation outside range.

    Parameters
    ----------
    x : np.array
        Array with positions to interpolate at
    xp : np.array
        Array with positions of known values
    fp : np.array
        Array with values as known positions to interpolate between

    Returns
    -------
    np.array
        interpolated array
    """
    if ll > 0:
        fp[fp < ll] = ll

    f = _interpolate_1d(x, xp, fp)
    if clip01:
        f = np.clip(f, 0, 1)

    return f


def log_interpolate_1d(x, xp, fp, ll=1e-20, clip01=False):
    """Similar to interpolate_1d, but interpolates in log-space"""
    if ll > 0:
        fp[fp < ll] = ll

    f = np.exp(_interpolate_1d(x, xp, np.log(fp)))
    if clip01:
        f = np.clip(f, 0, 1)

    return f


def beta_interpolate_1d(x, xp, fp, ll=1e-20, clip01=False):
    """Similar to interpolate_1d, but interpolates in beta-space"""
    if ll > 0:
        fp[fp < ll] = ll

    f = norm.sf(_interpolate_1d(x, xp, norm.isf(fp)))
    if clip01:
        f = np.clip(f, 0, 1)

    return f
