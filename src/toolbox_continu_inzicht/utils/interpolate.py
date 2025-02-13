import numpy as np


def interpolate_1d(x, xp, fp):
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
    # Determine lower bounds
    intidx = np.minimum(np.maximum(0, np.searchsorted(xp, x) - 1), len(xp) - 2)
    # Determine interpolation fractions
    fracs = (x - xp[intidx]) / (xp[intidx + 1] - xp[intidx])
    # Interpolate (1-frac) * f_low + frac * f_up
    f = (1 - fracs) * fp[intidx] + fp[intidx + 1] * fracs

    return f


def log_interpolate_1d(x, xp, fp):
    """Similar to interpolate_1d, but interpolates in log-space"""
    fp[fp < 1e-20] = 1e-20
    return np.exp(interpolate_1d(x, xp, np.log(fp)))
