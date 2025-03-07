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


def interpolate_1d(
    x: np.ndarray,
    xp: np.ndarray,
    fp: np.ndarray,
    ll: float = 1e-20,
    clip01: bool = False,
) -> np.ndarray:
    """
    Interpolate an array along the given axis.
    Similar to np.interp, but with extrapolation outside range.

    Parameters
    ----------
    x : np.ndarray
        Array with positions to interpolate at
    xp : np.ndarray
        Array with positions of known values
    fp : np.ndarray
        Array with values as known positions to interpolate between
    ll : float
        Lower limit for values in fp
    clip01 : bool
        Clip values between 0 and 1

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


def log_interpolate_1d(
    x: np.ndarray,
    xp: np.ndarray,
    fp: np.ndarray,
    ll: float = 1e-20,
    clip01: bool = False,
) -> np.ndarray:
    """Similar to interpolate_1d, but interpolates in log-space

    Parameters
    ----------
    x : np.ndarray
        Array with positions to interpolate at
    xp : np.ndarray
        Array with positions of known values
    fp : np.ndarray
        Array with values as known positions to interpolate between
    ll : float
        Lower limit for values in fp
    clip01 : bool
        Clip values between 0 and 1

    Returns
    -------
    np.array
        interpolated array
    """
    if ll > 0:
        fp[fp < ll] = ll

    f = np.exp(_interpolate_1d(x, xp, np.log(fp)))
    if clip01:
        f = np.clip(f, 0, 1)

    return f


def beta_interpolate_1d(
    x: np.ndarray,
    xp: np.ndarray,
    fp: np.ndarray,
    ll: float = 1e-20,
    clip01: bool = False,
) -> np.ndarray:
    """Similar to interpolate_1d, but interpolates in beta-space

    Parameters
    ----------
    x : np.ndarray
        Array with positions to interpolate at
    xp : np.ndarray
        Array with positions of known values
    fp : np.ndarray
        Array with values as known positions to interpolate between
    ll : float
        Lower limit for values in fp
    clip01 : bool
        Clip values between 0 and 1

    Returns
    -------
    np.array
        interpolated array
    """
    if ll > 0:
        fp[fp < ll] = ll

    f = norm.sf(_interpolate_1d(x, xp, norm.isf(fp)))
    if clip01:
        f = np.clip(f, 0, 1)

    return f
