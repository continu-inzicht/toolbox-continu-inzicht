import numpy as np
from typing import Callable


def import_scipy():
    """Import scipy.stats.norm, raise ImportError if scipy is not installed but still lets the program run."""
    try:
        from scipy.stats import norm
    except ImportError:
        norm = None
        raise ImportError(
            "Scipy is not installed, use the dev pixi environment or install scipy"
        )
    return norm


def _interpolate_1d(x, xp, fp, side="left", sorter=None):
    # Computes the index of the lower bracket in xp to use for linear
    # interpolation of x. First np.searchsorted(xp, x) finds where each value
    # in x would be inserted into the (assumed sorted) array xp to keep order;
    # subtracting 1 turns that insertion index into the index of the element
    # just below (the lower neighbor).
    # Because searchsorted can return 0 or len(xp), the expression wraps that
    # result with clamps to keep intidx in the valid range: it never drops
    # below 0 and never exceeds len(xp) - 2. The upper clamp is len(xp) - 2 so
    # that later code can safely access xp[intidx + 1].
    intidx = np.searchsorted(xp, x, side=side, sorter=sorter) - 1
    intidx = np.clip(intidx, 0, len(xp) - 2)

    # Bepaal stapgrootte van de gegeven x-waarden. Om delen door 0 te voorkomen
    # gebruiken we een kleine waarde in plaats van 0
    xstep = xp[intidx + 1] - xp[intidx]
    xstep[xstep == 0] = 1e-16
    # Bepaal interpolatiefracties
    fracs = (x - xp[intidx]) / xstep
    # Interpolatie: (1 - frac) * f_low + frac * f_up
    f = (1 - fracs) * fp[intidx] + fracs * fp[intidx + 1]

    return f


def _transformed_x_interpolate_1d(
    x: np.ndarray,
    xp: np.ndarray,
    fp: np.ndarray,
    ll: float,
    clip01: bool,
    ftransform: Callable | None = None,
    finvtransform: Callable | None = None,
):
    if ll > 0:
        # Pas de lower limit toe op een kopie van de input
        fp = np.copy(fp)
        fp[fp < ll] = ll

    if ftransform is not None and finvtransform is not None:
        # Transformeer de fp-waarden
        f = finvtransform(_interpolate_1d(x, xp, ftransform(fp), side="left"))
    else:
        f = _interpolate_1d(x, xp, fp, side="left")

    if ll > 0:
        # Reset lower limit naar 0
        f[f <= ll] = 0
        f[np.isclose(f, ll, atol=0, rtol=1e-8)] = 0

    if clip01:
        f = np.clip(f, 0, 1)

    return f


def _transformed_y_interpolate_1d(
    y: np.ndarray,
    xp: np.ndarray,
    fp: np.ndarray,
    ll: float,
    ftransform: Callable | None = None,
):
    if ll > 0:
        fp = np.copy(fp)
        fp[fp < ll] = ll
        y = np.copy(y)
        y[y < ll] = ll

    if ftransform is not None:
        # Transformeer de fp-waarden
        f = _interpolate_1d(ftransform(y), ftransform(fp), xp, side="right")
    else:
        f = _interpolate_1d(y, fp, xp, side="right")

    return f


def interpolate_1d(
    x: np.ndarray,
    xp: np.ndarray,
    fp: np.ndarray,
    ll: float = 0.0,
    clip01: bool = False,
) -> np.ndarray:
    """
    Interpolatie van een 1d vector, gebaseerd op np.interp maar met
    extrapolatie buiten het opgegeven bereik.

    Parameters
    ----------
    x : np.ndarray
        X-waardes waarop geinterpoleerd moet worden
    xp : np.ndarray
        Referentievector van x-waardes
    fp : np.ndarray
        Referentievector van y-waardes
    ll : float
        Ondergrens voor de interpolatie, deze waarde of kleiner wordt als 0 gezien
    clip01 : bool
        Begrens resultaat tussen [0, 1]

    Returns
    -------
    np.array
        geinterpoleerde vector
    """
    return _transformed_x_interpolate_1d(x, xp, fp, ll, clip01)


def log_x_interpolate_1d(
    x: np.ndarray,
    xp: np.ndarray,
    fp: np.ndarray,
    ll: float = 1e-200,
    clip01: bool = False,
) -> np.ndarray:
    """interpolate_1d met y-waardes omgezet naar log-waardes

    Parameters
    ----------
    x : np.ndarray
        X-waardes waarop geinterpoleerd moet worden
    xp : np.ndarray
        Referentievector van x-waardes
    fp : np.ndarray
        Referentievector van y-waardes
    ll : float
        Ondergrens voor de interpolatie, deze waarde of kleiner wordt als 0 gezien
    clip01 : bool
        Begrens resultaat tussen [0, 1]

    Returns
    -------
    np.array
        geinterpoleerde vector
    """
    return _transformed_x_interpolate_1d(
        x, xp, fp, ll, clip01, ftransform=np.log, finvtransform=np.exp
    )


def beta_x_interpolate_1d(
    x: np.ndarray,
    xp: np.ndarray,
    fp: np.ndarray,
    ll: float = 1e-200,
    clip01: bool = False,
) -> np.ndarray:
    """interpolate_1d met y-waardes omgezet naar beta-waardes

    Parameters
    ----------
    x : np.ndarray
        X-waardes waarop geinterpoleerd moet worden
    xp : np.ndarray
        Referentievector van x-waardes
    fp : np.ndarray
        Referentievector van y-waardes
    ll : float
        Ondergrens voor de interpolatie, deze waarde of kleiner wordt als 0 gezien
    clip01 : bool
        Begrens resultaat tussen [0, 1]

    Returns
    -------
    np.array
        geinterpoleerde vector
    """
    norm = import_scipy()
    return _transformed_x_interpolate_1d(
        x, xp, fp, ll, clip01, ftransform=norm.isf, finvtransform=norm.sf
    )


def log_y_interpolate_1d(
    y: np.ndarray,
    xp: np.ndarray,
    fp: np.ndarray,
    ll: float = 1e-200,
) -> np.ndarray:
    """interpolate_1d met x-waardes omgezet naar log-waardes

    Parameters
    ----------
    y : np.ndarray
        Y-waardes waarop geinterpoleerd moet worden
    xp : np.ndarray
        Referentievector van x-waardes
    fp : np.ndarray
        Referentievector van y-waardes
    ll : float
        Ondergrens voor de interpolatie, deze waarde of kleiner wordt als 0 gezien

    Returns
    -------
    np.array
        geinterpoleerde vector
    """
    return _transformed_y_interpolate_1d(y, xp, fp, ll, ftransform=np.log)


def beta_y_interpolate_1d(
    y: np.ndarray,
    xp: np.ndarray,
    fp: np.ndarray,
    ll: float = 1e-200,
) -> np.ndarray:
    """interpolate_1d met y-waardes omgezet naar beta-waardes

    Parameters
    ----------
    y : np.ndarray
        Y-waardes waarop geinterpoleerd moet worden
    xp : np.ndarray
        Referentievector van x-waardes
    fp : np.ndarray
        Referentievector van y-waardes
    ll : float
        Ondergrens voor de interpolatie, deze waarde of kleiner wordt als 0 gezien

    Returns
    -------
    np.array
        geinterpoleerde vector
    """
    norm = import_scipy()
    return _transformed_y_interpolate_1d(y, xp, fp, ll, ftransform=norm.isf)
