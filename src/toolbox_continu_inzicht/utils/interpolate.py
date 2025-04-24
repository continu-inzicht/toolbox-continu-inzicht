import numpy as np
from scipy.stats import norm
from typing import Callable


def _interpolate_1d(x, xp, fp):
    # Bepaal lower bounds
    intidx = np.minimum(np.maximum(0, np.searchsorted(xp, x) - 1), len(xp) - 2)
    # Bepaal stapgrootte van de gegeven x-waarden. Om delen door 0 te voorkomen
    # gebruiken we een kleine waarde in plaats van 0
    xstep = xp[intidx + 1] - xp[intidx]
    xstep[xstep == 0] = 1e-16
    # Bepaal interpolatiefracties
    fracs = (x - xp[intidx]) / xstep
    # Interpolatie: (1 - frac) * f_low + frac * f_up
    f = (1 - fracs) * fp[intidx] + fracs * fp[intidx + 1]

    return f


def _transformed_interpolate_1d(
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
        f = finvtransform(_interpolate_1d(x, xp, ftransform(fp)))
    else:
        f = _interpolate_1d(x, xp, fp)

    if ll > 0:
        # Reset lower limit naar 0
        f[f <= ll] = 0
        f[np.isclose(f, ll, atol=0, rtol=1e-8)] = 0

    if clip01:
        f = np.clip(f, 0, 1)

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
    return _transformed_interpolate_1d(x, xp, fp, ll, clip01)


def log_interpolate_1d(
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
    return _transformed_interpolate_1d(
        x, xp, fp, ll, clip01, ftransform=np.log, finvtransform=np.exp
    )


def beta_interpolate_1d(
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
    return _transformed_interpolate_1d(
        x, xp, fp, ll, clip01, ftransform=norm.isf, finvtransform=norm.sf
    )
