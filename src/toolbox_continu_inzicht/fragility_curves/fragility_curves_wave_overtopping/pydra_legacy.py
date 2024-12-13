"""
pydra_legacy.py: functions from old pydra version
bretschneider: function to calculate wave conditions with Bretschneider.
"""

import numpy as np


def bretschneider(d, fe, u):
    """
    Bereken golfcondities met Bretschneider.
    Gebaseerd op "subroutine Bretschneider" in Hydra-NL, geprogrammeerd door
    Matthijs Duits

    Parameters
    ----------
    d : float
        Waterdiepte
    fe : float
        Strijklengte
    u : float
        Windsnelheid

    Returns
    -------
    hs : float
        Significante golfhoogte
    tp : float
        Piekperiode
    """

    # Corrigeer als input is float
    if isinstance(d, float):
        if (d <= 0.0) | (fe <= 0.0) | (u <= 0.0):
            hs = 0.0
            tp = 0.0

    # Corrigeer als input is array
    else:
        indices = (d <= 0.0) | (fe <= 0.0) | (u <= 0.0)

        hs_arr = np.zeros(indices.shape)
        tp_arr = np.zeros(indices.shape)

        u = u[~indices]
        fe = fe[~indices]
        d = d[~indices]

    # Initialiseer constanten
    g = 9.81

    # Bereken Hs en Tp
    dtilde = (d * g) / (u * u)
    v1 = np.tanh(0.53 * (dtilde**0.75))
    v2 = np.tanh(0.833 * (dtilde**0.375))

    ftilde = (fe * g) / (u * u)

    hhulp = (0.0125 / v1) * (ftilde**0.42)
    htilde = 0.283 * v1 * np.tanh(hhulp)
    hs = (u * u * htilde) / g

    thulp = (0.077 / v2) * (ftilde**0.25)
    ttilde = 2.4 * np.pi * v2 * np.tanh(thulp)
    tp = (1.08 * u * ttilde) / g

    if isinstance(d, float):
        return hs, tp

    else:
        hs_arr[~indices] = hs
        tp_arr[~indices] = tp
        return hs_arr, tp_arr


# Likely is in the newer version, but more difficult to access
# Create table with parameters
qcr_table = {
    ("closed", "EX", 1): 225.0,
    ("closed", "EX", 2): 100.0,
    ("closed", "EX", 3): 70.0,
    ("closed", "SDX", 1): 250.0,
    ("closed", "SDX", 2): 120.0,
    ("closed", "SDX", 3): 80.0,
    ("open", "EX", 1): 100.0,
    ("open", "EX", 2): 70.0,
    ("open", "EX", 3): 40.0,
    ("open", "SDX", 1): 120.0,
    ("open", "SDX", 2): 80.0,
    ("open", "SDX", 3): 50.0,
}


def _get_mu_sigma(EX, SDX):
    """
    Get mu and sigma of the normal distribution of the
    underlying log-normal distribution.
    - EX and SDX are the expectancy or standard deviation of the lognormal distribution
    - mu2 en sigma2 are the expectancy or standard deviation of the underlying normal
    distribution (the usual parameters of the log-normal distribution)
    http://stackoverflow.com/questions/41464753/generate-random-numbers-from-lognormal-distribution-in-python

    Parameters
    ----------
    EX : float
        Expectancy of the log-normal distribution
    SDX : float
        Standard deviation of the log-normal distribution
    """
    # Variatiecoëfficiënt:
    Vx = SDX / EX

    # Standaarddeviatie van de normale verdeling:
    sigma2 = np.sqrt(np.log(1 + Vx**2))

    # Verwachtingswaarde van de normale verdeling:
    mu2 = np.log(EX) - 1 / 2 * np.log(1 + Vx**2)

    return mu2, sigma2


def get_qcr_dist(Hs, grass_quality):
    """
    Get distribution for critical overtopping discharge
    based on significant wave height

    Parameters
    ----------
    Hs : float
        Significant wave height
    grass_quality : str
        Grass quality (open or closed)
    """
    if grass_quality not in ["open", "closed"]:
        raise ValueError(
            f'Grass quality "{grass_quality}" not understood. Choose from "open" and "closed".'
        )

    # Get wave height class
    if Hs < 1.0:
        hs_class = 1
    elif Hs < 2.0:
        hs_class = 2
    else:
        hs_class = 3

    # Get parameters
    a = qcr_table[(grass_quality, "EX", hs_class)]
    b = qcr_table[(grass_quality, "SDX", hs_class)]

    mu, sigma = _get_mu_sigma(a, b)

    return mu, sigma
