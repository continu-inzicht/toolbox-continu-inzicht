"""
pydra_legacy.py: functies uit een oude Pydra-versie.
bretschneider: functie om de golfcondities met Bretschneider te berekenen.
"""

from typing import Tuple
import numpy as np


def bretschneider(d: float, fe: float, u: float) -> Tuple[float, float]:
    """
    Berekent golfcondities met Bretschneider.
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


# Waarschijnlijk in de nieuwere versie, maar moeilijker toegankelijk
# Creëer tabel met parameters
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


def _get_mu_sigma(EX: float, SDX: float) -> Tuple[float, float]:
    """
    Verkrijg mu en sigma voor de normaalverdeling van de
    onderliggende log-normal verdeling.
    - EX en SDX zijn de verwachting of standaard deviatie van de log-normaal verdeling
    - mu2 en sigma2 zijn de verwachting of standaard deviatie van de onderliggende normaalverdeling
    (de gebruikelijke parameters van de log-normaal verdeling)
    http://stackoverflow.com/questions/41464753/generate-random-numbers-from-lognormal-distribution-in-python

    Parameters
    ----------
    EX : float
        Verwachting van de log-normaal verdeling
    SDX : float
        Standaardverdeling van de log-normaal verdeling

    Returns
    -------
    mu2 : float
        Verwachting van de onderliggende normal verdeling
    sigma2 : float
         Standaardverdeling van de onderliggende normal verdeling
    """
    # Variatiecoëfficiënt:
    Vx = SDX / EX

    # Standaarddeviatie van de normale verdeling:
    sigma2 = np.sqrt(np.log(1 + Vx**2))

    # Verwachtingswaarde van de normale verdeling:
    mu2 = np.log(EX) - 1 / 2 * np.log(1 + Vx**2)

    return mu2, sigma2


def get_qcr_dist(Hs: float, grass_quality: str) -> Tuple[float, float]:
    """
    Verkrijg de verdeling voor de kritieke overtopping afvoer
    gebaseerd op significante golfhoogte

    Parameters
    ----------
    Hs : float
        Significante golfhoogte
    grass_quality : str
        Graskwaliteit (open of gesloten)

    Raises
    ------
    ValueError
        Als Graskwaliteit niet klopt, moet be "open" of  "closed" zijn.

    Returns
    -------
    Mu : float
        Verwachting van de verdeling
    Sigma : float
        Standaardverdeling van de verdeling
    """
    if isinstance(grass_quality, tuple):
        mu, sigma = grass_quality
    else:
        if grass_quality not in ["open", "closed"]:
            raise ValueError(
                f'Graskwaliteit "{grass_quality}" niet begrepen. Kies tussen "open" en "closed".'
            )

        # Verkrijg de golfhoogteklasse
        if Hs < 1.0:
            hs_class = 1
        elif Hs < 2.0:
            hs_class = 2
        else:
            hs_class = 3

        # Verkrijg parameters
        a = qcr_table[(grass_quality, "EX", hs_class)]
        b = qcr_table[(grass_quality, "SDX", hs_class)]

        mu, sigma = _get_mu_sigma(a, b)

    return mu, sigma
