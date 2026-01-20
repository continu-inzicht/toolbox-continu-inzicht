from typing import Tuple

import numpy as np
import pandas as pd
from scipy.stats import lognorm

import pydra_core
import pydra_core.common.enum

from toolbox_continu_inzicht.fragility_curves.fragility_curve_overtopping.pydra_legacy import (
    get_qcr_dist,
)


def make_winddirections(sectormin: float | int, sectorsize: float | int):
    return np.linspace(sectormin, sectormin + sectorsize, int(round(sectorsize))) % 360


def parse_profile_dataframe(df_profile: pd.DataFrame) -> pd.Series:
    """
    Parse het profiel dataframe naar een Series met parameters.
    """
    df_profile = df_profile.copy()
    if "parameters" in df_profile:
        df_profile.set_index("parameters", inplace=True)

    profile_series = df_profile["values"]
    for k in profile_series.index:
        try:
            profile_series.at[k] = float(profile_series.at[k])
        except ValueError:
            pass

    return profile_series


def validate_slopes(df_slopes: pd.DataFrame) -> None:
    if not all(
        [slopetype in [1, 2] for slopetype in df_slopes["slopetypeid"].unique()]
    ):
        raise UserWarning("Hellingen moeten van slopetypeid 1 of 2 zijn")


def build_pydra_profiles(
    df_slopes: pd.DataFrame, profile_series: pd.Series
) -> Tuple[pydra_core.Profile, pydra_core.Profile]:
    df_slope_dike = df_slopes[df_slopes["slopetypeid"] == 1]
    profiel_dict = {
        "profile_name": "profiel_CI",
        "dike_x_coordinates": df_slope_dike["x"].tolist(),
        "dike_y_coordinates": df_slope_dike["y"].tolist(),
        "dike_roughness": df_slope_dike["r"].tolist(),
        "dike_orientation": profile_series["orientation"],
        "dike_crest_level": profile_series["crestlevel"],
    }

    basis_profiel = pydra_core.Profile.from_dictionary(profiel_dict)

    foreland_profile = {}
    df_slope_foreland = df_slopes.loc[df_slopes["slopetypeid"] == 2]
    if len(df_slope_foreland) > 0:
        foreland_profile["foreland_x_coordinates"] = list(
            df_slope_foreland["x"].to_numpy()
        )
        foreland_profile["foreland_y_coordinates"] = list(
            df_slope_foreland["y"].to_numpy()
        )

    profiel_dict.update(foreland_profile)
    overtopping = pydra_core.Profile.from_dictionary(profiel_dict)

    if profile_series["dam"] != 0.0:
        breakwater_type = pydra_core.common.enum.Breakwater(int(profile_series["dam"]))
        overtopping.set_breakwater(
            breakwater_type=breakwater_type,
            breakwater_level=profile_series["damheight"],
        )

    return basis_profiel, overtopping


def build_waterlevel_grid(crestlevel: float, options: dict) -> np.ndarray:
    hstap = options.get("hstap", 0.05)
    lower_limit_coarse = options.get("lower_limit_coarse", 4)
    upper_limit_coarse = options.get("upper_limit_coarse", 2)
    upper_limit_fine = options.get("upper_limit_fine", 1.01)
    cl_rnd = np.round(crestlevel / hstap) * hstap
    return np.r_[
        np.arange(cl_rnd - lower_limit_coarse, cl_rnd - upper_limit_coarse, hstap * 2),
        np.arange(cl_rnd - upper_limit_coarse, cl_rnd + upper_limit_fine, hstap),
    ]


def compute_failure_probability(
    qov: np.ndarray,
    qcr: float | str | tuple,
    hs: np.ndarray,
    onzkans: float,
) -> np.ndarray:
    qov = np.asarray(qov, dtype=float)
    prob_qcr = not isinstance(qcr, int | float | np.integer)

    if not prob_qcr:
        return (qov > qcr).astype(float) * onzkans

    qov = qov * 1000
    hs_1d = np.asarray(hs, dtype=float).ravel()
    inc = np.zeros_like(qov, dtype=float)
    for lower, upper in zip([0.0, 1.0, 2.0], [1.0, 2.0, np.inf]):
        idx = (hs_1d >= lower) & (hs_1d < upper) & (qov > 0.0)
        if not idx.any():
            continue

        mu, sigma = get_qcr_dist(lower + 0.5, qcr)
        inc[idx] = lognorm._cdf(qov[idx] / np.exp(mu), sigma) * onzkans

    return inc


def get_overtopping_options(global_variables: dict, key: str) -> dict:
    options = global_variables.get(key, {})
    defaults = {
        "gh_onz_mu": 0.96,
        "gh_onz_sigma": 0.27,
        "gp_onz_mu_tp": 1.03,
        "gp_onz_sigma_tp": 0.13,
        "gp_onz_mu_tspec": 1.03,
        "gp_onz_sigma_tspec": 0.13,
        "gh_onz_aantal": 7,
        "gp_onz_aantal": 7,
        "tp_tspec": 1.1,
        "lower_limit_coarse": 4.0,
        "upper_limit_coarse": 2.0,
        "upper_limit_fine": 1.01,
        "hstap": 0.05,
    }
    merged = defaults.copy()
    merged.update(options)
    return merged
