"""Reference-climate and minima-statistics utilities for GWDI.

These helpers are used to build `df_stats_minima` from historical climate
series and loaded Pastas models.
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import scipy.stats as st
from toolbox_continu_inzicht.utils.interpolate import log_y_interpolate_1d


def calc_return_period_pot(
    maxima: pd.Series,
    t_per: float,
    a: float = 0.3,
    b: float = 0.4,
    n: int | None = None,
) -> np.ndarray:
    """Compute return-period values for a peaks-over-threshold sample.

    Parameters
    ----------
    maxima : pd.Series
        Series with peak values.
    t_per : float
        Observation period in years.
    a : float
        Plotting-position coefficient. Default is `0.3`.
    b : float
        Plotting-position coefficient with constraint `2a + b = 1`.
        Default is `0.4`.
    n : int | None
        Effective sample size. If `None` (default), uses `len(maxima)`.

    Returns
    -------
    np.ndarray
        Return periods aligned to original `maxima` order.

    Raises
    ------
    ValueError
        If plotting-position coefficients are inconsistent.
    """
    if (2 * a + b) != 1.0:
        raise ValueError("2a + b moet gelijk zijn aan 1.0")

    order = np.argsort(maxima)[::-1]
    maxima_sorted = np.sort(maxima)[::-1]
    if n is None:
        n = len(maxima_sorted)
    k = np.arange(len(maxima_sorted)) + 1
    p = (n * (k + a + b - 1)) / ((n + b) * t_per)
    t = 1.0 / p
    return t[np.argsort(order)]


def get_mins_pot(
    df: pd.Series, thresh: float = 0.1, window: int = 30
) -> tuple[pd.Series, float, np.ndarray, pd.Series]:
    """Derive minima POT metrics from a groundwater-level series.

    Parameters
    ----------
    df : pd.Series
        Groundwater time series.
    thresh : float
        Quantile threshold used to select minima peaks. Default is `0.1`.
    window : int
        Rolling-window size used for local minima detection. Default is `30`.

    Returns
    -------
    tuple[pd.Series, float, np.ndarray, pd.Series]
        `(peaks_min, t_per, ts, hs)` used in minima-frequency fitting.
    """
    threshold_min = df.quantile(thresh)
    levels_min = df.rolling(window=window, center=True).min()
    peaks_min_all = df[(df == levels_min)]
    peakidx_min = peaks_min_all[peaks_min_all < threshold_min].index
    peaks_min = df.loc[peakidx_min]
    b_plot = 0.44
    t_per = (df.index[-1] - df.index[0]).total_seconds() / 3600 / 24 / 365
    h_pot = peaks_min.sort_values(ascending=True)
    t_pot = calc_return_period_pot(h_pot, t_per=t_per, a=b_plot, b=(1 - 2 * b_plot))
    return peaks_min, t_per, np.flip(t_pot), h_pot


def prepare_reference_climate(
    df_precipitation_regions: pd.DataFrame,
    df_evaporation: pd.DataFrame,
    precipitation_column: str = "prec_R-R",
    evaporation_column: str = "makkink",
    start: date | str = date(1906, 1, 1),
    end: date | str = date(2014, 12, 31),
) -> tuple[pd.Series, pd.Series]:
    """Prepare reference precipitation and evaporation series from dataframes.

    Parameters
    ----------
    df_precipitation_regions : pd.DataFrame
        Precipitation dataframe indexed by datetime and containing regional
        precipitation columns in millimeters.
    df_evaporation : pd.DataFrame
        Evaporation dataframe indexed by datetime and containing evaporation
        columns in millimeters.
    precipitation_column : str
        Precipitation column to select. Default is `"prec_R-R"`.
    evaporation_column : str
        Evaporation column to select. Default is `"makkink"`.
    start : date | str
        Start date of the reference window. Default is `1906-01-01`.
    end : date | str
        End date of the reference window. Default is `2014-12-31`.

    Returns
    -------
    tuple[pd.Series, pd.Series]
        Daily precipitation and evaporation reference series in meters.

    Raises
    ------
    TypeError
        If one of the inputs is not a dataframe.
    ValueError
        If required columns are missing or converted series are empty.
    """
    if not isinstance(df_precipitation_regions, pd.DataFrame):
        raise TypeError("`df_precipitation_regions` moet een pandas.DataFrame zijn.")
    if not isinstance(df_evaporation, pd.DataFrame):
        raise TypeError("`df_evaporation` moet een pandas.DataFrame zijn.")
    if precipitation_column not in df_precipitation_regions.columns:
        raise ValueError(
            f"Kolom `{precipitation_column}` ontbreekt in `df_precipitation_regions`."
        )
    if evaporation_column not in df_evaporation.columns:
        raise ValueError(f"Kolom `{evaporation_column}` ontbreekt in `df_evaporation`.")

    precip = df_precipitation_regions.copy()
    evap = df_evaporation.copy()
    precip.index = pd.to_datetime(precip.index)
    evap.index = pd.to_datetime(evap.index)
    precip = precip.sort_index()
    evap = evap.sort_index()

    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)

    precip_daily = precip[precipitation_column].resample("D").sum()
    prec100 = precip_daily.loc[start_ts:end_ts] / 1000
    evap100 = evap[evaporation_column].loc[start_ts:end_ts] / 1000

    if prec100.empty:
        raise ValueError("Neerslagreferentiereeks is leeg na datumselectie.")
    if evap100.empty:
        raise ValueError("Verdampingsreferentiereeks is leeg na datumselectie.")

    return prec100, evap100


def compute_df_stats_minima(
    info_locaties: pd.DataFrame,
    dict_models: dict[str, object],
    prec100: pd.Series,
    evap100: pd.Series,
    ps_module: object,
) -> pd.DataFrame:
    """Compute groundwater minima statistics for each configured location.

    Parameters
    ----------
    info_locaties : pd.DataFrame
        Location metadata indexed by location identifier.
    dict_models : dict[str, object]
        Loaded model mapping keyed by location.
    prec100 : pd.Series
        Reference precipitation series (m/day).
    evap100 : pd.Series
        Reference evaporation series (m/day).
    ps_module : object
        Pastas module instance providing `TarsoModel`.

    Returns
    -------
    pd.DataFrame
        Dataframe with return periods as index and locations as columns.
    """
    df_series = pd.DataFrame(index=prec100.index, columns=info_locaties.location.values)

    for loc in info_locaties.index:
        ml = dict_models[loc].copy()
        p_solved = ml.get_parameters()

        ml_cc = ml.copy(name="climate_timeseries")
        ml_cc.del_stressmodel("Tarso")

        sm = ps_module.TarsoModel(
            prec100,
            evap100,
            dmin=ml_cc.oseries.series.min(),
            dmax=ml_cc.oseries.series.max(),
        )
        ml_cc.add_stressmodel(sm)
        sim = ml_cc.simulate(
            p=p_solved,
            tmin="1906",
            tmax="2015",
            freq="D",
            warmup=365,
            return_warmup=False,
        )
        df_series[loc] = sim

    df_stats_minima = pd.DataFrame(
        index=[1, 3, 10, 30, 100, 300, 1000], columns=info_locaties.index
    )
    htrhresh_quant = 0.8
    tthresh = 2
    window = 183

    for loc in info_locaties.index:
        df_loc = df_series[loc]
        _, t_per, ts, hs = get_mins_pot(df_loc, window=window, thresh=htrhresh_quant)
        mask = ts > tthresh

        params = st.genextreme.fit(hs[mask])
        pfit = np.exp(np.arange(np.log(1e-5), np.log(1), 0.1))
        vfit = st.genextreme.ppf(pfit, *params)
        ffit = pfit * (len(hs[mask]) / t_per)

        for t in df_stats_minima.index:
            df_stats_minima.loc[t, loc] = float(
                log_y_interpolate_1d(
                    y=np.asarray([1 / float(t)], dtype=float),
                    xp=vfit,
                    fp=ffit,
                    lower_limit_mode="probability",
                )[0]
            )

    return df_stats_minima
