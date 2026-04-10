"""Core Pastas-based GWDI computation routines.

This module contains data validation and runtime computation logic for
groundwater-drought indicator inference at raster-cell level.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TypedDict

import numpy as np
import pandas as pd
import pastas as ps

from toolbox_continu_inzicht.utils.interpolate import log_x_interpolate_1d


class _LocationRuntimeContext(TypedDict):
    """Per-location constants reused during repeated fid calculations."""

    template_model: ps.model.Model
    p_solved: object
    dmin: float
    dmax: float
    xp: np.ndarray
    fp: np.ndarray


class GwdiModel:
    """Container for GWDI model validation and computation logic."""

    @staticmethod
    def _prepare_climate_inputs(
        df_precipitation: pd.DataFrame,
        df_evaporation: pd.DataFrame,
    ) -> pd.DataFrame:
        """Validate and combine precipitation and evaporation inputs.

        Parameters
        ----------
        df_precipitation : pd.DataFrame
            Climate input for precipitation with columns `time`, `fid`, `P`.
        df_evaporation : pd.DataFrame
            Climate input for evaporation with columns `time`, `fid`,
            `evaporation`.

        Returns
        -------
        pd.DataFrame
            Joined climate dataframe indexed by `time` and `fid`.

        Raises
        ------
        ValueError
            If duplicate `(time, fid)` rows are present or if both inputs do
            not share identical `(time, fid)` rows.
        """
        prepared_precipitation = df_precipitation.loc[:, ["time", "fid", "P"]].copy()
        prepared_evaporation = df_evaporation.loc[
            :, ["time", "fid", "evaporation"]
        ].copy()

        prepared_precipitation["time"] = pd.to_datetime(prepared_precipitation["time"])
        prepared_evaporation["time"] = pd.to_datetime(prepared_evaporation["time"])
        prepared_precipitation["fid"] = prepared_precipitation["fid"].astype(int)
        prepared_evaporation["fid"] = prepared_evaporation["fid"].astype(int)

        if prepared_precipitation[["time", "fid"]].duplicated().any():
            raise ValueError(
                "Input `df_precipitation` bevat dubbele rijen voor dezelfde (`time`, `fid`)."
            )
        if prepared_evaporation[["time", "fid"]].duplicated().any():
            raise ValueError(
                "Input `df_evaporation` bevat dubbele rijen voor dezelfde (`time`, `fid`)."
            )

        prepared_precipitation = prepared_precipitation.set_index(["time", "fid"])
        prepared_evaporation = prepared_evaporation.set_index(["time", "fid"])
        prepared_precipitation = prepared_precipitation.sort_index()
        prepared_evaporation = prepared_evaporation.sort_index()

        if not prepared_precipitation.index.equals(prepared_evaporation.index):
            missing_in_precipitation = prepared_evaporation.index.difference(
                prepared_precipitation.index
            )
            missing_in_evaporation = prepared_precipitation.index.difference(
                prepared_evaporation.index
            )
            raise ValueError(
                "Inputs `df_precipitation` en `df_evaporation` moeten identieke (`time`, `fid`)-rijen bevatten. "
                f"Ontbrekend in neerslag: {len(missing_in_precipitation)}; "
                f"ontbrekend in verdamping: {len(missing_in_evaporation)}."
            )

        return prepared_precipitation.join(prepared_evaporation, how="inner")

    @classmethod
    def _validate_runtime_inputs(
        cls,
        info_locaties: pd.DataFrame,
        dict_models: dict[str, ps.model.Model],
        df_stats_minima: pd.DataFrame,
    ) -> tuple[pd.DataFrame, dict[str, ps.model.Model], pd.DataFrame]:
        """Validate consistency between location info, models and statistics.

        Parameters
        ----------
        info_locaties : pd.DataFrame
            Location metadata used to resolve expected model names.
        dict_models : dict[str, ps.model.Model]
            Loaded Pastas models keyed by file stem.
        df_stats_minima : pd.DataFrame
            Precomputed minima statistics per location.

        Returns
        -------
        tuple[pd.DataFrame, dict[str, ps.model.Model], pd.DataFrame]
            Validated location metadata, location-keyed model mapping,
            and validated minima-statistics dataframe.

        Raises
        ------
        TypeError
            If runtime inputs have wrong base types.
        ValueError
            If expected model keys or statistic columns are missing.
        """
        if not isinstance(info_locaties, pd.DataFrame):
            raise TypeError("Input `info_locaties` moet een pandas.DataFrame zijn.")
        prepared_info = info_locaties.copy()
        prepared_info.index = prepared_info["location"]

        if not isinstance(dict_models, dict):
            raise TypeError(
                "Input `dict_models` moet een dictionary met Pastas-modellen zijn."
            )
        if len(dict_models) == 0:
            raise ValueError(
                "Input `dict_models` is leeg. Lever minimaal één model aan."
            )

        if not isinstance(df_stats_minima, pd.DataFrame):
            raise TypeError("Input `df_stats_minima` moet een pandas.DataFrame zijn.")

        expected_model_keys = {
            location: f"{location}_{prepared_info.loc[location, 'position']}_tarso"
            for location in prepared_info.index
        }
        missing_model_keys = [
            model_key
            for model_key in expected_model_keys.values()
            if model_key not in dict_models
        ]
        if missing_model_keys:
            raise ValueError(
                "Pastas-modelinput mist verplichte model-sleutels:\n- "
                + "\n- ".join(sorted(missing_model_keys))
            )

        location_models = {
            location: dict_models[expected_model_keys[location]]
            for location in prepared_info.index
        }

        missing_stats_columns = [
            location
            for location in prepared_info.index
            if location not in df_stats_minima.columns
        ]
        if missing_stats_columns:
            raise ValueError(
                "Input `df_stats_minima` mist verplichte locatiekolommen:\n- "
                + "\n- ".join(sorted(missing_stats_columns))
            )

        try:
            stats_index = pd.Index(df_stats_minima.index).astype(float)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "Ongeldige index in `df_stats_minima`; numerieke herhalingstijden verwacht."
            ) from exc

        prepared_stats = df_stats_minima.copy()
        prepared_stats.index = stats_index
        prepared_stats = prepared_stats.loc[:, prepared_info.index].astype(float)

        return prepared_info, location_models, prepared_stats

    @staticmethod
    def _build_location_runtime_contexts(
        info_locaties: pd.DataFrame,
        dict_models: dict[str, ps.model.Model],
        df_stats_minima: pd.DataFrame,
    ) -> dict[str, _LocationRuntimeContext]:
        """Precompute per-location constants reused for each fid run.

        Parameters
        ----------
        info_locaties : pd.DataFrame
            Validated location metadata.
        dict_models : dict[str, ps.model.Model]
            Location-keyed Pastas model mapping.
        df_stats_minima : pd.DataFrame
            Validated minima-statistics table.

        Returns
        -------
        dict[str, _LocationRuntimeContext]
            Runtime context per location.
        """
        fp_values = 1 / df_stats_minima.index.to_numpy().astype(float)
        location_contexts: dict[str, _LocationRuntimeContext] = {}

        for loc in info_locaties.index:
            source_model = dict_models[loc]
            p_solved = source_model.get_parameters()

            template_model = source_model.copy(name=f"{loc}_template")
            template_model.del_stressmodel("Tarso")

            xp_values = df_stats_minima[loc].to_numpy().astype(float)
            order = np.argsort(xp_values, kind="stable")
            location_contexts[loc] = {
                "template_model": template_model,
                "p_solved": p_solved,
                "dmin": float(template_model.oseries.series.min()),
                "dmax": float(template_model.oseries.series.max()),
                "xp": xp_values[order],
                "fp": fp_values[order],
            }

        return location_contexts

    @classmethod
    def _get_gwdroogte_1loc(
        cls,
        prec_m: pd.Series | pd.DataFrame,
        evap_m: pd.Series | pd.DataFrame,
        location: str,
        location_context: _LocationRuntimeContext,
    ) -> pd.DataFrame:
        """Compute GWDI indicator series for one monitoring location."""
        ml = location_context["template_model"].copy(name=f"{location}_inference")
        sm = ps.TarsoModel(
            prec_m,
            evap_m,
            dmin=location_context["dmin"],
            dmax=location_context["dmax"],
        )
        ml.add_stressmodel(sm)
        sim = ml.simulate(
            p=location_context["p_solved"],
            freq="D",
            tmin=prec_m.index[0],
            tmax=prec_m.index[-1],
            warmup=3650,
            return_warmup=False,
        )
        ts = log_x_interpolate_1d(
            x=sim.to_numpy().astype(float),
            xp=location_context["xp"],
            fp=location_context["fp"],
            ll=0.0,
            clip01=False,
        )
        gwdroogte = pd.DataFrame(index=sim.index, data=ts, columns=["indicator"])
        gwdroogte[gwdroogte > 10] = 10
        return 1 / gwdroogte

    @classmethod
    def _get_gwdroogte_all(
        cls,
        prec: pd.Series | pd.DataFrame,
        evap: pd.Series | pd.DataFrame,
        info_locaties: pd.DataFrame,
        location_contexts: dict[str, _LocationRuntimeContext],
    ) -> pd.DataFrame:
        """Compute GWDI indicator time series for all configured locations."""
        prec_m = prec / 1000
        evap_m = evap / 1000
        df_gw_indicator_all = pd.DataFrame(
            columns=info_locaties.index, index=prec.index, dtype=float
        )

        for loc in info_locaties.index:
            gw_indicator = cls._get_gwdroogte_1loc(
                prec_m=prec_m,
                evap_m=evap_m,
                location=loc,
                location_context=location_contexts[loc],
            )
            df_gw_indicator_all[loc] = gw_indicator["indicator"]
        return df_gw_indicator_all

    @classmethod
    def compute_gwdi(
        cls,
        df_precipitation: pd.DataFrame,
        df_evaporation: pd.DataFrame,
        info_locaties: pd.DataFrame,
        dict_models: dict[str, ps.model.Model],
        df_stats_minima: pd.DataFrame,
    ) -> pd.DataFrame:
        """Compute GWDI output rows for all fid values in climate inputs.

        Parameters
        ----------
        df_precipitation : pd.DataFrame
            Precipitation input per `time` and `fid`.
        df_evaporation : pd.DataFrame
            Evaporation input per `time` and `fid`.
        info_locaties : pd.DataFrame
            Monitoring-location metadata with `location` and `position`.
        dict_models : dict[str, ps.model.Model]
            Loaded Pastas models keyed as `{location}_{position}_tarso`.
        df_stats_minima : pd.DataFrame
            Precomputed minima-statistics table per location.

        Returns
        -------
        pd.DataFrame
            GWDI rows in Toolbox processor output format.

        Raises
        ------
        ValueError
            If climate inputs are empty or inconsistent.
        RuntimeError
            If no output rows are produced.
        """
        df_climate = cls._prepare_climate_inputs(
            df_precipitation=df_precipitation,
            df_evaporation=df_evaporation,
        )
        info_locaties, location_models, df_stats_minima = cls._validate_runtime_inputs(
            info_locaties=info_locaties,
            dict_models=dict_models,
            df_stats_minima=df_stats_minima,
        )
        peilbuisid_dict = dict(
            zip(info_locaties["location"], list(range(len(info_locaties))))
        )
        location_contexts = cls._build_location_runtime_contexts(
            info_locaties=info_locaties,
            dict_models=location_models,
            df_stats_minima=df_stats_minima,
        )

        fids_to_process = pd.Index(df_climate.index.get_level_values("fid")).unique()
        fids_to_process = fids_to_process.to_numpy(dtype=int)
        if len(fids_to_process) == 0:
            raise ValueError("Geen `fid`-waarden gevonden in GWDI-klimaatinput.")

        rows = []
        for fid_raster in fids_to_process:
            df_fid = df_climate.xs(int(fid_raster), level="fid").sort_index()
            prec_series = pd.Series(
                data=df_fid["P"].to_numpy(),
                index=pd.DatetimeIndex(df_fid.index),
                name="P",
            )
            evap_series = pd.Series(
                data=df_fid["evaporation"].to_numpy(),
                index=pd.DatetimeIndex(df_fid.index),
                name="evaporation",
            )

            gw_indicator_all = cls._get_gwdroogte_all(
                prec=prec_series,
                evap=evap_series,
                info_locaties=info_locaties,
                location_contexts=location_contexts,
            )

            df_gwdi = gw_indicator_all.rename_axis("datetime").reset_index()
            df_gwdi = pd.melt(
                df_gwdi,
                id_vars="datetime",
                value_vars=info_locaties["location"].values,
                var_name="location",
                value_name="value",
            )
            df_gwdi["fid"] = int(fid_raster)

            max_date = df_gwdi["datetime"].max().date()
            df_gwdi = df_gwdi[
                df_gwdi["datetime"].dt.date > (max_date - timedelta(days=35))
            ]

            df_gwdi["datetime"] = (df_gwdi["datetime"].astype("int64") // 10**6).astype(
                "int64"
            )
            df_gwdi["parameterid"] = 4
            df_gwdi["methodid"] = 1
            df_gwdi["peilbuisid"] = df_gwdi["location"].map(peilbuisid_dict)
            df_gwdi = df_gwdi[
                [
                    "fid",
                    "parameterid",
                    "methodid",
                    "datetime",
                    "value",
                    "peilbuisid",
                ]
            ]
            rows.append(df_gwdi)

        if not rows:
            raise RuntimeError("Er zijn geen GWDI-uitvoerrijen geproduceerd.")

        return pd.concat(rows, axis=0, ignore_index=True)
