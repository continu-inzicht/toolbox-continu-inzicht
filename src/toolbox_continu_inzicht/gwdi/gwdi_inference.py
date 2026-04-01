"""Toolbox classes for groundwater-droughtindicator (GWDI) inference

This module contains the Toolbox-facing runner classes for groundwater-drought
indicator (GWDI) inference. Runtime inputs are loaded through named
`DataAdapter` entries and forwarded to :class:`GwdiModel`.
"""

from __future__ import annotations

import logging
from typing import ClassVar

import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht.base.base_module import ToolboxBase
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.gwdi.gwdi_model import GwdiModel


@dataclass(config={"arbitrary_types_allowed": True})
class GwdiInference(ToolboxBase):
    """Run GWDI inference for all raster cells in the climate inputs.

    Attributes
    ----------
    data_adapter : DataAdapter
        Toolbox data-adapter used for all configured input and output access.
    df_in : dict[str, object] | None
        Cached runtime inputs loaded by :meth:`_load_inputs`.
    df_out : pd.DataFrame | None
        Cached GWDI output after :meth:`run`.
    input_order : ClassVar[tuple[str, ...]]
        Required order of GWDI input adapter names.
    precipitation_schema : ClassVar[dict[str, str | list[str]]]
        Schema contract for precipitation input (`time`, `fid`, `P`).
    evaporation_schema : ClassVar[dict[str, str | list[str]]]
        Schema contract for evaporation input (`time`, `fid`,
        `evaporation`).
    location_info_schema : ClassVar[dict[str, str | list[str]]]
        Schema contract for location metadata input (`location`,
        `position`).
    """

    data_adapter: DataAdapter
    df_in: dict[str, object] | None = None
    df_out: pd.DataFrame | None = None
    input_order: ClassVar[tuple[str, ...]] = (
        "precipitation",
        "evaporation",
        "location_info",
        "pastas_models",
        "df_stats_minima",
    )
    precipitation_schema: ClassVar[dict[str, str | list[str]]] = {
        "time": ["object", "datetime"],
        "fid": "integer",
        "P": "number",
    }
    evaporation_schema: ClassVar[dict[str, str | list[str]]] = {
        "time": ["object", "datetime"],
        "fid": "integer",
        "evaporation": "number",
    }
    location_info_schema: ClassVar[dict[str, str | list[str]]] = {
        "location": ["object", "text"],
        "position": ["object", "text", "integer"],
    }

    @staticmethod
    def _validate_location_info(info_locaties: pd.DataFrame) -> pd.DataFrame:
        """Validate and normalize `location_info` dataframe values.

        Parameters
        ----------
        info_locaties : pd.DataFrame
            Adapter-loaded GWDI location metadata with columns `location` and
            `position`.

        Returns
        -------
        pd.DataFrame
            Normalized location metadata with string columns.

        Raises
        ------
        UserWarning
            If `location`/`position` contain empty values, or if `location`
            contains duplicate values.
        """
        if info_locaties["location"].isna().any():
            raise UserWarning("GWDI location_info bevat lege waarden in `location`.")
        if info_locaties["position"].isna().any():
            raise UserWarning("GWDI location_info bevat lege waarden in `position`.")

        prepared_info = info_locaties.copy()
        prepared_info["location"] = prepared_info["location"].astype(str)
        prepared_info["position"] = prepared_info["position"].astype(str)

        if prepared_info["location"].str.strip().eq("").any():
            raise UserWarning("GWDI location_info bevat lege waarden in `location`.")
        if prepared_info["position"].str.strip().eq("").any():
            raise UserWarning("GWDI location_info bevat lege waarden in `position`.")

        duplicate_locations = prepared_info["location"][
            prepared_info["location"].duplicated()
        ].unique()
        if len(duplicate_locations) > 0:
            raise UserWarning(
                "GWDI location_info bevat dubbele `location`-waarden: "
                + ", ".join(sorted(duplicate_locations))
            )

        return prepared_info

    @staticmethod
    def _sync_pastas_logger(tbci_logger: logging.Logger | None) -> None:
        """Align the Pastas logger with the configured Toolbox logger.

        Parameters
        ----------
        tbci_logger : logging.Logger | None
            Logger configured by the Toolbox `DataAdapter`.
        """
        pastas_logger = logging.getLogger("pastas")

        if tbci_logger is None:
            pastas_logger.setLevel(logging.ERROR)
            pastas_logger.propagate = True
            return

        pastas_logger.setLevel(tbci_logger.level)
        pastas_logger.handlers = list(tbci_logger.handlers)
        pastas_logger.propagate = False

    def _load_inputs(
        self, input: list[str]
    ) -> tuple[
        pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, object], pd.DataFrame
    ]:
        """Load GWDI runtime inputs from adapter names in fixed order.

        Parameters
        ----------
        input : list[str]
            Names of input adapters in this order:
            `precipitation, evaporation, location_info, pastas_models,
            df_stats_minima`.

        Returns
        -------
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, object], pd.DataFrame]
            Loaded precipitation, evaporation, location metadata, model dict,
            and minima-statistics dataframe.

        Raises
        ------
        UserWarning
            If the input list does not contain exactly five adapter names.
        """
        if len(input) != 5:
            raise UserWarning(
                "Input variabele moet 5 string waarden bevatten in deze volgorde: "
                + ", ".join(self.input_order)
            )

        precipitation_adapter = input[0]
        evaporation_adapter = input[1]
        location_info_adapter = input[2]
        pastas_models_adapter = input[3]
        df_stats_minima_adapter = input[4]

        df_precipitation = self.data_adapter.input(
            precipitation_adapter, schema=self.precipitation_schema
        )
        df_evaporation = self.data_adapter.input(
            evaporation_adapter, schema=self.evaporation_schema
        )
        info_locaties = self.data_adapter.input(
            location_info_adapter, schema=self.location_info_schema
        )
        info_locaties = self._validate_location_info(info_locaties)
        dict_models = self.data_adapter.input(pastas_models_adapter)
        df_stats_minima = self.data_adapter.input(df_stats_minima_adapter)
        self.df_in = {
            "precipitation": df_precipitation,
            "evaporation": df_evaporation,
            "location_info": info_locaties,
            "pastas_models": dict_models,
            "df_stats_minima": df_stats_minima,
        }
        return (
            df_precipitation,
            df_evaporation,
            info_locaties,
            dict_models,
            df_stats_minima,
        )

    def run(self, input: list[str], output: str) -> None:
        """Execute GWDI inference for all available `fid` values.

        Parameters
        ----------
        input : list[str]
            Input adapter names in this exact order:
            `[precipitation, evaporation, location_info, pastas_models, df_stats_minima]`.

            Expected adapter payload contracts:

            - `precipitation`: dataframe with `time`, `fid`, `P`.
            - `evaporation`: dataframe with `time`, `fid`, `evaporation`.
            - `location_info`: dataframe with `location`, `position`.
            - `pastas_models`: dictionary of loaded Pastas models.
            - `df_stats_minima`: dataframe with return periods as index and
              location names as columns.
        output : str
            Output adapter name that receives GWDI rows with columns:
            `fid, parameterid, methodid, datetime, value, peilbuisid`.
        """
        self._sync_pastas_logger(self.data_adapter.logger)
        df_precip, df_evap, info_locs, dict_models, df_stats = self._load_inputs(input)

        self.df_out = GwdiModel.compute_gwdi(
            df_precipitation=df_precip,
            df_evaporation=df_evap,
            info_locaties=info_locs,
            dict_models=dict_models,
            df_stats_minima=df_stats,
        )
        self.data_adapter.output(output, self.df_out)
