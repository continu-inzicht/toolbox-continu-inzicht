import io
from datetime import timedelta
import os
from typing import ClassVar, Optional

import pandas as pd
from pydantic.dataclasses import dataclass
import requests
import xarray as xr

from toolbox_continu_inzicht.base.base_module import ToolboxBase
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.gwdi.retrieval.gwdi_base_retrieval import (
    GwdiWiwbRetrievalBase,
)


@dataclass(config={"arbitrary_types_allowed": True})
class GwdiKnmiRetrieval(ToolboxBase, GwdiWiwbRetrievalBase):
    """Retrieve KNMI EV24 evaporation for point locations and publish table output.

    Reference:
    - KNMI EV24 daily evaporation product documentation:
      https://dataplatform.knmi.nl/dataset/ev24-2
    """

    data_adapter: DataAdapter

    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    input_schema: ClassVar[dict[str, str | list[str]]] = {
        "fid": "integer",
        "name": ["object", "text"],
        "x": "number",
        "y": "number",
    }

    @staticmethod
    def default_options() -> dict:
        return {
            "knmi_api_base_url": "https://api.dataplatform.knmi.nl/open-data/v1",
            "knmi_api_key_env": "KNMI_API_KEY",
            "knmi_dataset_name": "EV24",
            "knmi_dataset_version": "2",
            "knmi_file_template": "INTER_OPER_R___EV24____L3__{start}_{end}_0003.nc",
            "lag_days": 0,
            "publish_days": 35,
            "input_crs": "EPSG:4326",
            "time_name": "time",
            "x_name": "x",
            "y_name": "y",
        }

    def run(self, input: str, output: str) -> None:
        options = self._combined_options()

        self.df_in = self.normalize_locations_table(
            self.data_adapter.input(input, schema=self.input_schema)
        )

        publish_start, publish_end = self.resolve_publish_window(
            calc_time=self.data_adapter.config.global_variables["calc_time"],
            publish_days=options["publish_days"],
            target_date=options.get("target_date"),
        )

        with requests.Session() as session:
            df_out = self._retrieve_dataset(
                df_locations=self.df_in,
                publish_start=publish_start,
                publish_end=publish_end,
                options=options,
                session=session,
            )
        if len(df_out) == 0:
            raise UserWarning(
                "KNMI verdamping bevat geen data voor het publicatievenster."
            )
        if df_out[["time", "fid"]].duplicated().any():
            raise UserWarning("KNMI verdamping bevat dubbele (`time`, `fid`)-rijen.")

        self.df_out = df_out.sort_values(["time", "fid"]).reset_index(drop=True)
        self.data_adapter.output(output, self.df_out)

    def fetch_evaporation_day(
        self,
        options: dict,
        day: pd.Timestamp,
        session: requests.Session | None = None,
    ) -> xr.Dataset:
        content = self._download_evaporation_day_bytes(
            options=options,
            day=day,
            session=session,
        )
        with xr.open_dataset(io.BytesIO(content)) as ds:
            return ds.load()

    def _download_evaporation_day_bytes(
        self,
        options: dict,
        day: pd.Timestamp,
        session: requests.Session | None = None,
    ) -> bytes:
        request_client = session if session is not None else requests
        api_key_env = str(options["knmi_api_key_env"])
        if api_key_env not in os.environ:
            raise UserWarning(
                f"Omgevingsvariabele `{api_key_env}` ontbreekt voor KNMI-authenticatie."
            )

        dataset_name = str(options["knmi_dataset_name"])
        dataset_version = str(options["knmi_dataset_version"])
        start_time = day.strftime("%Y%m%dT%H%M%S")
        end_time = (day + timedelta(days=1)).strftime("%Y%m%dT%H%M%S")
        file_name = str(options["knmi_file_template"]).format(
            start=start_time,
            end=end_time,
        )

        base_url = str(options["knmi_api_base_url"]).rstrip("/")
        file_url = (
            f"{base_url}/datasets/{dataset_name}/versions/{dataset_version}"
            f"/files/{file_name}/url"
        )
        headers = {"Authorization": os.environ[api_key_env]}
        response = request_client.get(file_url, headers=headers, timeout=120)
        if response.status_code != 200:
            raise UserWarning(
                f"KNMI bestand-url niet beschikbaar voor {day.strftime('%Y-%m-%d')} "
                f"({response.status_code}): {response.text}"
            )
        payload = response.json()
        if payload.get("message") == "Not Found":
            raise UserWarning(
                f"KNMI EV24-bestand niet gevonden voor {day.strftime('%Y-%m-%d')}."
            )
        temporary_download_url = payload.get("temporaryDownloadUrl")
        if temporary_download_url is None:
            raise UserWarning("KNMI antwoord bevat geen `temporaryDownloadUrl`.")

        file_response = request_client.get(temporary_download_url, timeout=300)
        if file_response.status_code != 200:
            raise UserWarning(
                "KNMI EV24-download mislukt "
                f"({file_response.status_code}): {file_response.text}"
            )
        return bytes(file_response.content)

    def _combined_options(self) -> dict:
        global_variables = self.data_adapter.config.global_variables
        if "GwdiKnmiRetrieval" not in global_variables:
            raise UserWarning(
                "Verplichte configuratiesectie `GwdiKnmiRetrieval` ontbreekt."
            )
        options = self.default_options()
        options.update(global_variables["GwdiKnmiRetrieval"])
        return options

    def _retrieve_dataset(
        self,
        df_locations: pd.DataFrame,
        publish_start: pd.Timestamp,
        publish_end: pd.Timestamp,
        options: dict,
        session: requests.Session | None = None,
    ) -> pd.DataFrame:
        time_name = str(options.get("time_name", "time"))
        source_start, source_end = self.resolve_source_window(
            publish_start=publish_start,
            publish_end=publish_end,
            options=options,
        )

        dataframes: list[pd.DataFrame] = []
        for day in pd.date_range(source_start, source_end, freq="D"):
            netcdf_content = self._download_evaporation_day_bytes(
                options=options,
                day=day,
                session=session,
            )
            with xr.open_dataset(io.BytesIO(netcdf_content)) as ds_raw:
                ds_prepared = self._prepare_evaporation_dataset(
                    dataset=ds_raw,
                    df_locations=df_locations,
                    options=options,
                    day=day,
                ).load()
            if len(ds_prepared[time_name]) == 0:
                raise UserWarning(
                    "Verdampingsbrondata bevat geen dagwaarde voor "
                    f"{day.strftime('%Y-%m-%d')}."
                )
            df_day = ds_prepared["evaporation"].to_dataframe().reset_index()
            if time_name != "time":
                df_day = df_day.rename(columns={time_name: "time"})
            dataframes.append(df_day.loc[:, ["time", "fid", "evaporation"]])

        if len(dataframes) == 0:
            raise UserWarning(
                "Geen KNMI verdampingsdata beschikbaar voor het publicatievenster."
            )

        df_out = pd.concat(dataframes, ignore_index=True)
        df_out["time"] = pd.to_datetime(df_out["time"])
        df_out["fid"] = df_out["fid"].astype(int)
        df_out = df_out[df_out["time"].between(publish_start, publish_end)]

        return df_out

    def _prepare_evaporation_dataset(
        self,
        dataset: xr.Dataset,
        df_locations: pd.DataFrame,
        options: dict,
        day: pd.Timestamp,
    ) -> xr.Dataset:
        time_name = str(options.get("time_name", "time"))
        if "evaporation" in dataset.data_vars:
            variable_name = "evaporation"
        elif "prediction" in dataset.data_vars:
            variable_name = "prediction"
        else:
            variable_name = list(dataset.data_vars)[0]

        ds = dataset
        if variable_name != "evaporation":
            ds = ds.rename({variable_name: "evaporation"})
        ds_points = self.sample_points_from_dataset(
            dataset=ds,
            variable_name="evaporation",
            locations_table=df_locations,
            window_start=day,
            window_end=day,
            time_name=time_name,
            x_name=str(options["x_name"]),
            y_name=str(options["y_name"]),
            input_crs=options.get("input_crs"),
        )
        return ds_points.sortby(time_name)
