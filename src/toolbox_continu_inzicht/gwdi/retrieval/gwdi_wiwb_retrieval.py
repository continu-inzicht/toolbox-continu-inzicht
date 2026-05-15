import io
from datetime import timedelta
import os
import time
from typing import ClassVar

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
class GwdiWiwbRetrieval(ToolboxBase, GwdiWiwbRetrievalBase):
    """Retrieve WIWB precipitation for point locations.

    Parameters
    ----------
    data_adapter : DataAdapter
        Data adapter used for reading input locations and writing output tables.

    Attributes
    ----------
    data_adapter : DataAdapter
        Data adapter instance.
    df_in : pd.DataFrame | None
        Normalized input locations after ``run``.
    df_out : pd.DataFrame | None
        Retrieved precipitation table after ``run``.
    input_schema : ClassVar[dict[str, str | list[str]]]
        Validation schema for location input.

    Notes
    -----
    WIWB API reference used by this module:
    https://portal.hydronet.com/data/files/Technische%20Instructies%20WIWB%20API.pdf
    """

    data_adapter: DataAdapter

    df_in: pd.DataFrame | None = None
    df_out: pd.DataFrame | None = None

    input_schema: ClassVar[dict[str, str | list[str]]] = {
        "fid": "integer",
        "name": ["object", "text"],
        "x": "number",
        "y": "number",
    }

    @staticmethod
    def default_options() -> dict[str, object]:
        """Return default runtime options.

        Returns
        -------
        dict[str, object]
            Default WIWB retrieval options.
        """
        return {
            "wiwb_api_url": "https://wiwb.hydronet.com/api",
            "wiwb_auth_url": "https://login.hydronet.com/auth/realms/hydronet/protocol/openid-connect/token",
            "wiwb_client_id_env": "WIWB_CLIENT_ID",
            "wiwb_client_secret_env": "WIWB_KEY",
            "wiwb_poll_interval_seconds": 5,
            "wiwb_poll_timeout_seconds": 300,
            "wiwb_precipitation_source_code": "Knmi.International.Radar.Composite.Combined",
            "lag_days": 0,
            "publish_days": 35,
            "align_daily_to_start_label": True,
            "input_crs": "EPSG:4326",
            "time_name": "time",
            "x_name": "x",
            "y_name": "y",
        }

    def run(self, input: str, output: str) -> None:
        """Execute WIWB retrieval for configured locations.

        Parameters
        ----------
        input : str
            Input adapter key containing location table.
        output : str
            Output adapter key for precipitation table.

        Raises
        ------
        UserWarning
            If retrieval returns no data or duplicate ``(time, fid)`` rows.
        """
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
                "WIWB neerslag bevat geen data voor het publicatievenster."
            )
        if df_out[["time", "fid"]].duplicated().any():
            raise UserWarning("WIWB neerslag bevat dubbele (`time`, `fid`)-rijen.")

        self.df_out = df_out.sort_values(["time", "fid"]).reset_index(drop=True)
        self.data_adapter.output(output, self.df_out)

    def fetch_precipitation(
        self,
        options: dict[str, object],
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        session: requests.Session | None = None,
    ) -> xr.Dataset:
        """Fetch WIWB precipitation dataset for a date range.

        Parameters
        ----------
        options : dict[str, object]
            Retrieval options.
        start_date : pd.Timestamp
            Request start timestamp.
        end_date : pd.Timestamp
            Request end timestamp.
        session : requests.Session | None, optional
            Existing requests session.

        Returns
        -------
        xarray.Dataset
            Loaded source dataset.
        """
        content = self._download_precipitation_bytes(
            options=options,
            start_date=start_date,
            end_date=end_date,
            session=session,
        )
        with xr.open_dataset(io.BytesIO(content)) as ds:
            return ds.load()

    def _download_precipitation_bytes(
        self,
        options: dict[str, object],
        start_date: pd.Timestamp,
        end_date: pd.Timestamp,
        session: requests.Session | None = None,
    ) -> bytes:
        """Download WIWB precipitation as NetCDF bytes.

        Parameters
        ----------
        options : dict[str, object]
            Retrieval options.
        start_date : pd.Timestamp
            Request start timestamp.
        end_date : pd.Timestamp
            Request end timestamp.
        session : requests.Session | None, optional
            Existing requests session.

        Returns
        -------
        bytes
            NetCDF payload bytes.

        Raises
        ------
        UserWarning
            If WIWB request, polling or download fails.
        """
        request_client = session if session is not None else requests
        token = self._get_token(options=options, session=session)
        api_url = str(options["wiwb_api_url"]).rstrip("/")
        source_code = str(options["wiwb_precipitation_source_code"])
        headers = {
            "content-type": "application/json",
            "Authorization": f"Bearer {token}",
        }

        request_payload = {
            "Readers": [
                {
                    "DataSourceCode": source_code,
                    "Settings": {
                        "StartDate": start_date.strftime("%Y%m%d%H%M%S"),
                        "EndDate": end_date.strftime("%Y%m%d%H%M%S"),
                        "VariableCodes": ["P"],
                        # Ask WIWB to aggregate native radar timesteps to daily values.
                        # See WIWB API docs linked in class docstring.
                        "Interval": {"Type": "Days", "Value": 1},
                        "StructureType": "Grid",
                    },
                }
            ],
            "Exporter": {"DataFormatCode": "netcdf4.cf1p6"},
            "DataFlowTypeCode": "Download",
            "DataSourceCode": source_code,
        }

        response = request_client.post(
            f"{api_url}/grids/createdownload",
            headers=headers,
            json=request_payload,
            timeout=120,
        )
        if response.status_code != 200:
            raise UserWarning(
                f"WIWB download-aanvraag mislukt ({response.status_code}): {response.text}"
            )
        data_flow_id = response.json().get("DataFlowId")
        if data_flow_id is None:
            raise UserWarning("WIWB download-aanvraag bevat geen `DataFlowId`.")

        poll_timeout = int(options["wiwb_poll_timeout_seconds"])
        poll_interval = int(options["wiwb_poll_interval_seconds"])
        status_payload = {"DataFlowId": [data_flow_id]}
        start_seconds = time.time()
        progress = 0
        while progress < 100:
            if time.time() - start_seconds > poll_timeout:
                raise UserWarning(
                    f"WIWB download timeout voor DataFlowId={data_flow_id}."
                )
            status_response = request_client.post(
                f"{api_url}/entity/dataflows/get",
                headers=headers,
                json=status_payload,
                timeout=120,
            )
            if status_response.status_code != 200:
                raise UserWarning(
                    "WIWB status-aanvraag mislukt "
                    f"({status_response.status_code}): {status_response.text}"
                )
            progress = int(
                status_response.json()["DataFlows"][str(data_flow_id)]["Progress"]
            )
            if progress < 100:
                time.sleep(poll_interval)

        download_response = request_client.get(
            f"{api_url}/grids/downloadfile?dataflowid={data_flow_id}",
            headers=headers,
            timeout=300,
        )
        if download_response.status_code != 200:
            raise UserWarning(
                f"WIWB downloaden mislukt ({download_response.status_code}): {download_response.text}"
            )
        return bytes(download_response.content)

    def _get_token(
        self,
        options: dict[str, object],
        session: requests.Session | None = None,
    ) -> str:
        """Retrieve WIWB OAuth access token.

        Parameters
        ----------
        options : dict[str, object]
            Retrieval options with env var keys for client id/secret.
        session : requests.Session | None, optional
            Existing requests session.

        Returns
        -------
        str
            Access token.

        Raises
        ------
        UserWarning
            If required env vars are missing or token request fails.
        """
        client_id_env_key = str(options["wiwb_client_id_env"])
        if client_id_env_key not in os.environ:
            raise UserWarning(
                f"Omgevingsvariabele `{client_id_env_key}` ontbreekt voor WIWB-authenticatie."
            )
        client_id = os.environ[client_id_env_key]

        secret_env_key = str(options["wiwb_client_secret_env"])
        if secret_env_key not in os.environ:
            raise UserWarning(
                f"Omgevingsvariabele `{secret_env_key}` ontbreekt voor WIWB-authenticatie."
            )
        client_secret = os.environ[secret_env_key]

        request_client = session if session is not None else requests
        response = request_client.post(
            str(options["wiwb_auth_url"]),
            data={"grant_type": "client_credentials"},
            auth=(client_id, client_secret),
            timeout=60,
        )
        if response.status_code != 200:
            raise UserWarning(
                f"WIWB token-aanvraag mislukt ({response.status_code}): {response.text}"
            )
        token = response.json().get("access_token")
        if token is None:
            raise UserWarning("WIWB token-aanvraag gaf geen `access_token` terug.")
        return str(token)

    def _combined_options(self) -> dict[str, object]:
        """Merge default options with config overrides.

        Returns
        -------
        dict[str, object]
            Effective options.

        Raises
        ------
        UserWarning
            If required config section is missing.
        """
        global_variables = self.data_adapter.config.global_variables
        if "GwdiWiwbRetrieval" not in global_variables:
            raise UserWarning(
                "Verplichte configuratiesectie `GwdiWiwbRetrieval` ontbreekt."
            )
        options = self.default_options()
        options.update(global_variables["GwdiWiwbRetrieval"])
        return options

    def _retrieve_dataset(
        self,
        df_locations: pd.DataFrame,
        publish_start: pd.Timestamp,
        publish_end: pd.Timestamp,
        options: dict[str, object],
        session: requests.Session | None = None,
    ) -> pd.DataFrame:
        """Retrieve and prepare WIWB precipitation table for publish window.

        Parameters
        ----------
        df_locations : pd.DataFrame
            Location table.
        publish_start : pd.Timestamp
            Publish window start.
        publish_end : pd.Timestamp
            Publish window end.
        options : dict[str, object]
            Effective retrieval options.
        session : requests.Session | None, optional
            Existing requests session.

        Returns
        -------
        pd.DataFrame
            Output with columns ``time``, ``fid``, ``P``.

        Raises
        ------
        UserWarning
            If WIWB source data is unavailable for the requested window.
        """
        time_name = str(options.get("time_name", "time"))
        source_start, source_end = self.resolve_source_window(
            publish_start=publish_start,
            publish_end=publish_end,
            options=options,
        )
        align_daily_to_start = bool(options.get("align_daily_to_start_label", True))
        request_start = source_start
        request_end = source_end
        if align_daily_to_start:
            # WIWB daily values are end-labeled for this source. To map them to
            # start-labeled daily periods after shifting -1 day, request one extra
            # day at the end.
            request_end = source_end + timedelta(days=1)

        netcdf_content = self._download_precipitation_bytes(
            options=options,
            start_date=request_start,
            end_date=request_end,
            session=session,
        )
        with xr.open_dataset(io.BytesIO(netcdf_content)) as ds_raw:
            # Select locations and time before loading into memory.
            # Note: this method only slices/samples; no temporal aggregation.
            ds_prepared = self.sample_points_from_dataset(
                dataset=ds_raw,
                locations_table=df_locations,
                window_start=request_start,
                window_end=request_end,
                variable_name="P",
                time_name=time_name,
                x_name=str(options["x_name"]),
                y_name=str(options["y_name"]),
                input_crs=options.get("input_crs"),
            ).load()

        if len(ds_prepared[time_name]) == 0:
            raise UserWarning("Geen WIWB neerslagdata beschikbaar.")

        df_out = ds_prepared["P"].to_dataframe().reset_index()
        if time_name != "time":
            df_out = df_out.rename(columns={time_name: "time"})
        df_out = df_out.loc[:, ["time", "fid", "P"]].copy()

        # Empirically WIWB daily timestamp is end-labeled for this source/interval.
        # Shift to start-label so daily value at day D represents [D, D+1), which
        # aligns better with downstream intuition and KNMI EV24 handling.
        if align_daily_to_start:
            df_out["time"] = pd.to_datetime(df_out["time"]) - timedelta(days=1)

        df_out = df_out[df_out["time"].between(publish_start, publish_end)]
        df_out = df_out.sort_values(["time", "fid"]).reset_index(drop=True)

        return df_out
