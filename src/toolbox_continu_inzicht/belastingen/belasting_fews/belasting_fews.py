from datetime import datetime, timedelta, timezone
from pydantic.dataclasses import dataclass
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
import pandas as pd
from typing import Optional
from toolbox_continu_inzicht.utils.datetime_functions import (
    epoch_from_datetime,
    datetime_from_string,
)
from toolbox_continu_inzicht.utils.fetch_functions import fetch_data


@dataclass(config={"arbitrary_types_allowed": True})
class BelastingFews:
    """
    Met deze functie wordt er bij de opgegeven FEWS omgeving via
    REST gegevens opgehaald.
    """

    data_adapter: DataAdapter
    input: str
    output: str

    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    async def run(self, input=None, output=None) -> pd.DataFrame:
        """
        De runner van de Belasting Fews.

        Args:

        Returns:
            Dataframe: Pandas dataframe met opgehaalde gegevens uit FEWS.
        """
        if input is None:
            input = self.input
        if output is None:
            output = self.output

        self.df_in = self.data_adapter.input(input)

        dt_now = datetime.now(timezone.utc)
        t_now = datetime(
            dt_now.year,
            dt_now.month,
            dt_now.day,
            dt_now.hour,
            0,
            0,
        ).replace(tzinfo=timezone.utc)

        options = self.data_adapter.config.global_variables["BelastingFews"]

        url = self.create_url(options=options)
        parameters = self.create_params(
            t_now=t_now, options=options, locations=self.df_in
        )
        status, json_data = await fetch_data(
            url=url, params=parameters, mime_type="json"
        )

        if status is None and json_data is not None:
            self.df_out = self.create_dataframe(
                options=options, t_now=t_now, json_data=json_data, locations=self.df_in
            )

            self.data_adapter.output(output=output, df=self.df_out)

        return self.df_out

    def create_url(self, options: dict) -> str:
        """
        Maak een rest url voor FEWS

        Args:
            options (_type_): Options uit de invoer yaml

        Returns:
            str: URL
        """
        host = options["host"]
        port = options["port"]
        region = options["region"]

        return f"{host}:{port}/FewsWebServices/rest/{region}/v1/timeseries"

    def create_params(
        self, t_now: datetime, options: dict, locations: pd.DataFrame
    ) -> dict:
        """
        Maak een lijst van FEWS parameters om mee te sturen bij het ophalen van data.

        Args:
            t_now (datetime): T0 in UTC
            options (_type_): options uit de invoer yaml

        Returns:
            dict: lijst met parameters
        """
        params = {}
        moments = options["moments"]

        if len(moments) > 0:
            n_moments = len(moments) - 1
            starttime = t_now + timedelta(hours=int(moments[0]))
            endtime = t_now + timedelta(hours=int(moments[n_moments]))

            params["filterId"] = options["filter"]
            params["useDisplayUnits"] = False
            params["showThresholds"] = True
            params["omitMissing"] = True
            params["onlyHeaders"] = False
            params["showEnsembleMemberIds"] = False
            params["documentVersion"] = options["version"]
            params["documentFormat"] = "PI_JSON"
            params["forecastCount"] = 1
            params["parameterIds"] = options["parameters"]
            params["locationIds"] = locations["name"].tolist()
            params["startTime"] = starttime.strftime("%Y-%m-%dT%H:%M:%SZ")
            params["endTime"] = endtime.strftime("%Y-%m-%dT%H:%M:%SZ")

        return params

    def create_dataframe(
        self, options: dict, t_now: datetime, json_data: str, locations: pd.DataFrame
    ) -> pd.DataFrame:
        """Maak een pandas dataframe 

        Args:
            json_data (str): JSON data

        Returns:
            Dataframe: Pandas dataframe geschikt voor uitvoer
        """
        h10 = 1
        h10v = 2

        dataframe = pd.DataFrame()
        if "timeSeries" in json_data:
            records = []
            for serie in json_data["timeSeries"]:
                parameter = serie["header"]["parameterId"]
                locationcode = serie["header"]["locationId"]
                measuringstation = locations[locations["name"] == locationcode]

                if len(measuringstation) > 0:
                    measuringstationid = int(measuringstation["id"].iloc[0])

                    if parameter in options["parameters"]:
                        for event in serie["events"]:
                            datestr = f"{event['date']}T{event['time']}Z"
                            utc_dt = datetime_from_string(datestr, "%Y-%m-%dT%H:%M:%SZ")

                            parameterid = h10
                            if utc_dt > t_now:
                                parameterid = h10v

                            if parameterid > 0:
                                if event["value"]:
                                    value = float(event["value"])
                                else:
                                    value = options["MISSING_VALUE"]

                                record = {
                                    "objectid": measuringstationid,
                                    "objecttype": "measuringstation",
                                    "parameterid": parameterid,
                                    "datetime": epoch_from_datetime(utc_dt=utc_dt),
                                    "value": value,
                                    "calculating": True,
                                }
                                records.append(record)

            dataframe = pd.DataFrame.from_records(records)

        return dataframe
