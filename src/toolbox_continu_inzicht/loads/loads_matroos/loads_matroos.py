from datetime import datetime, timedelta, timezone
import warnings
from pydantic.dataclasses import dataclass
import pandas as pd
from typing import Optional

import requests


from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.utils.datetime_functions import epoch_from_datetime


@dataclass(config={"arbitrary_types_allowed": True})
class LoadsMatroos:
    """
    Belasting gegevens ophalen van rijkswaterstaat waterwebservices https://noos.matroos.rws.nl/
    """

    data_adapter: DataAdapter
    input: str
    output: str

    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    url_retrieve_series_noos: str = "noos.matroos.rws.nl/direct/get_series.php?"
    url_retrieve_series_matroos: str = "matroos.rws.nl/direct/get_series.php?"
    url_retrieve_series_vitaal: str = "vitaal.rws.nl/direct/get_series.php?"

    async def run(self, input=None, output=None) -> None:
        """
        De runner van de Belasting matroos.
        """
        if input is None:
            input = self.input
        if output is None:
            output = self.output

        # haal opties en dataframe van de config
        global_variables = self.data_adapter.config.global_variables
        options = global_variables["BelastingMatroos"]

        self.df_in = self.data_adapter.input(input)

        # doe een data type check
        if "measuringstationid" not in self.df_in.columns:
            raise UserWarning(
                f"Input data missing 'measuringstationid' in columns {self.df_in.columns}"
            )
        wanted_measuringstationid = list(self.df_in["measuringstationid"].values)

        # zet tijd goed
        dt_now = datetime.now(timezone.utc)
        t_now = datetime(
            dt_now.year,
            dt_now.month,
            dt_now.day,
            dt_now.hour,
            0,
            0,
        ).replace(tzinfo=timezone.utc)

        # maak een url aan
        for parameter in options["parameters"]:
            request_forecast_url = self.generate_url(
                t_now, options, global_variables, parameter, wanted_measuringstationid
            )
            res = await self.get_series(request_forecast_url)
            json_data = res.json()
            if "results" in json_data:
                self.df_out = self.create_dataframe(options, t_now, json_data)

    @staticmethod
    def create_dataframe(
        options: dict, t_now: datetime, json_data: list
    ) -> pd.DataFrame:
        """Maakt een dataframe met waardes van de rws water webservices

        Args:
            json_data (str): JSON data

        Returns:
            Dataframe: Pandas dataframe geschikt voor uitvoer
        """
        h10 = 1
        h10v = 2

        dataframe = pd.DataFrame()
        records = []
        # loop over de lijst met data heen
        for serie in json_data["results"]:
            # hier zit ook coordinaten in
            measuringstationid = (
                serie["location"]["properties"]["locationName"].lower().replace(" ", "")
            )
            measurement_or_observation = serie["source"]["name"]
            # process per lijst en stop het in een record
            for event in serie["events"]:
                datestr = event["timeStamp"]
                utc_dt = datetime.fromisoformat(datestr)

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
                        "measurementcode": measurement_or_observation,
                    }
                    records.append(record)

            # voeg de records samen
            dataframe = pd.DataFrame.from_records(records)
        return dataframe

    def generate_url(
        self, t_now, options, global_variables, parameter, location_names
    ) -> str:
        """Returns the needed url to make the request to the Noos server

        Args:
            t_now: datetime
                Huidige tijd, wordt gebruikt om meest recente voorspelling op te halen
            options: dict
                opties die door gebruiker zijn opgegeven, in dit geval is source het belangrijkst
        """
        if "website" not in options:
            raise UserWarning(
                "Specify a `website` to consult in config: Noos, Matroos or Vitaal. In case of the later two, also provide a password and username"
            )
        else:
            match options["website"]:
                case "noos":
                    base_url = "https://" + self.url_retrieve_series_noos
                case "matroos":
                    if (
                        "matroos_user" not in global_variables
                        and "matroos_password" not in global_variables
                    ):
                        raise UserWarning(
                            "Matroos.rws.nl is a password protected service, \
                                          either opt for `website: 'noos'` or provide `matroos_user` and `matroos_password` in the `.env` file"
                        )
                    base_url = (
                        "https://"
                        + global_variables["matroos_user"]
                        + ":"
                        + global_variables["matroos_password"]
                        + "@"
                        + self.url_retrieve_series_matroos
                    )
                case "vitaal":
                    if (
                        "vitaal_user" not in global_variables
                        and "vitaal_password" not in global_variables
                    ):
                        raise UserWarning(
                            "Vitaal.matroos.rws.nl is a password protected service, \
                                          either opt for `website: 'noos'` or provide `vitaal_user` and `vitaal_password` in the `.env` file"
                        )
                    base_url = (
                        "https://"
                        + global_variables["vitaal_user"]
                        + ":"
                        + global_variables["vitaal_password"]
                        + "@"
                        + self.url_retrieve_series_matroos
                    )

        moments = global_variables["moments"]
        tstart = (t_now + timedelta(hours=int(moments[0]))).strftime("%Y%m%d%H%M")
        tend = (t_now + timedelta(hours=int(moments[-1]))).strftime("%Y%m%d%H%M")
        source = options["source"]
        # Multiple locations can be specified by separating them with a semicolon ';'.
        location_names_str = ";".join(location_names)
        url = (
            base_url
            + f"loc={location_names_str}&"
            + f"source={source}&"
            + f"unit={parameter}&"
            + f"tstart={tstart}&"
            + f"tend={tend}&"
            + "format=dd_2.0.0&"
            + "timezone=GMT&"
            + "zip=0&"
        )
        return url

    async def get_series(self, url):
        """
        Haal een netcdf bestand op gegeven de url en slaat die op in de tijdelijke map
        """
        res = requests.get(url)
        if res.status_code == 404:
            warnings.warn(res.text)

        return res
