from datetime import datetime, timedelta, timezone
import warnings
from pydantic.dataclasses import dataclass
import pandas as pd
from typing import Optional

from toolbox_continu_inzicht.loads.loads_matroos.get_matroos_locations import (
    get_matroos_locations,
    get_matroos_models,
)
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.utils.fetch_functions import fetch_data

aquo_matroos_dict = {"WATHTE": "waterlevel"}
matroos_aquo_dict = {"waterlevel": "WATHTE"}
aquo_id_dict = {"WATHTE": 4724}


@dataclass(config={"arbitrary_types_allowed": True})
class LoadsMatroos:
    """
    Belasting gegevens ophalen van rijkswaterstaat waterwebservices https://noos.matroos.rws.nl/
    """

    data_adapter: DataAdapter

    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    url_retrieve_series_noos: str = "noos.matroos.rws.nl/direct/get_series.php?"
    url_retrieve_series_matroos: str = "matroos.rws.nl/direct/get_series.php?"
    url_retrieve_series_vitaal: str = "vitaal.matroos.rws.nl/direct/get_series.php?"

    async def run(self, input: str, output: str) -> None:
        """
        De runner van de Belasting matroos.
        """

        # haal opties en dataframe van de config
        global_variables = self.data_adapter.config.global_variables
        options = global_variables["LoadsMatroos"]
        if "MISSING_VALUE" not in options:
            options["MISSING_VALUE"] = -999

        self.df_in = self.data_adapter.input(input)

        # doe een data type check
        if "measurement_location_code" not in self.df_in.columns:
            raise UserWarning(
                f"Input data 'measurement_location_code' ontbreekt in kollomen {self.df_in.columns}"
            )
        else:
            df_sources = await get_matroos_models()
            # maak een lijst met alle parameter namen, noos herhekend ook een heleboel aliases
            list_aliases = []
            for alias in list(df_sources["source_alias"]):
                list_aliases.extend(alias.split(";"))

            if options["model"] not in list_aliases:
                raise UserWarning(
                    "Gegeven model bestaat niet, indeien nodig gebruik get_matroos_models() voor meer info"
                )

            # haal de locaties op die bij de bron horen
            gdf_locations = await get_matroos_locations(source=options["model"])
            available_location_names = list(gdf_locations["measurement_location_code"])
            # maak een set van de namen en formateer ze zonder spaties en hoofdletters
            available_location_names = set(
                self.format_location_names(available_location_names)
            )

            # herhaal formateren voor de gegeven locatie namen
            supplied_location_names = self.format_location_names(
                list(self.df_in["measurement_location_code"].values)
            )
            # die je wilt ophalen is het overlap tussen de twee
            wanted_location_names = available_location_names.intersection(
                supplied_location_names
            )

            # geef een warning als we locatie namen mee geven die niet bestaan
            if len(wanted_location_names) < len(supplied_location_names):
                locations_not_found = set(supplied_location_names).difference(
                    wanted_location_names
                )
                warnings.warn(f"location {locations_not_found}")

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
            aquo_parameter = aquo_matroos_dict[parameter]  # "WATHTE -> waterlevel"
            request_forecast_url = self.generate_url(
                t_now, options, global_variables, aquo_parameter, wanted_location_names
            )
            status, json_data = await fetch_data(
                url=request_forecast_url, params={}, mime_type="json"
            )
            if status is None and json_data is not None:
                if "results" in json_data:
                    self.df_out = self.create_dataframe(options, t_now, json_data)
                else:
                    raise ConnectionError(
                        f"No results in data, only: {json_data.keys()}"
                    )
            else:
                raise ConnectionError(f"Connection failed:{status}")

    @staticmethod
    def format_location_names(location_names: list[str]) -> list[str]:
        """takes a list with locations names and removes spaces and make lower case"""
        return [location.lower().replace(" ", "") for location in location_names]

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
        dataframe = pd.DataFrame()
        records = []
        # loop over de lijst met data heen
        for serie in json_data["results"]:
            # hier zit ook coordinaten in
            measurement_location_id = serie["location"]["properties"]["locationId"]
            measurement_location_name = serie["location"]["properties"]["locationName"]
            measurement_location_code = measurement_location_name.lower().replace(
                " ", ""
            )
            parameter_matroos = serie["observationType"]["quantityName"]
            # "waterlevel -> WATHTE", code matchign WATHTE
            parameter_code = matroos_aquo_dict[parameter_matroos]
            parameter_id = aquo_id_dict[parameter_code]
            # process per lijst en stop het in een record
            for event in serie["events"]:
                datestr = event["timeStamp"]
                utc_dt = datetime.fromisoformat(datestr)

                if utc_dt > t_now:
                    value_type = "verwachting"
                else:
                    value_type = "meting"

                if event["value"]:
                    value = float(event["value"])
                else:
                    value = options["MISSING_VALUE"]

                record = {
                    "measurement_location_id": measurement_location_id,
                    "measurement_location_code": measurement_location_code,
                    "measurement_location_description": measurement_location_name,
                    "parameter_id": parameter_id,
                    "parameter_code": parameter_code,
                    "datetime": utc_dt,
                    "value": value,
                    "value_type": value_type,
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
                        + self.url_retrieve_series_vitaal
                    )

        moments = global_variables["moments"]
        tstart = (t_now + timedelta(hours=int(moments[0]))).strftime("%Y%m%d%H%M")
        tend = (t_now + timedelta(hours=int(moments[-1]))).strftime("%Y%m%d%H%M")
        source = options["model"]
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
