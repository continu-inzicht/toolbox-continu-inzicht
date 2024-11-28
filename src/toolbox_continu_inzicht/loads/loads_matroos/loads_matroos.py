from datetime import datetime, timedelta
import warnings
from pydantic.dataclasses import dataclass
import pandas as pd
from typing import Optional

from toolbox_continu_inzicht.loads.loads_matroos.get_matroos_locations import (
    get_matroos_locations,
    get_matroos_models,
)
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.utils.fetch_functions import fetch_data_get

aquo_matroos_dict = {"WATHTE": "waterlevel"}
matroos_aquo_dict = {"waterlevel": "WATHTE"}
aquo_id_dict = {"WATHTE": 4724}


@dataclass(config={"arbitrary_types_allowed": True})
class LoadsMatroos:
    """
    De LoadsMatroos klasse haalt belastinggegevens op van de Rijkswaterstaat Waterwebservices.

    Args:
        data_adapter: DataAdapter
            De data adapter voor het ophalen en opslaan van gegevens.
        df_in: Optional[pd.DataFrame] | None = None
            Het invoerdataframe.
        df_out: Optional[pd.DataFrame] | None = None
            Het uitvoerdataframe.
    """

    data_adapter: DataAdapter

    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    url_retrieve_series_noos: str = "noos.matroos.rws.nl/direct/get_series.php?"
    url_retrieve_series_matroos: str = "matroos.rws.nl/direct/get_series.php?"
    url_retrieve_series_vitaal: str = "vitaal.matroos.rws.nl/direct/get_series.php?"

    def run(self, input: str, output: str) -> None:
        """
        Voert de functie uit om gegevens op te halen en te verwerken voor de matroos-toolbox.

        Parameters
        ----------
        input: str
            Naam van de dataadapter met invoergegevens.
        output: str
            Naam van de dataadapter om uitvoergegevens op te slaan.
        """

        # haal opties en dataframe van de config
        global_variables = self.data_adapter.config.global_variables
        if "LoadsMatroos" not in global_variables:
            raise UserWarning(
                "LoadsMatroos sectie niet aanwezig in global_variables (config)"
            )

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
            df_sources = get_matroos_models()
            # maak een lijst met alle parameter namen, noos herhekend ook een heleboel aliases
            list_aliases = []
            for alias in list(df_sources["source_label"]):
                list_aliases.extend(alias.split(";"))

            # TODO geef melding van unknown_source als ophalen van waterstanden niet goed gaat.
            unknown_source: str = ""
            if options["model"] not in list_aliases:
                unknown_source = options["model"]
                UserWarning(
                    f"Gegeven model: {unknown_source} niet gevonden, we proberen het toch op te halen."
                )

            # haal de locaties op die bij de bron horen
            gdf_locations = get_matroos_locations(source=options["model"])
            available_location_names = list(gdf_locations["measurement_location_code"])

            # maak een set van de namen en formateer ze zonder spaties en hoofdletters
            available_location_names = set(available_location_names)

            # herhaal formateren voor de gegeven locatie namen
            supplied_location_names = list(
                self.df_in["measurement_location_code"].values
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
        calc_time = global_variables["calc_time"]

        lst_dfs = []
        # maak een url aan
        for parameter in options["parameters"]:
            aquo_parameter = aquo_matroos_dict[parameter]  # "WATHTE -> waterlevel"
            request_forecast_url = self.generate_url(
                calc_time,
                options,
                global_variables,
                aquo_parameter,
                wanted_location_names,
            )
            status, json_data = fetch_data_get(
                url=request_forecast_url, params={}, mime_type="json"
            )
            if status is None and json_data is not None:
                if "results" in json_data:
                    lst_dfs.append(
                        self.create_dataframe(options, self.df_in, calc_time, json_data)
                    )
                    # TODO voeg de id uit de input to aan het resultaat en schrijf die weg
                else:
                    raise ConnectionError(
                        f"No results in data, only: {json_data.keys()}"
                    )
            else:
                raise ConnectionError(f"Connection failed:{status}")

        self.df_out = pd.concat(lst_dfs, axis=0)
        self.data_adapter.output(output, self.df_out)

    @staticmethod
    def format_location_names(location_names: list[str]) -> list[str]:
        """Neemt een lijst met locatienamen en verwijdert spaties en maakt ze allemaal in kleine letters"""
        return [location.lower().replace(" ", "") for location in location_names]

    @staticmethod
    def create_dataframe(
        options: dict, df_in: pd.DataFrame, calc_time: datetime, json_data: list
    ) -> pd.DataFrame:
        """
        Maakt een dataframe met waardes van de rws water webservices

        Parameters
        ----------
        options: dict
            Een dictionary met opties uit de config
        calc_time: datetime
            De huidige tijd
        json_data: list
            Een lijst met JSON data


        Returns:
        --------
            pd.Dataframe
                Pandas dataframe geschikt voor uitvoer
        """
        dataframe = pd.DataFrame()
        records = []
        # loop over de lijst met data heen
        for serie in json_data["results"]:
            # hier zit ook coordinaten in
            measurement_location_name = serie["location"]["properties"]["locationName"]
            measurement_location_code = (
                measurement_location_name  # .lower().replace(" ", "")
            )
            measurement_location_id = df_in[
                df_in["measurement_location_code"].apply(lambda x: str(x))
                == str(measurement_location_code)
            ].iloc[0]["measurement_location_id"]
            parameter_matroos = serie["observationType"]["quantityName"]
            # "waterlevel -> WATHTE", code matchign WATHTE
            parameter_code = matroos_aquo_dict[parameter_matroos]
            parameter_id = aquo_id_dict[parameter_code]
            # process per lijst en stop het in een record
            for event in serie["events"]:
                datestr = event["timeStamp"]
                utc_dt = datetime.fromisoformat(datestr)

                if utc_dt > calc_time:
                    value_type = "verwachting"
                else:
                    value_type = "meting"

                if event["value"]:
                    # matroos is in meters
                    # https://publicwiki.deltares.nl/display/NETCDF/Matroos+Standard+names
                    value = float(event["value"])
                    value_cm = value * 100
                else:
                    value = options["MISSING_VALUE"]

                record = {
                    "measurement_location_id": measurement_location_id,
                    "measurement_location_code": measurement_location_code,
                    "measurement_location_description": measurement_location_name,
                    "parameter_id": parameter_id,
                    "parameter_code": parameter_code,
                    "date_time": utc_dt,
                    "unit": "cm",
                    "value": value_cm,
                    "value_type": value_type,
                }
                records.append(record)

            # voeg de records samen
            dataframe = pd.DataFrame.from_records(records)
        return dataframe

    def generate_url(
        self, calc_time, options, global_variables, parameter, location_names
    ) -> str:
        """
        Geeft de benodigde URL terug om het verzoek naar de Noos-server te maken

        Parameters
        ----------
        calc_time: datetime
            Huidige tijd, wordt gebruikt om de meest recente voorspelling op te halen
        options: dict
            Opties die door de gebruiker zijn opgegeven, in dit geval is 'source' het belangrijkst
        global_variables: dict
            Globale variabelen die nodig zijn om de URL te genereren
        parameter: str
            Eenheid van de parameter waarvoor gegevens worden opgehaald
        location_names: list
            Lijst van locatienamen waarvoor gegevens worden opgehaald

        Returns
        -------
            str
                De gegenereerde URL voor het verzoek aan de Noos-server
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
        tstart = (calc_time + timedelta(hours=int(moments[0]))).strftime("%Y%m%d%H%M")
        tend = (calc_time + timedelta(hours=int(moments[-1]))).strftime("%Y%m%d%H%M")
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
