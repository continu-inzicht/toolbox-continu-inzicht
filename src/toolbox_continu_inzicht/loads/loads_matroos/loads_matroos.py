from datetime import datetime, timedelta
import warnings
from pydantic.dataclasses import dataclass
import pandas as pd
from typing import Optional
from pathlib import Path
import tempfile
import xarray as xr

from toolbox_continu_inzicht.loads.loads_matroos.get_matroos_locations import (
    get_matroos_locations,
    get_matroos_sources,
)
from toolbox_continu_inzicht.base.base_module import ToolboxBase
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.utils.fetch_functions import fetch_data_get
from toolbox_continu_inzicht.base.aquo import read_aquo


# dit is functie specifiek omdat waterlevel niet in de aquo standaard zit
matroos_aquo_synoniem = {"water height": "waterlevel"}


@dataclass(config={"arbitrary_types_allowed": True})
class LoadsMatroos(ToolboxBase):
    """
    Haalt matroos tijdserie informatie op uit de Noos, Matroos of Vitaal server.

    De Matroos informatie is beschikbaar op de volgende websites:
    [https://noos.matroos.rws.nl/maps1d/](https://noos.matroos.rws.nl/maps1d/)

    Met de `functie get_matroos_sources()` kan je de beschikbare bronnen ophalen.
    Met de functie `get_matroos_locations(source='...')` kan je de bijbehorende beschikbare locaties ophalen.

    Attributes
    ----------
    data_adapter: DataAdapter
        Data adapter object for input and output data.
    df_in: Optional[pd.DataFrame] | None
        Input dataframe containing measurement location codes.
    df_out: Optional[pd.DataFrame] | None
        Output dataframe containing processed data.
    url_retrieve_series_noos: str
        URL for retrieving series from Noos server.
    url_retrieve_series_matroos: str
        URL for retrieving series from Matroos server.
    url_retrieve_series_vitaal: str
        URL for retrieving series from Vitaal server.
    """

    data_adapter: DataAdapter

    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    url_retrieve_series_noos: str = "noos.matroos.rws.nl/direct/get_series.php?"
    url_retrieve_series_matroos: str = "matroos.rws.nl/direct/get_series.php?"
    url_retrieve_series_vitaal: str = "vitaal.matroos.rws.nl/direct/get_series.php?"

    def run(self, input: str, output: str) -> None:
        """
        Voert de functie uit om gegevens op te halen en te verwerken voor matroos gegevens.

        Parameters
        ----------
        input: str
            Naam van de dataadapter met invoergegevens.
        output: str
            Naam van de dataadapter om uitvoergegevens op te slaan.

        Raises
        ------
        UserWarning
            Als de 'LoadsMatroos' sectie niet aanwezig is in de global_variables (config).
            Als de 'measurement_location_code' ontbreekt in de inputdata.
            Als het gegeven model niet wordt herkend.
            Als de locaties niet worden gevonden.
        ConnectionError
            Als er geen resultaten in de data zitten.
            Als de verbinding mislukt.
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

        wanted_location_names = self.get_matroos_available_locations(
            self.df_in, options, endpoint_model="timeseries"
        )

        # zet tijd goed
        calc_time = global_variables["calc_time"]

        lst_dfs = []
        # maak een url aan
        for parameter in options["parameters"]:
            parameter_code, aquo_grootheid_dict = read_aquo(parameter, global_variables)
            aquo_parameter = matroos_aquo_synoniem[
                aquo_grootheid_dict["label_en"]
            ]  # "WATHTE -> waterlevel"
            request_forecast_url = self.generate_url(
                options,
                global_variables,
            )

            moments = global_variables["moments"]
            tstart = (calc_time + timedelta(hours=int(moments[0]))).strftime(
                "%Y%m%d%H%M"
            )
            tend = (calc_time + timedelta(hours=int(moments[-1]))).strftime(
                "%Y%m%d%H%M"
            )
            source = options["model"]
            # Multiple locations can be specified by separating them with a semicolon ';'.
            location_names_str = ";".join(wanted_location_names)
            params = {
                "loc": location_names_str,
                "source": source,
                "unit": aquo_parameter,
                "tstart": tstart,
                "tend": tend,
                "format": "dd_2.0.0",
                "timezone": "GMT",
                "zip": "0",
            }
            status, json_data = fetch_data_get(
                url=request_forecast_url, params=params, mime_type="json", timeout=120
            )
            if status is None and json_data is not None:
                if "results" in json_data:
                    lst_dfs.append(
                        self.create_dataframe(
                            options, self.df_in, calc_time, json_data, global_variables
                        )
                    )
                else:
                    raise ConnectionError(
                        f"No results in data, only: {json_data.keys()}"
                    )
            else:
                raise ConnectionError(f"Connection failed:{status}")

        self.df_out = pd.concat(lst_dfs, axis=0)
        self.data_adapter.output(output, self.df_out)

    def get_matroos_available_locations(
        self, df_in, options, endpoint_model
    ) -> pd.DataFrame:
        # doe een data type check
        if "measurement_location_code" not in df_in.columns:
            raise UserWarning(
                f"Input data 'measurement_location_code' ontbreekt in kolommen {df_in.columns}"
            )
        else:
            df_sources = get_matroos_sources(endpoint=endpoint_model)
            # maak een lijst met alle parameter namen, noos herkent ook een heleboel aliases
            list_aliases = []
            for alias in list(df_sources["source_label"]):
                list_aliases.extend(alias.split(";"))

            # geef melding van unknown_source als ophalen van waterstanden niet goed gaat.
            unknown_source: str = ""
            if options["model"] not in list_aliases:
                unknown_source = options["model"]
                UserWarning(
                    f"Gegeven model: {unknown_source} niet gevonden, we proberen het toch op te halen."
                )

            # haal de locaties op die bij de bron horen
            gdf_locations = get_matroos_locations(source=options["model"])
            available_location_names = list(gdf_locations["measurement_location_code"])
            # available_location_names += list(gdf_locations["measurement_location_id"])

            # maak een set van de namen en formateer ze zonder spaties en hoofdletters
            available_location_names = set(available_location_names)

            # herhaal formateren voor de gegeven locatie namen
            supplied_location_names = list(df_in["measurement_location_code"].values)

            # die je wilt ophalen is het overlap tussen de twee
            wanted_location_names = available_location_names.intersection(
                supplied_location_names
            )

            # geef een warning als we locatie namen mee geven die niet bestaan
            if len(wanted_location_names) < len(supplied_location_names):
                locations_not_found = set(supplied_location_names).difference(
                    wanted_location_names
                )
                msg = f"location {locations_not_found}"
                self.data_adapter.logger.warning(msg)
                warnings.warn(msg)

            return wanted_location_names

    @staticmethod
    def format_location_names(location_names: list[str]) -> list[str]:
        """Neemt een lijst met locatienamen en verwijdert spaties en maakt ze allemaal in kleine letters"""
        return [location.lower().replace(" ", "") for location in location_names]

    @staticmethod
    def create_dataframe(
        options: dict,
        df_in: pd.DataFrame,
        calc_time: datetime,
        json_data: list,
        global_variables: dict,
    ) -> pd.DataFrame:
        """
        Maakt een dataframe met waardes van de rws water webservices

        Parameters
        ----------
        options: dict
            Een dictionary met opties uit de config
        df_in: pd.DataFrame
            Het invoerdataframe
        calc_time: datetime
            De huidige tijd
        json_data: list
            Een lijst met ogehaalde JSON data
        global_variables: dict
            Een dictionary met globale variabelen

        Returns
        -------
        dataframe: pd.Dataframe
            Pandas dataframe met de voor uitvoer
        """
        dataframe = pd.DataFrame()
        records = []
        # loop over de lijst met data heen
        for index, serie in enumerate(json_data["results"]):
            # hier zit ook coördinaten in
            measurement_location_name = serie["location"]["properties"]["locationName"]
            measurement_location_code = (
                measurement_location_name  # .lower().replace(" ", "")
            )

            # here we lookup the location id in the input dataframe
            str_location_codes = df_in["measurement_location_code"].apply(
                lambda x: str(x)
            )
            df_location = df_in[str_location_codes == str(measurement_location_code)]
            # check if we indeed found one:
            if len(df_location) == 1:
                measurement_location_id = df_location.iloc[0]["measurement_location_id"]
            else:
                # otherwise, map the series to the input dataframe as backup
                measurement_location_id = df_in.iloc[index]["measurement_location_id"]

            # we krijgen water height, maar we willen waterlevel
            parameter_matroos = serie["observationType"]["quantityName"]
            # "waterlevel -> WATHTE", code matching WATHTE
            parameter_code, aquo_grootheid_dict = read_aquo(
                parameter_matroos, global_variables
            )
            parameter_id = aquo_grootheid_dict["id"]
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
                    if parameter_code == "WATHTE":
                        value = value * 100
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
                    "value": value,
                    "value_type": value_type,
                }
                records.append(record)

            # voeg de records samen
            dataframe = pd.DataFrame.from_records(records)
        return dataframe

    def generate_url(
        self,
        options: dict,
        global_variables: dict,
    ) -> str:
        """
        Geeft de benodigde URL terug om het verzoek naar de Noos-server te maken

        Parameters
        ----------
        options: dict
            Opties die door de gebruiker zijn opgegeven, in dit geval is 'source' het belangrijkst
        global_variables: dict
            Globale variabelen die nodig zijn om de URL te genereren

        Returns
        -------
        url: str
            De gegenereerde URL voor het verzoek aan de Noos-server


        Raises
        ------
        UserWarning
            Als de 'website' niet is opgegeven in de opties
            Als de 'website' niet overeenkomt met de opties 'noos', 'matroos' of 'vitaal'
            Als de 'website' 'matroos' of 'vitaal' is en de gebruiker geen gebruikersnaam en wachtwoord heeft opgegeven
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

        return base_url


@dataclass(config={"arbitrary_types_allowed": True})
class LoadsMatroosNetCDF(LoadsMatroos):
    """
    Haal de Matroos informatie op uit de maps1d database.

    De Matroos informatie is beschikbaar op de volgende websites:
    https://noos.matroos.rws.nl/maps1d/
    Met de functie get_matroos_sources(endpoint='maps1d') kan je de beschikbare bronnen ophalen.
    Met de functie get_matroos_locations(source='bron') kan je de bijbehorende beschikbare locaties ophalen.

    Attributes
    ----------
    data_adapter: DataAdapter
        Data adapter object for input and output data.
    df_in: Optional[pd.DataFrame] | None
        Input dataframe containing measurement location codes.
    df_out: Optional[pd.DataFrame] | None
        Output dataframe containing processed data.
    ds_in: Optional[xr.Dataset] | None
        Input xarray dataset containing forecast data from matroos.
    url_retrieve_series_noos: str
        URL for retrieving series from Noos server.
    url_retrieve_series_matroos: str
        URL for retrieving series from Matroos server.
    url_retrieve_series_vitaal: str
        URL for retrieving series from Vitaal server.
    """

    data_adapter: DataAdapter

    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None
    ds_in: Optional[xr.Dataset] | None = None

    url_retrieve_series_noos: str = "noos.matroos.rws.nl/direct/get_netcdf.php?"
    url_retrieve_series_matroos: str = "matroos.rws.nl/direct/get_netcdf.php?"
    url_retrieve_series_vitaal: str = "vitaal.matroos.rws.nl/direct/get_netcdf.php?"

    def run(self, input: str, output: str) -> None:
        """
        Voert de functie uit om gegevens op te halen en te verwerken voor de matroos-toolbox.

        Parameters
        ----------
        input: str
            Naam van de dataadapter met invoergegevens.
        output: str
            Naam van de dataadapter om uitvoergegevens op te slaan.

        Raises
        ------
        UserWarning
            Als de 'LoadsMatroos' sectie niet aanwezig is in de global_variables (config).
            Als de 'measurement_location_code' ontbreekt in de inputdata.
            Als het gegeven model niet wordt herkend.
            Als de locaties niet worden gevonden.
        ConnectionError
            Als er geen resultaten in de data zitten.
            Als de verbinding mislukt.
        """

        # haal opties en dataframe van de config
        global_variables = self.data_adapter.config.global_variables
        if "LoadsMatroosNetCDF" not in global_variables:
            raise UserWarning(
                "LoadsMatroosNetCDF sectie niet aanwezig in global_variables (config)"
            )

        options = global_variables["LoadsMatroosNetCDF"]
        if "MISSING_VALUE" not in options:
            options["MISSING_VALUE"] = -999

        self.df_in = self.data_adapter.input(input)
        map_location_to_id = self.df_in.set_index("measurement_location_code")[
            "measurement_location_id"
        ].to_dict()

        wanted_location_names = self.get_matroos_available_locations(
            self.df_in,
            options,
            endpoint_model="maps1d",  # netcdf uses a different endpoint to timeseries
        )

        # zet tijd goed
        # TODO: zet dit later wel goed om alleen tijden rond calc_time te pakken
        # calc_time = global_variables["calc_time"]

        lst_dfs = []
        # maak een url aan
        for parameter in options["parameters"]:
            parameter_code, aquo_grootheid_dict = read_aquo(parameter, global_variables)
            aquo_parameter = matroos_aquo_synoniem[
                aquo_grootheid_dict["label_en"]
            ]  # "WATHTE -> waterlevel"
            request_forecast_url = self.generate_url(
                options,
                global_variables,
            )
            params = {
                "database": "maps1d",
                "source": options["model"],
                "zip": "0",
            }
            status, response = fetch_data_get(
                url=request_forecast_url, params=params, mime_type="NETCDF", timeout=120
            )
            if "is not available in database" in response.text:
                raise UserWarning(
                    f"{response.text}, check available sources with `get_matroos_sources(endpoint='maps1d')`"
                )
            if status is None and response is not None:
                # sla tijdelijk op
                temp_dir = tempfile.TemporaryDirectory()
                temp_dir_path = Path(temp_dir.name)
                file_path = temp_dir_path / f"Matroos_{options['model']}.nc"
                with open(file_path, "wb") as file:
                    file.write(response.content)

                ds = xr.open_dataset(file_path)
                # Aanname: Maar 1 analysis time (tot nu toe)
                ds_0 = ds.isel(analysis_time=0).compute()
                # maak een koppeling tussen station number and names
                ds_0["nodenames"] = ds_0["nodenames"].astype("str")
                station_name_dict = (
                    ds_0["nodenames"].to_dataframe()["nodenames"].to_dict()
                )
                name_station_dict = {v: k for k, v in station_name_dict.items()}
                if aquo_parameter in ds_0.variables:
                    for location in wanted_location_names:
                        df = ds_0.sel(stations=name_station_dict[location])[
                            aquo_parameter
                        ].to_dataframe()
                        df.drop(
                            columns=["analysis_time", "lat", "lon", "y", "x", "z"],
                            inplace=True,
                        )
                        df["measurement_location_code"] = location
                        df["measurement_location_id"] = map_location_to_id[location]
                        df["measurement_location_description"] = location
                        df["parameter_id"] = aquo_grootheid_dict["id"]
                        df["parameter_code"] = parameter_code
                        df["unit"] = "cm"
                        # Voor nu alleen verwachtingen
                        df["value_type"] = "verwachting"
                        df.rename(columns={aquo_parameter: "value"}, inplace=True)
                        df.index.name = "date_time"
                        df.reset_index(drop=False, inplace=True)
                        df = df[
                            [
                                "measurement_location_id",
                                "measurement_location_code",
                                "measurement_location_description",
                                "parameter_id",
                                "parameter_code",
                                "date_time",
                                "unit",
                                "value",
                                "value_type",
                            ]
                        ]

                        lst_dfs.append(df)
                else:
                    raise UserWarning(
                        f"{parameter=} not found for {options['model']}, only {ds_0.data_vars.keys()}"
                    )
                ds.close()
            else:
                raise ConnectionError(f"Connection failed:{status}")

        self.df_out = pd.concat(lst_dfs, axis=0)
        self.data_adapter.output(output, self.df_out)
