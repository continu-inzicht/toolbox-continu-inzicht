from datetime import timedelta
from typing import Any, ClassVar, Optional
from zoneinfo import ZoneInfo

import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht.base.base_module import ToolboxBase
from toolbox_continu_inzicht.base.aquo import read_aquo
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.utils.datetime_functions import (
    datetime_from_string,
)
from toolbox_continu_inzicht.utils.fetch_functions import fetch_data_get


@dataclass(config={"arbitrary_types_allowed": True})
class LoadsWaterinfo(ToolboxBase):
    """
    Belastinggegevens ophalen van Rijkswaterstaat Waterinfo

    Attributes
    ----------
    data_adapter: DataAdapter
        De data adapter voor het ophalen en opslaan van gegevens.
    df_in: Optional[pd.DataFrame] | None
        Dataframe met meetlocaties.
    df_out: Optional[pd.DataFrame] | None
        Dataframe met belastinggegevens.
    url: str
        De url van de Waterinfo API.

    input_waterinfo_schema: ClassVar[dict[str, str]]
        Het schema van de invoer data meetlocaties.

    Notes
    -----
    Hiervoor wordt de url gebruikt: [https://waterinfo.rws.nl/#/publiek/waterhoogte](https://waterinfo.rws.nl/#/publiek/waterhoogte
    """

    data_adapter: DataAdapter

    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    url: str = "https://waterinfo.rws.nl/api/chart/get"

    # Kolommen schema van de invoer data meetlocaties
    input_waterinfo_schema: ClassVar[dict[str, str]] = {
        "measurement_location_id": "int64",
        "measurement_location_code": "object",
        "measurement_location_description": "object",
    }

    def run(self, input: str, output: str) -> None:
        """
        De runner van de Belasting Waterinfo functie.

        Parameters
        ----------
        input: str
            Naam van de dataadapter met invoergegevens.
        output: str
            Naam van de dataadapter om uitvoergegevens op te slaan.

        Raises
        ------
        UserWarning
            Wanneer de belasting niet kan worden opgehaald.
            Wanneer moments niet aanwezig in global_variables (config)
            Wanneer de opgegeven parameter(s) komen niet voor in Waterinfo.
            Wanneer de opgegeven locatie niet voorkomt in Waterinfo
        """
        self.data_adapter.logger.debug("Start LoadsMatroos")

        # Haal opties en dataframe van de config
        global_variables = self.data_adapter.config.global_variables

        if "LoadsWaterinfo" not in global_variables:
            raise UserWarning(
                "LoadsWaterinfo sectie niet aanwezig in global_variables (config)"
            )

        options = global_variables["LoadsWaterinfo"]

        # moments eventueel toevoegen aan options
        if "moments" not in options and "moments" in global_variables:
            options["moments"] = global_variables["moments"]
        elif "moments" not in options:
            raise UserWarning("moments niet aanwezig in global_variables (config)")

        # missing value controleren
        if "MISSING_VALUE" not in options:
            if "MISSING_VALUE" in self.data_adapter.config.global_variables:
                options["MISSING_VALUE"] = self.data_adapter.config.global_variables[
                    "MISSING_VALUE"
                ]
            else:
                options["MISSING_VALUE"] = -999

        # Dit zijn de meetlocaties vanuit invoer
        self.df_in = self.data_adapter.input(input, self.input_waterinfo_schema)

        self.df_out = pd.DataFrame()

        # zet tijd goed
        calc_time = global_variables["calc_time"]
        moments = global_variables["moments"]

        min_date_time = calc_time
        max_date_time = calc_time

        for moment in moments:
            min_date_time = min(min_date_time, calc_time + timedelta(hours=moment))
            max_date_time = max(max_date_time, calc_time + timedelta(hours=moment))

        # observedhours,predictionhours
        # -672, 0   | achtentwintig dagen terug
        # -216, 48  | negen dagen terug en 2 dagen vooruit
        #   -6, 3   | zes uur terug, en 3 uur vooruit
        #  -48, 48  | twee dagen terug en 2 dagen vooruit

        # TODO: hard coded op een data type: nl de eerste
        datatype = "waterhoogte"
        if "parameters" in options:
            if type(options["parameters"]) is list and len(options["parameters"]) > 0:
                datatype = options["parameters"][0]

        maptype_schema = self.get_maptype(datatype, global_variables)
        if maptype_schema is not None:
            # bepaal welke range opgehaald moet worden
            observedhours_moments = options["moments"][0]
            values = self.get_value_by_observedhours(
                maptype_schema=maptype_schema,
                observedhours_moments=observedhours_moments,
            )

            # Loop over alle meetstations
            for _, measuringstation in self.df_in.iterrows():
                params = {
                    "mapType": datatype,
                    "locationCodes": measuringstation["measurement_location_code"],
                    "values": f"{values}",
                }

                # Ophalen json data van de Waterinfo api
                status, json_data = fetch_data_get(
                    url=self.url, params=params, mime_type="json"
                )

                if status is None and json_data is not None:
                    dataframe = self.create_dataframe(
                        options=options,
                        maptype_schema=maptype_schema,
                        measuringstation=measuringstation.to_dict(),
                        json_data=json_data,
                    )

                    if not self.df_out.empty:
                        lst_dfs = [self.df_out, dataframe]
                        self.df_out = pd.concat(lst_dfs, ignore_index=True)
                    else:
                        self.df_out = dataframe

                else:
                    raise UserWarning(
                        f"Locatie: {measuringstation.measurement_location_code} geeft geen resultaat in Waterinfo."
                    )
        else:
            raise UserWarning("De opgegeven parameter(s) komen niet voor in Waterinfo.")

        # filer alleen de opgegeven periode
        self.df_out = self.df_out[
            (self.df_out["date_time"] >= min_date_time)
            & (self.df_out["date_time"] <= max_date_time)
        ]
        # df_filter = df_filter[((df_filter['date_time'] >= calc_time) & (df_filter['value_type'] == "verwachting"))
        #                      |((df_filter['date_time'] <= calc_time) & (df_filter['value_type'] == "meting"))]

        if self.df_out is None:
            self.df_out = pd.DataFrame()

        self.data_adapter.output(output=output, df=self.df_out)

    def create_dataframe(
        self,
        options: dict,
        maptype_schema: dict,
        measuringstation: dict,
        json_data: dict[str, Any],
    ) -> pd.DataFrame:
        """Maak een pandas dataframe van de opgehaalde data uit Waterinfo

        Parameters
        ----------
        options : dict
            Opties opgegeven in de yaml.
        maptype_schema : dict
            Gegevens van de maptype.
        measuringstation : dict
            Gegevens van het meetstation.
        json_data : dict[str, Any]
            JSON data met opgehaalde belasting data.

        Returns
        -------
        pd.DataFrame
            Pandas dataframe geschikt voor uitvoer.

        Notes
        -----
        Het dataframe bevat de volgende kolommen:

        - Meetlocatie id (measurement_location_id)
        - Meetlocatie code (measurement_location_code)
        - Meetlocatie omschrijving/naam (measurement_location_description)
        - Parameter id overeenkomstig Aquo-standaard: '4724' (parameter_id)
        - Parameter code overeenkomstig Aquo-standaard: 'WATHTE' (parameter_code)
        - Parameter omschrijving overeenkomstig Aquo-standaard: 'Waterhoogte' (parameter_description)
        - Eenheid (unit)
        - Datum en tijd (date_time)
        - Waarde (value)
        - Type waarde: meting of verwachting (value_type)
        """
        dataframe = pd.DataFrame()

        if "series" in json_data:
            records = []

            # haal uit de aquo
            parameter_id = maptype_schema["parameter_id"]
            parameter_code = maptype_schema["parameter_code"]

            for serie in json_data["series"]:
                value_type = "meting"
                parameter_name = serie["meta"]["parameterName"]
                parameter_description = serie["meta"]["displayName"]
                unit = serie["unit"].lower()

                if "verwacht" in parameter_name.lower():
                    value_type = "verwachting"
                if "astronomisch" in parameter_name.lower():
                    value_type = "verwachting (astronomisch)"

                if value_type in ["meting", "verwachting"]:
                    for data in serie["data"]:
                        utc_dt = (
                            datetime_from_string(data["dateTime"], "%Y-%m-%dT%H:%M:%SZ")
                            .replace(tzinfo=ZoneInfo("Europe/Amsterdam"))
                            .astimezone(ZoneInfo("UTC"))
                        )

                        if data["value"]:
                            value = float(data["value"])
                        else:
                            value = options["MISSING_VALUE"]

                        record = {
                            "measurement_location_id": measuringstation[
                                "measurement_location_id"
                            ],
                            "measurement_location_code": measuringstation[
                                "measurement_location_code"
                            ],
                            "measurement_location_description": measuringstation[
                                "measurement_location_description"
                            ],
                            "parameter_id": parameter_id,
                            "parameter_code": parameter_code,
                            "parameter_description": parameter_description,
                            "unit": unit,
                            "date_time": utc_dt,
                            "value": value,
                            "value_type": value_type,
                        }

                        records.append(record)

            dataframe = pd.DataFrame.from_records(records)

        return dataframe

    def get_maptype(self, maptype: str, global_variables: dict) -> dict:
        """
        Bepaalt welk schema moet worden gebruikt voor het ophalen van de belasting.

        Parameters
        ----------
        maptype : str
            Het type kaart:

            - waterhoogte,
            - wind,
            - golfhoogte,
            - watertemperatuur,
            - luchttemperatuur,
            - astronomische-getij,
            - waterafvoer,
            - zouten

        global_variables : dict
            De globale variabelen van de configuratie.

        Returns
        -------
        str
            De query van het bereik als een string. Bijvoorbeeld: -48,0
        """
        waterinfo_series = [
            {
                "maptype": "waterhoogte",
                "values": [
                    {"observedhours": 48, "predictionhours": 48, "query": "-48,48"},
                    {"observedhours": 6, "predictionhours": 3, "query": "-6,3"},
                    {"observedhours": 216, "predictionhours": 48, "query": "-216,48"},
                    {"observedhours": 672, "predictionhours": 0, "query": "-672,0"},
                ],
            },
            {
                "maptype": "wind",
                "values": [
                    {"observedhours": 48, "predictionhours": 48, "query": "-48,48"},
                    {"observedhours": 6, "predictionhours": 3, "query": "-6,3"},
                    {"observedhours": 216, "predictionhours": 48, "query": "-216,48"},
                    {"observedhours": 672, "predictionhours": 0, "query": "-672,0"},
                ],
            },
            {
                "maptype": "golfhoogte",
                "values": [
                    {"observedhours": 48, "predictionhours": 48, "query": "-48,48"},
                    {"observedhours": 6, "predictionhours": 3, "query": "-6,3"},
                    {"observedhours": 216, "predictionhours": 48, "query": "-216,48"},
                    {"observedhours": 672, "predictionhours": 0, "query": "-672,0"},
                ],
            },
            {
                "maptype": "watertemperatuur",
                "values": [
                    {"observedhours": 48, "predictionhours": 0, "query": "-48,0"},
                    {"observedhours": 6, "predictionhours": 0, "query": "-6,0"},
                    {"observedhours": 216, "predictionhours": 0, "query": "-216,0"},
                    {"observedhours": 672, "predictionhours": 0, "query": "-672,0"},
                ],
            },
            {
                "maptype": "luchttemperatuur",
                "values": [
                    {"observedhours": 48, "predictionhours": 0, "query": "-48,0"},
                    {"observedhours": 6, "predictionhours": 0, "query": "-6,0"},
                    {"observedhours": 216, "predictionhours": 0, "query": "-216,0"},
                    {"observedhours": 672, "predictionhours": 0, "query": "-672,0"},
                ],
            },
            {
                "maptype": "astronomische-getij",
                "values": [
                    {"observedhours": 48, "predictionhours": 48, "query": "-48,48"},
                    {"observedhours": 6, "predictionhours": 3, "query": "-6,3"},
                    {"observedhours": 216, "predictionhours": 48, "query": "-216,48"},
                    {"observedhours": 672, "predictionhours": 0, "query": "-672,0"},
                ],
            },
            {
                "maptype": "stroming",
                "values": [
                    {"observedhours": 48, "predictionhours": 0, "query": "-48,0"},
                    {"observedhours": 6, "predictionhours": 0, "query": "-6,0"},
                    {"observedhours": 216, "predictionhours": 0, "query": "-216,0"},
                    {"observedhours": 672, "predictionhours": 0, "query": "-672,0"},
                ],
            },
            {
                "maptype": "waterafvoer",
                "values": [
                    {"observedhours": 48, "predictionhours": 48, "query": "-48,48"},
                    {"observedhours": 6, "predictionhours": 3, "query": "-6,3"},
                    {"observedhours": 216, "predictionhours": 48, "query": "-216,48"},
                    {"observedhours": 672, "predictionhours": 0, "query": "-672,0"},
                ],
            },
            {
                "maptype": "zouten",
                "values": [
                    {"observedhours": 48, "predictionhours": 48, "query": "-48,0"},
                    {"observedhours": 6, "predictionhours": 3, "query": "-6,0"},
                    {"observedhours": 216, "predictionhours": 48, "query": "-216,0"},
                    {"observedhours": 672, "predictionhours": 0, "query": "-672,0"},
                ],
            },
        ]

        for item in waterinfo_series:
            if item["maptype"] == maptype:
                parameter_code, aquo_grootheid_dict = read_aquo(
                    maptype, global_variables
                )
                item["parameter_code"] = parameter_code
                item["parameter_id"] = aquo_grootheid_dict["id"]

                return item

    def get_value_by_observedhours(
        self, maptype_schema: dict, observedhours_moments: int
    ) -> str | None:
        """
        bepaal welke range gebruikt moet worden voor het ophalen van de belasting

        Parameters
        ----------
        maptype_schema: dict
            schema met mogelijke ranges.
            Voorbeeld:
            ```json
                {"observedhours": 48, "predictionhours": 48, "query": "-48,0"},
                {"observedhours": 6, "predictionhours": 3, "query": "-6,0"},
                {"observedhours": 216, "predictionhours": 48, "query": "-216,0"},
                {"observedhours": 672, "predictionhours": 0, "query": "-672,0"}
            ```
        observedhours_moments: int
            het laagste moment.

        Returns
        -------
        de query van de range als string: str
            voorbeeld: -48,0
        None : None
            als er geen range gevonden kan worden
        """

        observedhours = 6
        if observedhours_moments < -216:
            observedhours = 672
        elif observedhours_moments < -48:
            observedhours = 216
        elif observedhours_moments < -6:
            observedhours = 48

        for value in maptype_schema["values"]:
            if value["observedhours"] == observedhours:
                return value["query"]

        return None
