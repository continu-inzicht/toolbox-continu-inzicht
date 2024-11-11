from pydantic.dataclasses import dataclass
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
import pandas as pd
from typing import Optional
from toolbox_continu_inzicht.utils.datetime_functions import (
    datetime_from_string,
)
from toolbox_continu_inzicht.utils.fetch_functions import fetch_data_get


@dataclass(config={"arbitrary_types_allowed": True})
class LoadsWaterinfo:
    """
    Belasting gegevens ophalen van rijkswaterstaat Waterinfo https://waterinfo.rws.nl/#/publiek/waterhoogte
    """

    data_adapter: DataAdapter

    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    url: str = "https://waterinfo.rws.nl/api/chart/get"

    # Kolommen schema van de invoer data meetlocaties
    input_schema = {
        "measurement_location_id": "int64",
        "measurement_location_code": "object",
        "measurement_location_description": "object",
    }

    def run(self, input: str, output: str) -> None:
        """
        De runner van de Belasting Waterinfo functie.

        Args:
            json_data (str): JSON data

        Returns:
            Dataframe: Pandas dataframe geschikt voor uitvoer.
        """

        # Haal opties en dataframe van de config
        global_variables = self.data_adapter.config.global_variables
        options = global_variables["LoadsWaterinfo"]

        # moments eventueel toevoegen aan options
        if "moments" not in options and "moments" in global_variables:
            options["moments"] = global_variables["moments"]
        elif "moments" not in options:
            options["moments"] = [-24, 0, 24, 48]

        # missing value controleren
        if "MISSING_VALUE" not in options:
            if "MISSING_VALUE" in self.data_adapter.config.global_variables:
                options["MISSING_VALUE"] = self.data_adapter.config.global_variables[
                    "MISSING_VALUE"
                ]
            else:
                options["MISSING_VALUE"] = -999

        # Dit zijn de meetlocaties vanuit invoer
        self.df_in = self.data_adapter.input(input, self.input_schema)

        self.df_out = pd.DataFrame()

        # observedhours,predictionhours
        # -672, 0   | achtentwintig dagen terug
        # -216, 48  | negen dagen terug en 2 dagen vooruit
        #   -6, 3   | zes uur teru, en 3 uur vooruit
        #  -48, 48  | twee dagen terug en 2 dagen vooruit

        datatype = "waterhoogte"
        if "parameters" in options:
            if type(options["parameters"]) is list and len(options["parameters"]) > 0:
                datatype = options["parameters"][0]

        maptype_schema = self.get_maptype(datatype)
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
                        measuringstation=measuringstation,
                        json_data=json_data,
                    )

                    if not self.df_out.empty:
                        self.df_out = pd.concat(
                            [self.df_out, dataframe], ignore_index=True
                        )
                    else:
                        self.df_out = dataframe

                else:
                    raise UserWarning(
                        f"Locatie: {measuringstation.measurement_location_code} geeft geen resultaat in Waterinfo."
            )
        else:
            raise UserWarning(
                f"De opgegeven parameter(s) komen niet voor in Waterinfo."
            )
        
        self.data_adapter.output(output=output, df=self.df_out)
        return self.df_out

    def create_dataframe(
        self,
        options: dict,
        maptype_schema: dict,
        measuringstation: dict,
        json_data: str,
    ) -> pd.DataFrame:
        """Maak een pandas dataframe van de opgehaalde data uit Waterinfo

        Args:
            options (dict): options opgegeven in de yaml
            maptype_schema (dict): gegevens van de maptype
            measuringstation (dict): gegevens van het meetstation
            json_data (str): JSON data met opgehaalde belasting data

        Returns:
            Dataframe: Pandas dataframe geschikt voor uitvoer:
            definition:
                - Meetlocatie id (measurement_location_id)
                - Meetlocatie code (measurement_location_code)
                - Meetlocatie omschrijving/naam (measurement_location_description)
                - Parameter id overeenkomstig Aquo-standaard: ‘4724’ (parameter_id)
                - Parameter code overeenkomstig Aquo-standaard: ‘WATHTE’ (parameter_code)
                - Parameter omschrijving overeenkomstig Aquo-standaard: ‘Waterhoogte’ (parameter_description)
                - Eenheid (unit)
                - Datum en tijd (date_time)
                - Waarde (value)
                - Type waarde: meting of verwachting (value_type)
        """
        dataframe = pd.DataFrame()

        if "series" in json_data:
            records = []
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
                        utc_dt = datetime_from_string(
                            data["dateTime"], "%Y-%m-%dT%H:%M:%SZ"
                        )

                        if data["value"]:
                            value = float(data["value"])
                        else:
                            value = options["MISSING_VALUE"]

                        record = {
                            "measurement_location_id": measuringstation.measurement_location_id,
                            "measurement_location_code": measuringstation.measurement_location_code,
                            "measurement_location_description": measuringstation.measurement_location_description,
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

    def get_maptype(self, maptype: str):
        """
        bepaal welke schema gebruikt moet worden voor het ophalen van de belasting

        Args:
            maptype (str): maptypes:
                - waterhoogte,
                - wind,
                - golfhoogte,
                - watertemperatuur,
                - luchttemperatuur,
                - astronomische-getij,
                - waterafvoer,
                - zouten

        returns:
            de query van de range als string. voorbeeld: -48,0
        """
        waterinfo_series = [
            {
                "maptype": "waterhoogte",
                "parameter_id": 4724,
                "parameter_code": "WATHTE",
                "values": [
                    {"observedhours": 48, "predictionhours": 48, "query": "-48,48"},
                    {"observedhours": 6, "predictionhours": 3, "query": "-6,3"},
                    {"observedhours": 216, "predictionhours": 48, "query": "-216,48"},
                    {"observedhours": 672, "predictionhours": 0, "query": "-672,0"},
                ],
            },
            {
                "maptype": "wind",
                "parameter_id": -9999,
                "parameter_code": "P_WIND",
                "values": [
                    {"observedhours": 48, "predictionhours": 48, "query": "-48,48"},
                    {"observedhours": 6, "predictionhours": 3, "query": "-6,3"},
                    {"observedhours": 216, "predictionhours": 48, "query": "-216,48"},
                    {"observedhours": 672, "predictionhours": 0, "query": "-672,0"},
                ],
            },
            {
                "maptype": "golfhoogte",
                "parameter_id": -9999,
                "parameter_code": "P_GOLFHOOGTE",
                "values": [
                    {"observedhours": 48, "predictionhours": 48, "query": "-48,48"},
                    {"observedhours": 6, "predictionhours": 3, "query": "-6,3"},
                    {"observedhours": 216, "predictionhours": 48, "query": "-216,48"},
                    {"observedhours": 672, "predictionhours": 0, "query": "-672,0"},
                ],
            },
            {
                "maptype": "watertemperatuur",
                "parameter_id": -9999,
                "parameter_code": "P_WATERTEMPERATUUR",
                "values": [
                    {"observedhours": 48, "predictionhours": 0, "query": "-48,0"},
                    {"observedhours": 6, "predictionhours": 0, "query": "-6,0"},
                    {"observedhours": 216, "predictionhours": 0, "query": "-216,0"},
                    {"observedhours": 672, "predictionhours": 0, "query": "-672,0"},
                ],
            },
            {
                "maptype": "luchttemperatuur",
                "parameter_id": -9999,
                "parameter_code": "P_LUCHTTEMPERATUUR",
                "values": [
                    {"observedhours": 48, "predictionhours": 0, "query": "-48,0"},
                    {"observedhours": 6, "predictionhours": 0, "query": "-6,0"},
                    {"observedhours": 216, "predictionhours": 0, "query": "-216,0"},
                    {"observedhours": 672, "predictionhours": 0, "query": "-672,0"},
                ],
            },
            {
                "maptype": "astronomische-getij",
                "parameter_id": -9999,
                "parameter_code": "P_ASTRONOMISCHE-GETIJ",
                "values": [
                    {"observedhours": 48, "predictionhours": 48, "query": "-48,48"},
                    {"observedhours": 6, "predictionhours": 3, "query": "-6,3"},
                    {"observedhours": 216, "predictionhours": 48, "query": "-216,48"},
                    {"observedhours": 672, "predictionhours": 0, "query": "-672,0"},
                ],
            },
            {
                "maptype": "stroming",
                "parameter_id": -9999,
                "parameter_code": "P_STROMING",
                "values": [
                    {"observedhours": 48, "predictionhours": 0, "query": "-48,0"},
                    {"observedhours": 6, "predictionhours": 0, "query": "-6,0"},
                    {"observedhours": 216, "predictionhours": 0, "query": "-216,0"},
                    {"observedhours": 672, "predictionhours": 0, "query": "-672,0"},
                ],
            },
            {
                "maptype": "waterafvoer",
                "parameter_id": -9999,
                "parameter_code": "P_WATERAFVOER",
                "values": [
                    {"observedhours": 48, "predictionhours": 48, "query": "-48,48"},
                    {"observedhours": 6, "predictionhours": 3, "query": "-6,3"},
                    {"observedhours": 216, "predictionhours": 48, "query": "-216,48"},
                    {"observedhours": 672, "predictionhours": 0, "query": "-672,0"},
                ],
            },
            {
                "maptype": "zouten",
                "parameter_id": -9999,
                "parameter_code": "P_ZOUTEN",
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
                return item

        return None

    def get_value_by_observedhours(
        self, maptype_schema: dict, observedhours_moments: int
    ):
        """
        bepaal welke range gebruikt moet worden voor het ophalen van de belasting

        Args:
            maptype_schema (dict): schema met mogelijke ranges. Voorbeeld:
                {"observedhours": 48, "predictionhours": 48, "query": "-48,0"},
                {"observedhours": 6, "predictionhours": 3, "query": "-6,0"},
                {"observedhours": 216, "predictionhours": 48, "query": "-216,0"},
                {"observedhours": 672, "predictionhours": 0, "query": "-672,0"}
            observedhours_moments (int): het laagste moment.

        returns:
            de query van de range als string. voorbeeld: -48,0
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
