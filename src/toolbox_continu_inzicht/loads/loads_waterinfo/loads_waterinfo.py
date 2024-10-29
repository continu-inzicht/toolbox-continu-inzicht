from pydantic.dataclasses import dataclass
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
import pandas as pd
from typing import Optional
from toolbox_continu_inzicht.utils.datetime_functions import (
    datetime_from_string,
)
from toolbox_continu_inzicht.utils.fetch_functions import fetch_data


@dataclass(config={"arbitrary_types_allowed": True})
class LoadsWaterinfo:
    """
    Belasting gegevens ophalen van rijkswaterstaat Waterinfo https://waterinfo.rws.nl/#/publiek/waterhoogte
    """

    data_adapter: DataAdapter

    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    url: str = "https://waterinfo.rws.nl/api/chart/get"

    # Kolommen schema van de invoer data
    input_schema = {"id": "int64", "name": "object"}

    async def run(self, input: str, output: str) -> None:
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

        # moments eventueel toevoegen aan options
        if "MISSING_VALUE" not in options and "MISSING_VALUE" in global_variables:
            options["MISSING_VALUE"] = global_variables["MISSING_VALUE"]      
        elif "MISSING_VALUE" not in options:     
            options["MISSING_VALUE"] = -9999

        # Dit zijn de meetlocaties vanuit invoer
        self.df_in = self.data_adapter.input(input, self.input_schema)

        self.df_out = pd.DataFrame()
        
        # observedhours,predictionhours
        # -672, 0   | achtentwintig dagen terug
        # -216, 48  | negen dagen terug en 2 dagen vooruit
        #   -6, 3   | zes uur teru, en 3 uur vooruit
        #  -48, 48  | twee dagen terug en 2 dagen vooruit
        observedhours = -48
        predictionhours = 48
        if options["moments"][0] < observedhours:
            observedhours = -216

        datatype = "waterhoogte"
        if "parameters" in options:
            if type(options["parameters"]) is list and len(options["parameters"]) > 0:
                datatype = options["parameters"][0]

        # Loop over alle meetstations
        for _, measuringstation in self.df_in.iterrows():
            params = {
                "mapType": datatype,
                "locationCodes": measuringstation["code"],
                "values": f"{observedhours},{predictionhours}",
            }

            # Ophalen json data van de Waterinfo api
            status, json_data = await fetch_data(
                url=self.url, params=params, mime_type="json"
            )

            if status is None and json_data is not None:
                dataframe = self.create_dataframe(
                    options=options,
                    measuringstation=measuringstation,
                    json_data=json_data,
                )

                if not self.df_out.empty:
                    self.df_out = pd.concat([self.df_out, dataframe], ignore_index=True)
                else:
                    self.df_out = dataframe

        self.data_adapter.output(output=output, df=self.df_out)
        return self.df_out

    def create_dataframe(
        self, options: dict, measuringstation, json_data: str
    ) -> pd.DataFrame:
        """Maak een pandas dataframe van de opgehaalde data uit Waterinfo

        Args:
            options (dict):
            t_now (datetime):
            json_data (str): JSON data
            locations (Dataframe):

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

            for serie in json_data["series"]:
                parameter_id = 4724
                value_type = "meting"
                location_name = serie["meta"]["locationName"]
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
                            "measurement_location_id": measuringstation.id,
                            "measurement_location_code": measuringstation.code,
                            "measurement_location_description": location_name,
                            "parameter_id": parameter_id,
                            "parameter_code": parameter_name,
                            "parameter_description": parameter_description,
                            "unit": unit,
                            "date_time": utc_dt,
                            "value": value,
                            "value_type": value_type,
                        }

                        records.append(record)

            dataframe = pd.DataFrame.from_records(records)

        return dataframe
