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
class LoadsWaterinfo:
    """
    Belasting gegevens ophalen van rijkswaterstaat Waterinfo https://waterinfo.rws.nl/#/publiek/waterhoogte
    """

    data_adapter: DataAdapter

    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    url: str = "https://waterinfo.rws.nl/api/chart/get"

    async def run(self, input: str, output: str) -> None:
        """
        De runner van de Belasting Waterinfo.
        """

        # Haal opties en dataframe van de config
        global_variables = self.data_adapter.config.global_variables
        options = global_variables["LoadsWaterinfo"]

        # Dit zijn de meetlocaties vanuit invoer
        self.df_in = self.data_adapter.input(input)
        self.df_out = pd.DataFrame()

        # Loop over alle meetstations
        for _, measuringstation in self.df_in.iterrows():
            params = {
                "mapType": options["datatype"],
                "locationCodes": measuringstation["code"],
                "values": f"-{options['observedhours']},{options['predictionhours']}",
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
            Dataframe: Pandas dataframe geschikt voor uitvoer
        """
        dataframe = pd.DataFrame()

        h10 = 1
        h10v = 2
        h10a = 99

        if "series" in json_data:
            records = []

            for serie in json_data["series"]:
                parameterid = h10
                parameter_name = serie["meta"]["parameterName"]

                if "verwacht" in parameter_name.lower():
                    parameterid = h10v
                if "astronomisch" in parameter_name.lower():
                    parameterid = h10a

                if parameterid > 0:
                    conversion_to_meter = 1.0
                    if serie["unit"].lower() == "cm":
                        conversion_to_meter = 0.01

                    for data in serie["data"]:
                        utc_dt = datetime_from_string(
                            data["dateTime"], "%Y-%m-%dT%H:%M:%SZ"
                        )

                        if data["value"]:
                            value = float(data["value"]) * conversion_to_meter
                        else:
                            value = options["MISSING_VALUE"]

                        record = {
                            "code": measuringstation.code,
                            "objectid": measuringstation.id,
                            "objecttype": "measuringstation",
                            "parameterid": parameterid,
                            "datetime": epoch_from_datetime(utc_dt=utc_dt),
                            "value": value,
                            "calculating": True,
                        }

                        records.append(record)

            dataframe = pd.DataFrame.from_records(records)

        return dataframe
