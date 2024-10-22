import asyncio
from datetime import datetime, timezone, timedelta
from pydantic.dataclasses import dataclass
import pandas as pd
from typing import Optional

from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.utils.fetch_functions import fetch_data_post
from toolbox_continu_inzicht.utils.datetime_functions import (
    epoch_from_datetime,
)


@dataclass(config={"arbitrary_types_allowed": True})
class LoadsWaterwebservicesRWS:
    """
    Belasting gegevens ophalen van rijkswaterstaat waterwebservices https://waterwebservices.rijkswaterstaat.nl/
    """

    data_adapter: DataAdapter
    input: str
    output: str

    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    url_catalog: str = "https://waterwebservices.rijkswaterstaat.nl/METADATASERVICES_DBO/OphalenCatalogus"
    url_retrieve_observations: str = "https://waterwebservices.rijkswaterstaat.nl/ONLINEWAARNEMINGENSERVICES_DBO/OphalenWaarnemingen"

    async def run(self, input=None, output=None) -> None:
        """
        De runner van de Belasting WaterwebservicesRWS.
        """
        if input is None:
            input = self.input
        if output is None:
            output = self.output

        # haal opties en dataframe van de config
        global_variables = self.data_adapter.config.global_variables
        options = global_variables["BelastingWaterwebservicesRWS"]

        self.df_in = self.data_adapter.input(input)

        # doe een data type check
        if "measuringstationid" not in self.df_in.columns:
            raise UserWarning(
                f"Input data missing 'measuringstationid' in columns {self.df_in.columns}"
            )

        df_available_locations = await self.get_available_locations()
        # uit de dataframe haal je een lijst met meetlocatie ids
        wanted_measuringstationid = list(self.df_in["measuringstationid"].values)
        # met de meet locatie id's halen selecteren we de informatie uit de catalogus
        wanted_locations = df_available_locations.loc[wanted_measuringstationid]

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

        # maak een lijst met jsons met de info die we opvragen aan de API
        verwachting = "WATHTEVERWACHT"
        lst_json_verwachting = self.create_json_list(
            verwachting, t_now, global_variables, wanted_locations
        )

        # herhaal dit ook met waarmeningen, niet alleen verwachtingen
        waarnmeningen = "WATHTE"
        lst_json_waarnmeningen = self.create_json_list(
            waarnmeningen, t_now, global_variables, wanted_locations
        )

        lst_json = lst_json_verwachting + lst_json_waarnmeningen

        # haal deze data a-synchroon op
        tasks = [
            asyncio.create_task(
                fetch_data_post(self.url_retrieve_observations, json, mime_type="json")
            )
            for json in lst_json
        ]
        observation_data = await asyncio.gather(*tasks)

        # post_process de data & maak een dataframe
        lst_observations = [value for _, value in observation_data]
        self.df_out = self.create_dataframe(options, t_now, lst_observations)

        # output de dataframe
        self.data_adapter.output(output=output, df=self.df_out)

    @staticmethod
    def create_dataframe(
        options: dict, t_now: datetime, lst_data: list
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
        for serie_in in lst_data:
            # als er geen data is, zit er geen waarnemingen lijst in
            if "WaarnemingenLijst" in serie_in:
                serie = serie_in["WaarnemingenLijst"][0]
                measuringstationid = serie["Locatie"]["Locatie_MessageID"]
                measurement_or_observation = serie["AquoMetadata"]["Grootheid"]["Code"]
                # process per lijst en stop het in een record
                for event in serie["MetingenLijst"]:
                    datestr = event["Tijdstip"]
                    utc_dt = datetime.fromisoformat(datestr)

                    parameterid = h10
                    if utc_dt > t_now:
                        parameterid = h10v

                    if parameterid > 0:
                        if event["Meetwaarde"]:
                            value = float(event["Meetwaarde"]["Waarde_Numeriek"])
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

    @staticmethod
    def create_json_list(
        measurement: str,
        t_now: datetime,
        global_variables: dict,
        locations: pd.DataFrame,
    ) -> list:
        """
        Maak een lijst van FEWS parameters om mee te sturen bij het ophalen van data.

        Args:
            t_now: T0 in UTC
            global_variables: globale variable uit de invoer yaml
            locations: dataframe with locations wanted

        Returns:
            dict: lijst met parameters
        """
        lst_json = []
        moments = global_variables["moments"]
        code_eenheid = "cm"
        code_compartiment = "OW"

        for _, row in locations.iterrows():
            if len(moments) > 0:
                starttime = t_now + timedelta(hours=int(moments[0]))
                endtime = t_now + timedelta(hours=int(moments[-1]))
                x = getattr(row, "X")
                y = getattr(row, "Y")
                locatie = getattr(row, "Code")
                json = {
                    "Locatie": {"X": x, "Y": y, "Code": locatie},
                    "AquoPlusWaarnemingMetadata": {
                        "AquoMetadata": {
                            "Compartiment": {"Code": code_compartiment},
                            "Grootheid": {"Code": measurement},
                            "Eenheid": {"Code": code_eenheid},
                        }
                    },
                    "Periode": {
                        "Begindatumtijd": starttime.strftime(
                            "%Y-%m-%dT%H:%M:%S.000+00:00"
                        ),
                        "Einddatumtijd": endtime.strftime(
                            "%Y-%m-%dT%H:%M:%S.000+00:00"
                        ),
                    },
                }
                lst_json.append(json)
        return lst_json

    async def get_available_locations(self) -> pd.DataFrame:
        """Haal locaties die bekend zijn bij de RWS webservice."""

        # haal voor all locaties de informatie op: catalogus met data
        body_catalog: dict = {
            "CatalogusFilter": {"Compartimenten": True, "Grootheden": True}
        }
        _, catalog_data = await fetch_data_post(
            self.url_catalog, body_catalog, mime_type="json"
        )

        if catalog_data is not None:
            df_available_locations = pd.DataFrame(catalog_data["LocatieLijst"])
            df_available_locations = df_available_locations.set_index(
                "Locatie_MessageID"
            )
        else:
            raise ConnectionError("Catalog not found")

        return df_available_locations

    async def get_data(self, lst_json: list):
        """
        Haal de data a-synchroon op van de api gegevne een lijst met Json objecten
        """
        tasks = [
            asyncio.create_task(fetch_data_post(self.url_retrieve_observations, json))
            for json in lst_json
        ]
        return await asyncio.gather(*tasks)
