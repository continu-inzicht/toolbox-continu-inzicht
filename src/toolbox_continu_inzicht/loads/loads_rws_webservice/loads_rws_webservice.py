import asyncio
from datetime import datetime, timezone, timedelta
from pydantic.dataclasses import dataclass
import pandas as pd
from typing import Optional

from toolbox_continu_inzicht.loads.loads_rws_webservice.get_rws_webservices_locations import (
    get_rws_webservices_locations,
)
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.utils.fetch_functions import fetch_data_post

aquo_id_dict = {"WATHTE": 4724}
RWS_webservices_verwacht = {"WATHTEVERWACHT": "WATHTE"}


@dataclass(config={"arbitrary_types_allowed": True})
class LoadsWaterwebservicesRWS:
    """
    Belasting gegevens ophalen van rijkswaterstaat waterwebservices https://waterwebservices.rijkswaterstaat.nl/

    Attributes
    ----------
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

    url_retrieve_observations: str = "https://waterwebservices.rijkswaterstaat.nl/ONLINEWAARNEMINGENSERVICES_DBO/OphalenWaarnemingen"

    async def run(self, input: str, output: str) -> None:
        """
        De runner van de Belasting WaterwebservicesRWS.
        """
        # haal opties en dataframe van de config
        global_variables = self.data_adapter.config.global_variables
        options = global_variables["LoadsWaterwebservicesRWS"]
        if "MISSING_VALUE" not in options:
            options["MISSING_VALUE"] = -999

        self.df_in = self.data_adapter.input(input)

        # doe een data type check
        if "measurement_location_id" not in self.df_in.columns:
            raise UserWarning(
                f"Input data missing 'measurement_location_id' in columns {self.df_in.columns}"
            )

        df_available_locations = await get_rws_webservices_locations()
        # uit de dataframe haal je een lijst met meetlocatie ids
        wanted_measuringstationid = list(self.df_in["measurement_location_id"].values)
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

        # TODO DIT GAAT VERANDEREN
        # https://rijkswaterstaatdata.nl/projecten/beta-waterwebservices/#:~:text=00.000%2B01%3A00%22%7D%7D-,Voorbeelden,-Een%20aantal%20specifieke
        # Verwachte waterstand over een uur
        # Elke 6 uur worden er waterstanden voorspeld op basis van het weer.
        # De speciale grootheden ‘WATHTEVERWACHT’ en 'QVERWACHT' komen niet meer voor.
        # In plaats daarvan wordt de grootheid gebruikt (resp. WATHTE en Q) waarbij onderscheid
        # gemaakt wordt in de waardebepalingsmethode en het procestype.

        # maak een lijst met jsons met de info die we opvragen aan de API
        # herhaal dit ook met waarmeningen, niet alleen verwachtingen
        lst_json = []
        if "WATHTE" in options["parameters"]:
            options["parameters"].append("WATHTEVERWACHT")
        for parameter in options["parameters"]:
            lst_json += self.create_json_list(
                parameter, t_now, global_variables, wanted_locations
            )

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

        rws_missing_value = 999999999.0  # implemented by default
        if options["MISSING_VALUE"] != rws_missing_value:
            self.df_out["value"] = self.df_out["value"].apply(
                lambda x: options["MISSING_VALUE"] if x == rws_missing_value else x
            )
        # output de dataframe
        self.data_adapter.output(output=output, df=self.df_out)

    @staticmethod
    def create_dataframe(
        options: dict, t_now: datetime, lst_data: list
    ) -> pd.DataFrame:
        """Maakt een dataframe met waardes van de rws water webservices

        Parameters
        ----------
        options: dict
            Een dictionary met opties uit de config
        t_now: datetime
            De huidige tijd
        lst_data: list
            Een lijst met JSON data uit de post request

        Returns:
        --------
            pd.Dataframe
                Pandas dataframe geschikt voor uitvoer
        """
        dataframe = pd.DataFrame()
        records = []
        # loop over de lijst met data heen
        for serie_in in lst_data:
            # als er geen data is, zit er geen waarnemingen lijst in
            if "WaarnemingenLijst" in serie_in:
                serie = serie_in["WaarnemingenLijst"][0]
                measurement_location_id = serie["Locatie"]["Locatie_MessageID"]
                measurement_location_code = serie["Locatie"]["Code"]
                measurement_location_name = serie["Locatie"]["Naam"]
                parameter_code = serie["AquoMetadata"]["Grootheid"]["Code"]
                # binnen aquo is WATHTEVERWACHT niks, dus zet de code terug
                if parameter_code in RWS_webservices_verwacht:
                    parameter_code = RWS_webservices_verwacht[parameter_code]
                unit = serie["AquoMetadata"]["Eenheid"]["Code"]
                parameter_id = aquo_id_dict[parameter_code]
                # process per lijst en stop het in een record
                for event in serie["MetingenLijst"]:
                    datestr = event["Tijdstip"]
                    utc_dt = datetime.fromisoformat(datestr)

                    if utc_dt > t_now:
                        value_type = "verwachting"
                    else:
                        value_type = "meting"

                    if event["Meetwaarde"]:
                        value = float(event["Meetwaarde"]["Waarde_Numeriek"])
                    else:
                        value = options["MISSING_VALUE"]

                    record = {
                        "measurement_location_id": measurement_location_id,
                        "measurement_location_code": measurement_location_code,
                        "measurement_location_description": measurement_location_name,
                        "parameter_id": parameter_id,
                        "parameter_code": parameter_code,
                        "datetime": utc_dt,
                        "unit": unit,
                        "value": value,
                        "value_type": value_type,
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

    async def get_data(self, lst_json: list):
        """
        Haal de data a-synchroon op van de api gegevne een lijst met Json objecten
        """
        tasks = [
            asyncio.create_task(fetch_data_post(self.url_retrieve_observations, json))
            for json in lst_json
        ]
        return await asyncio.gather(*tasks)
