import warnings
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht.base.base_module import ToolboxBase
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.base.aquo import read_aquo
from toolbox_continu_inzicht.loads.loads_rws_webservice.get_rws_webservices_locations import (
    get_rws_webservices_locations,
)
from toolbox_continu_inzicht.utils.fetch_functions import fetch_data_post


@dataclass(config={"arbitrary_types_allowed": True})
class LoadsWaterwebservicesRWS(ToolboxBase):
    """
    Belastinggegevens ophalen van rijkswaterstaat waterwebservices

    Notes
    -----
    Link: [https://waterwebservices.rijkswaterstaat.nl/](https://waterwebservices.rijkswaterstaat.nl/)

    Attributes
    ----------
    data_adapter: DataAdapter
        De data adapter voor het ophalen en opslaan van gegevens.
    df_in: Optional[pd.DataFrame] | None
        Het invoerdataframe.
    df_out: Optional[pd.DataFrame] | None
        Het uitvoerdataframe.
    url_retrieve_observations: str
        De url voor het ophalen van waarnemingen.

    """

    data_adapter: DataAdapter

    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    url_retrieve_observations: str = "https://waterwebservices.rijkswaterstaat.nl/ONLINEWAARNEMINGENSERVICES_DBO/OphalenWaarnemingen"

    def run(self, input: str, output: str) -> None:
        """
        De runner van de Belasting WaterwebservicesRWS.

        Parameters
        ----------
        input: str
            De naam van de invoerdataadapter.
        output: str
            De naam van de uitvoerdataadapter.

        Raises
        ------
        UserWarning
            Wanneer de inputdata niet de kolom 'measurement_location_id' bevat.
            Wanneer de inputdata geen 'measurement_location_code' bevat.
            Wanneer de 'measurement_location_code' geen getal is.
            Wanneer de 'LoadsWaterwebservicesRWS' sectie niet aanwezig is in global_variables (config).
        """
        # haal opties en dataframe van de config
        global_variables = self.data_adapter.config.global_variables

        if "LoadsWaterwebservicesRWS" not in global_variables:
            raise UserWarning(
                "LoadsWaterwebservicesRWS sectie niet aanwezig in global_variables (config)"
            )

        options = global_variables["LoadsWaterwebservicesRWS"]
        if "MISSING_VALUE" not in options:
            options["MISSING_VALUE"] = -999

        self.df_in = self.data_adapter.input(input)

        # doe een data type check
        if "measurement_location_id" not in self.df_in.columns:
            raise UserWarning(
                f"Input data missing 'measurement_location_id' in columns {self.df_in.columns}"
            )

        df_available_locations = get_rws_webservices_locations()
        # uit de dataframe haal je een lijst met meetlocatie ids
        wanted_measuringstationcode = list(
            self.df_in["measurement_location_code"].values
        )

        wanted_measuringstationcode_ints = []
        for item in wanted_measuringstationcode:
            if str(item).isnumeric():
                wanted_measuringstationcode_ints.append(int(item))
            else:
                raise UserWarning("measurement_location_code moeten getallen zijn")

        # met de meet locatie code's selecteren we de informatie uit de catalogus
        wanted_locations = df_available_locations.loc[wanted_measuringstationcode_ints]

        # zet tijd goed
        calc_time = global_variables["calc_time"]

        # TODO: DIT GAAT VERANDEREN - zie TBCI-154
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
                parameter, calc_time, global_variables, wanted_locations
            )

        # haal de de data op & maak een dataframe
        lst_observations = []
        missing_data = []
        for json in lst_json:
            result, data = fetch_data_post(
                self.url_retrieve_observations, json, mime_type="json"
            )
            if data is None:
                missing_data.append(result)
            else:
                lst_observations.append(data)

        if len(lst_observations) == 0:
            raise UserWarning(
                f"Fout bij het ophalen van gegevens voor locatie(s) met code(s) {wanted_locations}: {missing_data[0]}"
            )

        elif len(missing_data) > 0 and len(lst_observations) > 0:
            msg = f"Ontbrekende gegevens voor {len(missing_data)} locaties, controleer de invoer op fouten \n doorgaan met {len(lst_observations)} locaties"
            self.data_adapter.logger.warning(msg)
            warnings.warn(msg)

        self.df_out = self.create_dataframe(
            options,
            calc_time,
            lst_observations,
            self.df_in,
            global_variables,
        )

        if not self.df_out.empty:
            rws_missing_value = 999999999.0  # implemented by default
            if options["MISSING_VALUE"] != rws_missing_value:
                self.df_out["value"] = self.df_out["value"].apply(
                    lambda x: options["MISSING_VALUE"] if x == rws_missing_value else x
                )

        else:
            raise UserWarning(
                f"Geen gegevens beschikbaar voor locatie(s): {wanted_locations}"
            )

        # output de dataframe
        self.data_adapter.output(output=output, df=self.df_out)

    @staticmethod
    def create_dataframe(
        options: dict,
        calc_time: datetime,
        lst_data: list,
        df_in: pd.DataFrame,
        global_variables: dict,
    ) -> pd.DataFrame:
        """Maakt een dataframe met waardes van de rws water webservices

        Parameters
        ----------
        options: dict
            Een dictionary met opties uit de config
        calc_time: datetime
            De huidige tijd
        lst_data: list
            Een lijst met JSON data uit de post request
        df_in: pd.DataFrame
            Het invoerdataframe
        global_variables: dict
            De globale variabelen uit de config

        Returns
        -------
        dataframe: pd.Dataframe
            Pandas dataframe geschikt voor uitvoer
        """
        dataframe = pd.DataFrame()
        records = []
        # loop over de lijst met data heen
        for serie_in in lst_data:
            # als er geen data is, zit er geen waarnemingen lijst in
            if "WaarnemingenLijst" in serie_in:
                serie = serie_in["WaarnemingenLijst"][0]
                # dit is een beetje verwarrend, maar de Location_MessageID is wel uniek, de CODE niet: vandaar dat we de id en code hier wisselen.
                measurement_location_id = serie["Locatie"]["Locatie_MessageID"]
                measurement_location_id = df_in[
                    df_in["measurement_location_code"].apply(lambda x: str(x))
                    == str(measurement_location_id)
                ].iloc[0]["measurement_location_id"]
                measurement_location_code = serie["Locatie"]["Locatie_MessageID"]

                measurement_location_name = serie["Locatie"]["Naam"]
                parameter_code = serie["AquoMetadata"]["Grootheid"]["Code"]
                unit = serie["AquoMetadata"]["Eenheid"]["Code"]
                # de read_aquo functie geeft zelf de juiste naam terug
                # is WATHTEVERWACHT niks, dus zet de code terug
                parameter_code, aquo_grootheid_dict = read_aquo(
                    parameter_code, global_variables
                )
                parameter_id = aquo_grootheid_dict["id"]
                # process per lijst en stop het in een record
                for event in serie["MetingenLijst"]:
                    datestr = event["Tijdstip"]
                    utc_dt = datetime.fromisoformat(datestr)

                    if utc_dt > calc_time:
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
                        "date_time": utc_dt,
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
        calc_time: datetime,
        global_variables: dict,
        locations: pd.DataFrame,
    ) -> list:
        """
        Maak een lijst van FEWS parameters om mee te sturen bij het ophalen van data.

        Parameters
        ----------
        measurement: str
            De naam van de parameter die je wilt ophalen.
        calc_time : datetime
            De huidige tijd in UTC.
        global_variables : dict
            De globale variabelen uit de invoer yaml.
        locations : pd.DataFrame
            Dataframe met de gewenste locaties.

        Returns
        -------
        list
            Lijst met parameters.
        """
        lst_json = []
        moments = global_variables["moments"]
        code_eenheid = "cm"
        code_compartiment = "OW"

        for _, row in locations.iterrows():
            if len(moments) > 0:
                starttime = calc_time + timedelta(hours=int(moments[0]))
                endtime = calc_time + timedelta(hours=int(moments[-1]))
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
