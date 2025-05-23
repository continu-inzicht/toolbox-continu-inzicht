import warnings
from datetime import datetime, timedelta
from typing import Optional

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
class LoadsFews(ToolboxBase):
    """
    Met deze functie worden gegevens uit de opgegeven FEWS omgeving opgehaald via REST.
    """

    data_adapter: DataAdapter

    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    # Kolommen schema van de invoer data meetlocaties
    input_schema = {
        "measurement_location_id": "int64",
        "measurement_location_code": "object",
        "measurement_location_description": "object",
    }

    def run(self, input: str, output: str) -> None:
        """
        De runner van de Loads FEWS.

        Parameters
        ----------
        input: str
            De naam van de input data met meetlocaties bekend in FEWS
        output: str
            De naam van de output data met resultaten uit FEWS

        Returns
        -------
        None

        Raises
        ------
        UserWarning
            Als de LoadsFews niet in de globale variabelen staat
        """

        self.df_in = self.data_adapter.input(input, self.input_schema)

        global_variables = self.data_adapter.config.global_variables
        calc_time = global_variables["calc_time"]

        # fews is te specifiek dus moet configuratie hebben
        if "LoadsFews" not in global_variables:
            raise UserWarning(
                "LoadsFews niet in "
            )  # FIXME: Incomplete print-statement?

        options = global_variables["LoadsFews"]

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

        url = self.create_url(options=options)
        parameters = self.create_params(
            calc_time=calc_time,
            options=options,
            moments=options["moments"],
            locations=self.df_in,
        )
        status, json_data = fetch_data_get(
            url=url, params=parameters, mime_type="json", path_certificate=None
        )

        if status is None and json_data is not None:
            self.df_out = self.create_dataframe(
                options=options,
                calc_time=calc_time,
                json_data=json_data,
                locations=self.df_in,
                global_variables=global_variables,
            )

            self.data_adapter.output(output=output, df=self.df_out)

        return self.df_out

    def create_url(self, options: dict) -> str:
        """
        Maak een REST-URL voor FEWS

        Parameters
        ----------
        options: dict
             Options uit de invoer yaml

        Returns
        -------
        URL: str
            De URL voor de FEWS REST API
        """
        host = options["host"]
        port = options["port"]
        region = options["region"]

        return f"{host}:{port}/FewsWebServices/rest/{region}/v1/timeseries"

    def create_params(
        self, calc_time: datetime, options: dict, moments: list, locations: pd.DataFrame
    ) -> dict:
        """
        Maak een lijst van FEWS parameters om mee te sturen bij het ophalen van data.

        Parameters
        ----------
        calc_time: datetime
            T0 in UTC
        options: dict
            options uit de invoer yaml
        moments: list
            Lijst van momenten in uren
        locations: pd.DataFrame
            Dataframe met meetlocaties

        Returns
        -------
        lijst met parameters: dict
            De parameters die meegegeven moeten worden aan de FEWS API
        """
        params = {}

        if len(moments) > 0:
            n_moments = len(moments) - 1
            starttime = calc_time + timedelta(hours=int(moments[0]))
            endtime = calc_time + timedelta(hours=int(moments[n_moments]))

            params["filterId"] = options["filter"]
            params["useDisplayUnits"] = False
            params["showThresholds"] = True
            params["omitMissing"] = True
            params["onlyHeaders"] = False
            params["showEnsembleMemberIds"] = False
            params["documentVersion"] = options["version"]
            params["documentFormat"] = "PI_JSON"
            params["forecastCount"] = 1
            params["parameterIds"] = options["parameters"]
            params["locationIds"] = locations["measurement_location_code"].tolist()
            params["startTime"] = starttime.strftime("%Y-%m-%dT%H:%M:%SZ")
            params["endTime"] = endtime.strftime("%Y-%m-%dT%H:%M:%SZ")

        return params

    def create_dataframe(
        self,
        options: dict,
        calc_time: datetime,
        json_data: str,
        locations: pd.DataFrame,
        global_variables: dict,
    ) -> pd.DataFrame:
        """Maak een pandas dataframe

        Parameters
        ----------
        options: dict
            Opties uit de invoer yaml
        calc_time: datetime
            T0 in UTC
        json_data: str
            JSON data van FEWS
        locations: pd.DataFrame
            Dataframe met meetlocaties
        global_variables: dict
            Globale variabelen uit de invoer yaml

        Returns
        -------
        DataFrame: pandas.core.Dataframe
            Met daar in:

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
        if "timeSeries" in json_data:
            records = []
            for serie in json_data["timeSeries"]:
                # parameter_id = 4724
                parameter_code = serie["header"]["parameterId"]
                parameter_description = serie["header"]["parameterId"]
                measurement_location_code = serie["header"]["locationId"]
                # measurement_location_description = serie["header"]["stationName"]
                unit = serie["header"]["units"]

                measuringstation = locations[
                    locations["measurement_location_code"] == measurement_location_code
                ]

                if len(measuringstation) > 0:
                    measurement_location_id = int(
                        measuringstation["measurement_location_id"].iloc[0]
                    )
                    measurement_location_description = str(
                        measuringstation["measurement_location_description"].iloc[0]
                    )

                    if parameter_code in options["parameters"]:
                        try:
                            parameter_code_aquo, aquo_grootheid_dict = read_aquo(
                                parameter_code, global_variables
                            )
                            parameter_id_aquo = aquo_grootheid_dict["id"]
                        except Exception as e:
                            message = f"""
                            {e=}
                            Fews parameters kunnen niet worden vertaald naar Aquo standaard, geef dit zelf op via
                            \nGlobalVariables:\n\taquo_allias\n\t\tFEWS_param: 'Aquo_param'
                            Default waarde parameter:{parameter_code} en id:4724 is nu gebruikt.
                            """
                            warnings.warn(message)
                            parameter_code_aquo = parameter_code
                            parameter_id_aquo = 4724
                        if "events" in serie:
                            for event in serie["events"]:
                                datestr = f"{event['date']}T{event['time']}Z"
                                utc_dt = datetime_from_string(
                                    datestr, "%Y-%m-%dT%H:%M:%SZ"
                                )
                                value_type = "meting"

                                if utc_dt > calc_time:
                                    value_type = "verwachting"

                                if event["value"]:
                                    value = float(event["value"])
                                else:
                                    value = options["MISSING_VALUE"]

                                record = {
                                    "measurement_location_id": measurement_location_id,
                                    "measurement_location_code": measurement_location_code,
                                    "measurement_location_description": measurement_location_description,
                                    "parameter_id": parameter_id_aquo,
                                    "parameter_code": parameter_code_aquo,
                                    "parameter_description": parameter_description,
                                    "unit": unit,
                                    "date_time": utc_dt,
                                    "value": value,
                                    "value_type": value_type,
                                }

                                records.append(record)

            dataframe = pd.DataFrame.from_records(records)

        return dataframe
