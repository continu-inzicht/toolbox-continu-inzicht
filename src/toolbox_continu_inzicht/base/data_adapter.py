from pydantic import BaseModel as PydanticBaseModel
import pandas as pd

import warnings
from typing import Any, Optional, Dict

from dotenv import load_dotenv, dotenv_values
from toolbox_continu_inzicht.base.config import Config
import toolbox_continu_inzicht.base.adapters.input as input_package
import toolbox_continu_inzicht.base.adapters.output as output_package
from toolbox_continu_inzicht.base.adapters.validate_dataframe import validate_dataframe
from toolbox_continu_inzicht.base.adapters.load_data_adapters import (
    get_adapter_functions_from_plugin_path,
    get_functions_from_package,
)
from toolbox_continu_inzicht.base.adapters.data_adapter_utils import (
    check_file_and_path,
    check_plugin_path,
    check_rootdir,
)


class DataAdapter(PydanticBaseModel):
    """Basis DataAdapter"""

    config: Config
    input_types: dict = {}
    output_types: dict = {}

    def initialize_input_types(self):
        # externe functies laden
        prefix = "input_"
        externe_functies = {}
        plugin_path = check_plugin_path(self.config.global_variables, prefix)
        if plugin_path is not None:
            externe_functies.update(
                get_adapter_functions_from_plugin_path(
                    plugin_path, remove_prefix=prefix
                )
            )
        # interne input functies laden
        self.input_types.update(
            get_functions_from_package(input_package, remove_prefix="input_")
        )

        # combineer alle functions (externe functies overschrijven input functions)
        self.input_types.update(externe_functies)

    def initialize_output_types(self):
        prefix = "output_"
        externe_functies = {}
        plugin_path = check_plugin_path(self.config.global_variables, prefix)
        if plugin_path is not None:
            externe_functies.update(
                get_adapter_functions_from_plugin_path(
                    plugin_path, remove_prefix=prefix
                )
            )
        # interne input functies laden
        self.output_types.update(
            get_functions_from_package(output_package, remove_prefix="output_")
        )

        # combineer alle functions (externe functies overschrijven input functions)
        self.output_types.update(externe_functies)

    def input(self, input: str, schema: Optional[Dict] = None) -> pd.DataFrame:
        """Gegeven het config, stuurt de juiste input waarde aan

        Parameters:
        -----------
        input: str
               Naam van de data adapter die gebruikt wordt.

        opties: dict
                  extra informatie die ook naar de functie moet om het bestand te lezen

        """
        self.initialize_input_types()  # maak een dictionary van type: functie

        # initieer een leeg dataframe
        df = pd.DataFrame()

        # controleer of de adapter bestaat
        if input in self.config.data_adapters:
            # haal de input configuratie op van de functie
            function_input_config = self.config.data_adapters[input]

            # leid het data type af
            data_type = function_input_config["type"]

            check_rootdir(self.config.global_variables)
            check_file_and_path(function_input_config, self.config.global_variables)

            # uit het .env bestand halen we de extra waardes en laden deze in de config
            # .env is een lokaal bestand waar wachtwoorden in kunnen worden opgeslagen, zie .evn.template
            environmental_variables = {}
            dotenv_path = None
            if "dotenv_path" in self.config.global_variables:
                dotenv_path = self.config.global_variables["dotenv_path"]

            if load_dotenv(dotenv_path=dotenv_path):
                environmental_variables = dict(dotenv_values(dotenv_path=dotenv_path))
            else:
                warnings.warn(
                    "Het bestand `.env` in niet aanwezig in de hoofdmap, code negeert deze melding.",
                    UserWarning,
                )

            # in eerste instantie alleen beschikbaar voor de data adapters
            function_input_config.update(environmental_variables)

            # maar je wilt er  vanuit de functies ook bij kunnen
            self.config.global_variables.update(environmental_variables)

            # roep de bijbehorende functie bij het data type aan en geef het input pad mee.
            if data_type in self.input_types:
                correspinding_function = self.input_types[data_type]
                df = correspinding_function(function_input_config)

                # Controleer of er data is opgehaald.
                if len(df) == 0:
                    raise UserWarning(
                        f"Ophalen van gegevens van {input} heeft niets opgeleverd."
                    )

                # Als schema is meegegeven, controleer of de data aan het schema voldoet.
                if schema is not None:
                    status, message = validate_dataframe(df=df, schema=schema)
                    if status > 0:
                        raise UserWarning(message)

            else:
                # Adapter bestaat niet
                message = f"Adapter van het type '{data_type}' niet gevonden."
                raise UserWarning(message)
        else:
            # Adapter sleutel staat niet in het yaml-bestand
            message = f"Adapter met de naam '{input}' niet gevonden in de configuratie (yaml)."
            raise UserWarning(message)

        return df

    def output(self, output: str, df: pd.DataFrame) -> None:
        """Gegeven het config, stuurt de juiste input waarde aan

        Parameters:
        -----------
        output: name of the data adapter to use
        df: pd.Dataframe
            pandas dataframe om weg te schrijven

        opties: dict
                extra informatie die ook naar de functie moet om het bestand te schrijven

        """

        self.initialize_output_types()  # maak een dictionary van type: functie
        # haal de input configuratie op van de functie
        functie_output_config = self.config.data_adapters[output]

        # leid het data type af
        data_type = functie_output_config["type"]

        # check of de rootdir bestaat
        check_rootdir(self.config.global_variables)
        check_file_and_path(functie_output_config, self.config.global_variables)

        # uit het .env bestand halen we de extra waardes en laden deze in de config
        environmental_variables = {}
        if load_dotenv():
            environmental_variables = dict(dotenv_values())
        else:
            warnings.warn(
                "A `.env` file is not present in the root directory, continuing without",
                UserWarning,
            )
        # voeg alle environmental variables toe aan de functie output config
        functie_output_config.update(environmental_variables)

        # roep de bijbehorende functie bij het data type aan en geef het input pad mee.
        bijbehorende_functie = self.output_types[data_type]
        bijbehorende_functie(functie_output_config, df)

    def set_global_variable(self, key: str, value: Any):
        """
        Functie voor het dynamisch overschrijven van global variable.

        Parameters:
        -----------
        key: str
            naam van de waarde om te overschrijven

        value: Any
            Object om mee te geven
        """
        self.config.global_variables[key] = value

    def set_dataframe_adapter(
        self, key: str, df: pd.DataFrame, if_not_exist: str = "raise"
    ) -> None:
        """
        Functie om een dataframe mee te geven aan een data adapter met `type: python`.
        Let er zelf op dat de kollom namen en datatypes overeen komen met de beoogde functie.

        Parameters:
        -----------
        key: str
            naam van de data adapter zoals opgegeven in de configuratie yaml

        df: pd.Dataframe
            Object om mee te geven

        if_not_exist: str[raise, create]
            Geeft aan wat te doen als de data adapter niet bestaat,
            bij raise krijg je een error, bij create wordt er een nieuwe data adapter aangemaakt.
        """

        if key in self.config.data_adapters:
            data_adapter_config = self.config.data_adapters[key]
            if data_adapter_config["type"] == "python":
                data_adapter_config["dataframe_from_python"] = df
            else:
                raise UserWarning(
                    "Deze functionaliteit is voor data adapters van type `python`, "
                )
        elif if_not_exist == "raise":
            raise UserWarning(
                f"Data adapter `{key}` niet gevonden, zorg dat deze goed in het config bestand staat met type `python`"
            )
        elif if_not_exist == "create":
            self.config.data_adapters[key] = {
                "type": "python",
                "dataframe_from_python": df,
            }

        else:
            raise UserWarning(
                f"Data adapter `{key=}` niet gevonden, en {if_not_exist=} is ongeldig, moet `raise` of `create` zijn"
            )
