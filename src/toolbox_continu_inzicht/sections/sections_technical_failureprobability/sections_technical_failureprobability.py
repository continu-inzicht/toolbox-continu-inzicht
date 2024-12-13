from pydantic.dataclasses import dataclass
from scipy.interpolate import interp1d
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from typing import Optional

import numpy as np
import pandas as pd


@dataclass(config={"arbitrary_types_allowed": True})
class SectionsTechnicalFailureprobability:
    """
    Bepaal de belasting op een dijkvak
    """

    data_adapter: DataAdapter

    df_in_section_loads: Optional[pd.DataFrame] | None = None
    df_in_fragility_curves: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    # Faalmechanismes:
    # Code| omschrijving
    # COMB: Combinatie faalmechanismen
    # GEKB: Overloop en overslag dijken
    # STPH: Opbarsten en piping dijken
    # STBI: Stabiliteit binnenwaarts dijken
    # HTKW: Overloop en overslag langsconstructies
    # STKWl: Stabiliteit langsconstructies
    # PKW: Piping langsconstructies

    # fragility curve per dijkvak
    input_schema_fragility_curves = {
        "section_id": "int64",
        "failuremechanism": "object",
        "hydraulicload": "float64",
        "failureprobability": "float64",
    }

    # belasting per moment per dijkvak
    input_schema_loads = {
        "section_id": "int64",
        "parameter_id": "int64",
        "unit": "object",
        "date_time": "datetime64[ns, UTC]",
        "value": "float64",
        "value_type": "object",
    }

    def run(self, input: list[str], output: str) -> None:
        """
        Uitvoeren van het bepalen van de faalkans van een dijkvak.

        Args:
            input List(str): [0] fragility curve per dijkvak
                             [1] belasting per dijkvak
            output (str): uitvoer sectie van het yaml-bestand:
                          koppeling van de maatgevende meetlocaties per dijkvak

        Returns: TODO RW aanpassen
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

        if not len(input) == 2:
            raise UserWarning(
                "Input variabele moet 2 string waarden bevatten. (fragility curve per dijkvak/belasting per dijkvak)"
            )

        # invoer 1: fragility curve per dijkvak per faalmechanisme
        self.df_in_fragility_curves = self.data_adapter.input(
            input[0], self.input_schema_fragility_curves
        )

        # invoer 2: belasting per dijkvak
        self.df_in_belasting = self.data_adapter.input(
            input[1], self.input_schema_loads
        )

        # uitvoer: belasting per dijkvak
        self.df_out = pd.DataFrame()

        # Unieke combinaties van section_id en failuremechanism
        unique_combinations = (
            self.df_in_fragility_curves[["section_id", "failuremechanism"]]
            .drop_duplicates(subset=["section_id", "failuremechanism"])
            .reset_index(drop=True)
        )

        for _, combination in unique_combinations.iterrows():
            section_id = combination["section_id"]
            failuremechanism = combination["failuremechanism"]

            # Filter de DataFrames
            filtered_df_values = self.df_in_belasting[
                self.df_in_belasting["section_id"] == section_id
            ].copy()
            filtered_df_fragility_curves = self.df_in_fragility_curves[
                (self.df_in_fragility_curves["section_id"] == section_id)
                & (self.df_in_fragility_curves["failuremechanism"] == failuremechanism)
            ].copy()

            # Vervang nulwaarden door een kleine positieve waarde
            small_positive_value = 1e-10
            filtered_df_fragility_curves["failureprobability"] = (
                filtered_df_fragility_curves[
                    "failureprobability"
                ].replace(0, small_positive_value)
            )

            x_unique, unique_indices = np.unique(
                filtered_df_fragility_curves["hydraulicload"], return_index=True
            )
            y_unique = filtered_df_fragility_curves["failureprobability"].iloc[
                unique_indices
            ]

            # Logaritmische interpolatie en extrapolatie functie voor failureprobability
            log_interp_func = interp1d(
                x_unique, np.log(y_unique), fill_value="extrapolate"
            )  # type: ignore

            # Toepassen van logaritmische interpolatie en extrapolatie
            log_failureprobability = log_interp_func(filtered_df_values["value"])
            filtered_df_values["failureprobability"] = np.exp(log_failureprobability)

            # Voeg de failuremechanism kolom toe
            filtered_df_values["failuremechanism"] = failuremechanism

            # Vervang kleine positieve waarde door een 0
            # TODO RW is het nodig om de kans terug te zetten naar 0.0?
            # filtered_df_values['failureprobability'] = filtered_df_values['failureprobability'].replace(small_positive_value, 0.0)

            # Voeg de gefilterde DataFrame toe aan de hoofd DataFrame
            self.df_out = pd.concat(
                [self.df_out, filtered_df_values], ignore_index=True
            )

        self.data_adapter.output(output=output, df=self.df_out)
