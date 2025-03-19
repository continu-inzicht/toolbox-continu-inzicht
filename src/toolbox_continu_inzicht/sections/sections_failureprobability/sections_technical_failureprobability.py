"""
Bepaal de technische faalkans van een dijkvak
"""

from pydantic.dataclasses import dataclass
from scipy.interpolate import interp1d
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from typing import ClassVar, Optional

import numpy as np
import pandas as pd


@dataclass(config={"arbitrary_types_allowed": True})
class SectionsTechnicalFailureprobability:
    """
    Bepaal de technische faalkans van een dijkvak

    Attributes
    ----------
    data_adapter : DataAdapter
        DataAdapter object voor het verwerken van gegevens.
    df_in_section_loads : Optional[pd.DataFrame] | None
        Invoer DataFrame met belasting per dijkvak. Standaardwaarde is None.
    df_in_fragility_curves : Optional[pd.DataFrame] | None
        Invoer DataFrame met fragiliteitscurves per dijkvak. Standaardwaarde is None.
    df_out : Optional[pd.DataFrame] | None
        Uitvoer DataFrame met faalkansen per dijkvak. Standaardwaarde is None.
    input_schema_fragility_curves : ClassVar[dict[str, str]]
        Schema voor de invoer van fragiliteitscurves per dijkvak.
    input_schema_loads : ClassVar[dict[str, str]]
        Schema voor de invoer van belasting per dijkvak.

    Notes
    -----
    **Input schema's**

    *input_schema_sections*: schema voor de lijst met dijkvakken

    - id: int64                         : id van het dijkvak
    - name: str                         : naam van de dijkvak

    *input_schema_loads*: schema voor belasting per moment per meetlocaties

    - measurement_location_id: int64    : id van het meetstation
    - parameter_id: int64               : id van de belastingparameter (1,2,3,4)
    - unit: str                         : eenheid van de belastingparameter
    - date_time: datetime64[ns, UTC]    : datum/ tijd van de tijdreeksitem
    - value: float64                    : waarde van de tijdreeksitem
    - value_type: str                   : type waarde van de tijdreeksitem (meting of verwacht)

    *input_schema_section_fractions*: schema voor koppeling van de maatgevende meetlocaties per dijkvak

    - id: int64                         : id van de dijkvak
    - idup: int64                       : id van bovenstrooms meetstation
    - iddown: int64                     : id van benedenstrooms meetstation
    - fractionup: float64               : fractie van bovenstrooms meetstation
    - fractiondown: float64             : fractie van benedestrooms meetstation


    **Output schema**

    *df_out (DataFrame)*: uitvoer

    - id: int64                         : id van het dijkvak
    - name; str                         : naam van de dijkvak
    - date_time: datetime64[ns, UTC]    : datum/ tijd van de tijdreeksitem
    - value: float64                    : waarde van de tijdreeksitem
    - unit: str                         : eenheid van de belastingparameter
    - parameter_id: int64               : id van de belastingparameter (1,2,3,4)
    - value_type: str                   : type waarde van de tijdreeksitem (meting of verwacht)
    """

    data_adapter: DataAdapter

    df_in_section_loads: Optional[pd.DataFrame] | None = None
    df_in_fragility_curves: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    # fragility curve per dijkvak
    input_schema_fragility_curves: ClassVar[dict[str, str]] = {
        "section_id": "int64",
        "failuremechanism": "object",
        "hydraulicload": "float64",
        "failureprobability": "float64",
    }

    # belasting per moment per dijkvak
    input_schema_loads: ClassVar[dict[str, str]] = {
        "section_id": "int64",
        "parameter_id": "int64",
        "unit": "object",
        "date_time": ["datetime64[ns, UTC]", "object"],
        "value": "float64",
        "value_type": "object",
    }

    def run(self, input: list[str], output: str) -> None:
        """
        Bepalen faalkans van een dijkvak.

        Parameters
        ----------
        input : list[str]
            Lijst met namen van data adapters (2) voor tijdreeks met belasting op de dijkvak en fragility curves voor de dijkvak
        output : str
            Uitvoer data adapter naam.

        Returns
        -------
        None

        Raises
        ------
        UserWarning
            Als de lengte van de input variabele niet gelijk is aan 2.
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

        # Datum als string omzetten naar datetime object
        if not pd.api.types.is_datetime64_any_dtype(self.df_in_belasting["date_time"]):
            self.df_in_belasting["date_time"] = pd.to_datetime(
                self.df_in_belasting["date_time"]
            )

        # Unieke combinaties van section_id en failuremechanism
        unique_combinations = (
            self.df_in_fragility_curves[["section_id", "failuremechanism"]]
            .drop_duplicates(subset=["section_id", "failuremechanism"])
            .reset_index(drop=True)
        )
        self.df_out = self.iterate_combinations(
            unique_combinations,
            self.df_in_belasting,
            self.df_in_fragility_curves,
            pd.DataFrame(),  # initialiseer lege DataFrame
        )
        self.data_adapter.output(output=output, df=self.df_out)

    @staticmethod
    def iterate_combinations(
        unique_combinations, df_in_belasting, df_in_fragility_curves, df_out
    ):
        for _, combination in unique_combinations.iterrows():
            section_id = combination["section_id"]
            failuremechanism = combination["failuremechanism"]

            # Filter de DataFrames
            filtered_df_values = df_in_belasting[
                df_in_belasting["section_id"] == section_id
            ].copy()
            filtered_df_fragility_curves = df_in_fragility_curves[
                (df_in_fragility_curves["section_id"] == section_id)
                & (df_in_fragility_curves["failuremechanism"] == failuremechanism)
            ].copy()

            # Vervang nulwaarden door een kleine positieve waarde
            small_positive_value = 1e-10
            filtered_df_fragility_curves["failureprobability"] = (
                filtered_df_fragility_curves["failureprobability"].replace(
                    0, small_positive_value
                )
            )

            x_unique, unique_indices = np.unique(
                filtered_df_fragility_curves["hydraulicload"], return_index=True
            )
            y_unique = filtered_df_fragility_curves["failureprobability"].iloc[
                unique_indices
            ]

            # Logaritmische interpolatie en extrapolatie functie voor failureprobability
            log_interp_func = interp1d(
                x_unique,
                np.log(y_unique),
                fill_value="extrapolate",  # type: ignore
            )

            # Toepassen van logaritmische interpolatie en extrapolatie
            log_failureprobability = log_interp_func(filtered_df_values["value"])
            filtered_df_values["failureprobability"] = np.exp(log_failureprobability)

            # Voeg de failuremechanism kolom toe
            filtered_df_values["failuremechanism"] = failuremechanism
            if "measure_id" in combination:
                filtered_df_values["measure_id"] = combination["measure_id"]

            # Vervang kleine positieve waarde door een 0
            # TODO is het nodig om de kans terug te zetten naar 0.0?: Zie TBCI-157
            # filtered_df_values['failureprobability'] = filtered_df_values['failureprobability'].replace(small_positive_value, 0.0)

            # Voeg de gefilterde DataFrame toe aan de hoofd DataFrame
            df_out = pd.concat([df_out, filtered_df_values], ignore_index=True)
        return df_out
