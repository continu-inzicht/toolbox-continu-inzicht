"""
Bepaal de faalkans door een maatregel van een dijkvak
"""

from typing import ClassVar, Optional

import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.sections.sections_failureprobability.sections_technical_failureprobability import (
    SectionsTechnicalFailureprobability,
)


@dataclass(config={"arbitrary_types_allowed": True})
class SectionsMeasureFailureprobability(SectionsTechnicalFailureprobability):
    """
    Bepaal de faalkans door een maatregel van een dijkvak

    Attributes
    ----------
    data_adapter : DataAdapter
        De data adapter voor het in- en uitvoeren van gegevens.
    df_in_section_loads : Optional[pd.DataFrame] | None
        DataFrame: Tijdreeks met belasting op het dijkvak.
    df_in_fragility_curves : Optional[pd.DataFrame] | None
        DataFrame: Fragility curves voor het dijkvak.
    df_out : Optional[pd.DataFrame] | None
        DataFrame: Uitvoer.
    input_schema_fragility_curves : ClassVar[dict[str, str]]
        Het invoerschema voor de fragility curves per dijkvak.
    input_schema_loads : ClassVar[dict[str, str | list[str]]]
        Het invoerschema voor de belasting per moment per dijkvak

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
        "measure_id": "int64",
        "failuremechanism": "object",
        "hydraulicload": "float64",
        "failureprobability": "float64",
    }

    # belasting per moment per dijkvak
    input_schema_loads: ClassVar[dict[str, str | list[str]]] = {
        "section_id": "int64",
        "parameter_id": "int64",
        "unit": "object",
        "date_time": ["datetime64[ns, UTC]", "object"],
        "value": "float64",
        "value_type": "object",
    }

    def run(self, input: list[str], output: str) -> None:
        """
        Bepalen faalkans van een dijkvak met maatregel.

        Parameters
        ----------
        input : list[str]
            Lijst met namen van data adapters voor tijd reeks met belasting op het dijkvak en fragility curves voor het dijkvak.
        output : str
            Uitvoer data adapter voor de faalkans van een dijkvak met maatregel.

        Returns
        -------
        None

        Raises
        ------
        UserWarning
            Als de lengte van de input niet gelijk is aan 2.
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

        # uitvoer: belasting per dijkvak
        self.df_out = pd.DataFrame()

        # Unieke combinaties van section_id en failuremechanism
        unique_combinations = (
            self.df_in_fragility_curves[
                ["section_id", "failuremechanism", "measure_id"]
            ]
            .drop_duplicates(subset=["section_id", "failuremechanism", "measure_id"])
            .reset_index(drop=True)
        )

        # hergebruik code uit SectionsTechnicalFailureprobability voor minder code duplicatied
        self.df_out = self.iterate_combinations(
            unique_combinations,
            self.df_in_belasting,
            self.df_in_fragility_curves,
            pd.DataFrame(),  # initialiseer lege DataFrame
        )

        self.data_adapter.output(output=output, df=self.df_out)
