"""
Bepaal de technische faalkans van een dijkvak
"""

from pydantic.dataclasses import dataclass
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from typing import Optional

import pandas as pd


@dataclass(config={"arbitrary_types_allowed": True})
class SectionsExpertJudgementFailureprobability:
    """
    Bepaal de beheerdersoordeel faalkans van een dijkvak

    ## Input schema's
    **input_schema_fragility_curves (DataFrame): schema voor fragility curves voor de dijkvak\n
    - section_id: int64                 : id van het dijkvak
    - parameter_id: int64               : id van de faalkans parameter (5,100,101,102)
    - date_time: datetime64[ns, UTC]    : datum/ tijd van de tijdreeksitem
    - value_type: str                   : type waarde van de tijdreeksitem (meting of verwacht)
    - failureprobability: float64       : faalkans bepaald voor de tijdreeksitem
    - failuremechanism: str             : code van het faalmechanisme

    ## Output schema
    **df_out (DataFrame): uitvoer\n
    - section_id: int64                 : id van het dijkvak
    - parameter_id: int64               : id van de faalkans parameter (5,100,101,102)
    - unit: str                         : eenheid van de belastingparameter
    - date_time: datetime64[ns, UTC]    : datum/ tijd van de tijdreeksitem
    - value_type: str                   : type waarde van de tijdreeksitem (meting of verwacht)
    - failureprobability: float64       : faalkans bepaald voor de tijdreeksitem
    - failuremechanism: str             : code van het faalmechanisme
    """

    data_adapter: DataAdapter

    df_in_section_expertjudgement: Optional[pd.DataFrame] | None = None
    """DataFrame: beheerdersoordeel voor de dijkvak."""

    df_out: Optional[pd.DataFrame] | None = None
    """DataFrame: uitvoer."""

    # Beheerdersoordeel per dijkvak
    input_schema = {
        "section_id": "int64",
        "parameter_id": "int64",
        "unit": "object",
        "date_time": ["datetime64[ns, UTC]", "object"],
        "state_id": "int64",
        "value_type": "object",
        "failureprobability": "float64",
        "failuremechanism": "object",
    }

    def run(self, input: str, output: str) -> None:
        """
        Uitvoeren van het bepalen van de faalkans van een dijkvak.

        Args:\n
            input str: lijst met beheerderoordelen
            output (str): uitvoer sectie van het yaml-bestand.
        """

        self.df_in = self.data_adapter.input(input, self.input_schema)

        # Datum als string omzetten naar datetime object
        if not pd.api.types.is_datetime64_any_dtype(self.df_in["date_time"]):
            self.df_in["date_time"] = pd.to_datetime(self.df_in["date_time"])

        # uitvoer: belasting per dijkvak
        self.df_out = self.df_in.copy()  # pd.DataFrame()

        self.data_adapter.output(output=output, df=self.df_out)
