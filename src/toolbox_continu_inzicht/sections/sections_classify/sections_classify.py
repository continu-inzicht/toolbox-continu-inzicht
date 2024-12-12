"""
Bepaal de status van een dijkvak
"""

from pydantic.dataclasses import dataclass
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from typing import Optional

import pandas as pd


@dataclass(config={"arbitrary_types_allowed": True})
class SectionsClassify:
    """
    Bepaal de status van een dijkvak

    ## Input schema's
    **input_schema_thresholds (DataFrame): schema voor klassegrenzen per dijkvak\n
    - lower_boundary: float64           : ondergrens van de klassegrens
    - upper_boundary: float64           : bovengrens van de klassegrens
    - state_id: int64                   : id van de klassegrens

    **input_schema_failureprobability (DataFrame): schema voor faalkans per moment per dijkvak\n
    - section_id: int64                 : id van het dijkvak
    - date_time: datetime64[ns, UTC]    : datum/ tijd van de tijdreeksitem
    - value: float64                    : faalkans van de tijdreeksitem

    ## Output schema
    **df_out (DataFrame): uitvoer\n
    - failureprobability_id: in64       : id van de dijkvak/faalmechanisme/maatregel combinatie
    - section_id: int64                 : id van het dijkvak
    - value_parameter_id                : id van de belasting parameter (1,2,3,4)
    - failuremechanism_id: int64        : id van het faalmechanisme
    - failuremechanism: str             : code van het faalmechanisme
    - measures_id: int                  : id van de maatregel
    - measure str                       : naam van de maatregel
    - parameter_id: int64               : id van de faalkans parameter (5,100,101,102)
    - unit: str                         : eenheid van de belastingparameter
    - date_time: datetime64[ns, UTC]    : datum/ tijd van de tijdreeksitem
    - value: float64                    : faalkans van de tijdreeksitem
    - state_id: int64                   : id van de klassegrens
    """

    data_adapter: DataAdapter

    df_in_thresholds: Optional[pd.DataFrame] | None = None
    df_in_failureprobability: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    # klassegrenzen per dijkvak
    input_schema_thresholds = {
        "lower_boundary": "float64",
        "upper_boundary": "float64",
        "state_id": "int",
    }

    # faalkans per moment per dijkvak
    input_schema_failureprobability = {
        "section_id": "int64",
        "date_time": "datetime64[ns, UTC]",
        "value": "float64",
    }

    def run(self, input: list[str], output: str) -> None:
        """
        Bepaal de status van een dijkvak

        Args:
            input List(str): [0] klassegrenzen
                             [1] faalkans per dijkvak
            output (str): uitvoer sectie van het yaml-bestand:
                          koppeling van de maatgevende meetlocaties per dijkvak
        """
        if not len(input) == 2:
            raise UserWarning(
                "Input variabele moet 2 string waarden bevatten. (klassegrenzen/faalkans per dijkvak)"
            )

        # invoer 1: klassegrenzen
        self.df_in_thresholds = self.data_adapter.input(
            input[0], self.input_schema_thresholds
        )

        # invoer 2: faalskans per dijkvak
        self.df_in_failureprobability = self.data_adapter.input(
            input[1], self.input_schema_failureprobability
        )

        # uitvoer: belasting per dijkvak
        self.df_out = self.df_in_failureprobability.copy()

        # Functie om de juiste threshold te vinden
        def find_threshold(value, thresholds):
            for i, row in thresholds.iterrows():
                if (
                    pd.isna(row["lower_boundary"]) or value >= row["lower_boundary"]
                ) and value < row["upper_boundary"]:
                    return row["state_id"]
            return None

        self.df_out["state_id"] = self.df_out["value"].apply(
            lambda x: find_threshold(x, self.df_in_thresholds)
        )

        self.data_adapter.output(output=output, df=self.df_out)
