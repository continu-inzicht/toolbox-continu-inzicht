from typing import ClassVar, Optional

import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht.base.base_module import ToolboxBase
from toolbox_continu_inzicht.base.data_adapter import DataAdapter


@dataclass(config={"arbitrary_types_allowed": True})
class LoadsClassify(ToolboxBase):
    """
    Met deze functie worden de waterstanden met opgegeven grenzen geclassificeerd.

    Attributes
    ----------
    data_adapter : DataAdapter
        De data adapter die wordt gebruikt om de data in te laden en op te slaan.
    df_in_thresholds : Optional[pd.DataFrame] | None
        Dataframe met drempelwaarden per meetlocatie.
    df_in_loads : Optional[pd.DataFrame] | None
        Dataframe met belasting per moment per meetlocaties.
    df_out : Optional[pd.DataFrame] | None
        Dataframe met geclassificeerde waterstanden voor opgegeven momenten.
    input_schema_thresholds : ClassVar[dict[str, str]]
        Schema voor drempelwaarden per meetlocatie.
    input_schema_loads : ClassVar[dict[str, str | list[str]]]
        Schema voor belasting per moment per meetlocaties.

    Notes
    -----

    **Input schema's**

    *input_schema_thresholds*: schema voor drempelwaarden per meetlocatie

    - measurement_location_id: int64    : id van het meetstation
    - lower_boundary: float64           : ondergrens van de drempelwaarde
    - upper_boundary: float64           : bovengrens van de drempelwaarde
    - color: str                        : kleurcode voor de drempelwaarde
    - label: str                        : label voor de drempelwaarde
    - unit: str                         : eenheid van de drempelwaarde


    *input_schema_loads*: schema voor belasting per moment per meetlocaties

    - measurement_location_id: int64    : id van het meetstation
    - parameter_id: int64               : id van de belastingparameter (1,2,3,4)
    - unit: str                         : eenheid van de belastingparameter
    - date_time: datetime64[ns, UTC]    : datum/ tijd van de tijdreeksitem
    - value: float64                    : waarde van de tijdreeksitem
    - value_type: str                   : type waarde van de tijdreeksitem (meting of verwacht)


    """

    data_adapter: DataAdapter

    df_in_thresholds: Optional[pd.DataFrame] | None = None
    df_in_loads: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    # Lijst met drempelwaarden per meetlocatie
    input_schema_thresholds: ClassVar[dict[str, str]] = {
        "measurement_location_id": "int64",
        "lower_boundary": "float64",
        "upper_boundary": "float64",
        "color": "object",
        "label": "object",
        "unit": "object",
    }

    # belasting per moment per meetlocaties
    input_schema_loads: ClassVar[dict[str, str | list[str]]] = {
        "measurement_location_id": "int64",
        "parameter_id": "int64",
        "unit": "object",
        "date_time": ["datetime64[ns, UTC]", "object"],
        "value": "float64",
        "value_type": "object",
        "hours": "int64",
    }

    def run(self, input: list[str], output: str) -> None:
        """
        De runner van de Loads Classify.

        parameters
        ----------
        input: list[str]
            Lijst met namen van de data adapter voor de drempelwaarde en belasting per meetlocatie.
        output: str
            Data adapter voor output van de koppeling van de maatgevende meetlocaties per dijkvak
        """

        if not len(input) == 2:
            raise UserWarning("Input variabele moet 2 string waarden bevatten.")

        # drempelwaarden per meetlocatie
        self.df_in_thresholds = self.data_adapter.input(
            input[0], self.input_schema_thresholds
        )

        if len(self.df_in_thresholds) == 0:
            raise UserWarning(
                "Er zijn geen klassegrenzen beschikbaar voor de gegeven belastingen!"
            )

        # belasting per moment per meetlocaties
        self.df_in_loads = self.data_adapter.input(input[1], self.input_schema_loads)

        df_thresholds = self.df_in_thresholds.copy()
        df_thresholds["lower_boundary"] = df_thresholds["lower_boundary"].fillna(
            -999900
        )
        df_thresholds["upper_boundary"] = df_thresholds["upper_boundary"].fillna(
            +999900
        )

        # waterstanden in centimeters
        df_loads = self.df_in_loads.copy()

        df_loads.set_index("measurement_location_id")
        df_thresholds.set_index("measurement_location_id")

        self.df_out = df_loads.merge(
            df_thresholds, on="measurement_location_id", how="outer"
        )
        self.df_out = self.df_out[
            [
                "measurement_location_id",
                "date_time",
                "value",
                "lower_boundary",
                "upper_boundary",
                "color",
                "label",
                "hours",
            ]
        ]

        self.df_out = self.df_out[
            (self.df_out["value"] <= self.df_out["upper_boundary"])
            & (self.df_out["value"] > self.df_out["lower_boundary"])
        ]

        self.data_adapter.output(output=output, df=self.df_out)
