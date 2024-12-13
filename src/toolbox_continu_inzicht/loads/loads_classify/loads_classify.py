from pydantic.dataclasses import dataclass
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
import pandas as pd
from typing import Optional


@dataclass(config={"arbitrary_types_allowed": True})
class LoadsClassify:
    """
    Met deze functie worden de waterstanden met opgegeven grenzen geclassificeerd.

    """

    data_adapter: DataAdapter

    df_in_thresholds: Optional[pd.DataFrame] | None = None
    df_in_loads: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    # Lijst met drempelwaarden per meetlocatie
    input_schema_thresholds = {
        "measurement_location_id": "int64",
        "lower_boundary": "float64",
        "upper_boundary": "float64",
        "color": "object",
        "label": "object",
        "unit": "object",
    }

    # belasting per moment per meetlocaties
    input_schema_loads = {
        "measurement_location_id": "int64",
        "parameter_id": "int64",
        "unit": "object",
        "date_time": "object",
        "value": "float64",
        "value_type": "object",
        "hours": "int64",
    }

    def run(self, input: list[str], output: str) -> None:
        """
        De runner van de Loads Classify.

        Args:
            input List(str): [0] lijst met drempelwaarden per meetlocatie
                             [1] belasting per moment per meetlocaties
            output (str):    uitvoer sectie van het yaml-bestand:
                             koppeling van de maatgevende meetlocaties per dijkvak

        Returns:
            Dataframe: Pandas dataframe met geclassificeerde waterstanden voor opgegeven momenten.
        """

        if not len(input) == 2:
            raise UserWarning("Input variabele moet 2 string waarden bevatten.")

        # drempelwaarden per meetlocatie
        # TODO we kunnen er toch niet vanuit gaan dat de eerste (nulde) input string de verwijzing naar deklassegrenzen is...?
        self.df_in_thresholds = self.data_adapter.input(
            input[0], self.input_schema_thresholds
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
            (self.df_out["value"] < self.df_out["upper_boundary"])
            & (self.df_out["value"] > self.df_out["lower_boundary"])
        ]

        self.data_adapter.output(output=output, df=self.df_out)
