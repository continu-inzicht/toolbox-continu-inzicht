from pydantic.dataclasses import dataclass
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
import pandas as pd
from typing import Optional


@dataclass(config={"arbitrary_types_allowed": True})
class LoadsClassify:
    """
    Met deze functie worden de waterstanden met opgegeven grenzen geclassificeerd
    """

    data_adapter: DataAdapter

    df_in_thresholds: Optional[pd.DataFrame] | None = None
    df_in_loads: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    # Lijst met drempelwaarden per meetlocatie
    input_schema_thresholds = {
        "measurement_location_code": "object",
        "van": "float64",
        "tot": "float64",
        "kleur": "object",
        "label": "object",
    }

    # belasting per moment per meetlocaties
    input_schema_loads = {
        "parameter_id": "int64",
        "unit": "object",
        "date_time": "object",
        "value": "float64",
        "value_type": "object",
    }

    async def run(self, input: list[str], output=None) -> pd.DataFrame:
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
        self.df_in_thresholds = self.data_adapter.input(
            input[0], self.input_schema_thresholds
        )

        # belasting per moment per meetlocaties
        self.df_in_loads = self.data_adapter.input(input[1], self.input_schema_loads)

        df_thresholds = self.df_in_thresholds.copy()
        df_thresholds["van"] = df_thresholds["van"].fillna(-999900)
        df_thresholds["tot"] = df_thresholds["tot"].fillna(+999900)

        # waterstanden in centimeters
        df_loads = self.df_in_loads.copy()

        df_loads.set_index("measurement_location_code")
        df_thresholds.set_index("measurement_location_code")

        self.df_out = df_loads.merge(
            df_thresholds, on="measurement_location_code", how="outer"
        )
        self.df_out = self.df_out[
            [
                "measurement_location_code",
                "date_time",
                "value",
                "van",
                "tot",
                "kleur",
                "label",
            ]
        ]
        self.df_out = self.df_out[
            (self.df_out["value"] < self.df_out["tot"])
            & (self.df_out["value"] > self.df_out["van"])
        ]

        self.data_adapter.output(output=output, df=self.df_out)

        return self.df_out
