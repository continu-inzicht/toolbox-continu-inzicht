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
    input: str
    input2: str
    output: str

    df_in: Optional[pd.DataFrame] | None = None
    df_in2: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    async def run(self, input=None, input2=None, output=None) -> pd.DataFrame:
        """
        De runner van de Loads Classify.

        Args:

        Returns:
            Dataframe: Pandas dataframe met geclassificeerde waterstanden voor opgegeven momenten.
        """
        if input is None:
            input = self.input
        if input2 is None:
            input2 = self.input2
        if output is None:
            output = self.output

        # thresholds
        self.df_in = self.data_adapter.input(input)

        # waterstanden
        self.df_in2 = self.data_adapter.input(input2)

        df_thresholds = self.df_in.copy()
        df_thresholds["van"] = df_thresholds["van"].fillna(-999900)
        df_thresholds["tot"] = df_thresholds["tot"].fillna(+999900)

        # waterstanden in centimeters
        df_waterstanden = self.df_in2.copy()
        df_waterstanden["value"] = df_waterstanden["value"] * 100

        self.df_out = df_waterstanden.merge(df_thresholds, on="code", how="outer")
        self.df_out = self.df_out[["code", "datetime", "value", "van", "tot", "kleur", "label"]]
        self.df_out = self.df_out[
            (self.df_out["value"] < self.df_out["tot"])
            & (self.df_out["value"] > self.df_out["van"])
        ]

        self.data_adapter.output(output=output, df=self.df_out)

        return self.df_out
