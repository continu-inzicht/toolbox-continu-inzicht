from pydantic.dataclasses import dataclass
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
import pandas as pd
from typing import Optional


@dataclass(config={"arbitrary_types_allowed": True})
class ValuesDivideTwo:
    """
    Voorbeeld class die laat zien hoe de architectuur werkt

    """

    data_adapter: DataAdapter
    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    input_schema = {
        "value": "float",
    }

    def run(self, input: str, output: str):
        """Runt de funcies en stuur de df terug"""
        self.df_in = self.data_adapter.input(input, schema=self.input_schema)

        self.df_out = self.divide_two(self.df_in)
        self.data_adapter.output(output, self.df_out)

    @staticmethod
    def divide_two(dataframe: pd.DataFrame) -> pd.DataFrame:
        """Vermenigvuldigd de meetstation waardes met 2 als voorbeeld"""
        df = dataframe.copy(deep=True)
        df["value"] = df["value"] / 2
        return df


@dataclass(config={"arbitrary_types_allowed": True})
class ValuesTimesTwo:
    """
    Voorbeeld class die laat zien hoe de architectuur werkt

    Args:
        data_adapter: DataAdapter

    """

    data_adapter: DataAdapter
    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    input_schema = {
        "value": "float",
    }

    def run(self, input: str, output: str):
        """Runt de funcies en stuur de df terug"""

        self.df_in = self.data_adapter.input(input, self.input_schema)
        self.df_out = self.times_two(self.df_in)
        self.data_adapter.output(output, self.df_out)

    @staticmethod
    def times_two(dataframe: pd.DataFrame) -> pd.DataFrame:
        """Deelt de meetstation waardes door 2 als voorbeeld"""
        df = dataframe.copy(deep=True)
        df["value"] = df["value"] * 2
        return df
