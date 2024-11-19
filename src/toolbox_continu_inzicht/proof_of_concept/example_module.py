from pydantic.dataclasses import dataclass
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
import pandas as pd
from typing import Optional


@dataclass(config={"arbitrary_types_allowed": True})
class ValuesDivideTwo:
    """
    Voorbeeld class die laat zien hoe de arcitectuur werkt


    """

    data_adapter: DataAdapter
    # pydantic heeft problemen met pd, dus optioneel
    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    def run(self, input: str, output: str):
        """Runt de funcies en stuur de df terug"""
        self.df_in = self.data_adapter.input(input)

        if self.df_in is not None:
            # check of de data klopt
            cols = ["objectid", "objecttype", "parameterid", "date_time", "value"]
            list_bool_cols = [col in self.df_in.columns for col in cols]
            if all(list_bool_cols):
                self.df_out = self.divide_two(self.df_in)
                self.data_adapter.output(output, self.df_out)

            else:
                raise UserWarning(
                    f"Data moet de {len(cols)} kollom(en) {cols} hebben, maar heeft {self.df_in.columns}"
                )
        else:
            raise UserWarning(
                "Problemen bij het lezen van de input, check de configuratie"
            )

    # Maar dan een hoop risico berekeningen
    @staticmethod
    def divide_two(dataframe: pd.DataFrame) -> pd.DataFrame:
        """Vermenigvuldigd de meetstation waardes met 2 als voorbeeld"""
        df = dataframe.copy(
            deep=True
        )  # maak een copie om het verschil te zien tussen in en output
        df["value"] = df["value"] / 2
        return df


@dataclass(config={"arbitrary_types_allowed": True})
class ValuesTimesTwo:
    """
    Voorbeeld class die laat zien hoe de arcitectuur werkt

    Args:
        data_adapter: DataAdapter
                     Leest

    """

    data_adapter: DataAdapter
    # pydantic heeft problemen met pd, dus optioneel
    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    def run(self, input: str, output: str):
        """Runt de funcies en stuur de df terug"""

        self.df_in = self.data_adapter.input(input)

        if self.df_in is not None:
            # check of de data klopt
            cols = ["objectid", "objecttype", "parameterid", "date_time", "value"]
            list_bool_cols = [col in self.df_in.columns for col in cols]
            if all(list_bool_cols):
                self.df_out = self.times_two(self.df_in)
                self.data_adapter.output(output, self.df_out)

            else:
                raise UserWarning(
                    f"Data moet de {len(cols)} kollom(en) {cols} hebben, maar heeft {self.df_in.columns}"
                )

        else:
            raise UserWarning(
                "Problemen bij het lezen van de input, check de configuratie"
            )

    # Maar dan een hoop risico berekeningen
    @staticmethod
    def times_two(dataframe: pd.DataFrame) -> pd.DataFrame:
        """Deelt de meetstation waardes door 2 als voorbeeld"""
        df = dataframe.copy(
            deep=True
        )  # maak een copie om het verschil te zien tussen in en output
        df["value"] = df["value"] * 2
        return df
