from pydantic.dataclasses import dataclass
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
import pandas as pd
from typing import Optional


@dataclass(config={"arbitrary_types_allowed": True})
class LoadsCIWhatIf:
    """
    Met deze functie worden belasting opgehaald en weggeschreven.
    """

    data_adapter: DataAdapter

    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    def run(self, input: list[str], output=None) -> pd.DataFrame:
        """
        De runner van de Loads Classify.

        Args:
            input (str):

            output (str):


        Returns:
            Dataframe: Pandas dataframe met ...
        """
        # input: belasting voor alle meetstations
        self.df_in = self.data_adapter.input(input=input, schema=None)

        # output: belasting voor alle meetstations
        self.df_out = self.df_in.copy()

        self.data_adapter.output(output=output, df=self.df_out)

        return self.df_out
