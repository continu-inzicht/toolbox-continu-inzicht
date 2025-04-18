from typing import Optional

import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht.base.base_module import ToolboxBase
from toolbox_continu_inzicht.base.data_adapter import DataAdapter


@dataclass(config={"arbitrary_types_allowed": True})
class LoadsCIWhatIf(ToolboxBase):
    """
    Met deze functie worden belasting opgehaald en weggeschreven.
    """

    data_adapter: DataAdapter

    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    def run(self, input: str, output: str) -> None:
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
        calc_time = self.data_adapter.config.global_variables["calc_time"]
        # dupliceer calc_time laatste waarde zodat de lijn in de viewer mooi doorloopt
        verwachting = self.df_out[(self.df_out["date_time"] >= calc_time)].index
        meting = self.df_out[(self.df_out["date_time"] <= calc_time)].index
        self.df_out.loc[verwachting, "value_type"] = "verwachting"
        self.df_out.loc[meting, "value_type"] = "meting"
        self.data_adapter.output(output=output, df=self.df_out)
