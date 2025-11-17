from typing import Optional

import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht.base.base_module import ToolboxBase
from toolbox_continu_inzicht.base.data_adapter import DataAdapter


@dataclass(config={"arbitrary_types_allowed": True})
class CalculateFloodScenarioProbability(ToolboxBase):
    """
    Met deze functie worden de waterstanden met opgegeven grenzen geclassificeerd.

    Attributes
    ----------
    data_adapter : DataAdapter
        De data adapter die wordt gebruikt om de data in te laden en op te slaan.
    df_in_grouped_sections_failure_probability : Optional[pd.DataFrame] | None
        Dataframe met dijkvakkansen
    df_in_sections_to_segment : Optional[pd.DataFrame] | None
        Dataframe met koppeling van dijkvakken naar deeltrajecten
    df_out : Optional[pd.DataFrame] | None
        Dataframe met geclassificeerde waterstanden voor opgegeven momenten.

    Notes
    -----


    """

    data_adapter: DataAdapter

    df_in_grouped_sections_failure_probability: Optional[pd.DataFrame] | None = None
    df_in_sections_to_segment: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    def run(self, input: list[str], output: str) -> None:
        """
        De runner van de Calculate Flood Scenario Probability module.

        parameters
        ----------
        input: list[str]
            Lijst met namen van de data adapter voor de dijkvakkansen (df_in_sections_failure_probability) en de vertaling van vakken naar trajecten
        output: str
            Data adapter voor output van scenario kansen per deeltraject
        """

        if not len(input) == 2:
            raise UserWarning("Input variabele moet 2 string waarden bevatten.")

        # drempelwaarden per meetlocatie
        self.df_in_grouped_sections_failure_probability = self.data_adapter.input(
            input[0], self.df_in_grouped_sections_failure_probability
        )

        # belasting per moment per meetlocaties
        self.df_in_sections_to_segment = self.data_adapter.input(
            input[1], self.df_in_sections_to_segment
        )

        # placeholder

        self.df_out = self.df_in_sections_to_segment

        self.data_adapter.output(output=output, df=self.df_out)
