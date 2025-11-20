from typing import ClassVar, Optional

import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht.base.base_module import ToolboxBase
from toolbox_continu_inzicht.base.data_adapter import DataAdapter


@dataclass(config={"arbitrary_types_allowed": True})
class CalculateFloodScenarioProbability(ToolboxBase):
    """
    Met deze functie worden de dijkvakkansen gecombineerd naar deeltrajectkansen.

    Attributes
    ----------
    data_adapter : DataAdapter
        De data adapter die wordt gebruikt om de data in te laden en op te slaan.
    df_in_grouped_sections_failure_probability : Optional[pd.DataFrame] | None
        Dataframe met dijkvakkansen
    df_in_sections_to_segment : Optional[pd.DataFrame] | None
        Dataframe met koppeling van dijkvakken naar deeltrajecten
    df_out : Optional[pd.DataFrame] | None
        Dataframe met gecombineerde deeltrajectkansen.
    schema_sections_to_segment : ClassVar[dict[str, str]]
        Schema voor de input dataframe met koppeling van dijkvakken naar deeltrajecten
    schema_grouped_sections_failure_probability : ClassVar[dict[str, str]]
        Schema voor de input dataframe met gecombineerde dijkvakkansen

    Notes
    -----

    schema voor sections_to_segment

    - section_id: int
    - segment_id: int

    schema voor grouped_sections_failure_probability
    - section_id: int
    - failure_probability: float

    """

    data_adapter: DataAdapter

    df_in_grouped_sections_failure_probability: Optional[pd.DataFrame] | None = None
    df_in_sections_to_segment: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None
    schema_sections_to_segment: ClassVar[dict[str, str]] = {
        "section_id": "int",
        "segment_id": "int",
    }
    schema_grouped_sections_failure_probability: ClassVar[dict[str, str]] = {
        "section_id": "int",
        "failure_probability": "float",
    }

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
            input[0], schema=self.schema_grouped_sections_failure_probability
        )

        # belasting per moment per meetlocaties
        self.df_in_sections_to_segment = self.data_adapter.input(
            input[1], schema=self.schema_sections_to_segment
        )

        segments = self.df_in_sections_to_segment["segment_id"].unique()
        segments = self.df_in_sections_to_segment["segment_id"].unique()
        segment_failure = {}
        for segment in segments:
            # sections die horen bij een segment
            df_sections = self.df_in_sections_to_segment[
                self.df_in_sections_to_segment["segment_id"] == segment
            ]
            dict_sections_to_length = dict(
                zip(df_sections["section_id"], df_sections["length"])
            )
            # haal voor die sections de bijbehorende kansen op
            df_prob = self.df_in_grouped_sections_failure_probability.copy()
            df_prob_segment = df_prob[
                df_prob["section_id"].apply(
                    lambda x: x in dict_sections_to_length.keys()
                )
            ].copy()
            # zoek in de input section de lengte op
            df_prob_segment["length"] = df_prob_segment.apply(
                lambda row: dict_sections_to_length[row["section_id"]], axis=1
            )
            # deze kansen wegen we nu naar lengte
            total_length = df_prob_segment["length"].sum()
            df_prob_segment["weighted_failure_probability"] = (
                df_prob_segment["failure_probability"]
                * df_prob_segment["length"]
                / total_length
            )
            """Combineer onafhankelijk: P(fail,comb|h) = 1 - PROD(1 - P(fail,i|h))"""
            failure = 1 - (1 - df_prob_segment["weighted_failure_probability"]).prod()
            segment_failure[segment] = failure

        # placeholder

        self.df_out = pd.DataFrame.from_dict(
            segment_failure, orient="index", columns=["failure_probability"]
        ).reset_index()
        self.df_out = self.df_out.rename(columns={"index": "segment_id"})

        self.data_adapter.output(output=output, df=self.df_out)
