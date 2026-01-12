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
    df_in_sections_failure_probability : Optional[pd.DataFrame] | None
        Dataframe met dijkvakkansen per faalmechanisme
    df_in_sections_to_segment : Optional[pd.DataFrame] | None
        Dataframe met koppeling van dijkvakken naar deeltrajecten
    df_out : Optional[pd.DataFrame] | None
        Dataframe met gecombineerde deeltrajectkansen.
    df_out_combined_failure: Optional[pd.DataFrame] | None
        Dataframe met gecombineerde dijkvakkansen over alle dijkvakken
    schema_sections_to_segment : ClassVar[dict[str, str]]
        Schema voor de input dataframe met koppeling van dijkvakken naar deeltrajecten
    schema_sections_failure_probability : ClassVar[dict[str, str]]
        Schema voor de input dataframe met gecombineerde dijkvakkansen
    schema_failuremechanism : ClassVar[dict[str, str]]
        Schema voor de input dataframe met faalmechanismen

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
    df_in_sections_failure_probability: Optional[pd.DataFrame] | None = None
    df_in_sections_to_segment: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None
    df_out_combined_failure: Optional[pd.DataFrame] | None = None
    schema_sections_to_segment: ClassVar[dict[str, str]] = {
        "section_id": "int",
        "segment_id": "int",
        "length": "float",
    }
    schema_sections_failure_probability: ClassVar[dict[str, str]] = {
        "section_id": "int",
    }
    schema_failuremechanism: ClassVar[dict[str, str]] = {
        "failure_mechanism_id": "int",
        "name": "object",
        "description": "object",
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

        if not len(input) == 3:
            raise UserWarning("Input variabele moet 3 string waarden bevatten.")
        if not len(output) == 2:
            raise UserWarning("Output variabele moet 2 string waarden bevatten.")

        # failure probability per meetlocatie
        self.df_in_sections_failure_probability = self.data_adapter.input(
            input[0], schema=self.schema_sections_failure_probability
        )

        # sections to segment mapping
        self.df_in_sections_to_segment = self.data_adapter.input(
            input[1], schema=self.schema_sections_to_segment
        )
        self.df_in_failuremechanism = self.data_adapter.input(
            input[2], schema=self.schema_failuremechanism
        )

        # overstromingskans gebied is het combineren van de faalkansen van de dijkvakken in dat gebied
        df_all_sections = self.df_in_sections_to_segment.copy()
        combined_dike_failure_probability_per_mechanism = (
            self.calculate_failure_probability_for_given_sections(
                df_sections=df_all_sections
            )
        )

        # can faalkans per mechanisme combineren naar totale faalkans van gebied
        failure = 1
        for prob in combined_dike_failure_probability_per_mechanism.values():
            failure *= 1 - prob
        combined_dikesystem_failure_prob = 1 - failure

        # scenario kan is de lengthe factor * overstromingskans van het gebied
        # met de kans per segement doe we uigeindelijk niks.
        segments = self.df_in_sections_to_segment["segment_id"].unique()
        _unused_segment_failure = {}  # met de kans per segement doe we uigeindelijk niks.
        segment_scenario_failure = {}
        # Dan pas opbossen.
        for segment in segments:
            # sections die horen bij een segment
            df_sections = self.df_in_sections_to_segment[
                self.df_in_sections_to_segment["segment_id"] == segment
            ]

            # hier alleen voor de sections die bij een segment horen
            failure_per_mechanims = (
                self.calculate_failure_probability_for_given_sections(
                    df_sections=df_sections,
                )
            )
            # combined failure per segment per mechanisme (niet gebruikt)
            _unused_segment_failure[segment] = failure_per_mechanims

            # segment scenario failure is what is used
            segment_scenario_failure[segment] = (
                self.calculate_scenario_failure_probability(
                    df_sections=df_sections,
                    combined_dikesystem_failure_prob=combined_dikesystem_failure_prob,
                )
            )

        self.df_out_combined_failure = pd.DataFrame.from_dict(
            {0: combined_dikesystem_failure_prob},
            orient="index",
            columns=["combined_failure_probability"],
        )

        self.df_out = pd.DataFrame.from_dict(
            segment_scenario_failure,
            orient="index",
            columns=["scenario_failure_probability"],
        ).reset_index()

        self.df_out = self.df_out.rename(columns={"index": "segment_id"})

        self.data_adapter.output(output=output[0], df=self.df_out)
        self.data_adapter.output(output=output[1], df=self.df_out_combined_failure)

    def calculate_failure_probability_for_given_sections(
        self, df_sections: pd.DataFrame
    ) -> float:
        """
        Bereken de faalkans voor één gegeven aantal segment/gebied door de faalkansen van de gebieden te combineren.
        Voor GEKB wordt de max genomen, voor de rest onafhankelijk gecombineerd.

        parameters
        ----------
        df_sections: pd.DataFrame
            Dataframe met de sections die horen bij een segment
        returns
        -------
        float
            Faalkans voor het segment
        """

        # haal voor die sections de bijbehorende kansen op
        df_prob = self.df_in_sections_failure_probability.copy()
        # filter in the faalkansen alleen de sections die bij dit segment horen
        df_prob_segment = df_prob[
            df_prob["section_id"].apply(lambda x: x in df_sections["section_id"].values)
        ].copy()
        failure_mechanism_ids = self.df_in_failuremechanism[
            "failure_mechanism_id"
        ].values
        failure_mechanism_names = self.df_in_failuremechanism.set_index("name").copy()
        failure_mechanism_id_GEKB = failure_mechanism_names.loc[
            "GEKB", "failure_mechanism_id"
        ]
        # Combineer onafhankelijk: P(fail,comb|h) = 1 - PROD(1 - P(fail,i|h)) voor alle mechanismes behalve GEKB, die is max
        #                          failure = 1 - failure_prod
        failure_per_mechanims = {}
        for fm_id in failure_mechanism_ids:
            failure_prod = 1
            # TODO: what to do if empty? -> should be set to 0?
            df_prob_fm = df_prob_segment[f"failure_probability_{fm_id}"]
            if fm_id == failure_mechanism_id_GEKB:
                failure_prod = max(df_prob_fm)
            else:
                failure_prod *= (1 - df_prob_fm).prod()
            failure_per_mechanims[fm_id] = 1 - failure_prod

        return failure_per_mechanims

    def calculate_scenario_failure_probability(
        self, df_sections, combined_dikesystem_failure_prob
    ):
        """
        Bereken de faalkans voor het per trajectdeel, dit is een factor van de scenario kans van het hele systeem.
        """
        # totale lengte van alle sections
        total_length = self.df_in_sections_to_segment["length"].sum()
        length_segment = df_sections["length"].sum()
        return combined_dikesystem_failure_prob * length_segment / total_length
