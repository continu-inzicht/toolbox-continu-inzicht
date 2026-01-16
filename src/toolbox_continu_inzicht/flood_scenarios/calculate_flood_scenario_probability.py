from typing import ClassVar, Optional

import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht.base.base_module import ToolboxBase
from toolbox_continu_inzicht.base.data_adapter import DataAdapter


@dataclass(config={"arbitrary_types_allowed": True})
class CalculateFloodScenarioProbability(ToolboxBase):
    """
    Met deze functie worden scenariokansen berekend uit de kansen per sectie en per faalmechanisme.

    Attributes
    ----------
    data_adapter : DataAdapter
        De data adapter die wordt gebruikt om de data in te laden en op te slaan.
    df_in_failuremechanism : Optional[pd.DataFrame] | None
        Dataframe met faalmechanismen
    df_in_sections_failure_probability : Optional[pd.DataFrame] | None
        Dataframe met kansen per sectie en per faalmechanisme
    df_in_sections_in_segment : Optional[pd.DataFrame] | None
        Dataframe met secties per deeltraject (segment)
    df_out_scenario_failure_prob_segments: Optional[pd.DataFrame] | None
        Dataframe met scenariokansen over alle secties per deeltraject (segment) en faalmechanismen
    df_out_combined_failure_prob_all_sections: Optional[pd.DataFrame] | None
        Dataframe met gecombineerde kansen over alle secties en faalmechanismen
    schema_failuremechanism : ClassVar[dict[str, str]]
        Schema voor de input dataframe met faalmechanismen
    schema_sections_failure_probability : ClassVar[dict[str, str]]
        Schema voor de input dataframe met kansen per sectie en per faalmechanisme
    schema_sections_in_segment : ClassVar[dict[str, str]]
        Schema voor de input dataframe met koppeling van dijkvakken en deeltrajecten (segmenten)

    Notes
    -----

    schema voor failuremechanism
    - failuremechanism_id: int
    - name : str
    - description : str

    schema voor sections_failure_probability
    - section_id: int
    - failuremechanism_id: int
    - combined_failure_probability: float

    schema voor sections_in_segment
    - section_id: int
    - segment_id: int

    """

    data_adapter: DataAdapter
    df_in_failuremechanism: Optional[pd.DataFrame] | None = None
    df_in_sections_failure_probability: Optional[pd.DataFrame] | None = None
    df_in_sections_in_segment: Optional[pd.DataFrame] | None = None
    df_out_scenario_failure_prob_segments: Optional[pd.DataFrame] | None = None
    df_out_combined_failure_prob_all_sections: Optional[pd.DataFrame] | None = None

    # schemas voor de input dataframes
    schema_failuremechanism: ClassVar[dict[str, str]] = {
        "failuremechanism_id": "int",
        "name": "object",
        "description": "object",
    }
    schema_sections_failure_probability: ClassVar[dict[str, str]] = {
        "section_id": "int",
        "failuremechanism_id": "int",
        "failure_probability": "float",
    }
    schema_sections_in_segment: ClassVar[dict[str, str]] = {
        "section_id": "int",
        "segment_id": "int",
    }

    def run(self, input: list[str], output: str) -> None:
        """
        De runner van de Calculate Flood Scenario Probability module.

        parameters
        ----------
        input: list[str]
            Lijst met namen van de data adapters
        output: str
            Data adapter voor output van scenario kansen per deeltraject (segment)
        """

        if not len(input) == 3:
            raise UserWarning("Input variabele moet 3 string waarden bevatten.")
        if not len(output) == 2:
            raise UserWarning("Output variabele moet 2 string waarden bevatten.")

        # faalmechanismen
        self.df_in_failuremechanism = self.data_adapter.input(
            input[0], schema=self.schema_failuremechanism
        )

        # faalkans per sectie per faalmechanisme
        self.df_in_sections_failure_probability = self.data_adapter.input(
            input[1], schema=self.schema_sections_failure_probability
        )

        # secties per deeltraject (segment)
        self.df_in_sections_in_segment = self.data_adapter.input(
            input[2], schema=self.schema_sections_in_segment
        )

        # eerst de kans voor alle secties in het gebied bepalen door het combineren van de faalkansen van alle secties in een gebied per faalmechanisme
        df_all_sections = self.df_in_sections_in_segment.copy()
        combined_area_failure_probability_per_mechanism = (
            self.calculate_failure_probability_for_given_sections(
                df_sections=df_all_sections
            )
        )

        # dan de faalkans per faalmechanisme combineren naar een totale faalkans voor alle secties in een gebied en voor alle faalmechanismen
        failure = 1
        for prob in combined_area_failure_probability_per_mechanism.values():
            failure *= 1 - prob
        combined_area_failure_prob = 1 - failure

        # bereken vervolgens de gecombineerde faalkans voor de secties per segment en over alle faalmechanismen
        # TODO: pas de lengteeffectfactor toe bij de berekening van de faalkans per segment per faalmechanisme
        segments = self.df_in_sections_in_segment["segment_id"].unique()
        segment_failure_prob = {}

        for segment in segments:
            # bepaal de secties die horen bij een segment
            df_sections = self.df_in_sections_in_segment[
                self.df_in_sections_in_segment["segment_id"] == segment
            ]

            # combineer de faalkansen nu alleen voor de secties die bij een specifiek segment horen
            failure_prob_per_mechanims = (
                self.calculate_failure_probability_for_given_sections(
                    df_sections=df_sections,
                )
            )

            # combineer de faalkansen over alle faalmechanismen voor het segment
            failure = 1
            for prob in failure_prob_per_mechanims.values():
                failure *= 1 - prob
            segment_failure_prob[segment] = 1 - failure

        # bepaal nu per segment de scenariokans: de scenariokans wordt benaderd door quotient van de segment faalkans en som van faalkansen van alle segmentemen vermenigvuldigd met de gecombineerde faalkans van het gebied
        segment_scenario_failure_prob = {
            "segment_id": [],
            "scenario_failure_probability": [],
        }

        # bepaal de som van de faalkansen van alle segments
        total_failure_prob = sum(segment_failure_prob.values())
        # bepaal de faalkans per segment en bereken de scenariokans per segment door quotient van de faalkans van het segment en de som van faalkansen van alle segmenten vermenigvuldigd met de gecombineerde faalkans van het gebied
        for segment in segments:
            # haal de faalkans segment op
            segment_failure_prob_value = segment_failure_prob[segment]
            quotient_segment = segment_failure_prob_value / total_failure_prob

            segment_scenario_failure_prob["segment_id"].append(segment)
            segment_scenario_failure_prob["scenario_failure_probability"].append(
                quotient_segment * combined_area_failure_prob
            )

        # maak een lookup voor de faalmechanisme namen en bepaal de id van COMB
        failure_mechanism_names = self.df_in_failuremechanism.set_index("name").copy()
        failure_mechanism_id_COMB = failure_mechanism_names.loc[
            "COMB", "failuremechanism_id"
        ]

        # schrijf de resultaten weg naar dataframes
        # schrijf de inhoud van segment_scenario_failure_prob weg naar een dataframe met de kolommen segment_id en scenario_failure_probability en per segement een rij
        self.df_out_scenario_failure_prob_segments = pd.DataFrame(
            segment_scenario_failure_prob
        ).sort_values(by="segment_id")

        self.data_adapter.output(
            output=output[0], df=self.df_out_scenario_failure_prob_segments
        )

        # schrijf de gecombineerde faalkans voor alle secties en faalmechanismen weg naar een dataframe met de kolommen failuremechanism_id en combined_failure_probability
        self.df_out_combined_failure_prob_all_sections = pd.DataFrame(
            {
                "failuremechanism_id": [failure_mechanism_id_COMB],
                "combined_failure_probability": [combined_area_failure_prob],
            }
        )

        self.data_adapter.output(
            output=output[1], df=self.df_out_combined_failure_prob_all_sections
        )

    def calculate_failure_probability_for_given_sections(
        self, df_sections: pd.DataFrame
    ) -> float:
        """
        Bereken de faalkans voor een meerdere secties in een deeltraject(segment) of gebied door de faalkansen te combineren per faalmechanisme.
        Voor het faalmechanisme GEKB wordt verondersteld dat de kansen volledig afhankelijk zijn (bereken de maximale kans van de secties).
        Voor de overige faalmechanismen worden de kansen onafhankelijk gecombineerd (bereken 1 - PROD(1 - P(fail,i|h))).

        parameters
        ----------
        df_sections: pd.DataFrame
            Dataframe met de sections die horen bij een segment of gebied
        returns
        -------
        float
            Faalkans voor het segment of gebied
        """

        # haal voor die sections de bijbehorende kansen op
        df_prob = self.df_in_sections_failure_probability.copy()

        # filter in the faalkansen alleen de sections die bij dit segment horen
        df_prob_given_sections = df_prob[
            df_prob["section_id"].apply(lambda x: x in df_sections["section_id"].values)
        ].copy()

        # haal de faalmechanisme ids op
        failure_mechanism_ids = self.df_in_failuremechanism[
            "failuremechanism_id"
        ].values

        # maak een lookup voor de faalmechanisme namen en bepaal de id van GEKB en COMB
        failure_mechanism_names = self.df_in_failuremechanism.set_index("name").copy()
        failure_mechanism_id_GEKB = failure_mechanism_names.loc[
            "GEKB", "failuremechanism_id"
        ]
        failure_mechanism_id_COMB = failure_mechanism_names.loc[
            "COMB", "failuremechanism_id"
        ]

        # Combineer onafhankelijk: P(fail,comb|h) = 1 - PROD(1 - P(fail,i|h)) voor alle mechanismes behalve COMB (die wordt opnieuw berekend) en GEKB die is gelijk aan max(P(fail,i|h))
        failure_per_mechanims = {}
        for fm_id in failure_mechanism_ids:
            failure_prod = 1.0
            # filter df_prob_segment per faalmechanisme
            df_prob_fm = df_prob_given_sections[
                df_prob_given_sections["failuremechanism_id"] == fm_id
            ]["failure_probability"]

            # als er geen faalkansen zijn voor dit faalmechanisme, ga door naar de volgende en zet de faalkans op 0
            if df_prob_fm.empty:
                failure_per_mechanims[fm_id] = 0.0
                continue
            if fm_id == failure_mechanism_id_GEKB:
                failure_prod = max(df_prob_fm)
                failure_per_mechanims[fm_id] = failure_prod
            elif fm_id == failure_mechanism_id_COMB:
                continue
            else:
                failure_prod *= (1 - df_prob_fm).prod()
                failure_per_mechanims[fm_id] = 1 - failure_prod

        return failure_per_mechanims
