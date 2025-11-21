from typing import ClassVar, Optional

import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht.base.base_module import ToolboxBase
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.base.fragility_curve import FragilityCurve


@dataclass(config={"arbitrary_types_allowed": True})
class LoadFromFloodScenarioProbability(ToolboxBase):
    """
    Met deze functie wordt de belasting van een scenario bepaald.

    Attributes
    ----------
    data_adapter : DataAdapter
        De data adapter die wordt gebruikt om de data in te laden en op te slaan.
    df_in_segment_failure_probability : Optional[pd.DataFrame] | None
        Dataframe met deeltrajectkansen
    df_in_breach_to_segment_risk : Optional[pd.DataFrame] | None
        Dataframe met koppeling van Bresen naar deeltrajecten en maatgevende fragility curves
    df_in_fragility_curves : Optional[pd.DataFrame] | None
        Dataframe met koppeling van dijkvakken naar deeltrajecten
    df_out : Optional[pd.DataFrame] | None
        Dataframe met geclassificeerde waterstanden voor opgegeven momenten.
    schema_segment_failure_probability : ClassVar[dict[str, str]]
        Schema voor de input dataframe met deeltrajectkansen
    sechema_breach_to_segment_risk : ClassVar[dict[str, str]]
        Schema voor de input dataframe met koppeling van Bresen naar deeltrajecten en maatgevende fragility curves
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

    df_in_segment_failure_probability: Optional[pd.DataFrame] | None = None
    df_in_breach_to_segment_risk: Optional[pd.DataFrame] | None = None
    df_in_fragility_curves: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None
    schema_segment_failure_probability: ClassVar[dict[str, str]] = {
        "segment_id": "int",
        "failure_probability": "float",
    }
    sechema_breach_to_segment_risk: ClassVar[dict[str, str]] = {
        "segment_id": "int",
        "breach_id": "int",
        "representative_section_id_fragilitycurve": "int",
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
            Lijst met namen van de data adapter voor
        output: str
            Data adapter voor output van scenario kansen per deeltraject
        """

        if not len(input) == 3:
            raise UserWarning("Input variabele moet 2 string waarden bevatten.")

        self.df_in_segment_failure_probability = self.data_adapter.input(
            input[0], schema=self.schema_segment_failure_probability
        )
        self.df_in_segment_failure_probability.set_index("segment_id", inplace=True)

        # drempelwaarden per meetlocatie``
        self.df_in_breach_to_segment_risk = self.data_adapter.input(
            input[1], schema=self.sechema_breach_to_segment_risk
        )
        df_segment_to_curve = self.df_in_breach_to_segment_risk.set_index("segment_id")
        # belasting per moment per meetlocaties
        self.df_in_fragility_curves = self.data_adapter.input(
            input[2], schema=self.schema_grouped_sections_failure_probability
        )
        self.df_in_fragility_curves.set_index("section_id", inplace=True)

        segments = self.df_in_breach_to_segment_risk["segment_id"].unique()
        for segment in segments:
            segment_failure_probability = self.df_in_segment_failure_probability.loc[
                segment, "failure_probability"
            ]
            fragility_curve_id = df_segment_to_curve.loc[
                segment, "representative_section_id_fragilitycurve"
            ]
            # load curve based on fragility_curve_id
            fragility_curve = FragilityCurve.from_dataframe(
                self.df_in_fragility_curves.loc[fragility_curve_id]
            )
            # look up the water level for the given failure probability
            return fragility_curve, segment_failure_probability
            # fragility_curve.water_level_from_failure_probability(segment_failure_probability)

        self.data_adapter.output(output=output, df=self.df_out)
