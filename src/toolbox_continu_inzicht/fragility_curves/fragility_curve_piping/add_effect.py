from typing import Optional

import pandas as pd
from toolbox_continu_inzicht import DataAdapter
from toolbox_continu_inzicht.fragility_curves import (
    FragilityCurvePipingFixedWaterlevel,
)


class ShiftFragilityCurvePipingFixedWaterlevel(FragilityCurvePipingFixedWaterlevel):
    """Verschuift de fragility curve voor piping met een constante water niveau (simple) met een gegeven effect
        Attributes
    ----------
    data_adapter : DataAdapter
        Adapter for handling data input and output operations.
    df_prob_input : pd.DataFrame | None, optional
        DataFrame containing probabilistic input data.
    df_hydraulicload : pd.DataFrame | None, optional
        DataFrame containing hydraulic load data.
    df_out : pd.DataFrame | None, optional
        Output DataFrame containing the final fragility curve.
    df_result_uplift : pd.DataFrame | None, optional
        DataFrame containing the uplift mechanism results.
    df_result_heave : pd.DataFrame | None, optional
        DataFrame containing the heave mechanism results.
    df_result_sellmeijer : pd.DataFrame | None, optional
        DataFrame containing the Sellmeijer mechanism results.
    df_result_combined : pd.DataFrame | None, optional
        DataFrame containing the combined mechanism results.

    Notes
    -----
    De volgende bool opties kunnen worden ingesteld in de global_variables van de config:

    1. progress, Standaard is False
    1. debug, Standaard is False
    """

    data_adapter: DataAdapter
    df_prob_input: Optional[pd.DataFrame] | None = None
    df_hydraulicload: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    df_result_uplift: Optional[pd.DataFrame] | None = None
    df_result_heave: Optional[pd.DataFrame] | None = None
    df_result_sellmeijer: Optional[pd.DataFrame] | None = None
    df_result_combined: Optional[pd.DataFrame] | None = None

    def run(self, input: list[str], output: str, effect: float) -> None:
        """
        Runt de berekening van de fragility curve voor piping en shift deze.

        Parameters
        ----------
        input: list[str]
            Lijst namen van de input dataadapters: prob_input, hydraulicload
        output: str
            Naam van de dataadapter Fragility curve output
        effect: float
            De waarde waarmee de fragility curve wordt verschoven, eenheid is hetzelfde als je hydraulicload.

        Notes
        -----
        Zie de documentatie van probabilistic_piping.probabilistic_fixedwl.ProbPipingFixedWaterlevelSimple voor meer informatie.

        1. prob_input is afhankelijk van de probabilistische berekening die je wilt uitvoeren, zie externe documentatie.
        1. De hydraulicload data adapter geeft de waterlevel data door, deze moet de kolom hydraulicload bevatten met floats.

        """
        self.calculate_fragility_curve(input, output)
        self.shift(effect)
