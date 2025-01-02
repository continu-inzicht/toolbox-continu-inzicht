from toolbox_continu_inzicht import DataAdapter
from toolbox_continu_inzicht.fragility_curves import (
    FragilityCurvePipingFixedWaterlevel,
    FragilityCurvePipingFixedWaterlevelCombined,
)


class ShiftFragilityCurvePipingFixedWaterlevel(FragilityCurvePipingFixedWaterlevel):
    """Verschuift de fragility curve voor piping met een constante water niveau met een gegeven effect"""

    data_adapter: DataAdapter

    def run(self, input: list[str], output: str, effect: float) -> None:
        """
        Runt de berekening van de fragility curve voor piping

        Parameters
        ----------
        input: list[str]
               [0] df_slopes (pd.DataFrame),
               [1] df_waterlevels (pd.DataFrame),

        output: str
            Fragility curve (pd.DataFrame)

        Notes:
        ------
        input: list[str]

               [0] df_prob_input (pd.DataFrame)

                    DataFrame met data voor de probabilistische berekening.
                    De benodigen kolommen zijn afhankelijk van de probabilitische berekening.
                    Zie de documentatie van probabilistic_piping.probabilistic_fixedwl.ProbPipingFixedWaterlevelSimple voor meer informatie.


               [1] df_waterlevels (pd.DataFrame):
                    DataFrame met waterlevel data.
                    Moet de volgende kolommen bevatten:
                    - waterlevels : float

        """
        self.calculate_fragility_curve(input, output)
        self.shift(effect)


class ShiftFragilityCurvePipingFixedWaterlevelSimple(
    FragilityCurvePipingFixedWaterlevelCombined
):
    """Verschuift de fragility curve voor piping met een constante water niveau (simple) met een gegeven effect"""

    data_adapter: DataAdapter

    def run(self, input: list[str], output: str, effect: float) -> None:
        """
        Runt de berekening van de fragility curve voor piping

        Parameters
        ----------
        input: list[str]
               [0] df_slopes (pd.DataFrame),
               [1] df_waterlevels (pd.DataFrame),

        output: str
            Fragility curve (pd.DataFrame)

        Notes:
        ------
        input: list[str]

               [0] df_prob_input (pd.DataFrame)

                    DataFrame met data voor de probabilistische berekening.
                    De benodigen kolommen zijn afhankelijk van de probabilitische berekening.
                    Zie de documentatie van probabilistic_piping.probabilistic_fixedwl.ProbPipingFixedWaterlevelSimple voor meer informatie.


               [1] df_waterlevels (pd.DataFrame):
                    DataFrame met waterlevel data.
                    Moet de volgende kolommen bevatten:
                    - waterlevels : float

        """
        self.calculate_fragility_curve(input, output)
        self.shift(effect)
