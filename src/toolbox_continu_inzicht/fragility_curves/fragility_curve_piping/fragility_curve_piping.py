import pandas as pd
from pydantic.dataclasses import dataclass
from typing import Optional

from probabilistic_piping import (
    ProbInput,
    ProbPipingFixedWaterlevel,
    ProbPipingFixedWaterlevelSimple,
)
from toolbox_continu_inzicht import FragilityCurve, DataAdapter


@dataclass(config={"arbitrary_types_allowed": True})
class FragilityCurvePipingFixedWaterlevelCombined(FragilityCurve):
    """
    Maakt een fragility curve voor piping met een gegeven waterstand.
    De fragility curve wordt berekend met behulp van de probabilistic_piping package.
    Deze functie berekent fragility curves voor uplift, heave, Sellmeijer, en de gecombineerde mechanismes.
    De gecombineerde fragility curve is de standaard output, de andere kunnen worden opgevraagd met de df_result_uplift, df_result_heave, en df_result_sellmeijer attributen.

    Args:
        data_adapter (DataAdapter): DataAdapter object

    """

    data_adapter: DataAdapter
    df_prob_input: Optional[pd.DataFrame] | None = None
    df_waterlevels: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    df_result_uplift: Optional[pd.DataFrame] | None = None
    df_result_heave: Optional[pd.DataFrame] | None = None
    df_result_sellmeijer: Optional[pd.DataFrame] | None = None
    df_result_combined: Optional[pd.DataFrame] | None = None

    # TODO: add, first think about what to do with ids
    # input_prob_input = {
    #  ...
    # }

    # input_waterlevels = {
    #  ...
    # }

    def run(self, input: list[str], output: str) -> None:
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

    def calculate_fragility_curve(self, input: list[str], output: str) -> None:
        """
        Bereken de fragiliteitscurve op basis van de opgegeven input en sla het resultaat op in het opgegeven outputbestand.
        """
        self.df_prob_input = self.data_adapter.input(input[0])
        self.df_waterlevels = self.data_adapter.input(input[1])

        global_variables = self.data_adapter.config.global_variables
        progress: bool = False
        debug: bool = False
        if "FragilityCurvePipingFixedWaterlevelCombined" in global_variables:
            # can be used to set options for the calculation
            options: dict = global_variables[
                "FragilityCurvePipingFixedWaterlevelCombined"
            ]

            if "progress" in options:
                progress: bool = options["progress"]

            if "debug" in options:
                debug: bool = options["debug"]

        prob_input = ProbInput().from_dataframe(self.df_prob_input)
        prob_piping_fixed_waterlevel_simple = ProbPipingFixedWaterlevelSimple(
            progress=progress,
            debug=debug,
        )
        settings, result_uplift, result_heave, result_Sellmeijer, result_combined = (
            prob_piping_fixed_waterlevel_simple.fixed_waterlevel_fragilitycurve(
                prob_input=prob_input,
                hlist=self.df_waterlevels["waterlevels"].to_numpy(),
            )
        )

        # turn results into dataframes for each mechanism
        df_names = [
            "df_result_uplift",
            "df_result_heave",
            "df_result_sellmeijer",
            "df_result_combined",
        ]
        for name, result in zip(
            df_names, [result_uplift, result_heave, result_Sellmeijer, result_combined]
        ):
            self.__setattr__(
                name,
                pd.DataFrame(
                    data=[(res.h, res.prob_cond) for res in result.results],
                    columns=["waterlevel", "failure_probability"],
                ),
            )
        self.df_out = self.df_result_combined
        self.data_adapter.output(output, self.df_out)


@dataclass(config={"arbitrary_types_allowed": True})
class FragilityCurvePipingFixedWaterlevel(FragilityCurve):
    """
    Maakt een fragility curve voor piping met een gegeven waterstand.
    De fragility curve wordt berekend met behulp van de probabilistic_piping package.
    Deze functie berekent fragility curves voor één mechanisme, standaard is dit sellmeijer.
    Het mechanisme kan worden aangepast met de z_type parameter in de config.
    Dit kan zijn: 'sellmeijer', 'heave', 'uplift' of 'combi'.

    Args:
        data_adapter (DataAdapter): DataAdapter object

    """

    data_adapter: DataAdapter
    df_prob_input: Optional[pd.DataFrame] | None = None
    df_waterlevels: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    # TODO: add, first think about what to do with ids
    # input_prob_input = {
    #  ...
    # }

    # input_waterlevels = {
    #  ...
    # }

    def run(self, input: list[str], output: str) -> None:
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

    def calculate_fragility_curve(self, input: list[str], output: str) -> None:
        """
        Bereken de fragiliteitscurve op basis van de opgegeven input en sla het resultaat op in het opgegeven outputbestand.
        """
        self.df_prob_input = self.data_adapter.input(input[0])
        self.df_waterlevels = self.data_adapter.input(input[1])

        global_variables = self.data_adapter.config.global_variables
        z_type = "sellmeijer"  # by default
        progress: bool = False
        debug: bool = False
        if "FragilityCurvePipingFixedWaterlevel" in global_variables:
            # can be used to set options for the calculation
            options: dict = global_variables["FragilityCurvePipingFixedWaterlevel"]

            if "z_type" in options:
                z_type = options["z_type"]

            if "progress" in options:
                progress = options["progress"]

            if "debug" in options:
                debug = options["debug"]

        prob_input = ProbInput().from_dataframe(self.df_prob_input)
        prob_piping_fixed_waterlevel = ProbPipingFixedWaterlevel(
            progress=progress,
            debug=debug,
        )

        settings, result = prob_piping_fixed_waterlevel.fixed_waterlevel_fragilitycurve(
            prob_input=prob_input,
            hlist=self.df_waterlevels["waterlevels"].to_numpy(),
            z_type=z_type,
        )

        self.df_out = pd.DataFrame(
            data=[(res.h, res.prob_cond) for res in result.results],
            columns=["waterlevel", "failure_probability"],
        )
        self.data_adapter.output(output, self.df_out)
