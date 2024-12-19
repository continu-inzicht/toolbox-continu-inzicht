import pandas as pd
from toolbox_continu_inzicht import DataAdapter
from toolbox_continu_inzicht.fragility_curves import FragilityCurveOvertopping


class ShiftFragilityCurveOvertopping(FragilityCurveOvertopping):
    """Shifts the fragility curve with a given effect"""

    data_adapter: DataAdapter

    def run(self, input: list[str], output: str, effect: float) -> None:
        """
        Runt de berekening van de fragility curve voor golf overslag & shift de curve met een gegeven effect

        args:
            input list[str]:
                [0] df_slopes (pd.DataFrame): DataFrame met helling data. Moet de volgende kolommen bevatten:
                    - slopetypeid: int (1: dike or 2: slope)
                [1] df_profile (pd.DataFrame): DataFrame with profile data
                [2] df_bed_levels (pd.DataFrame): DataFrame with bed level data
            output str: fragility curve output
        """
        self.calculate_fragility_curve(input, output)
        self.shift(effect)


class ChangeCrestHeightFragilityCurveOvertopping(FragilityCurveOvertopping):
    """Shifts the fragility curve with a given effect"""

    data_adapter: DataAdapter

    def run(self, input: list[str], output: str, effect: float) -> None:
        """
        Runt de berekening van de fragility curve voor golf overslag & past de kruinhoogte aan met een gegeven effect

        args:
            input list[str]:
                [0] df_slopes (pd.DataFrame): DataFrame met helling data. Moet de volgende kolommen bevatten:
                    - slopetypeid: int (1: dike or 2: slope)
                [1] df_profile (pd.DataFrame): DataFrame with profile data
                [2] df_bed_levels (pd.DataFrame): DataFrame with bed level data
            output str: fragility curve output
        """
        df_profile = self.data_adapter.input(input[1])
        if "parameters" in df_profile:
            df_profile.set_index("parameters", inplace=True)
        for k in df_profile.index:
            try:
                df_profile.loc[k, "values"] = pd.to_numeric(df_profile.loc[k, "values"])
            except ValueError:
                # leave str
                pass
        df_profile.loc["crestlevel"] += effect
        self.data_adapter.set_dataframe_adapter(
            "changed_crest_profile", df_profile, if_not_exist="create"
        )
        self.calculate_fragility_curve(
            [input[0], "changed_crest_profile", input[2]], output
        )
