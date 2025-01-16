import pandas as pd
from toolbox_continu_inzicht import DataAdapter
from toolbox_continu_inzicht.fragility_curves import FragilityCurveOvertopping


class ShiftFragilityCurveOvertopping(FragilityCurveOvertopping):
    """Verschuift de fragility curve met een gegeven effect"""

    data_adapter: DataAdapter

    def run(self, input: list[str], output: str, effect: float) -> None:
        """
        Runt de berekening van de fragility curve voor golf overslag & shift de curve met een gegeven effect

        Parameters
        ----------
        input: list[str]
               [0] df_slopes (pd.DataFrame),
               [1] df_profile (pd.DataFrame),
               [2] df_bed_levels (pd.DataFrame)
        output: str
            Fragility curve output
        effect: float
            Verschuiving van de fragility curve

        Notes:
        ------
        input: list[str]

               [0] df_slopes (pd.DataFrame)

                    DataFrame met helling data.
                    Moet de volgende kolommen bevatten:
                    - x : float
                    - y : float
                    - r : float
                    - slopetypeid : int (1: dike or 2: slope)

               [1] df_profile (pd.DataFrame):
                    DataFrame met profiel data.
                    Moet de volgende kolommen bevatten:
                    - windspeed : float
                    - sectormin : float
                    - sectorsize : float
                    - orientation : float (in graden)
                    - crestlevel : float (in meters)
                    - dam : int (0: geen dam or 1: dam)
                    - damheight : float (in meters)
                    - qcr : float (waarde in m^3/s)
                        str (close | open)
                        tuple (waarden van mu en sigma)

               [2] df_bed_levels (pd.DataFrame):
                    DataFrame met bed level data.
                    Moet de volgende kolommen bevatten:
                    - direction : float
                    - bedlevel : float
                    - fetch : float
        """
        self.calculate_fragility_curve(input, output)
        self.shift(effect)


class ChangeCrestHeightFragilityCurveOvertopping(FragilityCurveOvertopping):
    """Verschuift de kruin hoogte met het gegeven effect en berekent de fragility curve"""

    data_adapter: DataAdapter

    def run(self, input: list[str], output: str, effect: float) -> None:
        """
        Runt de berekening van de fragility curve voor golf overslag & past de kruinhoogte aan met een gegeven effect

        Parameters
        ----------
        input: list[str]
               [0] df_slopes (pd.DataFrame),
               [1] df_profile (pd.DataFrame),
               [2] df_bed_levels (pd.DataFrame)
        output: str
            Fragility curve output
        effect: float
            Verschuiving van de kruinhoogte (in meters)

        Notes:
        ------
        input: list[str]

               [0] df_slopes (pd.DataFrame)

                    DataFrame met helling data.
                    Moet de volgende kolommen bevatten:
                    - x : float
                    - y : float
                    - r : float
                    - slopetypeid : int (1: dike or 2: slope)

               [1] df_profile (pd.DataFrame):
                    DataFrame met profiel data.
                    Moet de volgende kolommen bevatten:
                    - windspeed : float
                    - sectormin : float
                    - sectorsize : float
                    - orientation : float (in graden)
                    - crestlevel : float (in meters)
                    - dam : int (0: geen dam or 1: dam)
                    - damheight : float (in meters)
                    - qcr : float (waarde in m^3/s)
                        str (close | open)
                        tuple (waarden van mu en sigma)

               [2] df_bed_levels (pd.DataFrame):
                    DataFrame met bed level data.
                    Moet de volgende kolommen bevatten:
                    - direction : float
                    - bedlevel : float
                    - fetch : float
        """
        df_profile = self.data_adapter.input(input[1])
        if "parameters" in df_profile:
            df_profile.set_index("parameters", inplace=True)
        for k in df_profile.index:
            try:
                df_profile.loc[k, "values"] = pd.to_numeric(df_profile.loc[k, "values"])
            except ValueError:
                # If values are not numeric, pass as we want to keep them strings
                pass
        df_profile.loc["crestlevel"] += effect
        self.data_adapter.set_dataframe_adapter(
            "changed_crest_profile", df_profile, if_not_exist="create"
        )
        self.calculate_fragility_curve(
            [input[0], "changed_crest_profile", input[2]], output
        )
