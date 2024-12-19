import pandas as pd
from pydantic.dataclasses import dataclass
from typing import Optional

# pydra_core=0.0.1
import pydra_core
import pydra_core.common
import pydra_core.common.enum
import pydra_core.location

from toolbox_continu_inzicht.fragility_curves.fragility_curve_overtopping.calculate_fragility_curve_overtopping import (
    WaveOvertoppingCalculation,
)
from toolbox_continu_inzicht import FragilityCurve, DataAdapter


@dataclass(config={"arbitrary_types_allowed": True})
class FragilityCurveOvertopping(FragilityCurve):
    """
    Maakt een fragility curve voor golf overslag

    Args:
        data_adapter (DataAdapter): DataAdapter object

    """

    data_adapter: DataAdapter
    df_slopes: Optional[pd.DataFrame] | None = None
    df_bed_levels: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    # def __init__(self):
    #     super().__init__()

    def run(self, input: list[str], output: str) -> None:
        """
        Runt de berekening van de fragility curve voor golf overslag

        args:
            input list[str]:
                [0] df_slopes (pd.DataFrame): DataFrame met helling data. Moet de volgende kolommen bevatten:
                    - slopetypeid: int (1: dike or 2: slope)
                [1] df_profile (pd.DataFrame): DataFrame with profile data
                [2] df_bed_levels (pd.DataFrame): DataFrame with bed level data
            output str: fragility curve output
        """
        self.calculate_fragility_curve(input, output)

    def calculate_fragility_curve(self, input: list[str], output: str) -> None:
        # haal input op
        self.df_slopes = self.data_adapter.input(input[0])
        self.df_profile = self.data_adapter.input(input[1])
        self.df_bed_levels = self.data_adapter.input(input[2])

        # nabewerkeing op profile
        if "parameters" in self.df_profile:
            self.df_profile.set_index("parameters", inplace=True)
        profile_series = self.df_profile["values"]
        for k in profile_series.index:
            try:
                profile_series.at[k] = pd.to_numeric(profile_series.at[k])
            except ValueError:
                pass

        global_variables = self.data_adapter.config.global_variables
        if "FragilityCurveOvertopping" in global_variables:
            # can be used to set options for the calculation
            options: dict = global_variables["FragilityCurveOvertopping"]

        # voor nu default waardes uit de opties
        windspeed = profile_series["windspeed"]
        sectormin = profile_series["sectormin"]
        sectorsize = profile_series["sectorsize"]

        if not all(
            [
                slopetype in [1, 2]
                for slopetype in self.df_slopes["slopetypeid"].unique()
            ]
        ):
            raise UserWarning("Slopes should have a slopetypeid of 1 or 2")

        # Formateer de data uit het dataframe voor pydra
        df_slope_dike = self.df_slopes[self.df_slopes["slopetypeid"] == 1]
        profiel_dict = {
            "profile_name": "profiel_CI",
            "dike_x_coordinates": df_slope_dike["x"].tolist(),
            "dike_y_coordinates": df_slope_dike["y"].tolist(),
            "dike_roughness": df_slope_dike["r"].tolist(),
            "dike_orientation": profile_series["orientation"],
            "dike_crest_level": profile_series["crestlevel"],
        }

        basis_profiel = pydra_core.Profile.from_dictionary(profiel_dict)

        # Voorland doen we nu even apart zodat het zelfde is als de originele versie van pydra
        foreland_profile = {}
        df_slope_foreland = self.df_slopes.loc[self.df_slopes["slopetypeid"] == 2]
        if len(df_slope_foreland) > 0:
            foreland_profile["foreland_x_coordinates"] = list(
                df_slope_foreland["x"].to_numpy()
            )
            foreland_profile["foreland_y_coordinates"] = list(
                df_slope_foreland["y"].to_numpy()
            )

        profiel_dict.update(foreland_profile)

        overtopping = pydra_core.Profile.from_dictionary(profiel_dict)

        if profile_series["dam"] != 0.0:
            breakwater_type = pydra_core.common.enum.Breakwater(
                int(profile_series["dam"])
            )
            overtopping.set_breakwater(
                breakwater_type=breakwater_type,
                breakwater_level=profile_series["damheight"],
            )

        # Calculate curve
        niveaus, ovkansqcr = WaveOvertoppingCalculation.calculate_overtopping_curve(
            windspeed,
            sectormin,
            sectorsize,
            overtopping,
            basis_profiel,
            qcr=profile_series["qcr"],
            richtingen=self.df_bed_levels["direction"],
            bodemhoogte=self.df_bed_levels["bedlevel"],
            strijklengte=self.df_bed_levels["fetch"],
            closing_situation=profile_series["closing_situation"],
            options=options,
        )

        self.df_out = pd.DataFrame(
            {"waterlevels": niveaus, "failure_probability": ovkansqcr}
        )

        self.data_adapter.output(output=output, df=self.df_out)
