import pandas as pd
from pydantic.dataclasses import dataclass
from typing import Optional
from pathlib import Path

# pydra_core=0.0.1
import pydra_core
import pydra_core.common
import pydra_core.common.enum
import pydra_core.location

from toolbox_continu_inzicht.fragility_curves.fragility_curve_overtopping.calculate_fragility_curve_overtopping import (
    WaveOvertoppingCalculation,
)
from toolbox_continu_inzicht import FragilityCurve, DataAdapter, Config


@dataclass(config={"arbitrary_types_allowed": True})
class FragilityCurveOvertopping(FragilityCurve):
    """
    Maakt één fragility curve voor golf overslag.

    Args:
        data_adapter (DataAdapter): DataAdapter object


    Options in config
    ------------------
    Onzekerheden: float
        gh_onz_mu
            GolfHoogte onzekerheid mu: gemiddelde waarde van de onzekerheid van de golfhoogte (standaard 0.96)
        gh_onz_sigma
            GolfHoogte onzekerheid sigma: standaard afwijking waarde (standaard 0.27)
        gp_onz_mu_tp
            GolfPerioden onzekerheid mu: gemiddelde waarde van de onzekerheid van de golfperiode (standaard 1.03)
        gp_onz_sigma_tp
            GolfPerioden onzekerheid sigma: standaard afwijking waarde (standaard 0.13)
        gp_onz_mu_tspec
            GolfPerioden onzekerheid mu: gemiddelde waarde van de onzekerheid van de golfperiode (standaard 1.03)
        gp_onz_sigma_tspec
            GolfPerioden onzekerheid sigma: standaard afwijking waarde (standaard 0.13)
        gh_onz_aantal
            Aantal onzekerheden in de golfhoogte (standaard 7)
        gp_onz_aantal
            Aantal onzekerheden in de golfperiode (standaard 7)

    tp_tspec: float
        de verhouding tussen de piek periode van de golf (`$T_p$`) en de spectrale golfperiode (`$Tm_{-1,0}$`) (standaard 1.1).

    De waterniveaus waarmee probablistisch gerekend wordt is verdeelt in twee delen: grof en fijn.

    lower_limit_coarse: float
        De ondergrens van de waterstanden waarvoor de fragiliteitscurve wordt berekend in grove stappen (standaard 4.0m onder de kruin)
    upper_limit_coarse: float
        De bovengrens van de waterstanden waarvoor de fragiliteitscurve wordt berekend in grove stappen (standaard 2.0m onder de kruin).
        Er is geen lower_limit_fine omdat deze altijd gelijk is aan upper_limit_coarse.
    upper_limit_fine: float
        De bovengrens van de waterstanden waarvoor de fragiliteitscurve wordt berekend in fijne stappen (standaard 1.01m boven de kruin)
    hstap: float
        De fijne stapgrootte van de waterstanden waarvoor de fragiliteitscurve wordt berekend (standaard 0.05), de grove stapgrootte is 2 * hstap.

    """

    data_adapter: DataAdapter
    df_slopes: Optional[pd.DataFrame] | None = None
    df_bed_levels: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    def run(self, input: list[str], output: str) -> None:
        """
        Runt de berekening van de fragility curve voor golf overslag

        Parameters
        ----------
        input: list[str]
               [0] df_slopes (pd.DataFrame),
               [1] df_profile (pd.DataFrame),
               [2] df_bed_levels (pd.DataFrame)
        output: str
            Fragility curve output

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

    def calculate_fragility_curve(self, input: list[str], output: str) -> None:
        """
        Bereken de fragiliteitscurve op basis van de opgegeven input en sla het resultaat op in het opgegeven outputbestand.
        Parameters:
            input (list[str]): Een lijst met de bestandsnamen van de inputbestanden.
            output (str): De bestandsnaam waarin het resultaat moet worden opgeslagen.
        Returns:
            None
        """
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


@dataclass(config={"arbitrary_types_allowed": True})
class FragilityCurvesOvertopping(FragilityCurve):
    """
    Maakt een set van fragility curve voor golf overslag voor een dijkvak.

    Args:
        data_adapter (DataAdapter): DataAdapter object

    Options in config
    ------------------
    Onzekerheden: float
        gh_onz_mu
            GolfHoogte onzekerheid mu: gemiddelde waarde van de onzekerheid van de golfhoogte (standaard 0.96)
        gh_onz_sigma
            GolfHoogte onzekerheid sigma: standaard afwijking waarde (standaard 0.27)
        gp_onz_mu_tp
            GolfPerioden onzekerheid mu: gemiddelde waarde van de onzekerheid van de golfperiode (standaard 1.03)
        gp_onz_sigma_tp
            GolfPerioden onzekerheid sigma: standaard afwijking waarde (standaard 0.13)
        gp_onz_mu_tspec
            GolfPerioden onzekerheid mu: gemiddelde waarde van de onzekerheid van de golfperiode (standaard 1.03)
        gp_onz_sigma_tspec
            GolfPerioden onzekerheid sigma: standaard afwijking waarde (standaard 0.13)
        gh_onz_aantal
            Aantal onzekerheden in de golfhoogte (standaard 7)
        gp_onz_aantal
            Aantal onzekerheden in de golfperiode (standaard 7)

    tp_tspec: float
        de verhouding tussen de piek periode van de golf (`$T_p$`) en de spectrale golfperiode (`$Tm_{-1,0}$`) (standaard 1.1).

    De waterniveaus waarmee probablistisch gerekend wordt is verdeelt in twee delen: grof en fijn.

    lower_limit_coarse: float
        De ondergrens van de waterstanden waarvoor de fragiliteitscurve wordt berekend in grove stappen (standaard 4.0m onder de kruin)
    upper_limit_coarse: float
        De bovengrens van de waterstanden waarvoor de fragiliteitscurve wordt berekend in grove stappen (standaard 2.0m onder de kruin).
        Er is geen lower_limit_fine omdat deze altijd gelijk is aan upper_limit_coarse.
    upper_limit_fine: float
        De bovengrens van de waterstanden waarvoor de fragiliteitscurve wordt berekend in fijne stappen (standaard 1.01m boven de kruin)
    hstap: float
        De fijne stapgrootte van de waterstanden waarvoor de fragiliteitscurve wordt berekend (standaard 0.05), de grove stapgrootte is 2 * hstap.

    """

    data_adapter: DataAdapter
    df_slopes: Optional[pd.DataFrame] | None = None
    df_bed_levels: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    fragility_curve_function: FragilityCurve = FragilityCurveOvertopping
    effect: None | float = None

    # TODO: add, first think about what to do with ids
    # input_schema_slopes = {
    #     "slopetypeid": "int64",
    #     "x": "float64",
    #     "y": "float64",
    #     "r": "float64",
    # }

    # input_schema_bed_levels = {
    #     "direction": "float64",
    #     "bedlevel": "float64",
    #     "fetch": "float64",
    # }

    def run(self, input: list[str], output: str) -> None:
        """
        Runt de berekening van de fragility curve voor golf overslag

        Parameters
        ----------
        input: list[str]
               [0] df_slopes (pd.DataFrame),
               [1] df_profile (pd.DataFrame),
               [2] df_bed_levels (pd.DataFrame)
        output: str
            Fragility curve output

        Notes:
        ------
        input: list[str]

               [0] df_slopes (pd.DataFrame)

                    DataFrame met helling data.
                    Moet de volgende kolommen bevatten:
                    - section_id : int
                    - x : float
                    - y : float
                    - r : float
                    - slopetypeid : int (1: dike or 2: slope)

               [1] df_profile (pd.DataFrame):
                    DataFrame met profiel data.
                    Moet de volgende kolommen bevatten:
                    - section_id : int
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
                    - section_id : int
                    - direction : float
                    - bedlevel : float
                    - fetch : float

        """
        self.calculate_fragility_curve(input, output)

    def calculate_fragility_curve(self, input: list[str], output: str) -> None:
        """
        Bereken de fragiliteitscurve op basis van de opgegeven input en sla het resultaat op in het opgegeven outputbestand.
        Parameters:
            input (list[str]): Een lijst met de bestandsnamen van de inputbestanden.
            output (str): De bestandsnaam waarin het resultaat moet worden opgeslagen.
        Returns:
            None
        """
        # haal input op
        self.df_slopes = self.data_adapter.input(input[0])
        self.df_profile = self.data_adapter.input(input[1])
        self.df_bed_levels = self.data_adapter.input(input[2])

        section_ids = self.df_profile.section_id.unique()

        global_variables = self.data_adapter.config.global_variables
        if "FragilityCurvesOvertopping" in global_variables:
            # can be used to set options for the calculation
            options: dict = global_variables["FragilityCurvesOvertopping"]

        self.df_out: pd.DataFrame = pd.DataFrame(
            columns=["section_id", "waterlevels", "failure_probability"]
        )
        for section_id in section_ids:
            df_slopes = self.df_slopes[self.df_slopes["section_id"] == section_id]
            df_profile = self.df_profile[self.df_profile["section_id"] == section_id]
            df_bed_levels = self.df_bed_levels[
                self.df_bed_levels["section_id"] == section_id
            ]

            temp_config = Config(config_path=Path.cwd())
            temp_data_adapter = DataAdapter(config=temp_config)
            temp_data_adapter.config.global_variables["FragilityCurveOvertopping"] = (
                options
            )

            temp_data_adapter.set_dataframe_adapter(
                "df_slopes", df_slopes, if_not_exist="create"
            )
            df_profile = df_profile.iloc[0].T
            df_profile = df_profile.to_frame().rename(
                columns={df_profile.name: "values"}
            )
            temp_data_adapter.set_dataframe_adapter(
                "df_profile", df_profile, if_not_exist="create"
            )
            temp_data_adapter.set_dataframe_adapter(
                "df_bed_levels", df_bed_levels, if_not_exist="create"
            )
            temp_data_adapter.set_dataframe_adapter(
                "output", pd.DataFrame(), if_not_exist="create"
            )
            fragility_curve_overtopping = self.fragility_curve_function(
                data_adapter=temp_data_adapter
            )
            # dit zorgt er voor dat beheerdersoordeel ook mee kan worden genomen
            if self.effect is not None:
                fragility_curve_overtopping.run(
                    input=["df_slopes", "df_profile", "df_bed_levels"],
                    output="output",
                    effect=self.effect,
                )
            else:
                fragility_curve_overtopping.run(
                    input=["df_slopes", "df_profile", "df_bed_levels"], output="output"
                )

            df_fco = fragility_curve_overtopping.df_out
            df_fco["section_id"] = section_id
            if len(self.df_out) == 0:
                self.df_out = df_fco
            else:
                self.df_out = pd.concat([self.df_out, df_fco])

        self.data_adapter.output(output=output, df=self.df_out)
