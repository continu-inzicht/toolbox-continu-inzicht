from pathlib import Path
from typing import Optional

import pandas as pd

# pydra_core=0.0.1
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht import ToolboxBase, Config, DataAdapter, FragilityCurve
from toolbox_continu_inzicht.fragility_curves.fragility_curve_overtopping.calculate_fragility_curve_overtopping import (
    WaveOvertoppingCalculation,
)
from toolbox_continu_inzicht.fragility_curves.fragility_curve_overtopping.overtopping_utils import (
    build_pydra_profiles,
    get_overtopping_options,
    parse_profile_dataframe,
    validate_slopes,
)


@dataclass(config={"arbitrary_types_allowed": True})
class FragilityCurveOvertopping(FragilityCurve):
    """
    Maakt een enkele fragility curve voor golfoverslag.
    Attributes
    ----------
    data_adapter: DataAdapter
        DataAdapter object
    df_slopes: Optional[pd.DataFrame] | None
        DataFrame met helling data.
    df_profile: Optional[pd.DataFrame] | None
        DataFrame met profiel data.
    df_bed_levels: Optional[pd.DataFrame] | None
        DataFrame met bed level data.
    df_out: Optional[pd.DataFrame] | None
        DataFrame met de resultaten van de berekening.

    Notes
    -----
    Via de configuratie kunnen de volgende opties worden ingesteld, deze zijn float tenzij anders aangegeven.
    Onzekerheden:

    1. gh_onz_mu, GolfHoogte onzekerheid mu: gemiddelde waarde van de onzekerheid van de golfhoogte (standaard 0.96)
    1. gh_onz_sigma, GolfHoogte onzekerheid sigma: standaardafwijking waarde (standaard 0.27)
    1. gp_onz_mu_tp, GolfPerioden onzekerheid mu: gemiddelde waarde van de onzekerheid van de golfperiode (standaard 1.03)
    1. gp_onz_sigma_tp, GolfPerioden onzekerheid sigma: standaardafwijking waarde (standaard 0.13)
    1. gp_onz_mu_tspec, GolfPerioden onzekerheid mu: gemiddelde waarde van de onzekerheid van de golfperiode (standaard 1.03)
    1. gp_onz_sigma_tspec, GolfPerioden onzekerheid sigma: standaardafwijking waarde (standaard 0.13)
    1. gh_onz_aantal, Aantal onzekerheden in de golfhoogte (standaard 7)
    1. gp_onz_aantal, Aantal onzekerheden in de golfperiode (standaard 7)

    tp_tspec, de verhouding tussen de piekperiode van de golf (`$T_p$`) en de spectrale golfperiode (`$Tm_{-1,0}$`) (standaard 1.1).

    De waterniveaus waarmee probabilistisch gerekend wordt, is verdeeld in twee delen: grof en fijn.

    1. lower_limit_coarse, De ondergrens van de waterstanden waarvoor de fragility curve wordt berekend in grove stappen (standaard 4.0m onder de kruin)
    1. upper_limit_coarse, De bovengrens van de waterstanden waarvoor de fragility curve wordt berekend in grove stappen (standaard 2.0m onder de kruin). Er is geen lower_limit_fine omdat deze altijd gelijk is aan upper_limit_coarse.
    1. upper_limit_fine, De bovengrens van de waterstanden waarvoor de fragility curve wordt berekend in fijne stappen (standaard 1.01m boven de kruin)
    1. hstap, De fijne stapgrootte van de waterstanden waarvoor de fragility curve wordt berekend (standaard 0.05), de grove stapgrootte is 2 * hstap.

    """

    data_adapter: DataAdapter
    df_slopes: Optional[pd.DataFrame] | None = None
    df_profile: Optional[pd.DataFrame] | None = None
    df_bed_levels: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    def run(self, input: list[str], output: str) -> None:
        """
        Runt de berekening van de fragility curve voor golfoverslag

        Parameters
        ----------
        input: list[str]
            Lijst namen van de input DataAdapters: slopes, profile en bed_levels
        output: str
            Naam van de DataAdapter Fragility curve output

        Notes
        -----
        De inputvolgorde is vrij specifiek, vandaar de extra details.
        Als er geen type is opgegeven, wordt standaard het type float gebruikt.

        De eerste (slopes) DataAdapter met hellingsdata moet de volgende kolommen bevatten:

        1. x, x-coördinaat
        1. y, y-coördinaat
        1. r, roughness
        1. slopetypeid, id de helling type (int, 1: dike or 2: slope)

        De tweede (profile) DataAdapter met profieldata moet de volgende kolommen bevatten:

        1. windspeed, windsnelheid
        1. sectormin, de minimale sectorhoek.
        1. sectorsize, de grootte van de sectorhoek.
        1. orientation, oriëntatie van het profiel in graden
        1. crestlevel, kruinhoogte in meters
        1. dam, wel of geen dam (int, 0: geen dam or 1: dam)
        1. damheight, dam hoogte in meters
        1. qcr, mag een van 3 zijn: een waarde in m^3/s (float), open of niet (str: close | open) of de waarden van mu en sigma (tuple).

        De derde (Bedlevelfetch) DataAdapter met bodemdata moet de volgende kolommen bevatten:

        1. direction, windrichtingen
        1. bedlevel, bodemprofielen
        1. fetch, lengte van fetch in meters
        """
        self.calculate_fragility_curve(input, output)

    def calculate_fragility_curve(self, input: list[str], output: str) -> None:
        """
        Bereken de fragility curve op basis van de opgegeven input en sla het resultaat op in het opgegeven outputbestand.

        Parameters
        ----------
        input: list[str]
            Lijst namen van de input DataAdapters: slopes, profile en bed_levels
        output: str
            Naam van de DataAdapter Fragility curve output

        """
        # haal input op
        self.df_slopes = self.data_adapter.input(input[0])
        self.df_profile = self.data_adapter.input(input[1])
        self.df_bed_levels = self.data_adapter.input(input[2])

        profile_series = parse_profile_dataframe(self.df_profile)

        global_variables = self.data_adapter.config.global_variables
        options = get_overtopping_options(global_variables, "FragilityCurveOvertopping")

        windspeed = profile_series["windspeed"]
        sectormin = profile_series["sectormin"]
        sectorsize = profile_series["sectorsize"]

        validate_slopes(self.df_slopes)

        basis_profiel, overtopping = build_pydra_profiles(
            self.df_slopes, profile_series
        )

        # Bereken curve
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

        self.hydraulicload = niveaus
        self.failure_probability = ovkansqcr

        self.data_adapter.output(output=output, df=self.as_dataframe())


@dataclass(config={"arbitrary_types_allowed": True})
class FragilityCurveOvertoppingMultiple(ToolboxBase):
    """
    Maakt een set van fragility curves voor golfoverslag voor een dijkvak.

    Attributes
    ----------
    data_adapter: DataAdapter
        DataAdapter object
    df_slopes: Optional[pd.DataFrame] | None
        DataFrame met hellingsdata.
    df_bed_levels: Optional[pd.DataFrame] | None
        DataFrame met bed level data.
    df_out: Optional[pd.DataFrame] | None
        DataFrame met de resultaten van de berekening.
    fragility_curve_function: FragilityCurve
        FragilityCurve object
    effect: float | None
        Effect van de maatregel (niet gebruikt)
    measure_id: int | None
        Maatregel id (niet gebruikt)

    Notes
    -----
    Via de configuratie kunnen de volgende opties worden ingesteld, deze zijn float tenzij anders aangegeven.
    Onzekerheden:

    1. gh_onz_mu, GolfHoogte onzekerheid mu: gemiddelde waarde van de onzekerheid van de golfhoogte (standaard 0.96)
    1. gh_onz_sigma, GolfHoogte onzekerheid sigma: standaardafwijking waarde (standaard 0.27)
    1. gp_onz_mu_tp, GolfPerioden onzekerheid mu: gemiddelde waarde van de onzekerheid van de golfperiode (standaard 1.03)
    1. gp_onz_sigma_tp, GolfPerioden onzekerheid sigma: standaardafwijking waarde (standaard 0.13)
    1. gp_onz_mu_tspec, GolfPerioden onzekerheid mu: gemiddelde waarde van de onzekerheid van de golfperiode (standaard 1.03)
    1. gp_onz_sigma_tspec, GolfPerioden onzekerheid sigma: standaardafwijking waarde (standaard 0.13)
    1. gh_onz_aantal, Aantal onzekerheden in de golfhoogte (standaard 7)
    1. gp_onz_aantal, Aantal onzekerheden in de golfperiode (standaard 7)

    tp_tspec, de verhouding tussen de piekperiode van de golf (`$T_p$`) en de spectrale golfperiode (`$Tm_{-1,0}$`) (standaard 1.1).

    De waterniveaus waarmee probablistisch gerekend wordt. Dit is verdeeld in twee delen: grof en fijn.

    1. lower_limit_coarse, De ondergrens van de waterstanden waarvoor de fragility curve wordt berekend in grove stappen (standaard 4.0m onder de kruin)
    1. upper_limit_coarse, De bovengrens van de waterstanden waarvoor de fragility curve wordt berekend in grove stappen (standaard 2.0m onder de kruin). Er is geen lower_limit_fine omdat deze altijd gelijk is aan upper_limit_coarse.
    1. upper_limit_fine, De bovengrens van de waterstanden waarvoor de fragility curve wordt berekend in fijne stappen (standaard 1.01m boven de kruin)
    1. hstap, De fijne stapgrootte van de waterstanden waarvoor de fragility curve wordt berekend (standaard 0.05), de grove stapgrootte is 2 * hstap.

    """

    # df_slopes, df_bed_levels, df_out, lower_limit, effect, measure_id
    data_adapter: DataAdapter
    df_slopes: Optional[pd.DataFrame] | None = None
    df_bed_levels: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    fragility_curve_function: FragilityCurve = FragilityCurveOvertopping
    effect: float | None = None
    measure_id: int | None = None

    def run(self, input: list[str], output: str) -> None:
        """
        Runt de berekening van de fragility curve voor golfoverslag

        Parameters
        ----------
        input: list[str]
            Lijst namen van de input DataAdapters: slopes, profile en bed_levels
        output: str
        """
        self.calculate_fragility_curve(input, output)

    def calculate_fragility_curve(self, input: list[str], output: str) -> None:
        """
        Bereken de fragility curve op basis van de opgegeven input en sla het resultaat op in het opgegeven outputbestand.

        Parameters
        ----------
        input: list[str]
            Lijst namen van de input DataAdapters: slopes, profile en bed_levels
        output: str
            Naam van de DataAdapter Fragility curve output
        """
        # haal input op
        self.df_slopes = self.data_adapter.input(input[0])
        self.df_profile = self.data_adapter.input(input[1])
        self.df_bed_levels = self.data_adapter.input(input[2])

        section_ids = self.df_profile.section_id.unique()

        global_variables = self.data_adapter.config.global_variables
        options = global_variables.get("FragilityCurveOvertoppingMultiple", {})

        self.df_out: pd.DataFrame = pd.DataFrame(
            columns=["section_id", "hydraulicload", "failure_probability"]
        )
        for section_id in section_ids:
            df_slopes = self.df_slopes[self.df_slopes["section_id"] == section_id]
            df_profile = self.df_profile[self.df_profile["section_id"] == section_id]
            df_bed_levels = self.df_bed_levels[
                self.df_bed_levels["section_id"] == section_id
            ]

            # maak een placeholder DataAdapter aan, dit zorgt dat je de modules ook los kan aanroepen

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
            # dit zorgt ervoor dat het beheerdersoordeel ook mee kan worden genomen
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

            df_fragility_curve_overtopping = fragility_curve_overtopping.as_dataframe()
            df_fragility_curve_overtopping["section_id"] = section_id

            if len(self.df_out) == 0:
                self.df_out = df_fragility_curve_overtopping
            else:
                self.df_out = pd.concat([self.df_out, df_fragility_curve_overtopping])

        self.df_out["failuremechanismid"] = 2  # GEKB: komt uit de
        if self.measure_id is not None:
            self.df_out["measureid"] = self.measure_id
        self.data_adapter.output(output=output, df=self.df_out)
