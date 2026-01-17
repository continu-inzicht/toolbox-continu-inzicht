from typing import ClassVar, Optional

import pandas as pd

# pydra_core=0.0.1
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht import DataAdapter, FragilityCurve, ToolboxBase
from toolbox_continu_inzicht.fragility_curves.fragility_curve_overtopping.fragility_curve_overtopping_base import (
    FragilityCurveOvertoppingBase,
)
from toolbox_continu_inzicht.fragility_curves.fragility_curve_overtopping.wave_provider import (
    BretschneiderWaveProvider,
)


@dataclass(config={"arbitrary_types_allowed": True})
class FragilityCurveOvertoppingBedlevelFetch(FragilityCurveOvertoppingBase):
    """
    Maakt een enkele fragility curve voor golfoverslag.
    Attributes
    ----------
    data_adapter: DataAdapter
        DataAdapter object
    df_bed_levels: Optional[pd.DataFrame] | None
        DataFrame met bed level data.
    options_key: ClassVar[str]
        Config key voor overtopping opties.

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
    df_bed_levels: Optional[pd.DataFrame] | None = None
    options_key: ClassVar[str] = "FragilityCurveOvertoppingBedlevelFetch"

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

    def _load_inputs(self, input: list[str]) -> None:
        self.df_slopes = self.data_adapter.input(input[0])
        self.df_profile = self.data_adapter.input(input[1])
        self.df_bed_levels = self.data_adapter.input(input[2])

    def _build_wave_provider(self, options: dict) -> BretschneiderWaveProvider:
        return BretschneiderWaveProvider(
            bedlevel=self.df_bed_levels["bedlevel"],
            fetch=self.df_bed_levels["fetch"],
            windrichtingen=self.df_bed_levels["direction"],
            tp_tspec=options.get("tp_tspec", 1.1),
        )


@dataclass(config={"arbitrary_types_allowed": True})
class FragilityCurveOvertoppingBedlevelFetchMultiple(ToolboxBase):
    """
    Maakt een set van fragility curves voor golfoverslag voor een dijkvak.

    Attributes
    ----------
    data_adapter: DataAdapter
        DataAdapter object
    df_slopes: Optional[pd.DataFrame] | None
        DataFrame met hellingsdata.
    df_profile: Optional[pd.DataFrame] | None
        DataFrame met profiel data.
    df_bed_levels: Optional[pd.DataFrame] | None
        DataFrame met bed level data.
    df_out: Optional[pd.DataFrame] | None
        DataFrame met de resultaten van de berekening.
    fc_function: FragilityCurve
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
    df_profile: Optional[pd.DataFrame] | None = None
    df_bed_levels: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    fc_function: FragilityCurve = FragilityCurveOvertoppingBedlevelFetch
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
        options = global_variables.get(
            "FragilityCurveOvertoppingBedlevelFetchMultiple", {}
        )

        temp_data_adapter = self.data_adapter
        temp_data_adapter.set_dataframe_adapter(
            "output", pd.DataFrame(), if_not_exist="create"
        )

        df_out = [
            self._calculate_section(
                section_id=section_id,
                input=input,
                options=options,
                temp_data_adapter=temp_data_adapter,
            )
            for section_id in section_ids
        ]

        self.df_out = pd.concat(df_out, ignore_index=True)
        self.df_out["failuremechanismid"] = 2  # GEKB: komt uit de
        if self.measure_id is not None:
            self.df_out["measureid"] = self.measure_id
        self.data_adapter.output(output=output, df=self.df_out)

    def _calculate_section(
        self,
        section_id: int,
        input: list[str],
        options: dict,
        temp_data_adapter: DataAdapter,
    ) -> pd.DataFrame:
        df_slopes = self.df_slopes[self.df_slopes["section_id"] == section_id]
        df_profile = self.df_profile[self.df_profile["section_id"] == section_id]
        df_bed_levels = self.df_bed_levels[
            self.df_bed_levels["section_id"] == section_id
        ]

        temp_data_adapter.config.global_variables[
            "FragilityCurveOvertoppingBedlevelFetch"
        ] = options

        df_profile = df_profile.iloc[0].T
        df_profile = df_profile.to_frame().rename(columns={df_profile.name: "values"})

        overrides = {
            input[0]: {"type": "python", "dataframe_from_python": df_slopes},
            input[1]: {"type": "python", "dataframe_from_python": df_profile},
            input[2]: {
                "type": "python",
                "dataframe_from_python": df_bed_levels,
            },
            "output": {
                "type": "python",
                "dataframe_from_python": pd.DataFrame(),
            },
        }
        with temp_data_adapter.temporary_adapters(overrides):
            # dit zorgt ervoor dat het beheerdersoordeel ook mee kan worden genomen
            fc_overtopping = self.fc_function(data_adapter=temp_data_adapter)
            if self.effect is not None:
                fc_overtopping.run(
                    input=[input[0], input[1], input[2]],
                    output="output",
                    effect=self.effect,
                )
            else:
                fc_overtopping.run(
                    input=[input[0], input[1], input[2]],
                    output="output",
                )

        df_fc_overtopping = fc_overtopping.as_dataframe()
        df_fc_overtopping["section_id"] = section_id
        return df_fc_overtopping
