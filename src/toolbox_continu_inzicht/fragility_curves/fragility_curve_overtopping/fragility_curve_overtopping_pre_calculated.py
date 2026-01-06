from typing import Optional

import pandas as pd

from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht import DataAdapter, FragilityCurve


@dataclass(config={"arbitrary_types_allowed": True})
class FragilityCurveOvertoppingPreCalculated(FragilityCurve):
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

        # pre-calculated overtopping probabilities where we firste extract the relevant columns
        self.df_bed_levels_unique = self.data_adapter.input(input[2])

        global_variables = self.data_adapter.config.global_variables
        options = global_variables.get("FragilityCurveOvertoppingPreCalculated", {})
        data_adapter_name = options.get(
            "pre_calculated_filter_data_adapter_name", "pre_calculated_filter"
        )

        # with the unique values we van
        direction_lower_bound = 0
        lst = []
        for direction_upperbound in self.df_bed_levels_unique["direction"]:
            temp_data_adapter = DataAdapter(
                config=self.data_adapter.config
            )  # duplicate data adapter to not mess up original
            temp_data_adapter.config.data_adapters[data_adapter_name][
                "direction_upper"
            ] = direction_upperbound
            temp_data_adapter.config.data_adapters[data_adapter_name][
                "direction_lower"
            ] = direction_lower_bound
            filtered_data = temp_data_adapter.input(data_adapter_name)

            # append to show it works, in reality you would process this data further
            lst.append(filtered_data)
            # for next iteration shift one, also juste for clarity
            direction_lower_bound = direction_upperbound

        self.df_out = pd.concat(lst, ignore_index=True)
        self.data_adapter.output(output=output, df=self.df_out)
