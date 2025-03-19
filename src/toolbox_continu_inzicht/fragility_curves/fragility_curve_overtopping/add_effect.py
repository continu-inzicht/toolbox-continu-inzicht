from toolbox_continu_inzicht import DataAdapter
from toolbox_continu_inzicht.fragility_curves import FragilityCurveOvertopping


class ShiftFragilityCurveOvertopping(FragilityCurveOvertopping):
    """Verschuift de fragility curve met een gegeven effect"""

    data_adapter: DataAdapter

    def run(self, input: list[str], output: str, effect: float) -> None:
        """
        Runt de berekening van de fragility curve voor golfoverslag & shift de curve met een gegeven effect

        Parameters
        ----------
        input: list[str]
            Lijst namen van de input data adapters: slopes, profile en bed_levels
        output: str
            Naam van de dataadapter Fragility curve output
        effect: float
            Verschuiving van de fragility curve

        Notes
        -----
        Deze input volgorde is wat specifiek, vandaar de extra details.
        Waar geen type is opgegeven moet het type float zijn.
        De eerste (slopes) data adapter moet de volgende kolommen bevatten:

        1. x, x-coördinaat
        1. y, y-coördinaat
        1. r, roughness
        1. slopetypeid, id de helling type (int, 1: dike or 2: slope)

        De tweede (profile) data adapter met profieldata moet de volgende kolommen bevatten:

        1. windspeed, windsnelheid
        1. sectormin, de minimale sectorhoek.
        1. sectorsize, de grootte van de sectorhoek.
        1. orientation, orientatie van het profiel in graden
        1. crestlevel, kruinhoogte in meters
        1. dam, wel of geen dam (int, 0: geen dam or 1: dam)
        1. damheight, dam hoogte in meters
        1. qcr, mag een van 3 zijn: een waarde in m^3/s (float), open of niet (str: close | open) of de waarden van mu en sigma (tuple).

        De derde (Bedlevelfetch) data adapter met bodem data moet de volgende kolommen bevatten:

        1. direction, windrichtingen
        1. bedlevel, bodem profielen
        1. fetch, lengte van fetch in meters
        """
        self.calculate_fragility_curve(input, output)
        self.shift(effect)


class ChangeCrestHeightFragilityCurveOvertopping(FragilityCurveOvertopping):
    """Verschuift de kruinhoogte met het gegeven effect en berekent de fragility curve"""

    data_adapter: DataAdapter

    def run(self, input: list[str], output: str, effect: float) -> None:
        """
        Runt de berekening van de fragility curve voor golfoverslag & past de kruinhoogte aan met een gegeven effect

        Parameters
        ----------
        input: list[str]
            Lijst namen van de input dataadapters: slopes, profile en bed_levels
        output: str
            Naam van de dataadapter Fragility curve output
        effect: float
            Verschuiving van de fragility curve

        Notes
        -----
        Deze input volgorde is wat specifiek, vandaar de extra details.
        Waar geen type is opgegeven moet het type float zijn.
        De eerste (slopes) data adapter moet de volgende kolommen bevatten:

        1. x, x-coördinaat
        1. y, y-coördinaat
        1. r, roughness
        1. slopetypeid, id de helling type (int, 1: dike or 2: slope)

        De tweede (profile) data adapter met profieldata moet de volgende kolommen bevatten:

        1. windspeed, windsnelheid
        1. sectormin, de minimale sectorhoek.
        1. sectorsize, de grootte van de sectorhoek.
        1. orientation, orientatie van het profiel in graden
        1. crestlevel, kruinhoogte in meters
        1. dam, wel of geen dam (int, 0: geen dam or 1: dam)
        1. damheight, dam hoogte in meters
        1. qcr, mag een van 3 zijn: een waarde in m^3/s (float), open of niet (str: close | open) of de waarden van mu en sigma (tuple).

        De derde (Bedlevelfetch) data adapter met bodem data moet de volgende kolommen bevatten:

        1. direction, windrichtingen
        1. bedlevel, bodem profielen
        1. fetch, lengte van fetch in meters

        """
        df_profile = self.data_adapter.input(input[1])
        if "parameters" in df_profile:
            df_profile.set_index("parameters", inplace=True)
        # converteer naar numeriek indien mogelijk, dit komt doordat de kolom zowel strings als floats bevat
        for k in df_profile.index:
            try:
                df_profile.loc[k, "values"] = float(df_profile.loc[k, "values"])
            except ValueError:
                # Sla waarden over als deze niet numeriek zijn, omdat ze behouden moeten blijven als string
                pass
        df_profile.loc["crestlevel"] += effect
        self.data_adapter.set_dataframe_adapter(
            "changed_crest_profile", df_profile, if_not_exist="create"
        )
        self.calculate_fragility_curve(
            [input[0], "changed_crest_profile", input[2]], output
        )
