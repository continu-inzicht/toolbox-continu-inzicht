from typing import Optional

import pandas as pd
import numpy as np
from pydantic.dataclasses import dataclass
from toolbox_continu_inzicht import DataAdapter, FragilityCurve, ToolboxBase
from toolbox_continu_inzicht.fragility_curves.fragility_curve_overtopping.wave_overtopping_calculation import (
    WaveOvertoppingCalculation,
)
from toolbox_continu_inzicht.fragility_curves.fragility_curve_overtopping.overtopping_utils import (
    build_pydra_profiles,
    get_overtopping_options,
    parse_profile_dataframe,
    validate_slopes,
)
from toolbox_continu_inzicht.fragility_curves.fragility_curve_overtopping.wave_provider import (
    PreCalculatedWaveProvider,
)
from toolbox_continu_inzicht.utils.interpolate import bracketing_indices


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
    df_out: Optional[pd.DataFrame] | None
        DataFrame met de resultaten van de berekening.

    Notes
    -----
    Deze implementatie gebruikt pre-berekende golfcondities (Hs, Tm-1,0 en
    golf richting) via DataAdapters i.p.v. Bretschneider. De berekening
    loopt via WaveOvertoppingCalculation met een PreCalculatedWaveProvider.

    """

    data_adapter: DataAdapter
    df_slopes: Optional[pd.DataFrame] | None = None
    df_profile: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    def run(self, input: list[str], output: str) -> None:
        """
        Runt de berekening van de fragility curve voor golfoverslag

        Parameters
        ----------
        input: list[str]
            Lijst namen van de input DataAdapters:
            slopes, profile, waveval_unique_windspeed, waveval_unique_winddir,
            waveval_unique_waveval_id en pre_calculated_filter
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

        De derde tot en met zesde DataAdapter leveren pre-berekende golfcondities
        met kolommen zoals waveval_type, windspeed, winddir, waterlevel en waveval.
        """
        self.calculate_fragility_curve(input, output)

    def calculate_fragility_curve(self, input: list[str], output: str) -> None:
        """
        Bereken de fragility curve op basis van de opgegeven input en sla het resultaat op in het opgegeven outputbestand.

        Parameters
        ----------
        input: list[str]
            Lijst namen van de input DataAdapters:
            slopes, profile, waveval_unique_windspeed, waveval_unique_winddir,
            waveval_unique_waveval_id en pre_calculated_filter
        output: str
            Naam van de DataAdapter Fragility curve output

        """
        # @TODO replace with real location
        location = "034-02_0228_9_HD_km0998"

        # haal input op
        self.df_slopes = self.data_adapter.input(input[0])
        self.df_profile = self.data_adapter.input(input[1])

        profile_series = parse_profile_dataframe(self.df_profile)

        windspeed = profile_series["windspeed"]
        sectormin = profile_series["sectormin"]
        sectorsize = profile_series["sectorsize"]
        windrichtingen = (
            np.linspace(sectormin, sectormin + sectorsize, int(round(sectorsize))) % 360
        )
        qcr = profile_series["qcr"]
        closing_situation = profile_series["closing_situation"]

        global_variables = self.data_adapter.config.global_variables
        options = get_overtopping_options(
            global_variables, "FragilityCurveOvertoppingPreCalculated"
        )

        validate_slopes(self.df_slopes)

        basis_profiel, overtopping = build_pydra_profiles(
            self.df_slopes, profile_series
        )

        overtopping.closing_situation = closing_situation
        basis_profiel.closing_situation = closing_situation

        da = self.data_adapter

        # unique windspeeds
        with da.temporary_adapter_config(input[2], {"location": location}):
            uniq_windspeed = da.input(input[2])["windspeed"].to_numpy()

        # unique winddirections
        with da.temporary_adapter_config(input[3], {"location": location}):
            uniq_winddir = da.input(input[3])["winddir"].to_numpy()

        # Get brackets
        i1a, i1b, _ = bracketing_indices(uniq_windspeed, windspeed)
        windspeed_idx = uniq_windspeed[[i1a, i1b]]
        winddir_idx = []
        for winddir in windrichtingen:
            i2a, i2b, _ = bracketing_indices(uniq_winddir, winddir, wrap=True)
            winddir_idx += [i2a, i2b]
        winddir_idx = uniq_winddir[np.unique(winddir_idx)]

        # Unique waveval_id
        with da.temporary_adapter_config(
            input[4],
            {
                "location": location,
                "windspeed_bracket": windspeed_idx,
                "winddir_bracket": winddir_idx,
            },
        ):
            df_waveval_id = da.input(input[4])
        uniq_waveval_id = np.sort(df_waveval_id["waveval_id"].unique())

        # get waveval data, merge with waveval_id
        with da.temporary_adapter_config(
            input[5], {"waveval_bracket": uniq_waveval_id}
        ):
            df = da.input(input[5])

        df_data = df_waveval_id.drop(columns="location").merge(df, on="waveval_id")
        df_data = df_data.drop(columns="waveval_id")
        wave_provider = PreCalculatedWaveProvider(df_data)

        niveaus, ovkansqcr = WaveOvertoppingCalculation.calculate_overtopping_curve(
            windspeed,
            sectormin,
            sectorsize,
            overtopping,
            basis_profiel,
            qcr=qcr,
            closing_situation=closing_situation,
            options=options,
            wave_provider=wave_provider,
        )

        self.hydraulicload = niveaus
        self.failure_probability = ovkansqcr

        self.data_adapter.output(output=output, df=self.as_dataframe())


@dataclass(config={"arbitrary_types_allowed": True})
class FragilityCurveOvertoppingPreCalculatedMultiple(ToolboxBase):
    """
    Maakt een set van fragility curves voor golfoverslag (pre-berekend) voor een dijkvak.

    Attributes
    ----------
    data_adapter: DataAdapter
        DataAdapter object
    df_slopes: Optional[pd.DataFrame] | None
        DataFrame met helling data.
    df_profile: Optional[pd.DataFrame] | None
        DataFrame met profiel data.
    df_waveval_id: Optional[pd.DataFrame] | None
        DataFrame met waveval_id data.
    df_waveval: Optional[pd.DataFrame] | None
        DataFrame met waveval data.
    df_out: Optional[pd.DataFrame] | None
        DataFrame met de resultaten van de berekening.
    fc_function: FragilityCurve
        FragilityCurve object
    measure_id: int | None
        Maatregel id (niet gebruikt)
    """

    data_adapter: DataAdapter
    df_slopes: Optional[pd.DataFrame] | None = None
    df_profile: Optional[pd.DataFrame] | None = None
    df_waveval_id: Optional[pd.DataFrame] | None = None
    df_waveval: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    fc_function: FragilityCurve = FragilityCurveOvertoppingPreCalculated
    measure_id: int | None = None

    def run(self, input: list[str], output: str) -> None:
        self.calculate_fragility_curve(input, output)

    def calculate_fragility_curve(self, input: list[str], output: str) -> None:
        self.df_slopes = self.data_adapter.input(input[0])
        self.df_profile = self.data_adapter.input(input[1])
        self.df_waveval_id = self.data_adapter.input(input[4])
        self.df_waveval = self.data_adapter.input(input[5])

        section_ids = self.df_profile.section_id.unique()

        global_variables = self.data_adapter.config.global_variables
        options = get_overtopping_options(
            global_variables, "FragilityCurveOvertoppingPreCalculated"
        )

        self.df_out: pd.DataFrame = pd.DataFrame(
            columns=["section_id", "hydraulicload", "failure_probability"]
        )

        for section_id in section_ids:
            df_slopes = self.df_slopes[self.df_slopes["section_id"] == section_id]
            df_profile = self.df_profile[self.df_profile["section_id"] == section_id]
            df_waveval_id = self.df_waveval_id[
                self.df_waveval_id["section_id"] == section_id
            ]
            df_waveval = (
                self.df_waveval[self.df_waveval["section_id"] == section_id]
                if "section_id" in self.df_waveval.columns
                else self.df_waveval
            )

            df_profile = df_profile.iloc[0].T
            df_profile = df_profile.to_frame().rename(
                columns={df_profile.name: "values"}
            )

            df_uniq_ws = pd.DataFrame(
                {"windspeed": np.sort(df_waveval_id["windspeed"].unique())}
            )
            df_uniq_wd = pd.DataFrame(
                {"winddir": np.sort(df_waveval_id["winddir"].unique())}
            )

            overrides = {
                input[0]: {"type": "python", "dataframe_from_python": df_slopes},
                input[1]: {"type": "python", "dataframe_from_python": df_profile},
                input[2]: {"type": "python", "dataframe_from_python": df_uniq_ws},
                input[3]: {"type": "python", "dataframe_from_python": df_uniq_wd},
                input[4]: {"type": "python", "dataframe_from_python": df_waveval_id},
                input[5]: {"type": "python", "dataframe_from_python": df_waveval},
                output: {"type": "python", "dataframe_from_python": pd.DataFrame()},
            }
            with self.data_adapter.temporary_adapters(overrides):
                self.data_adapter.config.global_variables[
                    "FragilityCurveOvertoppingPreCalculated"
                ] = options
                fc_overtopping = self.fc_function(data_adapter=self.data_adapter)
                fc_overtopping.run(input=input, output=output)

                df_fc_overtopping = fc_overtopping.as_dataframe()
                df_fc_overtopping["section_id"] = section_id

                if len(self.df_out) == 0:
                    self.df_out = df_fc_overtopping
                else:
                    self.df_out = pd.concat([self.df_out, df_fc_overtopping])

        self.df_out["failuremechanismid"] = 2
        if self.measure_id is not None:
            self.df_out["measureid"] = self.measure_id
        self.data_adapter.output(output=output, df=self.df_out)
