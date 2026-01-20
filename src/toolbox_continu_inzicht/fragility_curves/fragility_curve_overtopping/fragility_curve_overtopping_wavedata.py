from typing import ClassVar, Optional

import numpy as np
import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht import DataAdapter, FragilityCurve, ToolboxBase
from toolbox_continu_inzicht.fragility_curves.fragility_curve_overtopping.fragility_curve_overtopping_base import (
    FragilityCurveOvertoppingBase,
)
from toolbox_continu_inzicht.fragility_curves.fragility_curve_overtopping.overtopping_utils import (
    make_winddirections,
    parse_profile_dataframe,
)
from toolbox_continu_inzicht.fragility_curves.fragility_curve_overtopping.wave_provider import (
    WaveDataProvider,
    WaveType,
)
from toolbox_continu_inzicht.utils.interpolate import bracketing_indices


@dataclass(config={"arbitrary_types_allowed": True})
class FragilityCurveOvertoppingWaveData(FragilityCurveOvertoppingBase):
    """
    Maakt een enkele fragility curve voor golfoverslag.
    Attributes
    ----------
    data_adapter: DataAdapter
        DataAdapter object
    df_waveval_uncert: Optional[pd.DataFrame] | None
        DataFrame met golfonzekerheden.
    df_waveval_id: Optional[pd.DataFrame] | None
        DataFrame met golf metadata.
    df_waveval: Optional[pd.DataFrame] | None
        DataFrame met golfdata.
    options_key: ClassVar[str]
        Config key voor overtopping opties.

    Notes
    -----
    Deze implementatie gebruikt voorberekende golfcondities (Hs, Tm-1,0 en
    golfrichting) via DataAdapters i.p.v. Bretschneider. De berekening
    loopt via WaveOvertoppingCalculation met een WaveDataProvider.

    """

    data_adapter: DataAdapter
    df_waveval_uncert: Optional[pd.DataFrame] | None = None
    df_waveval_id: Optional[pd.DataFrame] | None = None
    df_waveval: Optional[pd.DataFrame] | None = None
    options_key: ClassVar[str] = "FragilityCurveOvertoppingWaveData"

    def run(self, input: list[str], output: str) -> None:
        """
        Runt de berekening van de fragility curve voor golfoverslag

        Parameters
        ----------
        input: list[str]
            Lijst namen van de input DataAdapters: slopes, profile, waveval_id en waveval
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

        De derde (df_waveval_uncert) DataAdapter moet de volgende kolommen bevatten:

        1. closing_situation, sluitsituatie (int)
        1. waveval_type, (int, 2: Hs, 6: Tm1,0 of 7: golfrichting)
        1. mean, gemiddelde (float)
        1. stddev standaardafwijking (float)

        De vierde (waveval_id) DataAdapter moet de volgende kolommen bevatten:

        1. waveval_id, golfcombinatie id (int)
        1. waveval_type, (int, 2: Hs, 6: Tm1,0 of 7: golfrichting)
        1. winddir, windrichting in graden
        1. windspeed windsnelheid in m/s

        De vijfde (waveval) DataAdapter moet de volgende kolommen bevatten:

        1. waveval_id, golfcombinatie id
        1. waterlevel, de waterstand in meters
        1. waveval, de waarde van de golfparameter (Hs: meters, Tm10: seconden, Wave direction: graden)
        """
        self.calculate_fragility_curve(input, output)

    def _load_inputs(self, input: list[str]) -> None:
        da = self.data_adapter
        self.df_slopes = da.input(input[0])
        self.df_profile = da.input(input[1])
        self.df_waveval_uncert = da.input(input[2])  # @TODO doe hier iets mee
        self.df_waveval_id = da.input(input[3])
        self.df_waveval = da.input(input[4])

    def _build_wave_provider(self, options: dict) -> WaveDataProvider:
        return WaveDataProvider(self.df_waveval_id, self.df_waveval)

    def _build_options(self) -> dict:
        options_raw = self.data_adapter.config.global_variables.get(
            self.options_key, {}
        )
        closing_situation = options_raw.get("closing_situation")
        overrides, context = self._get_waveval_uncertainty_overrides(closing_situation)
        return super()._build_options(overrides=overrides, context=context)

    def _get_waveval_uncertainty_overrides(
        self, closing_situation: int | None
    ) -> tuple[dict, dict]:
        if self.df_waveval_uncert is None or self.df_waveval_uncert.empty:
            return {}, {}

        df_uncert = self.df_waveval_uncert
        df_uncert = df_uncert[df_uncert["closing_situation"] == closing_situation]

        if df_uncert.empty:
            return {}, {}

        mapping = {
            WaveType.SIGNIFICANT_WAVEHEIGHT.value: ("gh_onz_mu", "gh_onz_sigma"),
            WaveType.SPECTRAL_WAVEPERIOD.value: (
                "gp_onz_mu_tspec",
                "gp_onz_sigma_tspec",
            ),
        }
        overrides = {}
        for waveval_type, (mu_key, sigma_key) in mapping.items():
            subset = df_uncert[df_uncert["waveval_type"] == waveval_type]
            if subset.empty:
                continue
            row = subset.iloc[0]
            mean_value = row["mean"]
            std_value = row["stddev"]
            if pd.isna(mean_value) or pd.isna(std_value):
                continue
            overrides[mu_key] = float(mean_value)
            overrides[sigma_key] = float(std_value)
        context = {"closing_situation": closing_situation}
        if "hr_locid" in df_uncert:
            context["hr_locid"] = df_uncert["hr_locid"].iloc[0]
        return overrides, context


@dataclass(config={"arbitrary_types_allowed": True})
class FragilityCurveOvertoppingWaveDataMultiple(ToolboxBase):
    """
    Maakt een set van fragility curves voor golfoverslag (voorberekend) voor een dijkvak.

    Attributes
    ----------
    data_adapter: DataAdapter
        DataAdapter object
    df_slopes: Optional[pd.DataFrame] | None
        DataFrame met helling data.
    df_profile: Optional[pd.DataFrame] | None
        DataFrame met profiel data.
    df_section_hrloc: Optional[pd.DataFrame] | None
        DataFrame met koppeling hr-locatie en faalmechanisme met profiel.
    df_waveval_uncert: Optional[pd.DataFrame] | None
        DataFrame met modelonzekerheden per hr-locatie.
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
    df_section_hrloc: Optional[pd.DataFrame] | None = None
    df_waveval_uncert: Optional[pd.DataFrame] | None = None
    df_waveval_id: Optional[pd.DataFrame] | None = None
    df_waveval: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    fc_function: FragilityCurve = FragilityCurveOvertoppingWaveData
    measure_id: int | None = None

    def run(self, input: list[str], output: str) -> None:
        self.calculate_fragility_curve(input, output)

    def calculate_fragility_curve(self, input: list[str], output: str) -> None:
        da = self.data_adapter
        self.df_slopes = da.input(input[0])
        self.df_profile = da.input(input[1])
        self.df_section_hrloc = da.input(input[2]).set_index(
            ["section_id", "failuremechanismid"]
        )
        self.df_wv_uncert = da.input(input[3])

        global_variables = da.config.global_variables
        defaults = FragilityCurveOvertoppingBase.default_options.copy()
        options = FragilityCurveOvertoppingBase.get_overtopping_options(
            global_variables, "FragilityCurveOvertoppingWaveData", defaults
        )

        section_ids = self.df_profile.section_id.unique()
        df_out = [
            self._calculate_section(
                section_id=section_id,
                input=input,
                output=output,
                options=options,
            )
            for section_id in section_ids
        ]

        # Concat fragility curves and add mechanism and measure id
        self.df_out = pd.concat(df_out, ignore_index=True)
        self.df_out["failuremechanismid"] = 2
        if self.measure_id is not None:
            self.df_out["measureid"] = self.measure_id

        da.output(output=output, df=self.df_out)

    def _calculate_section(
        self,
        section_id: int,
        input: list[str],
        output: str,
        options: dict,
    ) -> pd.DataFrame:
        da = self.data_adapter
        da.logger.info(
            "Calculating overtopping curve using wave data for section_id=%s",
            section_id,
        )

        df_slopes = self.df_slopes[self.df_slopes["section_id"] == section_id]
        df_profile = self.df_profile[self.df_profile["section_id"] == section_id]
        hr_loc = int(self.df_section_hrloc["hr_locid"].at[(section_id, 2)])

        # Wavevval model uncertainty
        df_wv_uncert = self.df_wv_uncert[self.df_wv_uncert["hr_locid"] == hr_loc]

        # Create profile series
        df_profile = df_profile.iloc[0].T
        df_profile = df_profile.to_frame().rename(columns={df_profile.name: "values"})
        profile_series = parse_profile_dataframe(df_profile)

        # Determine windspeed bracket
        windspeed = profile_series["windspeed"]
        windrichtingen = make_winddirections(
            profile_series["sectormin"],
            profile_series["sectorsize"],
        )

        # Query waveval_id table for this hr location
        with da.temporary_adapter_config(input[4], {"hr_locid": hr_loc}):
            df_wvid = da.input(input[4])

        # Unique windspeeds and winddirections for this hr location
        uniq_windspeed = np.sort(df_wvid["windspeed"].unique())
        uniq_winddir = np.sort(df_wvid["winddir"].unique())

        # Get brackets for the governing windspeed and winddirection
        i1a, i1b, _ = bracketing_indices(uniq_windspeed, windspeed)
        ws_bracket = uniq_windspeed[[i1a, i1b]]
        wd_bracket = []
        for winddir in windrichtingen:
            i2a, i2b, _ = bracketing_indices(uniq_winddir, winddir, wrap=True)
            wd_bracket += [i2a, i2b]
        wd_bracket = uniq_winddir[np.unique(wd_bracket)]

        # Reduce waveval_id to the found brackets
        df_wvid = df_wvid[
            df_wvid.windspeed.isin(ws_bracket) & df_wvid.winddir.isin(wd_bracket)
        ]

        # Query waveval based on the found waveval_ids
        uniq_wid = np.sort(df_wvid["waveval_id"].unique())
        with da.temporary_adapter_config(input[5], {"waveval_bracket": uniq_wid}):
            df_waveval = da.input(input[5])

        # Calculate fragility curve
        overrides = {
            input[0]: {"type": "python", "dataframe_from_python": df_slopes},
            input[1]: {"type": "python", "dataframe_from_python": df_profile},
            input[3]: {"type": "python", "dataframe_from_python": df_wv_uncert},
            input[4]: {"type": "python", "dataframe_from_python": df_wvid},
            input[5]: {"type": "python", "dataframe_from_python": df_waveval},
            output: {"type": "python", "dataframe_from_python": pd.DataFrame()},
        }
        with da.temporary_adapters(overrides):
            da.config.global_variables["FragilityCurveOvertoppingWaveData"] = options
            fc_overtopping = self.fc_function(data_adapter=da)
            fc_overtopping.run(
                input=[input[0], input[1], input[3], input[4], input[5]],
                output=output,
            )

            df_fc_overtopping = fc_overtopping.as_dataframe()
            df_fc_overtopping["section_id"] = section_id
            return df_fc_overtopping
