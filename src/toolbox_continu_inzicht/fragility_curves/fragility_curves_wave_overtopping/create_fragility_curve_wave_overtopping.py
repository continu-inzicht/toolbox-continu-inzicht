import pandas as pd
from pydantic.dataclasses import dataclass
from typing import ClassVar
import numpy as np

# pydra_core=0.0.1
import pydra_core
import pydra_core.common
import pydra_core.common.enum
import pydra_core.location

from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.fragility_curves.fragility_curves_wave_overtopping.calculate_fragility_curve_wave_overtopping import (
    WaveOvertoppingCalculation,
)


def loginextrp1d(x, xp, fp):
    fp[fp < 1e-20] = 1e-20
    return np.exp(inextrp1d(x, xp, np.log(fp)))


def inextrp1d(x, xp, fp):
    """
    Interpolate an array along the given axis.
    Similar to np.interp, but with extrapolation outside range.

    Parameters
    ----------
    x : np.array
        Array with positions to interpolate at
    xp : np.array
        Array with positions of known values
    fp : np.array
        Array with values as known positions to interpolate between

    Returns
    -------
    np.array
        interpolated array
    """
    # Determine lower bounds
    intidx = np.minimum(np.maximum(0, np.searchsorted(xp, x) - 1), len(xp) - 2)
    # Determine interpolation fractions
    fracs = (x - xp[intidx]) / (xp[intidx + 1] - xp[intidx])
    # Interpolate (1-frac) * f_low + frac * f_up
    f = (1 - fracs) * fp[intidx] + fp[intidx + 1] * fracs

    return f


# TODO: DO WE WANT THIS?
@dataclass(config={"arbitrary_types_allowed": True})
class FragilityCurve:
    """
    Class met een aantal gemakkelijke methoden om fragility curves
    op te slaan en aan te passen
    """

    sectionid: int
    failuremechanismid: int
    waterlevels: np.ndarray
    failureprobabilities: np.ndarray
    measureid: int = 0
    timedep: int = 0

    def as_array(self):
        """Geeft curve terug als numpy array, deze kunnen vervolgens worden gestacked en in een database geplaatst"""
        arr = np.zeros((len(self.waterlevels), 6))
        arr[:, 0] = self.sectionid
        arr[:, 1] = self.failuremechanismid
        arr[:, 2] = self.measureid
        arr[:, 3] = self.waterlevels
        arr[:, 4] = self.failureprobabilities
        arr[:, 5] = self.timedep
        return arr

    def shift(self, effect):
        """Schijf de waterstanden vna de fragility curve op (voor een noodmaatregel), en interpoleer de faalkansen
        op het oorspronkelijke waterstandsgrid"""
        if effect == 0.0:
            return None
        self.failureprobabilities = np.maximum(
            0.0,
            np.minimum(
                1.0,
                loginextrp1d(
                    self.waterlevels,
                    self.waterlevels + effect,
                    self.failureprobabilities,
                ),
            ),
        )

    def refine(self, waterlevels):
        """Interpolleer de fragility curve op de gegeven waterstanden"""
        self.failureprobabilities = np.maximum(
            0.0,
            np.minimum(
                1.0,
                loginextrp1d(waterlevels, self.waterlevels, self.failureprobabilities),
            ),
        )
        self.waterlevels = waterlevels.copy()


@dataclass(config={"arbitrary_types_allowed": True})
class CreateFragilityCurvesWaveOvertopping:
    """
    command: python start.py --schema="continuinzicht_wsrl_whatif"

    Steps:
    - Read measures
    - Determine which of the measures differ from the current curves (should be recalculated)
    - Read wind conditions, check if wind has changed. If so all sections should be recalculated
    - For the sections with measures, get original curves
    - Drop the curves for these sections from the fragility_curves_measures table
    - Calculate new curves, geotechnical by shifting, overtopping by Fragility curve creator
    - Determine total water level range and interpolate or extrapolate all curves to the range
    - Put wind status to 'modified = FALSE'
    """

    data_adapter: DataAdapter

    list_curves: ClassVar[list[FragilityCurve]] = []
    # Debug variable
    berekening: None | WaveOvertoppingCalculation = None

    def run(self, input: list[str], output: str) -> None:
        self.df_slopes = self.data_adapter.input(input[0])
        self.df_profiles = self.data_adapter.input(input[1])
        self.df_bed_levels = self.data_adapter.input(input[2])

        default_options = {
            "windchanged": False,
            "timedep": False,
            "windspeed": 20,
            "sectormin": 180.0,
            "sectorsize": 90.0,
            "effect": 0.0,
            "measureid": 0,
            "failuremechanismid": 0,
            "closing_situation": 0,
        }

        global_variables = self.data_adapter.config.global_variables
        if "CreateFragilityCurvesWaveOvertopping" in global_variables:
            options: dict = global_variables["CreateFragilityCurvesWaveOvertopping"]
            default_options.update(options)
            options.update(default_options)
        else:
            options = default_options

        effect = options["effect"]

        # voor nu default waardes uit de opties
        windspeed = options["windspeed"]
        sectormin = options["sectormin"]
        sectorsize = options["sectorsize"]
        measureid = options["measureid"]
        failuremechanismid = options["failuremechanismid"]
        closing_situation = options["closing_situation"]

        # sectionids = df_profiles["sectionid"].unique()
        for sectionid in self.df_profiles["sectionid"]:
            df_profile = self.df_profiles[
                self.df_profiles["sectionid"] == sectionid
            ].iloc[0]
            databaseid = df_profile["id"]
            df_slope = self.df_slopes[self.df_slopes["profileid"] == databaseid]

            df_slope_dike = df_slope[df_slope["slopetypeid"] == 1]
            profiel_dict = {
                "profile_name": f"profiel_{sectionid}",
                "dike_x_coordinates": list(df_slope_dike["x"].to_numpy()),
                "dike_y_coordinates": list(df_slope_dike["y"].to_numpy()),
                "dike_roughness": list(df_slope_dike["r"].to_numpy()),
                "dike_orientation": df_profile["orientation"],
                "dike_crest_level": df_profile["crestlevel"],
            }

            basis_profiel = pydra_core.Profile.from_dictionary(profiel_dict)

            foreland_profile = {}
            df_slope_foreland = df_slope.loc[df_slope["slopetypeid"] == 2]
            if len(df_slope_foreland) > 0:
                foreland_profile["foreland_x_coordinates"] = list(
                    df_slope_foreland["x"].to_numpy()
                )
                foreland_profile["foreland_y_coordinates"] = list(
                    df_slope_foreland["y"].to_numpy()
                )

            profiel_dict.update(foreland_profile)

            overtopping = pydra_core.Profile.from_dictionary(profiel_dict)

            if df_profile["dam"] != 0:
                breakwater_type = pydra_core.common.enum.Breakwater(df_profile["dam"])
                overtopping.set_breakwater(
                    breakwater_type=breakwater_type,
                    breakwater_level=df_profile["damheight"],
                )

            # Bodemhoogte en strijklengte
            df_bed_level = self.df_bed_levels[
                self.df_bed_levels["sectionid"] == sectionid
            ]
            richtingen = df_bed_level["direction"]
            bodemhoogte = df_bed_level["bedlevel"]
            strijklengte = df_bed_level["fetch"]

            # Backwards compatible code voor prob_qcr, graskwaliteit en qcr_dist
            prob_qcr = df_profile["prob_qcr"]
            graskwaliteit = df_profile["qcr_dist"]
            qcr = df_profile["qcr"]
            if qcr is not None:
                qcr = qcr / 1000.0  # m3/s/m

            # Calculate curve
            overtopping_calc = self.calculate_overtopping_curve(
                effect,
                windspeed,
                sectormin,
                sectorsize,
                overtopping,
                basis_profiel,
                prob_qcr,
                qcr,
                graskwaliteit,
                richtingen,
                bodemhoogte,
                strijklengte,
                closing_situation,
            )

            curve = FragilityCurve(
                sectionid, failuremechanismid, *overtopping_calc, measureid=measureid
            )
            self.list_curves.append(curve)

        # TODO: ensure the format matches the other functions
        lst_df = []
        for curve in self.list_curves:
            lst_df.append(
                pd.DataFrame(
                    columns=["waterlevel", "failure_probability"],
                    data=curve.as_array()[:, 3:5],
                )
            )

        self.df_out = pd.concat(lst_df, axis=0)
        self.data_adapter.output(output=output, df=self.df_out)

    def calculate_overtopping_curve(
        self,
        measure_effect,
        windspeed,
        sectormin,
        sectorsize,
        overtopping,
        basis_profiel,
        prob_qcr,
        qcr,
        graskwaliteit,
        richtingen,
        bodemhoogte,
        strijklengte,
        closing_situation,
    ):
        # Pas profiel aan voor maatregel (via kruinhoogte)
        overtopping.set_dike_crest_level(overtopping.dike_crest_level + measure_effect)
        overtopping.closing_situation = (
            closing_situation  # niet zo netjes maar het werkt
        )
        # Berekening
        berekening = WaveOvertoppingCalculation(overtopping)

        # Bereken fragility curve
        windrichtingen = (
            np.linspace(sectormin, sectormin + sectorsize, int(round(sectorsize))) % 360
        )
        bedlevel = np.interp(windrichtingen, richtingen, bodemhoogte, period=360)
        fetch = np.interp(windrichtingen, richtingen, strijklengte, period=360)

        # Voor dominante windrichting bepalen gebruik(te)/(en) we het profiel zonder voorland
        basis_profiel.set_dike_crest_level(
            basis_profiel.dike_crest_level + measure_effect
        )
        basis_profiel.closing_situation = (
            closing_situation  # niet zo netjes maar het werkt
        )
        # Berekening
        berekening_basis = WaveOvertoppingCalculation(basis_profiel)

        # Bepaal dominante windrichting
        ir = berekening_basis.bepaal_dominante_richting(
            overtopping.dike_crest_level - 0.5,
            windspeed,
            windrichtingen,
            bedlevel,
            fetch,
        )

        # TODO pass correcty to berekening
        # berekening.invoergeg.qcr = qcr
        # ir = 0
        # Calculate fragility curve
        niveaus, ovkansqcr = berekening.bereken_fc_cond(
            richting=windrichtingen[ir],
            windsnelheid=windspeed,
            bedlevel=bedlevel[ir],
            fetch=fetch[ir],
            prob_qcr=prob_qcr,
            qcr=qcr,
            graskwaliteit=graskwaliteit,
            crestlevel=overtopping.dike_crest_level,
            hstap=0.05,
            closing_situation=closing_situation,
        )
        # TODOL remove: only for debuging
        self.berekening = berekening
        return niveaus, ovkansqcr
