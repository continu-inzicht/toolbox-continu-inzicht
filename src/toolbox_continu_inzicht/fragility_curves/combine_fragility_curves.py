from dataclasses import field
import numpy as np
from pydantic.dataclasses import dataclass
import pandas as pd
from typing import Optional

from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.base.fragility_curve import FragilityCurve


@dataclass(config={"arbitrary_types_allowed": True})
class CombineFragilityCurvesPerSection:
    """
    Combineer fragility curves onafhankelijk voor verschillende faalmechanismen en maatregelen voor één dijkvak.
    """

    data_adapter: DataAdapter

    lst_df_in: list[pd.DataFrame] = field(default_factory=list)
    df_out: Optional[pd.DataFrame] | None = None

    def run(self, input: list[str], output: str) -> None:
        """


        Parameters
        ----------
        input: list[str]

            Lijst van namen van de data adapters met fragility curves.

        output: str

            Naam van de output data adapter.

        Notes:
        ------
        Elke fragility curve moet de volgende kolommen bevatten:
            - waterlevels: float
            - failure_probabilities: float
        """

        # haal opties en dataframe van de config
        # global_variables = self.data_adapter.config.global_variables
        # if "CombineFragilityCurves" in global_variables:
        #     options = global_variables["CombineFragilityCurves"]
        for key in input:
            df_in = self.data_adapter.input(key)
            self.lst_df_in.append(df_in)
        self.df_out = self.calculate_combined_curve(self.lst_df_in, self.data_adapter)
        self.data_adapter.output(output, self.df_out)

    @staticmethod
    def calculate_combined_curve(lst_fragility_curves, data_adapter):
        waterlevels_min = []
        waterlevels_max = []
        for df_in in lst_fragility_curves:
            waterlevels_min.append(df_in["waterlevels"].min())
            waterlevels_max.append(df_in["waterlevels"].max())

        waterlevels = np.arange(min(waterlevels_min), max(waterlevels_max) + 0.01, 0.05)
        onderschrijdingskans = np.ones(len(waterlevels))

        # interpolate fragility curves to the same waterlevels
        for index, fragility_curve in enumerate(lst_fragility_curves):
            fc = FragilityCurve(data_adapter=data_adapter, df_out=fragility_curve)
            fc.refine(waterlevels)
            lst_fragility_curves[index] = fc.df_out

        # make this more explicit
        # Combine independently: P(fail,comb|h) = 1 - (1 - P(fail,pip|h)) * (1 - P(fail,macro|h)) * (1 - P(fail,over|h))
        for fragility_curve in lst_fragility_curves:
            onderschrijdingskans *= 1 - fragility_curve["failure_probability"]

        return pd.DataFrame(
            {
                "waterlevels": waterlevels,
                "failure_probability": 1 - onderschrijdingskans,
                "failuremechanismid": 1,
            }
        )


class CombineFragilityCurves(CombineFragilityCurvesPerSection):
    """
    Combineer fragility curves onafhankelijk voor verschillende faalmechanismen en maatregelen voor meerdere dijkvakken.
    """

    data_adapter: DataAdapter

    lst_df_in: list[pd.DataFrame] = field(default_factory=list)
    df_out: Optional[pd.DataFrame] | None = None

    def run(self, input: list[str], output: str) -> None:
        """
        Combineer fragility curves voor verschillende faalmechanismen en maatregelen voor meerdere dijkvakken.

        Parameters
        ----------
        input: list[str]

            Lijst van namen van de data adapters met fragility curves.

        output: str

            Naam van de output data adapter.

        Notes:
        ------
        Elke fragility curve moet de volgende kolommen bevatten:
            - section_id: str
            - waterlevels: float
            - failure_probabilities: float
        """

        # haal opties en dataframe van de config
        # global_variables = self.data_adapter.config.global_variables
        # if "CombineFragilityCurves" in global_variables:
        #     options = global_variables["CombineFragilityCurves"]

        for key in input:
            df_in = self.data_adapter.input(key)
            self.lst_df_in.append(df_in)

        # split the dataframes by section_id
        lst_section_ids = []
        for df in self.lst_df_in:
            lst_section_ids.extend(df["section_id"].unique().tolist())

        curves_per_section = []
        for section_id in set(lst_section_ids):
            lst_fragility_curves = [
                df[df["section_id"] == section_id] for df in self.lst_df_in
            ]
            df_combined = self.calculate_combined_curve(
                lst_fragility_curves, self.data_adapter
            )
            df_combined["section_id"] = section_id
            curves_per_section.append(df_combined)

        self.df_out = pd.concat(curves_per_section)
        self.data_adapter.output(output, self.df_out)
