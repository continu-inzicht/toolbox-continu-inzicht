from typing import Optional

import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht import DataAdapter, FragilityCurve


@dataclass(config={"arbitrary_types_allowed": True})
class LoadCachedFragilityCurve(FragilityCurve):
    """
    Maakt het mogelijk om een vooraf berekende (cached) fragility curve in te laden.

    Deze class doet het voor een enkele curve.


    ----------
    data_adapter : DataAdapter
        Adapter for handling data input and output operations.
    df_out : Optional[pd.DataFrame] | None
        Output DataFrame containing the final fragility curve

    Notes
    -----


    """

    data_adapter: DataAdapter
    df_out: Optional[pd.DataFrame] | None = None

    def run(self, input: list[str], output: str) -> None:
        """
        Runt de berekening van de fragility curve voor piping

        Parameters
        ----------
        input: list[str]
            Lijst namen van de input dataadapters: ...
        output: str
            Naam van de dataadapter Fragility curve output

        Notes
        -----

        """
        # TODO implement for a single curve with caching
        pass


@dataclass(config={"arbitrary_types_allowed": True})
class LoadCachedFragilityCurveMultiple(FragilityCurve):
    """
    Maakt het mogelijk om een vooraf berekende (cached) fragility curve in te laden.

    Deze class doet het voor meerderere curves.

    ----------
    data_adapter : DataAdapter
        Adapter for handling data input and output operations.
    df_out : Optional[pd.DataFrame] | None
        Output DataFrame containing the final fragility curve

    Notes
    -----


    """

    data_adapter: DataAdapter
    df_out: Optional[pd.DataFrame] | None = None

    def run(self, input: list[str], output: str) -> None:
        """
        Runt de berekening van de fragility curve voor piping

        Parameters
        ----------
        input: list[str]
            Lijst namen van de input dataadapters: ...
        output: str
            Naam van de dataadapter Fragility curve output

        Notes
        -----

        """
        # TODO implement for a single curve with caching
        pass
