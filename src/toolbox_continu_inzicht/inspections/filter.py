from typing import Optional

import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht.base.base_module import ToolboxBase
from toolbox_continu_inzicht.base.data_adapter import DataAdapter


@dataclass(config={"arbitrary_types_allowed": True})
class Filter(ToolboxBase):
    """Filtert een DataFrame aan de hand van de opgegeven configuratie.

    Attributes
    ----------
    data_adapter: DataAdapter
        DataAdapter object
    df_in: Optional[pd.DataFrame] | None
        Input DataFrame om te filteren
    df_out: Optional[pd.DataFrame] | None
        Output DataFrame die gefilterd is.

    Notes
    -----
    Voor het filteren zijn drie opties te configureren:

    - query: SQL-achtige query om de DataFrame te filteren, zie ook [pandas.DataFrame.query](http://pandas.pydata.org/docs/reference/api/pandas.DataFrame.query.html). Gebruik een `'`'` voor kolomnamen met spaties, bijvoorbeeld `'`Meetpunt code` == 1'`.
    - drop_columns: Lijst van kolommen die verwijderd moeten worden
    - keep_columns: Lijst van kolommen die behouden moeten worden

    Als meerdere van deze opties zijn geconfigureerd, worden ze in bovenstaande volgorde toegepast.

    """

    data_adapter: DataAdapter
    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    def run(self, input: str, output: str):
        """Runt filtering van een input DataAdapter.

        Parameters
        ----------
        input: str
            Naam van de DataAdapter om te filteren
        output: str
            Naam van DataAdapter voor de output

        """
        global_variables = self.data_adapter.config.global_variables
        options = global_variables.get("Filter", {})
        query_filter = options.get("query", None)
        drop_columns = options.get("drop_columns", None)
        keep_columns = options.get("keep_columns", None)

        self.df_in = self.data_adapter.input(input)
        self.df_out = self.df_in.copy()

        if query_filter is not None:
            self.df_out = self.df_out.query(query_filter).copy()

        if drop_columns is not None:
            self.df_out = self.df_out.drop(columns=drop_columns).copy()

        if keep_columns is not None:
            if isinstance(keep_columns, str):
                keep_columns = [keep_columns]
            self.df_out = self.df_out[keep_columns].copy()

        self.data_adapter.output(output, self.df_out)
