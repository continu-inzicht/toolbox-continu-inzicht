from pathlib import Path
from typing import Optional

import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht.base.data_adapter import DataAdapter


@dataclass(config={"arbitrary_types_allowed": True})
class ClassifyInspections:
    """Classificeert Inspectie resultaten om weer te geven in de viewer.

    Attributes
    ----------
    data_adapter: DataAdapter
        Adapter for handling data input and output operations.
    df_in: Optional[pd.DataFrame] | None
        Input DataFrame om te classificeren
    df_opmaak: Optional[pd.DataFrame] | None
        Input DataFrame met opmaak informatie
    df_out: Optional[pd.DataFrame] | None
        Output DataFrame containing the filtered dataframe.

    Notes
    -----
    ...

    """

    data_adapter: DataAdapter
    df_in: Optional[pd.DataFrame] | None = None
    df_opmaak: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    def run(self, input: str | list[str], output: str):
        """Runt de integratie van een waterniveau overschrijdingsfrequentielijn met een fragility curve

        Parameters
        ----------
        input: str | list[str]
            Naam van de Data Adapters met inspectie resultaten en opmaak (indien gewenst), in die volgorde.

        output: str
            Naam van Data adapter voor de output

        Notes
        -----
        De input DataAdapters moet de volgende informatie bevatten

            - 'Inspectie resultaten': De resultaten die worden geclassificeerd.

        Indien gewenst kan ook opmaak opties worden meegegeven. Als deze niet meegegeven wordt, wordt de standaard opmaak gebruikt.

            - 'Opmaak': De opmaak tabel wordt gebruikt voor de stijl die toegekend wordt aan de inspectie resultaten.

        Raises
        ------
        AssertionError
            Als er maar één input DataAdapter wordt meegegeven als
            string in plaats van een lijst met minimaal twee items.
        """
        # Als alleen input string is, is zijn er alleen resultaten met standaard opmaak
        if isinstance(input, str):
            input = [input]
        else:
            assert len(input) == 2, (
                "Er moeten precies twee input DataAdapters worden meegegeven indien het een lijst is"
            )
            self.df_opmaak = self.data_adapter.input(input[1])

        self.df_in = self.data_adapter.input(input[0])
        self.df_out = self.df_in.copy()

        # Classificeer de inspectie resultaten
        global_variables = self.data_adapter.config.global_variables
        options = global_variables.get("ClassifyInspections", {})
        classify_columns = options.get("classify_column", None)
        if self.df_opmaak is None:
            self.df_opmaak = pd.read_csv(
                Path(__file__).parent / "default_styling" / "default_styling.csv"
            )
        self.df_opmaak.dropna(inplace=True)

        self.df_in.set_index(classify_columns)
        # transfer_columns = self.df_opmaak.columns
        for _, row in self.df_opmaak.iterrows():
            self.df_out.loc[
                (self.df_out[classify_columns] > row["lower_boundary"])
                & (self.df_out[classify_columns] <= row["upper_boundary"]),
                "color",
            ] = row["color"]

        self.data_adapter.output(output, self.df_out)
