from pathlib import Path
from typing import ClassVar, Optional

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
    df_styling: Optional[pd.DataFrame] | None
        Input DataFrame met opmaak informatie
    df_default_styling: Optional[pd.DataFrame] | None
        DataFrame met standaard opmaak informatie, wordt gebruikt als er geen opmaak informatie is meegegeven
        Beschikbaar via get_default_styling() en te vervangen met set_default_styling(df)
    df_out: Optional[pd.DataFrame] | None
        Output DataFrame containing the filtered dataframe.
    styling_schema: ClassVar[dict[str, type]]
        Schema dataframe met de opmaak informatie

    Notes
    -----
    Het classificeren van inspectie resultaten gebeurt op basis van de kolom 'classify_column' die is opgegeven in de global variables.
    De classificatie wordt gedaan op basis van de kolommen 'upper_boundary' en 'lower_boundary' in de opmaak DataFrame, deze wordt mee geven als tweede input.
    Waardes die niet geclassificeerd kunnen worden, krijgen de opmaak van de rij zonder waardes in de upper_boundary en lower_boundary kolommen.
    Als er geen opmaak DataFrame wordt meegegeven, wordt de standaard opmaak gebruikt voor alle waardes.
    De standaard opmaak is op te halen met get_default_styling() en te vervangen met set_default_styling(df).



    """

    data_adapter: DataAdapter
    df_in: Optional[pd.DataFrame] | None = None
    df_styling: Optional[pd.DataFrame] | None = None
    df_default_styling: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None
    styling_schema: ClassVar[dict[str, type]] = {
        "upper_boundary": "float",
        "lower_boundary": "float",
        "color": "object",
    }
    # inspection_schema: ClassVar[dict[str, type]] = {
    #     "x": float,
    #     "y": float,
    # }

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
        De input DataAdapters moet minimaal 'Inspectie resultaten' bevatten die worden geclassificeerd.
        De classificatie wordt gedaan op basis van de kolom 'classify_column' opgegeven in de global variables.

        Indien gewenst kan ook opmaak opties worden meegegeven.
        Als deze niet meegegeven wordt, wordt de standaard opmaak gebruikt.
        Deze is op te halen met get_default_styling() en te vervangen (geavanceerd) met set_default_styling(df).
        Deze moet de volgende kolommen bevatten:

            - 'color': kleur van de classificatie in hexadecimaal formaat
            - 'lower_boundary': ondergrens van de classificatie
            - 'upper_boundary': bovengrens van de classificatie

        Naast de verplichte kolommen zijn ook een aantal die ook worden meegenomen in de output.
        Indien deze niet aanwezig zijn, worden ze ook niet meegenomen in de output.
        Dit zijn:

            - 'name': Naam van de categorie
            - 'description': Omschrijving van de categorie
            - 'symbol': Symbool van de categorie

        Raises
        ------
        AssertionError
            Als er maar één input DataAdapter wordt meegegeven als
            string in plaats van een lijst met minimaal twee items.

        KeyError
            Als de kolom die gebruikt moet worden voor classificatie niet aanwezig is in de input data.
        """
        # Als alleen input string is, is zijn er alleen resultaten met standaard opmaak
        if isinstance(input, str):
            input = [input]
        else:
            assert len(input) == 2, (
                "Er moeten precies twee input DataAdapters worden meegegeven indien het een lijst is"
            )
            self.df_styling = self.data_adapter.input(
                input[1], schema=self.styling_schema
            )

        self.df_in = self.data_adapter.input(input[0])
        self.df_out = self.df_in.copy()

        # Classificeer de inspectie resultaten
        global_variables = self.data_adapter.config.global_variables
        options = global_variables.get("ClassifyInspections", {})
        classify_columns = options.get("classify_column", None)
        self.df_default_styling = self.get_default_styling()
        if self.df_styling is None:
            self.df_styling = self.df_default_styling
        possible_columns = ["color", "name", "description", "symbol"]
        columns_to_transfer = list(
            set(self.df_styling.columns).intersection(possible_columns)
        )
        # Haal waardes weg waarbij geen kleur is gedefinieerd
        filtered_df_styling = self.df_styling.dropna(
            subset=["upper_boundary", "lower_boundary"]
        )

        # Bewaar deze voor alles zonder keur
        index_unclassified_values = list(
            set(self.df_styling.index).difference(filtered_df_styling.index)
        )
        unclassified_values: dict = {}
        if len(index_unclassified_values) > 0:
            for column in columns_to_transfer:
                unclassified_values[column] = list(
                    self.df_styling.loc[index_unclassified_values, column].unique()
                )
                # bewaar de eerste
                unclassified_values[column] = unclassified_values[column][0]
        # Als er geen style is gedefinieerd, gebruik default
        else:
            for column in columns_to_transfer:
                unclassified_values[column] = self.df_default_styling.loc[0, column]

        # daadwerkelijke classificatie
        if classify_columns is None:
            for column in columns_to_transfer:
                self.df_out.loc[:, column] = None
        else:
            if classify_columns not in self.df_in.columns:
                raise KeyError(
                    f"De kolom '{classify_columns}' is niet aanwezig in de input data"
                )
            for _, row in filtered_df_styling.iterrows():
                for column in columns_to_transfer:
                    self.df_out.loc[
                        (self.df_in[classify_columns] > row["lower_boundary"])
                        & (self.df_in[classify_columns] <= row["upper_boundary"]),
                        column,
                    ] = row[column]

        # Voeg de kleur toe aan de waardes waarbij geen kleur is gedefinieerd
        for column in columns_to_transfer:
            replacement = unclassified_values[column]
            index_values_to_be_replaced = self.df_out[column].isna()
            self.df_out[column] = self.df_out[column].astype(type(replacement))
            self.df_out.loc[index_values_to_be_replaced, column] = replacement

        self.data_adapter.output(output, self.df_out)

    def get_default_styling(self) -> pd.DataFrame:
        """Haal de standaard opmaak op.

        Returns:
            pd.DataFrame: Het DataFrame met de standaard opmaak.
        """
        if self.df_default_styling is None:
            self.df_default_styling = pd.read_csv(
                Path(__file__).parent / "default_styling" / "default_styling.csv"
            )
        return self.df_default_styling

    def set_default_styling(self, df, permanent=False) -> None:
        """
        Vervangt de default styling.
        Parameters:
            df (pandas.DataFrame): Het DataFrame met de nieuwe default styling.
            permanent (bool, optional): Indien True, vervangt de default styling in het bestand. Standaard is False.
        Returns:
            None
        """
        self.df_default_styling = df
        if permanent:
            df.to_csv(Path(__file__).parent / "default_styling" / "default_styling.csv")
