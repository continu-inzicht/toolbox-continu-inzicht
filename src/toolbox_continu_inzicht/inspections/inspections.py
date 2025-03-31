from pathlib import Path
from typing import ClassVar, Optional
import warnings

import pandas as pd
from pydantic.dataclasses import dataclass
import geopandas as gpd

from toolbox_continu_inzicht.base.data_adapter import DataAdapter


@dataclass(config={"arbitrary_types_allowed": True})
class ClassifyInspections:
    """Classificeert Inspectie resultaten om weer te geven in de viewer.

    Attributes
    ----------
    data_adapter: DataAdapter
        Adapter for handling data input and output operations.
    df_in: Optional[pd.DataFrame | gpd.GeoDataFrame] | None
        Input DataFrame om te classificeren
    df_styling: Optional[pd.DataFrame] | None
        Input DataFrame met opmaak informatie
    df_default_styling: Optional[pd.DataFrame] | None
        DataFrame met standaard opmaak informatie, wordt gebruikt als er geen opmaak informatie is meegegeven
        Beschikbaar via get_default_styling() en te vervangen met set_default_styling(df)
    df_out: Optional[gpd.GeoDataFrame] | None
        Output DataFrame containing the filtered dataframe.
    df_legend_out: Optional[pd.DataFrame] | None
        Output DataFrame containing the legend information.
    styling_schema: ClassVar[dict[str, type]]
        Schema dataframe met de opmaak informatie

    Notes
    -----
    Het classificeren van inspectie resultaten gebeurt op basis van de kolom 'classify_column' die is opgegeven in de global variables.
    De classificatie wordt gedaan op basis van de kolommen 'upper_boundary' en 'lower_boundary' in de opmaak DataFrame, deze wordt mee geven als tweede input.
    Waardes die niet geclassificeerd kunnen worden, krijgen de opmaak van de rij zonder waardes in de upper_boundary en lower_boundary kolommen.
    Als er geen opmaak DataFrame wordt meegegeven, wordt de standaard opmaak gebruikt voor alle waardes.
    De standaard opmaak is op te halen met get_default_styling() en te vervangen met set_default_styling(df).

    Er zijn drie manier om de geodata mee te geven. Deze wordt gebruik voor de opmaak.
    Alle projecties worden ondersteund, maar wordt omgerekend naar WGS84 voor de viewer.
    In de Global Variables kan de projectie worden aangepast, standaard is EPSG:4326, alle andere projecties worden omgerekend naar deze projectie.

        - Bij het mee geven van een [GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html),
        wordt de opmaak toegepast op de geometrie van het GeoDataFrame.
        - Als er een 'geometry' kolom is uit de dataframe, wordt deze gebruikt om een GeoDataFrame te maken.
        - Indien beide bovenstaande niet het geval is, wordt gezocht naar een kolom met x en met y coördinaten en deze wordt gebruikt om een GeoDataFrame te maken.

    Het type geometry wordt automatisch bepaald, maar kan ook meegegeven worden in de Global Variables.
    Slechts een type per tabel is toe gestaan.
    De opties zijn:

        - Polygon
        - Polyline
        - CircleMarker
        - Marker

    De output DataFrame bevat de opmaak informatie die is toegepast op de inspectie resultaten.
    Als er geen opmaak informatie is meegegeven, wordt de standaard opmaak gebruikt.
    De output kan een met alleen geclassificeerde resultaten of twee dataframes met de inspectie resultaten en een met de legenda informatie.
    Ontbrekende kolommen in de opmaak DataFrame worden aangevuld met de standaard opmaak.
    """

    data_adapter: DataAdapter
    df_in: Optional[pd.DataFrame | gpd.GeoDataFrame] | None = None
    df_styling: Optional[pd.DataFrame] | None = None
    df_default_styling: Optional[pd.DataFrame] | None = None
    df_out: Optional[gpd.GeoDataFrame] | None = None
    df_legend_out: Optional[pd.DataFrame] | None = None
    styling_schema: ClassVar[dict[str, type]] = {
        "upper_boundary": "float",
        "lower_boundary": "float",
        "color": "object",
    }

    def run(self, input: str | list[str], output: str | list[str]):
        """Runt de integratie van een waterniveau overschrijdingsfrequentielijn met een fragility curve

        Parameters
        ----------
        input: str | list[str]
            Naam van de Data Adapters met inspectie resultaten en opmaak (indien gewenst), in die volgorde.

        output: str | list[str]
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

        global_variables = self.data_adapter.config.global_variables
        options = global_variables.get("ClassifyInspections", {})
        classify_columns = options.get("classify_column", None)
        if classify_columns not in self.df_in.columns:
            raise KeyError(
                f"De kolom '{classify_columns}' is niet aanwezig in de input data"
            )
        projection = options.get("projection", "EPSG:4326")
        geometry_type = options.get("geometry_type", None)

        # maak geoDataFrame aan indien nodig
        if not isinstance(self.df_in, gpd.GeoDataFrame):
            if "geometry" in self.df_in.columns:
                self.df_in = gpd.GeoDataFrame(self.df_in, geometry="geometry")
            else:
                if "x" in self.df_in.columns and "y" in self.df_in.columns:
                    self.df_in = gpd.GeoDataFrame(
                        self.df_in,
                        geometry=gpd.points_from_xy(self.df_in.x, self.df_in.y),
                    )
                else:
                    raise KeyError(
                        "Er is geen GeoDataFrame meegegeven en er is geen kolom 'geometry' of kolommen 'x' en 'y' voor de coördinaten"
                    )
        # zorg dat alle geo informatie goed staat
        if self.df_in.crs is None:
            self.df_in.crs = projection

        if self.df_in.crs.to_epsg() != 4326:
            self.df_in = self.df_in.to_crs(epsg=4326)

        if geometry_type is None:
            geometry_type = self.df_in.geometry.type.unique()
            assert len(geometry_type) == 1, (
                "Er zijn meerdere geometrie types gevonden in de data, geef een type geometrie soort mee"
            )
            geometry_type = geometry_type[0]
            map_geometry_type = {
                "Point": "Marker",
                "LineString": "Polyline",
                "Polygon": "Polygon",
            }
            geometry_type = map_geometry_type.get(geometry_type, None)

        self.df_out = self.df_in.copy()

        # haal standaard opmaak & benodigde styling kolommen op
        self.df_default_styling = self.get_default_styling()
        if self.df_styling is None:
            self.df_styling = self.df_default_styling
        possible_columns = self.get_possible_styling_columns()

        # zorg dat alle benodigde kolommen aanwezig zijn
        columns_to_transfer = list(
            set(self.df_styling.columns).intersection(possible_columns)
        )
        required_columns_for_dataset = self.get_possible_styling_columns(geometry_type)
        extra_required_columns = list(
            set(required_columns_for_dataset).difference(columns_to_transfer)
        )
        # als er benodigde kolommen missen, vul deze aan met de standaard opmaak
        for column in extra_required_columns:
            self.df_styling[column] = self.df_default_styling.loc[:, column]

        columns_to_transfer += extra_required_columns

        # Haal waardes weg waarbij geen kleur is gedefinieerd
        filtered_df_styling = self.df_styling.dropna(
            subset=["upper_boundary", "lower_boundary"]
        )

        # Bewaar deze verwijderde waardes voor alles zonder keur
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
            for _, row in filtered_df_styling.iterrows():
                for column in columns_to_transfer:
                    self.df_out.loc[
                        (self.df_in[classify_columns] > row["lower_boundary"])
                        & (self.df_in[classify_columns] <= row["upper_boundary"]),
                        column,
                    ] = row[column]

        # Voeg toe aan de waardes waarbij kolom niet is gedefinieerd
        warnings.filterwarnings("ignore", category=FutureWarning)

        for column in columns_to_transfer:
            replacement = unclassified_values[column]
            index_values_to_be_replaced = self.df_out[column].isna()
            ## dit was om future warning te voorkomen, maar sloopte het ook weer, later nog aanpassen.
            # self.df_out[column] = self.df_out[column].astype(type(replacement))
            self.df_out.loc[index_values_to_be_replaced, column] = replacement

        # exporteer ook de legenda als er een tweede data adapter is meegegeven
        if isinstance(output, str):
            self.data_adapter.output(output, self.df_out)
        else:
            self.data_adapter.output(output[0], self.df_out)

            self.df_legend_out = filtered_df_styling.copy()
            for column in extra_required_columns:
                if column not in self.df_legend_out.columns:
                    self.df_legend_out[:, column] = unclassified_values[column]
            self.data_adapter.output(output[1], self.df_legend_out)

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
            df.to_csv(
                Path(__file__).parent / "default_styling" / "default_styling.csv",
                index=False,
            )

    def get_possible_styling_columns(self, type=None) -> list:
        """Haal de mogelijke kolommen op voor de opmaak.

        De standaard waardes & mogelijke opties zijn:
        | Polyline     |        |         |                          |
        |--------------|--------|---------|--------------------------|
        | Option       | Type   | Default | Description              |
        | color        | string | #9e9e9e | hexcode van lijn         |
        | weight       | number | 3       | breedte lijn             |
        | opacity      | number | 1       | transparantie van lijn   |
        | dashArray    | string | null    | array voor dashed lijn   |

        | Polygon      |        |         |                          |
        |--------------|--------|---------|--------------------------|
        | Option       | Type   | Default | Description              |
        | color        | string | #9e9e9e | hexcode van stroke       |
        | weight       | number | 3       | breedte stroke           |
        | opacity      | number | 1       | transparantie van stroke |
        | dashArray    | string | null    | array voor dashed stroke |
        | fillColor    | string | #9e9e9e | hexcode van fill         |
        | fillOpacity  | number | 1       | transparantie van fill   |

        | Marker       |        |         |                          |
        |--------------|--------|---------|--------------------------|
        | Option       | Type   | Default | Description              |
        | color        | string | #9e9e9e | hexcode van stroke       |
        | opacity      | number | 1       | transparantie van marker |
        | fillColor    | string | #9e9e9e | hexcode van fill         |

        | CircleMarker |        |         |                          |
        |--------------|--------|---------|--------------------------|
        | Option       | Type   | Default | Description              |
        | color        | string | #9e9e9e | hexcode van stroke       |
        | weight       | number | 3       | breedte stroke           |
        | opacity      | number | 1       | transparantie van stroke |
        | dashArray    | string | null    | array voor dashed stroke |
        | fillColor    | string | #9e9e9e | hexcode van fill         |
        | fillOpacity  | number | 1       | transparantie van fill   |
        | radius       | number | 10      | groote van circle        |

        Default waardes worden hier getoond, maar deze hebben geen invloed op de output.
        Om default aan te passen gebruik set_default_styling(df).
        """
        Polyline = {
            "color": {
                "type": "string",
                "description": "hexcode van lijn",
            },
            "weight": {"type": "number", "description": "breedte lijn"},
            "opacity": {
                "type": "number",
                "description": "transparantie van lijn",
            },
            "dashArray": {
                "type": "string",
                "description": "array voor dashed lijn",
            },
        }

        Polygon = {
            "color": {
                "type": "string",
                "description": "hexcode van stroke",
            },
            "weight": {"type": "number", "default": 3, "description": "breedte stroke"},
            "opacity": {
                "type": "number",
                "description": "transparantie van stroke",
            },
            "dashArray": {
                "type": "string",
                "description": "array voor dashed stroke",
            },
            "fillColor": {
                "type": "string",
                "description": "hexcode van fill",
            },
            "fillOpacity": {
                "type": "number",
                "description": "transparantie van fill",
            },
        }

        Marker = {
            "color": {
                "type": "string",
                "description": "hexcode van stroke",
            },
            "opacity": {
                "type": "number",
                "description": "transparantie van marker",
            },
            "fillColor": {
                "type": "string",
                "description": "hexcode van fill",
            },
        }

        CircleMarker = {
            "color": {
                "type": "string",
                "description": "hexcode van stroke",
            },
            "weight": {"type": "number", "description": "breedte stroke"},
            "opacity": {
                "type": "number",
                "description": "transparantie van stroke",
            },
            "dashArray": {
                "type": "string",
                "description": "array voor dashed stroke",
            },
            "fillColor": {
                "type": "string",
                "description": "hexcode van fill",
            },
            "fillOpacity": {
                "type": "number",
                "description": "transparantie van fill",
            },
            "radius": {
                "type": "number",
                "description": "groote van circle",
            },
        }
        possible_columns = (
            list(Polyline.keys())
            + list(Polygon.keys())
            + list(Marker.keys())
            + list(CircleMarker.keys())
        )
        type_dict = {
            "Polyline": Polyline,
            "Polygon": Polygon,
            "Marker": Marker,
            "CircleMarker": CircleMarker,
        }
        if type is None:
            return list(set(possible_columns))
        else:
            return list(type_dict[type].keys())
