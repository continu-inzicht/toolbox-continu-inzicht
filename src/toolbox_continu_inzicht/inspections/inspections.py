import json
import warnings
from pathlib import Path
from typing import ClassVar, Optional

import geopandas as gpd
import numpy as np
import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht.base.base_module import ToolboxBase
from toolbox_continu_inzicht.base.data_adapter import DataAdapter


@dataclass(config={"arbitrary_types_allowed": True})
class ClassifyInspections(ToolboxBase):
    """Classificeert inspectieresultaten om weer te geven in de viewer.

    Attributes
    ----------
    data_adapter: DataAdapter
        DataAdapter object
    df_in: Optional[pd.DataFrame | gpd.GeoDataFrame] | None
        Input DataFrame om te classificeren
    df_styling: Optional[pd.DataFrame] | None
        Input DataFrame met opmaak informatie
    df_default_styling: Optional[pd.DataFrame] | None
        DataFrame met standaard opmaak informatie, wordt gebruikt als er geen opmaak informatie is meegegeven
        Beschikbaar via get_default_styling() en te vervangen met set_default_styling(df)
    df_out: Optional[gpd.GeoDataFrame] | None
        Output DataFrame containing the filtered DataFrame.
    df_legend_out: Optional[pd.DataFrame] | None
        Output DataFrame containing the legend information.
    styling_schema: ClassVar[dict[str, str]]
        Schema DataFrame met de opmaak informatie

    Notes
    -----
    Het classificeren van inspectieresultaten gebeurt op basis van de kolom 'classify_column' die is opgegeven in de global variables.
    De classificatie wordt gedaan op basis van de kolommen 'upper_boundary' en 'lower_boundary' in de opmaak DataFrame, deze wordt meegegeven als tweede input.
    Waardes die niet geclassificeerd kunnen worden, krijgen de opmaak van de rij zonder waardes in de upper_boundary en lower_boundary kolommen.
    Als er geen opmaak DataFrame wordt meegegeven, wordt de standaard opmaak gebruikt voor alle waardes.
    De standaard opmaak is op te halen met get_default_styling() en te vervangen met set_default_styling(df).

    Er zijn drie manier om geodata mee te geven. Deze wordt gebruik voor de opmaak.
    Alle projecties worden ondersteund, maar wordt omgezet naar EPSG:4326 voor de viewer.

    In de Global Variables kan de projectie worden aangepast, standaard is EPSG:4326, alle andere projecties worden omgezet naar deze projectie.

    - Bij het mee geven van een [GeoDataFrame](https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.html), wordt de opmaak toegepast op de geometrie van het GeoDataFrame.
    - Als er een 'geometry' kolom is in de DataFrame, wordt deze gebruikt om een GeoDataFrame te maken.
    - Indien beide bovenstaande niet het geval is, wordt gezocht naar een kolom met x en met y coördinaten en deze wordt gebruikt om een GeoDataFrame te maken.

    Het type geometry wordt automatisch bepaald, maar kan ook meegegeven worden in de Global Variables.
    Slechts een type per tabel is toe gestaan.

    De opties zijn:

    - Polygon
    - Polyline
    - CircleMarker
    - Marker

    De output DataFrame bevat de opmaak informatie die is toegepast op de inspectieresultaten.
    Als er geen opmaak informatie is meegegeven, wordt de standaard opmaak gebruikt.
    De output kan een met alleen geclassificeerde resultaten of twee DataFrames met de inspectieresultaten en een met de legenda informatie.
    Ontbrekende kolommen in de opmaak DataFrame worden aangevuld met de standaard opmaak.
    """

    data_adapter: DataAdapter
    df_in: Optional[pd.DataFrame | gpd.GeoDataFrame] | None = None
    df_styling: Optional[pd.DataFrame] | None = None
    df_default_styling: Optional[pd.DataFrame] | None = None
    df_out: Optional[gpd.GeoDataFrame] | None = None
    df_legend_out: Optional[pd.DataFrame] | None = None
    styling_schema: ClassVar[dict[str, str]] = {
        "lower_boundary": ["float", "int", "object"],
        "color": "object",
    }

    def run(self, input: str | list[str], output: str | list[str]):
        """Runt het classificeren van inspectieresultaten om vervolgens weer te geven in de viewer.

        Parameters
        ----------
        input: str | list[str]
            Naam van de Data Adapters met inspectieresultaten en legenda met opmaak (indien gewenst), in die volgorde.

        output: str | list[str]
            Naam van Data adapter voor de output

        Notes
        -----
        De input DataAdapters moet minimaal 'Inspectieresultaten' bevatten die worden geclassificeerd.
        De classificatie wordt gedaan op basis van de kolom 'classify_column' opgegeven in de global variables.
        Deze classificatiewaardes kunnen zowel numeriek zijn als text.

        Indien gewenst kan ook opmaak opties worden meegegeven.
        Als deze niet meegegeven wordt, wordt de standaard opmaak gebruikt.
        Deze is op te halen met get_default_styling() en te vervangen (geavanceerd) met set_default_styling(df).

        Deze moet de volgende kolommen bevatten:

        - 'color': kleur van de classificatie in hexadecimaal formaat
        - 'lower_boundary': ondergrens van de classificatie waarde. De inspectieresultaten moeten groter of gelijk zijn.

        De classificatie in 'lower_boundary' en 'classify_column' moet van hetzelfde type zijn.
        Als de classificatie op basis van een waarde is, mag ook de volgende kolom aanwezig zijn:

        - 'upper_boundary': bovengrens van de classificatie

        Bij text waardes wordt standaard tijdens de classificatie gekeken of de waarde identiek is aan de classificatie.
        Door de global variables kan dit ook worden aangepast naar een `match_text_on` check.
        De opties zijn:

        - 'contains': de classificatie waarde moet in de inspectieresultaten staan
        - 'equals': de classificatie waarde moet gelijk zijn aan de inspectieresultaten
        - 'startswith': de classificatie waarde moet aan het begin van de inspectieresultaten staan
        - 'endswith': de classificatie waarde moet aan het einde van de inspectieresultaten staan

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

        UserWarning
            Als de classificatie niet gelukt is, omdat de kolom die gebruikt moet worden voor classificatie geen tekst of getal is.
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
        match_text_on = options.get("classify_text_type", "equals")
        match_text_on_dict = {
            "contains": self._contains,
            "equals": self._equals,
            "endswith": str.endswith,
            "startswith": str.startswith,
        }
        if classify_columns is not None and classify_columns not in self.df_in.columns:
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
                        index=self.df_in.index,
                        data=self.df_in.to_dict(),
                        geometry=gpd.points_from_xy(x=self.df_in.x, y=self.df_in.y),
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
            if (  # haal op uit de styling indien aanwezig
                self.df_styling is not None
                and "geometry_type" in self.df_styling.columns
            ):
                geometry_type_list = self.df_styling["geometry_type"].unique()
            else:  # of haal uit het type geometrie
                geometry_type_list = self.df_in.geometry.type.unique()

            assert len(geometry_type_list) == 1, (
                "Er zijn meerdere geometrie types gevonden in de data, geef een type geometrie soort mee"
            )
            geometry_type = geometry_type_list[0]
            map_geometry_type = {
                "Point": "CircleMarker",
                "LineString": "Polyline",
                "Polygon": "Polygon",
            }
            if geometry_type in map_geometry_type.keys():
                geometry_type = map_geometry_type.get(geometry_type, None)

        # symbol in de CI viewer
        self.df_in["symbol"] = geometry_type

        self.df_out = self.df_in.copy()

        # haal standaard opmaak & benodigde styling kolommen op
        self.df_default_styling = self.get_default_styling()
        if self.df_styling is None:
            self.df_styling = self.df_default_styling
        possible_columns = self.get_possible_styling()

        # zorg dat alle benodigde kolommen aanwezig zijn
        columns_to_transfer = list(
            set(self.df_styling.columns).intersection(possible_columns)
        )
        required_columns_for_dataset = self.get_possible_styling(geometry_type)
        extra_required_columns = list(
            set(required_columns_for_dataset).difference(columns_to_transfer)
        )
        # als er benodigde kolommen missen, vul deze aan met de standaard opmaak
        for column in extra_required_columns:
            self.df_styling[column] = self.df_default_styling.loc[:, column]

        columns_to_transfer += extra_required_columns

        # Haal waardes weg waarbij geen kleur is gedefinieerd
        drop_subset = list(
            set(["upper_boundary", "lower_boundary"]).intersection(
                self.df_styling.columns
            )
        )
        filtered_df_styling = self.df_styling.dropna(subset=drop_subset)

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
                # als die leeg is, vervang door de default
                if isinstance(unclassified_values[column], float) and np.isnan(
                    unclassified_values[column]
                ):
                    unclassified_values[column] = self.df_default_styling.loc[0, column]
        # Als er geen style is gedefinieerd, gebruik default
        else:
            for column in columns_to_transfer:
                unclassified_values[column] = self.df_default_styling.loc[0, column]

        # daadwerkelijke classificatie
        if classify_columns is None:
            for column in columns_to_transfer:
                self.df_out.loc[:, column] = None
        else:
            values = self.df_in[classify_columns]
            # Classificatie voor text
            if self._check_dtype(values.dtype, "text"):
                assert self._check_dtype(
                    filtered_df_styling["lower_boundary"].dtype, "text"
                ), (
                    "Type van de classificatie is geen tekst, maar de inspectieresultaten zijn dat wel"
                )
                for _, row in filtered_df_styling.iterrows():
                    for column in columns_to_transfer:
                        self.df_out.loc[
                            match_text_on_dict[match_text_on](
                                values, row["lower_boundary"]
                            ),
                            column,
                        ] = row[column]
            # Classificatie voor getallen
            elif pd.api.types.is_numeric_dtype(values.dtype):
                assert self._check_dtype(
                    filtered_df_styling["lower_boundary"].dtype, "number"
                ), (
                    "Type van de classificatie is geen getal, maar de inspectieresultaten zijn dat wel"
                )
                for _, row in filtered_df_styling.iterrows():
                    for column in columns_to_transfer:
                        if "upper_boundary" in row:
                            self.df_out.loc[
                                (values >= row["lower_boundary"])
                                & (values < row["upper_boundary"]),
                                column,
                            ] = row[column]
                        else:
                            self.df_out.loc[
                                (values >= row["lower_boundary"]),
                                column,
                            ] = row[column]
            else:
                raise UserWarning(
                    f"De classificatie is niet gelukt, het type van kolom {classify_columns} is geen tekst of getal"
                )
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

    @staticmethod
    def _equals(waardes, classificatie):
        """Vergelijk classificatie en waardes element voor element: voor identieke waardes"""
        return waardes == classificatie

    @staticmethod
    def _contains(waardes, classificatie):
        """Vergelijk classificatie en waardes element voor element: voor waardes in andere"""
        return waardes.apply(lambda x: classificatie in x)

    @staticmethod
    def _check_dtype(values_dtype: pd.api.types, type: str) -> bool:
        """Controleert of de dtype van de waardes overeenkomt met het opgegeven type.

        Parameters
        ----------
        values_dtype: pd.api.types
            De dtype van de waardes.
        type: str
            Het type dat gecontroleerd moet worden. Mogelijke waardes zijn: 'text' of 'number'.

        Raises
        ------
        ValueError
            Als het opgegeven type niet geldig is geeft een ValueError.

        Returns
        -------
        bool
            True als de dtype overeenkomt met het opgegeven type, anders False.
        """
        if type == "text":
            return (
                pd.api.types.is_object_dtype(values_dtype)
                or pd.api.types.is_bool_dtype(values_dtype)
                or pd.api.types.is_string_dtype(values_dtype)
            )
        elif type == "number":
            return pd.api.types.is_numeric_dtype(values_dtype)
        else:
            raise ValueError(f"Type {type} is niet geldig, gebruik 'text' of 'number'")

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

    def get_possible_styling(
        self, type: str | None = None, dict_output: bool = False
    ) -> list:
        """Haal de mogelijke kolommen op voor de opmaak.

        Parameters
        ----------
        type: str | None
            Type van de laag, indien None worden alle kolommen opgehaald.
            Mogelijke waardes zijn: Polyline, Polygon, Marker, CircleMarker

        dict_output: bool
            Als True, wordt een dictionary met de kolommen en hun type terug gegeven.
            Anders wordt een lijst met de kolommen terug gegeven.

        Returns
        -------
        list[str] | dict[str, dict]
            Een lijst met de mogelijke kolommen of een dictionary met de kolommen en hun type.

        Notes
        -----

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
        | SVGname      | string | null    | naam van de marker       |

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
        Om default aan te passen gebruik `set_default_styling(df)`.
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
        all_possible_dict: dict = Polyline.copy()
        all_possible_dict.update(Polygon)
        all_possible_dict.update(Marker)
        all_possible_dict.update(CircleMarker)
        type_dict = {
            "Polyline": Polyline,
            "Polygon": Polygon,
            "Marker": Marker,
            "CircleMarker": CircleMarker,
        }
        if type is None and not dict_output:
            return list(set(all_possible_dict.keys()))
        # and dict_output:
        elif type is None:
            return all_possible_dict
        # and known type
        elif not dict_output:
            return list(type_dict[type].keys())
        # dict_output and type
        else:
            return list(type_dict[type])


@dataclass(config={"arbitrary_types_allowed": True})
class InspectionsToDatabase(ClassifyInspections):
    """Combineert de inspectieresultaten met de opmaak en slaat deze op in de database.

    Met deze functie wordt de gehele geojson onderdeel van 1 tabel in de database.
    Bij grote lagen is het aan te raden om deze als aparte tabel op te slaan,
    de aanpak voor het opslaan van grotere lagen in de database is te vinden onder modules `inspectieresultaten` in de documentatie.

    Attributes
    ----------
    data_adapter: DataAdapter
        DataAdapter object
    df_in_inspections: Optional[pd.DataFrame | gpd.GeoDataFrame] | None
        Input DataFrame met inspectieresultaten.
    df_in_legend: Optional[pd.DataFrame] | None
        DataFrame met standaard opmaak informatie.
    df_in_layers: Optional[pd.DataFrame] | None
        DataFrame met de lagen informatie.
    df_out: Optional[pd.DataFrame] | None
        Output DataFrame containing the filtered DataFrame.
    legend_schema: ClassVar[dict[str, str]]
        Schema DataFrame met de opmaak informatie
    layer_schema: ClassVar[dict[str, str]]
        Schema DataFrame met de layer informatie

    Notes
    -----
    Default waarden te overschrijven in de global variables:

    - max_rows = 10, Maximale toegestane rijen geodata in een database veld
    - index = 0, Index van df_in_layers waarin de geodata wordt opgeslagen

    De layers tabel geeft de mogelijkheid om de meer configuratie door te geven aan de viewer. Als deze niet aanwezig is, worden standaard opties gebruikt.
    Hier moet minimaal de volgende kolommen in zitten:

    - group_name: naam van de groep in de viewer waar de layer toegevoegd wordt.
    - layer_name: naam van de laag in de viewer.
    - layer_visible: of de laag direct zichtbaar is in de viewer (true), of dat hij moet worden aangezet door de gebruiker (false).

    Optionele kolommen voor de df_in_layers zijn:

    - layer_type: type van de laag, wordt standaard gevuld als geojson.
    - layer_tabel: naam van een overige tabel in de database die met geodaata is gevuld.
    - layer_wms_url: str
    URL van een WMS service die gebruikt kan worden voor de laag.
    Bij het inladen worden de volgende lagen opgehaald:
        - layer_wms_layer: str
        - layer_wms_style: str
    """

    data_adapter: DataAdapter
    df_in_inspections: Optional[pd.DataFrame | gpd.GeoDataFrame] | None = None
    df_in_legend: Optional[pd.DataFrame] | None = None
    df_in_layers: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None
    legend_schema: ClassVar[dict[str, str]] = {
        "id": "int",
        "color": "object",
    }
    layer_schema: ClassVar[dict[str, str]] = {
        "group_name": "object",
        "layer_name": "object",
        "layer_visible": "bool",
        "layer_type": "object",
    }

    def run(self, input: list[str], output: str):
        """Run het combineren van inspectieresultaten met opmaak voor het opslaan in de database.

        Parameters
        ----------
        input: list[str]
            Naam van de Data Adapters met inspectieresultaten, opmaak en lagen (in die volgorde).
            Resultaten en opmaak zijn verplicht, lagen zijn optioneel. Indien er geen informatie is meegegeven, worden standaard waardes gebruikt.

        output: str
            Naam van Data adapter voor de output

        Notes
        -----
        ...
        Raises
        ------
        UserWarning
            Als er meer dan max_rows rijen zijn in de inspectieresultaten.
        """
        self.df_in_inspections = self.data_adapter.input(input[0])
        self.df_in_legend = self.data_adapter.input(input[1], schema=self.legend_schema)
        if len(input) == 3:
            self.df_in_layers = self.data_adapter.input(
                input[2], schema=self.layer_schema
            )
        else:
            self.df_in_layers = pd.DataFrame(
                index=[0],
                data={
                    "group_name": ["Extra Kaartlagen"],
                    "layer_name": ["Inspectieresultaten"],
                    "layer_visible": ["true"],
                    "layer_type": ["geojson"],
                },
            )

        global_variables = self.data_adapter.config.global_variables
        options = global_variables.get("InspectionsToDatabase", {})
        max_rows = options.get("max_rows", 10)
        insert_layer_index = options.get("index", 0)

        if len(self.df_in_inspections) >= max_rows:
            raise UserWarning(
                f"Er zijn meer dan {max_rows} inspectieresultaten, dit kan de database belasten."
                + "Dit maximum is aan te passen met `max_rows` in de global variables"
            )

        self.df_out = self.df_in_layers.copy()

        if not isinstance(self.df_in_inspections, gpd.GeoDataFrame):
            self.df_in_inspections = gpd.GeoDataFrame(
                self.df_in_inspections, geometry=self.df_in_inspections["geometry"]
            )

        # voor een mooie popup in de legenda willen we de styling in een 'style' kolom
        style_columns = self.get_possible_styling()
        style_columns += ["x", "y", "symbol"]
        style_dicts = []
        for index, row in self.df_in_inspections.iterrows():
            style_dict = {}
            for col in style_columns:
                if col in self.df_in_inspections.columns:
                    style_dict[col] = row[col]
            style_dicts.append(style_dict)

        self.df_in_inspections = self.df_in_inspections.drop(
            columns=list(style_dicts[0].keys())
        )
        output_json = dict(json.loads(self.df_in_inspections.to_json()))
        number_features = len(output_json["features"])
        for i in range(number_features):
            output_json["features"][i]["properties"]["style"] = style_dicts[i]

        self.df_out["layer_data"] = ""
        self.df_out.loc[insert_layer_index, "layer_data"] = json.dumps(output_json)

        # if upper and lower boundary are present, combine them into a name
        if (
            set(["lower_boundary", "upper_boundary"]).issubset(
                self.df_in_legend.columns
            )
            and "name" not in self.df_in_legend.columns
        ):
            if "unit" in self.df_in_legend.columns:
                self.df_in_legend["name"] = self.df_in_legend.apply(
                    lambda x: f"{x['lower_boundary']} - {x['upper_boundary']} {x['unit']}",
                    axis=1,
                )
            else:
                self.df_in_legend["name"] = self.df_in_legend.apply(
                    lambda x: f"{x['lower_boundary']} - {x['upper_boundary']}", axis=1
                )
            self.df_in_legend = self.df_in_legend.drop(
                columns=["lower_boundary", "upper_boundary"]
            )

        # formateer voor de database
        # fix met nieuwe versie van pandas
        warnings.filterwarnings("ignore", category=FutureWarning)
        self.df_in_legend.fillna("", inplace=True)
        self.df_out["layer_legend"] = ""
        self.df_out.loc[insert_layer_index, "layer_legend"] = json.dumps(
            [value for key, value in self.df_in_legend.T.to_dict().items()]
        )
        self.data_adapter.output(output, self.df_out)

    def set_default_styling():
        raise NotImplementedError(
            "De standaard opmaak is niet van toepassing voor deze functie."
        )

    def get_default_styling():
        raise NotImplementedError(
            "De standaard opmaak is niet van toepassing voor deze functie."
        )
