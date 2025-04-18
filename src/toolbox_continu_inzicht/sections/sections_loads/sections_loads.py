"""
Bepaal de belasting op een dijkvak
"""

from pydantic.dataclasses import dataclass
from toolbox_continu_inzicht.base.base_module import ToolboxBase
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
import pandas as pd
from typing import ClassVar, Optional


@dataclass(config={"arbitrary_types_allowed": True})
class SectionsLoads(ToolboxBase):
    """
    Bepaal de belasting op een dijkvak gegeven een belasting

    Attributes
    ----------
    data_adapter: DataAdapter
        DataAdapter: De data adapter.
    df_in_sections: Optional[pd.DataFrame] | None
        DataFrame: lijst met dijkvakken.
    df_in_loads: Optional[pd.DataFrame] | None
        DataFrame: belasting per moment per meetlocaties.
    df_in_section_fractions: Optional[pd.DataFrame] | None
        DataFrame: koppeling van de maatgevende meetlocaties per dijkvak .
    df_out: Optional[pd.DataFrame] | None
        DataFrame: uitvoer.
    input_schema_sections: ClassVar[dict[str, str]]
        Schema voor de lijst met dijkvakken.
    input_schema_loads: ClassVar[dict[str, str]]
        Schema voor belasting per moment per meetlocaties.
    input_schema_section_fractions: ClassVar[dict[str, str]]
        Schema voor koppeling van de maatgevende meetlocaties per dijkvak door middel van verhoudingen.

    Notes
    -----
    **Input schema's**

    *input_schema_sections*: schema voor de lijst met dijkvakken

    - id: int64                         : id van het dijkvak
    - name: str                         : naam van de dijkvak

    *input_schema_loads*: schema voor belasting per moment per meetlocaties

    - measurement_location_id: int64    : id van het meetstation
    - parameter_id: int64               : id van de belastingparameter (1,2,3,4)
    - unit: str                         : eenheid van de belastingparameter
    - date_time: datetime64[ns, UTC]    : datum/ tijd van de tijdreeksitem
    - value: float64                    : waarde van de tijdreeksitem
    - value_type: str                   : type waarde van de tijdreeksitem (meting of verwacht)

    *input_schema_section_fractions*: schema voor koppeling van de maatgevende meetlocaties per dijkvak

    - id: int64                         : id van de dijkvak
    - idup: int64                       : id van bovenstrooms meetstation
    - iddown: int64                     : id van benedenstrooms meetstation
    - fractionup: float64               : fractie van bovenstrooms meetstation
    - fractiondown: float64             : fractie van benedestrooms meetstation


    **Output schema**

    *df_out (DataFrame)*: uitvoer

    - id: int64                         : id van het dijkvak
    - name; str                         : naam van de dijkvak
    - date_time: datetime64[ns, UTC]    : datum/ tijd van de tijdreeksitem
    - value: float64                    : waarde van de tijdreeksitem
    - unit: str                         : eenheid van de belastingparameter
    - parameter_id: int64               : id van de belastingparameter (1,2,3,4)
    - value_type: str                   : type waarde van de tijdreeksitem (meting of verwacht)
    """

    data_adapter: DataAdapter
    df_in_sections: Optional[pd.DataFrame] | None = None
    df_in_loads: Optional[pd.DataFrame] | None = None
    df_in_section_fractions: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    # Lijst met dijkvakken
    input_schema_sections: ClassVar[dict[str, str]] = {"id": "int64", "name": "object"}
    """Schema voor de lijst met dijkvakken"""

    # belasting per moment per meetlocaties
    input_schema_loads: ClassVar[dict[str, str]] = {
        "measurement_location_id": "int64",
        "parameter_id": "int64",
        "unit": "object",
        "date_time": ["datetime64[ns, UTC]", "object"],
        "value": "float64",
        "value_type": "object",
    }

    # koppeling van de maatgevende meetlocaties per dijkvak
    input_schema_section_fractions: ClassVar[dict[str, str]] = {
        "idup": "int64",
        "iddown": "int64",
        "fractionup": "float64",
        "fractiondown": "float64",
    }

    def run(self, input: list[str], output: str) -> None:
        """Bepalen de belasting op een dijkvak.

        Parameters
        ----------
        input: list[str]
            lijst van data adapters met: dijkvakken, belasting per moment per meetlocaties en koppeling van de maatgevende meetlocaties per dijkvak
        output: str
            Data adapter voor koppeling van de maatgevende meetlocaties per dijkvak

        Raises
        ------
        UserWarning
            Als de input variabele niet 3 string waarden bevat.
        """
        if not len(input) == 3:
            raise UserWarning("Input variabele moet 3 string waarden bevatten.")

        # invoer 1: lijst met dijkvakken
        self.df_in_sections = self.data_adapter.input(
            input[0], self.input_schema_sections
        )

        # invoer 2: belastingen van alle meetlocaties
        self.df_in_loads = self.data_adapter.input(input[1], self.input_schema_loads)

        # invoer 3: koppeling van de maatgevende meetlocaties per dijkvak
        self.df_in_section_fractions = self.data_adapter.input(
            input[2], self.input_schema_section_fractions
        )

        # Datum als string omzetten naar datetime object
        if not pd.api.types.is_datetime64_any_dtype(self.df_in_loads["date_time"]):
            self.df_in_loads["date_time"] = pd.to_datetime(
                self.df_in_loads["date_time"]
            )

        # uitvoer: belasting per dijkvak
        self.df_out = pd.DataFrame()

        df_sections = self.df_in_sections.copy()

        # Filter benodigde kolommen uit belastingdata
        df_loads = self.df_in_loads.copy()
        df_loads = df_loads[
            [
                "measurement_location_id",
                "date_time",
                "value",
                "unit",
                "parameter_id",
                "value_type",
            ]
        ]

        # Lijst met koppeling dijkvak en bovenstrooms en benedenstrooms meetstation
        df_section_station = self.df_in_section_fractions.copy()
        df_section_station = df_section_station.set_index("id")

        # voeg dijkvakken samen met de maatgevende meetlocatie-ids
        df_section_fractions = df_sections.merge(
            df_section_station, on="id", how="outer"
        )

        # tijdelijke tabellen voor koppeling met belasting
        df_loads_up = df_loads.copy()
        df_loads_up = df_loads_up.rename(
            columns={
                "measurement_location_id": "idup",
                "value": "value_up",
                "date_time": "date_time_up",
                "value_type": "value_type_up",
                "parameter_id": "parameter_id_up",
                "unit": "unit_up",
            }
        )

        df_loads_down = df_loads.copy()
        df_loads_down = df_loads_down.rename(
            columns={"measurement_location_id": "iddown", "value": "value_down"}
        )

        # voeg de dijkvakken en belastingen samen
        df_merged = df_section_fractions.merge(
            df_loads_up, on="idup", how="left"
        ).rename(columns={"date_time_up": "date_time"})
        df_merged = df_merged.merge(
            df_loads_down, on=["iddown", "date_time"], how="left"
        )
        df_merged["value"] = (
            df_merged["value_up"] * df_merged["fractionup"]
            + df_merged["value_down"] * df_merged["fractiondown"]
        )

        # verwijder alle rijen waar geen data voor is gevonden
        df_cleaned = df_merged.dropna(
            subset=["id", "date_time", "value", "unit", "parameter_id", "value_type"]
        )

        self.df_out = df_cleaned[
            ["id", "name", "date_time", "value", "unit", "parameter_id", "value_type"]
        ]
        self.df_out.set_index(["id", "name", "date_time"], inplace=False)

        self.data_adapter.output(output=output, df=self.df_out)
