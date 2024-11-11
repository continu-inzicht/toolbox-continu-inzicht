from pydantic.dataclasses import dataclass
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
import pandas as pd
from typing import Optional


@dataclass(config={"arbitrary_types_allowed": True})
class SectionsLoads:
    """
    Bepaal de belasting op een dijkvak
    """

    data_adapter: DataAdapter

    df_in_sections: Optional[pd.DataFrame] | None = None
    df_in_loads: Optional[pd.DataFrame] | None = None
    df_in_section_fractions: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    # Kolommen schema van de invoer data

    # Lijst met dijkvakken
    input_schema_sections = {"id": "int64", "name": "object"}

    # belasting per moment per meetlocaties
    input_schema_loads = {
        "measurement_location_id": "int64",
        "parameter_id": "int64",
        "unit": "object",
        "date_time": "object",
        "value": "float64",
        "value_type": "object",
    }

    # koppeling van de maatgevende meetlocaties per dijkvak
    input_schema_section_fractions = {
        "idup": "int64",
        "iddown": "int64",
        "fractionup": "float64",
        "fractiondown": "float64",
    }

    def run(self, input: list[str], output: str) -> None:
        """
        Uitvoeren van het bepalen van de belasting op een dijkvak.

        Args:
            input List(str): [0] lijst met dijkvakken
                             [1] belasting per moment per meetlocaties
                             [2] koppeling van de maatgevende meetlocaties per dijkvak
            output (str): uitvoer sectie van het yaml-bestand:
                          koppeling van de maatgevende meetlocaties per dijkvak

        Returns:
            Dataframe: Pandas dataframe
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

        self.df_out = df_merged[
            ["id", "name", "date_time", "value", "unit", "parameter_id", "value_type"]
        ]
        self.df_out.set_index(["id", "name", "date_time"], inplace=False)

        self.data_adapter.output(output=output, df=self.df_out)

        return self.df_out
