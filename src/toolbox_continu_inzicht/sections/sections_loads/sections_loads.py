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
    input: str
    input2: str
    input3: str
    output: str

    df_in: Optional[pd.DataFrame] | None = None
    df_in2: Optional[pd.DataFrame] | None = None
    df_in3: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    async def run(self, input=None, input2=None, input3=None, output=None) -> None:
        """
        Uitvoeren van het bepalen van de belasting op een dijkvak.

        Args:
            input  (str): input sectie van het yaml-bestand:
                          lijst met dijkvakken
            input2 (str): tweede input sectie van het yaml-bestand:
                          belastingen van alle meetlocaties
            output (str): uitvoer sectie van het yaml-bestand:
                          koppeling van de maatgevende meetlocaties per dijkvak

        Returns:
            Dataframe: Pandas dataframe
        """
        if input is None:
            input = self.input
        if input2 is None:
            input2 = self.input2
        if input3 is None:
            input3 = self.input3
        if output is None:
            output = self.output

        # invoer 1: lijst met dijkvakken
        self.df_in = self.data_adapter.input(input)

        # invoer 2: belastingen van alle meetlocaties
        self.df_in2 = self.data_adapter.input(input2)

        # invoer 3: koppeling van de maatgevende meetlocaties per dijkvak
        self.df_in3 = self.data_adapter.input(input3)

        # uitvoer: belasting per dijkvak
        self.df_out = pd.DataFrame()

        df_sections = self.df_in.copy()

        df_loads = self.df_in2.copy()
        df_loads = df_loads[["objectid", "parameterid", "datetime", "value"]]
        df_loads = df_loads[df_loads["parameterid"] == 1]

        df_section_station = self.df_in3.copy()
        df_section_station = df_section_station.set_index("id")

        # voeg dijkvakken samen met de maatgevende meetlocatie-ids
        df_fractions = df_sections.merge(df_section_station, on="id", how="outer")

        # tijdelijke tabellen voor koppeling met belasting
        df_loads_up = df_loads.copy()
        df_loads_up = df_loads_up.rename(
            columns={
                "objectid": "idup",
                "value": "value_up",
                "parameterid": "parameterid_up",
                "datetime": "datetime_up",
            }
        )
        df_loads_down = df_loads.copy()
        df_loads_down = df_loads_down.rename(
            columns={
                "objectid": "iddown",
                "value": "value_down",
                "parameterid": "parameterid_down",
            }
        )

        # voeg de dijkvakken en belastingen samen
        df_merged = df_fractions.merge(df_loads_up, on="idup", how="left").rename(
            columns={"datetime_up": "datetime"}
        )
        df_merged = df_merged.merge(
            df_loads_down, on=["iddown", "datetime"], how="left"
        )
        df_merged["value"] = (
            df_merged["value_up"] * df_merged["fractionup"]
            + df_merged["value_down"] * df_merged["fractiondown"]
        )

        self.df_out = df_merged[
            ["id", "name", "parameterid_up", "datetime", "value"]
        ].rename(columns={"parameterid_up": "parameterid"})
        self.data_adapter.output(output=output, df=self.df_out)

        return self.df_out
