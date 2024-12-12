from pydantic.dataclasses import dataclass
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from typing import Optional

import pandas as pd


@dataclass(config={"arbitrary_types_allowed": True})
class SectionsFailureprobability:
    """
    Bepaal de belasting op een dijkvak
    """

    data_adapter: DataAdapter

    df_in_section_loads: Optional[pd.DataFrame] | None = None
    df_in_fragility_curves: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    # faalkans per moment per dijkvak
    input_schema_failureprobability = {
        "section_id": "int64",
        "date_time": "datetime64[ns, UTC]",
        "value": "float64",
    }

    def run(self, input: str, output: str) -> None:
        """
        Uitvoeren van het bepalen van de faalkans van een dijkvak.

        Args:
            input List(str): faalkans per dijkvak
            output (str):    maatgevende faalkans per dijkvak

        Returns: TODO RW aanpassen
            Dataframe: Pandas dataframe geschikt voor uitvoer:
            definition:
                - Meetlocatie id (measurement_location_id)
                - Meetlocatie code (measurement_location_code)
                - Meetlocatie omschrijving/naam (measurement_location_description)
                - Parameter id overeenkomstig Aquo-standaard: ‘4724’ (parameter_id)
                - Parameter code overeenkomstig Aquo-standaard: ‘WATHTE’ (parameter_code)
                - Parameter omschrijving overeenkomstig Aquo-standaard: ‘Waterhoogte’ (parameter_description)
                - Eenheid (unit)
                - Datum en tijd (date_time)
                - Waarde (value)
                - Type waarde: meting of verwachting (value_type)
        """

        # invoer: faalskans per dijkvak
        self.df_in_failureprobability = self.data_adapter.input(
            input, self.input_schema_failureprobability
        )

        # uitvoer: belasting per dijkvak
        self.df_out = pd.DataFrame()

        df = self.df_in_failureprobability.copy()
        df = df[
            [
                "section_id",
                "failuremechanism_id",
                "value_parameter_id",
                "parameter_id",
                "date_time",
                "value",
            ]
        ].reset_index(drop=True)
        df = df.loc[
            df.groupby(
                ["section_id", "failuremechanism_id", "value_parameter_id", "date_time"]
            )["parameter_id"].idxmax()
        ]
        df = df.assign(measureid=0)

        self.df_out = df[
            [
                "section_id",
                "failuremechanism_id",
                "parameter_id",
                "value_parameter_id",
                "date_time",
                "value",
            ]
        ].reset_index(drop=True)
        self.df_out = self.df_out.rename(columns={"value": "failureprobability"})

        self.data_adapter.output(output=output, df=self.df_out)
