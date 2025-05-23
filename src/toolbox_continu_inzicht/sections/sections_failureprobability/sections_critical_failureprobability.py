"""
Bepaal de faalkans van een dijkvak
"""

from pydantic.dataclasses import dataclass
from toolbox_continu_inzicht.base.base_module import ToolboxBase
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from typing import ClassVar, Optional

import pandas as pd


@dataclass(config={"arbitrary_types_allowed": True})
class SectionsCriticalFailureprobability(ToolboxBase):
    """
    Bepaal de maatgevende faalkans van een dijkvak gegeven de technische faalkans, maatregel en beheerdersoordeel.

    Attributes
    ----------
    data_adapter : DataAdapter
        DataAdapter object voor het verwerken van gegevens.
    df_in_failureprobability : Optional[pd.DataFrame] | None
        Invoer DataFrame met faalkans per dijkvak. Standaardwaarde is None.
    df_out : Optional[pd.DataFrame] | None
        Uitvoer DataFrame met faalkans per dijkvak. Standaardwaarde is None.
    input_schema_failureprobability : ClassVar[dict[str, str]]
        Schema voor de invoer van de faalkans per dijkvak.

    Notes
    -----

    **Input schema's**

    *input_schema_failureprobability*: schema voor de lijst met dijkvakken

    - section_id: int64                 : id van de dijkvak
    - failuremechanism_id: int64        : id van het faalmechanisme
    - value_parameter_id: int64         : id van de belastingparameter (1,2,3,4)
    - parameter_id: int64               : id van de faalkans parameter (5,100,101,102)
    - date_time: datetime64[ns, UTC]    : datum/ tijd van de tijdreeksitem
    - value: float64                    : belasting van de tijdreeksitem

    **Output schema**

    *df_out*: uitvoer

    - section_id: int64                 : id van het dijkvak
    - failuremechanism_id: int64        : id van het faalmechanisme
    - value_parameter_id: int64         : id van de belastingparameter (1,2,3,4)
    - parameter_id: int64               : id van de faalkans parameter (5,100,101,102)
    - date_time: datetime64[ns, UTC]    : datum/ tijd van de tijdreeksitem
    - failureprobability: float64       : faalkans van de tijdreeksitem
    """

    data_adapter: DataAdapter
    df_in_failureprobability: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    # faalkans per moment per dijkvak
    input_schema_failureprobability: ClassVar[dict[str, str]] = {
        "section_id": "int64",
        "failuremechanism_id": "int64",
        "value_parameter_id": "int64",
        "parameter_id": "int64",
        "date_time": ["datetime64[ns, UTC]", "object"],
        "value": "float64",
    }

    def run(self, input: str, output: str) -> None:
        """
        Uitvoeren van het bepalen van de faalkans van een dijkvak.

        Parameters:
        ----------
        input: str
            Naam van de data adapter van Faalkans per dijkvak
        output: str

            uitvoer data adapter: koppeling van de maatgevende meetlocaties per dijkvak
            Dataframe: Pandas dataframe geschikt voor uitvoer:

            - Meetlocatie id (measurement_location_id)
            - Meetlocatie code (measurement_location_code)
            - Meetlocatie omschrijving/naam (measurement_location_description)
            - Parameter id overeenkomstig Aquo-standaard: '4724' (parameter_id)
            - Parameter code overeenkomstig Aquo-standaard: 'WATHTE' (parameter_code)
            - Parameter omschrijving overeenkomstig Aquo-standaard: 'Waterhoogte' (parameter_description)
            - Eenheid (unit)
            - Datum en tijd (date_time)
            - Waarde (value)
            - Type waarde: meting of verwachting (value_type)
        """

        # invoer: faalskans per dijkvak
        self.df_in_failureprobability = self.data_adapter.input(
            input, self.input_schema_failureprobability
        )

        # Datum als string omzetten naar datetime object
        if not pd.api.types.is_datetime64_any_dtype(
            self.df_in_failureprobability["date_time"]
        ):
            self.df_in_failureprobability["date_time"] = pd.to_datetime(
                self.df_in_failureprobability["date_time"]
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
