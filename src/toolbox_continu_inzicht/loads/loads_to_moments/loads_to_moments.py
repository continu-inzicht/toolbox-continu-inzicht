from datetime import datetime, timedelta
from typing import ClassVar, Optional

import pandas as pd
import pandas.api.types as ptypes
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht.base.base_module import ToolboxBase
from toolbox_continu_inzicht.base.data_adapter import DataAdapter


@dataclass(config={"arbitrary_types_allowed": True})
class LoadsToMoments(ToolboxBase):
    """
    Met deze klasse kunnen waterstandsgegevens worden omgezet naar bepaalde momenten.
    Deze klasse bevat een methode genaamd 'run' die de waterstandsgegevens verwerkt en de resulterende momenten opslaat in een dataframe.

    Attributes
    ----------
    data_adapter: DataAdapter
        Een object van de klasse DataAdapter.
    df_in: Optional[pd.DataFrame] | None
        Het invoerdataframe met waterstandsgegevens. Standaard is dit None.
    df_out: Optional[pd.DataFrame] | None
        Het uitvoerdataframe met de resulterende momenten. Standaard is dit None.

    input_schema_loads: ClassVar[dict[str, str | list[str]]]
        Het schema van het invoerdataframe met waterstandsgegevens.

    Notes
    -----
    Het schema van het invoerdataframe is:

    - measurement_location_id: int64
    - parameter_id: int64
    - unit: object
    - date_time: datetime64[ns, UTC] of object
    - value: float64
    - value_type: object

    """

    data_adapter: DataAdapter

    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    input_schema_loads: ClassVar[dict[str, str | list[str]]] = {
        "measurement_location_id": "int64",
        "parameter_id": "int64",
        "unit": "object",
        "date_time": ["datetime64[ns, UTC]", "object"],
        "value": "float64",
        "value_type": "object",
    }

    def run(self, input: str, output: str) -> None:
        """
        Verwerkt de invoergegevens om momenten te berekenen en genereert het uitvoerdataframe.

        Parameters
        ----------
        input : str
            Naam van de dataadapter met invoergegevens.
        output : str
            Naam van de dataadapter om uitvoergegevens op te slaan.

        Returns
        -------
        None

        Raises
        ------
        AssertionError
            Als de invoergegevens niet voldoen aan de vereiste schema's.
        """

        self.df_in = self.data_adapter.input(input, schema=self.input_schema_loads)
        global_variables = self.data_adapter.config.global_variables
        moments = global_variables["moments"]

        options = global_variables.get("LoadsToMoments", {})
        tide = options.get("tide", False)
        calc_time = global_variables["calc_time"]

        is_datetime = ptypes.is_datetime64_any_dtype(self.df_in["date_time"])
        if not is_datetime:
            self.df_in["date_time"] = self.df_in["date_time"].apply(
                lambda x: datetime.fromisoformat(x)
            )

        df_moments = self.df_in.set_index("date_time")
        lst_dfs = []
        # TODO: In sommige gevallen willen we de meest dichtstbijzijnde waarde: zie TBCI-155
        dt_moments = [
            {"date_time": calc_time + timedelta(hours=moment), "hours": moment}
            for moment in moments
        ]

        if tide:
            locations = df_moments["measurement_location_id"].unique()
            df_moments_per_location = [
                df_moments[df_moments["measurement_location_id"] == loc].copy()
                for loc in locations
            ]
            for df_per_location in df_moments_per_location:
                for moment in dt_moments:
                    time_interval = (
                        df_per_location.index
                        > (moment["date_time"] - timedelta(hours=12.25))
                    ) & (
                        df_per_location.index
                        < (moment["date_time"] + timedelta(hours=12.25))
                    )

                    df_per_location_time_interval = df_per_location.loc[time_interval]
                    if len(df_per_location_time_interval) > 0:
                        time_max = df_per_location_time_interval["value"].idxmax()
                        assert type(time_max) is pd.Timestamp
                        df_moment = df_per_location.loc[[time_max]]

                    else:
                        df_moment = self.get_moment_from_dataframe(
                            moment, df_per_location
                        )

                    lst_dfs.append(df_moment)
        else:
            for moment in dt_moments:
                df_moment = self.get_moment_from_dataframe(moment, df_moments)
                lst_dfs.append(df_moment)

        self.df_out = pd.concat(lst_dfs)
        self.data_adapter.output(output=output, df=self.df_out)

    @staticmethod
    def get_moment_from_dataframe(
        moment: dict, df_moments: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Haalt het moment op uit een dataframe van momenten.

        Parameters
        ----------
        moment: dict
            Het moment dat moet worden opgehaald.
        df_moments: pd.DataFrame
            Het dataframe van momenten.

        Returns
        -------
        Het dataframe met het opgehaalde moment: pd.DataFrame
        """

        df_moment = pd.DataFrame()

        if moment["date_time"] in df_moments.index:
            df_moment = df_moments.loc[[moment["date_time"]]]
        else:
            df_filter = df_moments[df_moments.index < moment["date_time"]]
            if not df_filter.empty:
                df_moment = df_filter.iloc[[-1]]

        if not df_moment.empty:
            df_moment_hours = df_moment.copy()
            df_moment_hours["hours"] = moment["hours"]
            df_moment = df_moment_hours

        return df_moment
