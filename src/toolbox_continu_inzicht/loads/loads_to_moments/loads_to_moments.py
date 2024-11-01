from pydantic.dataclasses import dataclass
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
import pandas as pd
from typing import Optional
from datetime import datetime, timezone, timedelta


@dataclass(config={"arbitrary_types_allowed": True})
class LoadsToMoments:
    """
    Met deze functie worden de waterstanden met opgegeven grenzen geclassificeerd
    """

    data_adapter: DataAdapter

    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    def run(self, input: str, output: str):
        self.df_in = self.data_adapter.input(input)
        # TODO add validate schema
        global_variables = self.data_adapter.config.global_variables
        moments = global_variables["moments"]
        options = global_variables["LoadsMaxima"]
        tide = options["tide"]

        dt_now = datetime.now(timezone.utc)
        t_now = datetime(
            dt_now.year,
            dt_now.month,
            dt_now.day,
            dt_now.hour,
            0,
            0,
        ).replace(tzinfo=timezone.utc)

        self.df_in["datetime"] = self.df_in["datetime"].apply(
            lambda x: datetime.fromisoformat(x)
        )
        df_moments = self.df_in.set_index("datetime")
        lst_dfs = []
        dt_moments = [t_now + timedelta(hours=moment) for moment in moments]

        if tide:
            locations = df_moments["measurement_location_id"].unique()
            df_moments_per_location = [
                df_moments[df_moments["measurement_location_id"] == loc].copy()
                for loc in locations
            ]
            for df_per_location in df_moments_per_location:
                for moment in dt_moments:
                    time_interval = (
                        df_per_location.index > (moment - timedelta(hours=12.25))
                    ) & (df_per_location.index < (moment + timedelta(hours=12.25)))

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
            for index, moment in enumerate(dt_moments):
                df_moment = self.get_moment_from_dataframe(moment, df_moments)
                lst_dfs.append(df_moment)

        self.df_out = pd.concat(lst_dfs)
        self.data_adapter.output(output=output, df=self.df_out)

    @staticmethod
    def get_moment_from_dataframe(moment, df_moments):
        if moment in df_moments.index:
            df_moment = df_moments.loc[[moment]]
        else:
            df_moment = df_moments[df_moments.index < moment].iloc[[-1]]
            ## evntueel ook een optie:
            # df_moment = df_moments[df_moments.index < moment].iloc[[0]]
        return df_moment
