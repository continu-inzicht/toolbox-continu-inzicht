from pydantic.dataclasses import dataclass
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
import pandas as pd
import pandas.api.types as ptypes
from typing import Optional
from datetime import datetime, timedelta


@dataclass(config={"arbitrary_types_allowed": True})
class LoadsToMoments:
    """
    Met deze klasse kunnen waterstandsgegevens worden omgezet naar bepaalde momenten.
    Deze klasse bevat een methode genaamd 'run' die de waterstandsgegevens verwerkt en de resulterende momenten opslaat in een dataframe.

    Attributes:
        data_adapter (DataAdapter): Een object van de klasse DataAdapter.
        df_in (Optional[pd.DataFrame] | None): Het invoerdataframe met waterstandsgegevens. Standaard is dit None.
        df_out (Optional[pd.DataFrame] | None): Het uitvoerdataframe met de resulterende momenten. Standaard is dit None.
    Methods:
        run(input: str, output: str): Verwerkt de waterstandsgegevens en slaat de resulterende momenten op in het uitvoerdataframe.
        get_moment_from_dataframe(moment, df_moments): Haalt het moment op uit het gegeven dataframe met waterstandsgegevens.

    """

    data_adapter: DataAdapter

    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    def run(self, input: str, output: str):
        """
        Verwerkt de invoergegevens om momenten te berekenen en genereert het uitvoerdataframe.

        Args:
            input (str): Naam van de dataadapter met invoergegevens.
            output (str): Naam van de dataadapter om uitvoergegevens op te slaan.
        Returns:
            None
        """

        self.df_in = self.data_adapter.input(input)
        # TODO add validate schema
        global_variables = self.data_adapter.config.global_variables
        moments = global_variables["moments"]

        # TODO: er kunnen meerdere opties zijn voor LoadsToMoments
        if "LoadsToMoments" in global_variables:
            options = global_variables["LoadsToMoments"]
            tide = options["tide"]
        else:
            tide = False

        calc_time = global_variables["calc_time"]

        is_datetime = ptypes.is_datetime64_any_dtype(self.df_in["date_time"])
        if not is_datetime:
            self.df_in["date_time"] = self.df_in["date_time"].apply(
                lambda x: datetime.fromisoformat(x)
            )

        df_moments = self.df_in.set_index("date_time")
        lst_dfs = []

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
    def get_moment_from_dataframe(moment, df_moments):
        """
        Haalt het moment op uit een dataframe van momenten.
        Parameters:
            - moment: Het moment dat moet worden opgehaald.
            - df_moments: Het dataframe van momenten.
        Returns:
            Het dataframe met het opgehaalde moment.
        """

        df_moment = pd.DataFrame()

        if moment["date_time"] in df_moments.index:
            df_moment = df_moments.loc[[moment["date_time"]]]
        else:
            df_filter = df_moments[df_moments.index < moment["date_time"]]
            if not df_filter.empty:
                df_moment = df_filter.iloc[[-1]]

            ## evntueel ook een optie:
            # df_moment = df_moments[df_moments.index < moment["date_time"]].iloc[[0]]

        if not df_moment.empty:
            df_moment["hours"] = moment["hours"]

        return df_moment
