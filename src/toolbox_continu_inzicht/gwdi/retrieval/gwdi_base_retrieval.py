import pandas as pd
from datetime import timedelta

import xarray as xr


class GwdiWiwbRetrievalBase:
    @staticmethod
    def normalize_locations_table(df_locations: pd.DataFrame):
        df_locations_norm = df_locations.copy()

        if df_locations_norm["fid"].isna().any():
            raise UserWarning("GWDI locaties bevatten lege `fid`-waarden.")

        if not df_locations_norm["fid"].is_unique:
            raise UserWarning("GWDI locaties bevatten dubbele `fid`-waarden")

        df_locations_norm["fid"] = df_locations_norm["fid"].astype(int)
        df_locations_norm["name"] = df_locations_norm["name"].astype(str)
        df_locations_norm["x"] = pd.to_numeric(df_locations_norm["x"], errors="raise")
        df_locations_norm["y"] = pd.to_numeric(df_locations_norm["y"], errors="raise")
        df_locations_norm = df_locations_norm.sort_values("fid").reset_index(drop=True)

        return df_locations_norm

    @staticmethod
    def resolve_publish_window(
        calc_time: pd.Timestamp | str,
        publish_days: int | str,
        target_date: pd.Timestamp | str | None = None,
    ) -> tuple[pd.Timestamp, pd.Timestamp]:
        calc_timestamp = pd.Timestamp(calc_time)
        if calc_timestamp.tz is not None:
            calc_timestamp = calc_timestamp.tz_localize(None)

        if target_date is None:
            publish_end = (calc_timestamp - timedelta(days=1)).normalize()
        else:
            publish_end = pd.Timestamp(target_date).normalize()
            if publish_end.tz is not None:
                publish_end = publish_end.tz_localize(None)

        publish_days_int = int(publish_days)
        if publish_days_int <= 0:
            raise UserWarning("`publish_days` moet groter zijn dan 0.")
        publish_start = publish_end - timedelta(days=publish_days_int - 1)

        return publish_start, publish_end

    @staticmethod
    def sample_points_from_dataset(
        dataset: xr.Dataset,
        locations_table: pd.DataFrame,
        window_start: pd.Timestamp,
        window_end: pd.Timestamp,
        variable_name: str,
        time_name: str,
        x_name: str,
        y_name: str,
    ) -> xr.Dataset:
        # 1) Validate input schema on the selected source variable.
        if variable_name not in dataset.data_vars:
            raise UserWarning(f"Variabele `{variable_name}` ontbreekt in brondata.")
        data_array = dataset[variable_name]
        if time_name not in data_array.dims:
            raise ValueError(f"Brondata mist tijdsdimensie `{time_name}`.")
        if x_name not in data_array.coords or y_name not in data_array.coords:
            raise ValueError(f"Brondata mist coördinaten (`{x_name}`/`{y_name}`).")

        # 2) Apply the requested time window and spatial nearest-neighbour sampling.
        data_array = data_array.sel({time_name: slice(window_start, window_end)})
        sample_x = locations_table["x"].to_numpy(dtype=float)
        sample_y = locations_table["y"].to_numpy(dtype=float)
        fid_values = locations_table["fid"].astype(int).to_numpy()
        x_indexer = xr.DataArray(sample_x, dims=("fid",), coords={"fid": fid_values})
        y_indexer = xr.DataArray(sample_y, dims=("fid",), coords={"fid": fid_values})
        data_array = data_array.sel(
            {x_name: x_indexer, y_name: y_indexer},
            method="nearest",
        )

        # 3) Collapse any remaining non-time/non-fid dimensions to one slice.
        extra_dims = [dim for dim in data_array.dims if dim not in {time_name, "fid"}]
        if len(extra_dims) > 0:
            data_array = data_array.isel({dim: 0 for dim in extra_dims}, drop=True)

        # 4) Normalize output ordering and keep only the coordinates needed downstream.
        data_array = data_array.sortby(time_name)
        ds_out = data_array.transpose(time_name, "fid").to_dataset(name=variable_name)
        keep_coords = {time_name, "fid", x_name, y_name}
        drop_coords = [coord for coord in ds_out.coords if coord not in keep_coords]
        if len(drop_coords) > 0:
            ds_out = ds_out.drop_vars(drop_coords)

        return ds_out

    @staticmethod
    def resample_timeseries(
        df: pd.DataFrame,
        value_column: str,
        options: dict,
    ) -> pd.DataFrame:
        required_columns = {"time", "fid", value_column}
        if not required_columns.issubset(df.columns):
            raise UserWarning(
                f"Resample-input mist verplichte kolommen: {sorted(required_columns)}."
            )

        df_prepared = df.loc[:, ["time", "fid", value_column]].copy()
        df_prepared["time"] = pd.to_datetime(df_prepared["time"])
        df_prepared["fid"] = df_prepared["fid"].astype(int)
        df_prepared = df_prepared.sort_values(["time", "fid"]).reset_index(drop=True)

        resample_frequency = options.get("resample_frequency")
        if resample_frequency in (None, ""):
            if df_prepared[["time", "fid"]].duplicated().any():
                raise UserWarning("Resample-input bevat dubbele (`time`, `fid`)-rijen.")
            return df_prepared

        period_start_raw = options.get("resample_period_start")
        period_end_raw = options.get("resample_period_end")
        if period_start_raw in (None, "") or period_end_raw in (None, ""):
            raise UserWarning(
                "Bij `resample_frequency` zijn ook `resample_period_start` en "
                "`resample_period_end` verplicht."
            )

        period_start = pd.Timestamp(period_start_raw)
        period_end = pd.Timestamp(period_end_raw)
        if period_start.tz is not None:
            period_start = period_start.tz_localize(None)
        if period_end.tz is not None:
            period_end = period_end.tz_localize(None)

        if period_end < period_start:
            raise UserWarning(
                "`resample_period_end` moet groter of gelijk zijn aan "
                "`resample_period_start`."
            )

        df_filtered = df_prepared[
            df_prepared["time"].between(period_start, period_end)
        ].copy()
        if len(df_filtered) == 0:
            raise UserWarning(
                "Resample-periode bevat geen data (`time` buiten opgegeven venster)."
            )

        df_resampled = (
            df_filtered.groupby("fid")
            .resample(
                str(resample_frequency),
                on="time",
                label="left",
                closed="left",
                origin=period_start,
            )[value_column]
            .sum()
            .reset_index()
        )
        df_resampled["time"] = pd.to_datetime(df_resampled["time"])
        df_resampled["fid"] = df_resampled["fid"].astype(int)
        df_resampled = df_resampled.sort_values(["time", "fid"]).reset_index(drop=True)

        if df_resampled[["time", "fid"]].duplicated().any():
            raise UserWarning("Resampled data bevat dubbele (`time`, `fid`)-rijen.")

        return df_resampled.loc[:, ["time", "fid", value_column]]
