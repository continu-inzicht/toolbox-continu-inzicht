import pandas as pd
from datetime import timedelta

import xarray as xr


class GwdiWiwbRetrievalBase:
    """Shared GWDI retrieval utilities for WIWB and KNMI climate inputs."""

    @staticmethod
    def normalize_locations_table(df_locations: pd.DataFrame) -> pd.DataFrame:
        """Validate and normalize GWDI sampling locations.

        Parameters
        ----------
        df_locations : pd.DataFrame
            Input location table with at least ``fid``, ``name``, ``x`` and ``y``.

        Returns
        -------
        pandas.DataFrame
            Normalized copy with integer ``fid``, numeric coordinates and rows sorted
            by ``fid``.

        Raises
        ------
        UserWarning
            If ``fid`` contains missing or duplicate values.
        """
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
        """Resolve publish window at daily precision.

        Parameters
        ----------
        calc_time : pd.Timestamp | str
            Calculation timestamp.
        publish_days : int | str
            Number of publish days to include.
        target_date : pd.Timestamp | str | None, optional
            Optional explicit publish end date. If omitted, ``calc_time - 1 day`` is
            used.

        Returns
        -------
        tuple[pd.Timestamp, pd.Timestamp]
            ``(publish_start, publish_end)``.

        Raises
        ------
        UserWarning
            If ``publish_days <= 0``.
        """
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
    def resolve_source_window(
        publish_start: pd.Timestamp,
        publish_end: pd.Timestamp,
        options: dict[str, object],
    ) -> tuple[pd.Timestamp, pd.Timestamp]:
        """Resolve source retrieval window from publish window.

        Parameters
        ----------
        publish_start : pd.Timestamp
            Start of the publish window.
        publish_end : pd.Timestamp
            End of the publish window.
        options : dict[str, object]
            Retrieval options containing optional ``lag_days``.

        Returns
        -------
        tuple[pd.Timestamp, pd.Timestamp]
            ``(source_start, source_end)``.

        Raises
        ------
        UserWarning
            If ``lag_days < 0`` or if the lagged end falls before publish start.

        Notes
        -----
        Source span follows publish span; only lag shifts the end backward.
        """
        lag_days = int(options.get("lag_days", 0))
        if lag_days < 0:
            raise UserWarning("`lag_days` moet groter of gelijk zijn aan 0.")

        source_end = publish_end - timedelta(days=lag_days)
        if source_end < publish_start:
            raise UserWarning(
                "Geen bronvenster beschikbaar binnen het publicatievenster."
            )

        return publish_start, source_end

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
        input_crs: str | None = None,
    ) -> xr.Dataset:
        """Sample a gridded dataset at point locations without temporal aggregation.

        Parameters
        ----------
        dataset : xr.Dataset
            Source dataset.
        locations_table : pd.DataFrame
            Location table with ``fid``, ``x`` and ``y``.
        window_start : pd.Timestamp
            Start of the time slice.
        window_end : pd.Timestamp
            End of the time slice.
        variable_name : str
            Source variable to sample.
        time_name : str
            Time dimension name.
        x_name : str
            X coordinate name.
        y_name : str
            Y coordinate name.
        input_crs : str | None, optional
            CRS of input location coordinates. If provided, points are transformed to
            source CRS prior to sampling.

        Returns
        -------
        xr.Dataset
            Dataset containing sampled values in ``(time, fid)`` shape.

        Raises
        ------
        UserWarning
            If the requested variable is missing.
        ValueError
            If required dimensions/coordinates are missing.

        Notes
        -----
        This method only slices and samples. It does not perform temporal
        aggregation or rate-to-amount conversion.
        """
        # 1) Validate input schema on the selected source variable.
        if variable_name not in dataset.data_vars:
            raise UserWarning(f"Variabele `{variable_name}` ontbreekt in brondata.")
        data_array = dataset[variable_name]
        if time_name not in data_array.dims:
            raise ValueError(f"Brondata mist tijdsdimensie `{time_name}`.")
        if x_name not in data_array.coords or y_name not in data_array.coords:
            raise ValueError(f"Brondata mist coördinaten (`{x_name}`/`{y_name}`).")

        locations_for_sampling = (
            GwdiWiwbRetrievalBase.transform_locations_to_dataset_crs(
                locations_table=locations_table,
                dataset=dataset,
                data_array=data_array,
                input_crs=input_crs,
                x_name=x_name,
                y_name=y_name,
            )
        )

        # 2) Apply the requested time window and spatial nearest-neighbour sampling.
        data_array = data_array.sel({time_name: slice(window_start, window_end)})
        sample_x = locations_for_sampling["x"].to_numpy(dtype=float)
        sample_y = locations_for_sampling["y"].to_numpy(dtype=float)
        fid_values = locations_for_sampling["fid"].astype(int).to_numpy()
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
    def infer_dataset_crs(dataset: xr.Dataset, data_array: xr.DataArray) -> str | None:
        """Infer dataset CRS from CF grid-mapping metadata.

        Parameters
        ----------
        dataset : xr.Dataset
            Source dataset containing grid-mapping variables.
        data_array : xr.DataArray
            Data variable with optional ``grid_mapping`` attribute.

        Returns
        -------
        str or None
            CRS representation suitable for ``pyproj`` (PROJ/WKT), or ``None`` when
            inference is not possible.
        """
        grid_mapping_name = data_array.attrs.get("grid_mapping")
        if grid_mapping_name is None:
            return None
        if grid_mapping_name not in dataset:
            return None

        mapping = dataset[grid_mapping_name]
        mapping_attrs = mapping.attrs

        proj4 = mapping_attrs.get("proj4_params")
        if isinstance(proj4, str) and proj4.strip() != "":
            return proj4

        crs_wkt = mapping_attrs.get("crs_wkt") or mapping_attrs.get("spatial_ref")
        if isinstance(crs_wkt, str) and crs_wkt.strip() != "":
            return crs_wkt

        # Fallback for datasets that provide only minimal CF metadata.
        # Build a PROJ string from available grid-mapping attributes.
        if mapping_attrs.get("grid_mapping_name") == "polar_stereographic":
            required_keys = {
                "longitude_of_projection_origin",
                "latitude_of_projection_origin",
                "standard_parallel",
                "false_easting",
                "false_northing",
                "semi_major_axis",
                "semi_minor_axis",
            }
            if not required_keys.issubset(mapping_attrs):
                return None

            lon0 = float(mapping_attrs["longitude_of_projection_origin"])
            lat0 = float(mapping_attrs["latitude_of_projection_origin"])
            lat_ts = float(mapping_attrs["standard_parallel"])
            x0 = float(mapping_attrs["false_easting"])
            y0 = float(mapping_attrs["false_northing"])
            a_raw = float(mapping_attrs["semi_major_axis"])
            b_raw = float(mapping_attrs["semi_minor_axis"])

            # WIWB radar exports often use km-based coordinates and km-sized axes in
            # the metadata. Convert axes to meters and keep projected units in km.
            if a_raw < 10000.0 and b_raw < 10000.0:
                a = a_raw * 1000.0
                b = b_raw * 1000.0
                units = "km"
            else:
                a = a_raw
                b = b_raw
                units = "m"

            return (
                f"+proj=stere +lat_0={lat0} +lat_ts={lat_ts} +lon_0={lon0} "
                f"+x_0={x0} +y_0={y0} +a={a} +b={b} +units={units}"
            )

        return None

    @staticmethod
    def transform_locations_to_dataset_crs(
        locations_table: pd.DataFrame,
        dataset: xr.Dataset,
        data_array: xr.DataArray,
        input_crs: str | None,
        x_name: str,
        y_name: str,
    ) -> pd.DataFrame:
        """Transform input points from input CRS to dataset CRS.

        Parameters
        ----------
        locations_table : pd.DataFrame
            Location table with ``x`` and ``y``.
        dataset : xr.Dataset
            Source dataset.
        data_array : xr.DataArray
            Source variable used for CRS inference.
        input_crs : str | None, optional
            CRS of input points (e.g. ``"EPSG:4326"``). If ``None`` or empty, no
            transformation is applied.
        x_name : str
            Name of source x-coordinate (reserved for interface consistency).
        y_name : str
            Name of source y-coordinate (reserved for interface consistency).

        Returns
        -------
        pd.DataFrame
            Copy of the input table with transformed ``x`` and ``y`` coordinates.

        Raises
        ------
        UserWarning
            If dataset CRS cannot be inferred or ``pyproj`` is unavailable.
        """
        if input_crs in (None, ""):
            return locations_table

        dataset_crs = GwdiWiwbRetrievalBase.infer_dataset_crs(
            dataset=dataset,
            data_array=data_array,
        )
        if dataset_crs is None:
            raise UserWarning(
                "Kan bron-CRS niet afleiden uit dataset metadata. "
                "Zet `input_crs` uit of gebruik brondata met geldige CRS metadata."
            )

        try:
            from pyproj import Transformer
        except ImportError as exc:
            raise UserWarning(
                "Pakket `pyproj` ontbreekt maar is nodig voor CRS-transformatie."
            ) from exc

        transformer = Transformer.from_crs(input_crs, dataset_crs, always_xy=True)
        transformed_x, transformed_y = transformer.transform(
            locations_table["x"].to_numpy(dtype=float),
            locations_table["y"].to_numpy(dtype=float),
        )
        transformed_x = pd.to_numeric(
            pd.Series(transformed_x), errors="raise"
        ).to_numpy(dtype=float)
        transformed_y = pd.to_numeric(
            pd.Series(transformed_y), errors="raise"
        ).to_numpy(dtype=float)
        locations_out = locations_table.copy()
        locations_out["x"] = transformed_x
        locations_out["y"] = transformed_y

        return locations_out
