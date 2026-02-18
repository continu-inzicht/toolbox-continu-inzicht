def merge_stage_geometries_soils(df_stages, df_geometry, df_soillayers, df_soils):
    # Merge stages met geometry (op geometry_id)
    df_merged = df_stages.merge(
        df_geometry, how="left", left_on="geometry_id", right_on="geometry_id"
    )

    # Merge met soillayers (op layer_id)
    df_merged = df_merged.merge(
        df_soillayers,
        how="left",
        left_on="layer_id",
        right_on="layer_id",
        suffixes=("", "_soillayers"),
    )

    # Merge met soils (op soil_id)
    df_merged = df_merged.merge(
        df_soils,
        how="left",
        left_on="soil_id",
        right_on="soil_id",
        suffixes=("", "_soil"),
    )

    # Kolommen herordenen (alleen bestaande kolommen)
    columns_order = [
        "stage_id",
        "stage_label",
        "scenario_id",
        "scenario_label",
        "geometry_id",
        "layer_id",
        "layer_label",
        "points",
        "soillayers_id",
        "soil_id",
        "name",
        "code",
        "color",
    ]

    # Filter alleen bestaande kolommen
    columns_order = [col for col in columns_order if col in df_merged.columns]

    df_final = df_merged[columns_order]

    return df_final