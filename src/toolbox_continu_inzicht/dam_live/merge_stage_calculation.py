def merge_stage_calculations(df_stages, df_calculationsettings):
    # Merge stages met calculationsettings (op calculationsettings_id)
    df_merged = df_stages.merge(
        df_calculationsettings,
        how="left",
        left_on="calculationsettings_id",
        right_on="calculationsettings_id",
    )

    # Kolommen herordenen (alleen bestaande kolommen)
    columns_order = [
        "stage_id",
        "stage_label",
        "scenario_id",
        "scenario_label",
        "calculationsettings_id",
        "analysis_type",
        "calculation_type",
        "model_factor_mean",
        "model_factor_std",
        "circle_center_x",
        "circle_center_z",
        "circle_radius",
        "content_version",
    ]

    columns_order = [col for col in columns_order if col in df_merged.columns]

    df_final = df_merged[columns_order]

    return df_final