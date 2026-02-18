def merge_stage_water(df_stages, df_waternet):
    
    """
    Merge stages met waternetlijnen en voeg kleuren toe per type lijn.

    Parameters
    ----------
    df_stages : pd.DataFrame
        Stage-data
    df_waternet : pd.DataFrame
        Waternet-data (met punten en line_type info)

    Returns
    -------
    pd.DataFrame
        Per stage de kernkolommen + alle waterlijnen + kleur per line_type
    """

    # Kolommen uit stages die we willen behouden
    stage_cols = ["stage_id", "stage_label", "scenario_id", "scenario_label"]

    df_stages_light = df_stages[stage_cols + ["waternet_id"]].drop_duplicates()

    # Merge met waternet
    df_merged = df_stages_light.merge(df_waternet, how="left", on="waternet_id")

    # Voeg kleuren toe per line_type
    line_color_map = {
        "Phreatic line (PL 1)": "#ff1493",  # deep pink
        "Head line 3 (PL 3)": "#ff8c00",  # helder donkerblauw
        "Waternet line phreatic line": "#00bfff",  # helder lichtblauw
        "Waternet line lower aquifer": "#00ff7f",  # fel groen
    }

    df_merged["color"] = (
        df_merged["line_label"].map(line_color_map).fillna("#cccccc")
    )  # fallback grijs

    # Sorteren zodat lijnen netjes doorlopen
    df_merged = df_merged.sort_values(
        by=["stage_id", "line_type", "line_id", "x"]
    ).reset_index(drop=True)

    return df_merged