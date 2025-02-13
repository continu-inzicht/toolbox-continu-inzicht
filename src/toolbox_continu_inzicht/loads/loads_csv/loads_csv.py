import pandas as pd
from datetime import datetime
from typing import Optional

def load_csv(input_path: str, schema: dict) -> pd.DataFrame:
    """
    Lees een CSV-bestand in en pas het schema toe op de kolommen.

    Args:
        input_path (str): Pad naar het CSV-bestand.
        schema (dict): Schema voor de kolommen.

    Returns:
        pd.DataFrame: DataFrame met de ingelezen gegevens.
    """
    df = pd.read_csv(input_path, dtype=schema)
    return df

def process_csv(input_path: str, output_path: str, schema: dict) -> Optional[pd.DataFrame]:
    """
    Verwerk een CSV-bestand en sla de resultaten op in een nieuw CSV-bestand.

    Args:
        input_path (str): Pad naar het invoer CSV-bestand.
        output_path (str): Pad naar het uitvoer CSV-bestand.
        schema (dict): Schema voor de kolommen.

    Returns:
        Optional[pd.DataFrame]: DataFrame met de verwerkte gegevens.
    """
    df_in = load_csv(input_path, schema)
    
    # Hier kun je extra verwerkingslogica toevoegen als dat nodig is
    # Bijvoorbeeld: filteren, berekeningen uitvoeren, etc.
    return df_in

# Voorbeeld gebruik:
input_schema = {
    "measurement_location_id": "int64",
    "measurement_location_code": "object",
    "measurement_location_description": "object",
}

input_path = R"D:\Python\Data\toolbox_continu_inzicht\data_peilbuizen"

df_processed = process_csv(input_path, input_schema)

print(df_processed)

