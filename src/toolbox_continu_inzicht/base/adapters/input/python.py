import pandas as pd


def input_python(input_config: dict) -> pd.DataFrame:
    """Wrapper voor de functionalitiet om DataFrames vanuit Python te ondersteunen

    Returns:
    --------
    pd.Dataframe
    """
    return input_config["dataframe_from_python"]
