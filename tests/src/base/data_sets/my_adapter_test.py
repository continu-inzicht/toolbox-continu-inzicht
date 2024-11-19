import pandas as pd


def input_my_adapter_test_in(input_config) -> pd.DataFrame:
    data = {"Naam": ["Tom", "Nick", "Krish", "Jack"], "Leeftijd": [20, 21, 19, 18]}

    # Converteer de dictionary naar een DataFrame
    return pd.DataFrame(data)


def fake_my_adapter_test_in2() -> pd.DataFrame:
    data = {"Naam": ["Tom", "Nick", "Krish", "Jack"], "Leeftijd": [20, 21, 19, 18]}

    # Converteer de dictionary naar een DataFrame
    return pd.DataFrame(data)


def output_my_adapter_test_out(output_config, df) -> None:
    data = {"Naam": ["Tom", "Nick", "Krish", "Jack"], "Leeftijd": [20, 21, 19, 18]}

    # Converteer de dictionary naar een DataFrame
    return pd.DataFrame(data)


def output_my_adapter_test_out2(output_config: dict, df: pd.DataFrame) -> None:
    data = {"Naam": ["Tom", "Nick", "Krish", "Jack"], "Leeftijd": [20, 21, 19, 18]}

    # Converteer de dictionary naar een DataFrame
    return pd.DataFrame(data)
