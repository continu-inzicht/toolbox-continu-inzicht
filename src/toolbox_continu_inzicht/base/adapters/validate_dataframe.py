from typing import Dict, Tuple
import pandas as pd


def validate_dataframe(df: pd.DataFrame, schema: Dict) -> Tuple[int, str]:
    expected_columns = list(schema.keys())

    actual_columns = df.columns.tolist()
    actual_dtypes = df.dtypes.to_dict()

    columns_match = set(expected_columns).issubset(set(actual_columns))

    def dtype_match(expected_dtype, actual_dtype):
        if isinstance(expected_dtype, list):
            return actual_dtype in expected_dtype
        return expected_dtype == actual_dtype

    dtypes_match = all(
        dtype_match(schema[key], actual_dtypes[key])
        for key in schema
        if key in actual_dtypes
    )

    # dtypes_match = all(
    #     schema[key] == actual_dtypes[key] for key in schema if key in actual_dtypes
    # )

    if columns_match and dtypes_match:
        return 0, "DataFrame is geldig."
    else:
        if not columns_match:
            return (
                1,
                f"Kolommen komen niet overeen. \nVerwachte kolommen: {schema}.\nHuidige kolommen: {actual_dtypes}.",
            )
        else:
            # not dtypes_match:
            return (
                2,
                f"Datatypes komen niet overeen.\nVerwachte kolommen: {schema}.\nHuidige kolommen: {actual_dtypes}.",
            )
