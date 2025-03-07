import pandas as pd
from toolbox_continu_inzicht.base.adapters.data_adapter_utils import get_kwargs


def input_test_text(input_config: dict):
    """Function to read a text file given a path"""
    # input_config contains all the information from reading the config file
    path = input_config["abs_path"]
    # we check with `get_kwargs` which options are compatible
    kwargs = get_kwargs(open, input_config)
    kwargs.pop("file")  # use the abs path instead of file
    with open(path, **kwargs) as f:
        data = f.read()
    # return the data in a dataframe, this is just an example
    return pd.DataFrame([data], columns=["text"])
