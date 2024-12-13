"""
End script voor Continu Inzicht Viewer
"""

from toolbox_continu_inzicht import DataAdapter
import pandas as pd


def calculation_end(data_adapter: DataAdapter, output: str) -> None:
    """
    Finalizes the calculation process.

    Deze functie neemt een DataAdapter-instantie en een uitvoerstring als parameters.
    Het gebruikt de DataAdapter om een lege DataFrame uit te voeren met de opgegeven uitvoerstring.

    Parameters:
    data_adapter (DataAdapter): Een instantie van DataAdapter die wordt gebruikt om de uitvoer te verwerken.
    output (str): De uitvoerstring die door de DataAdapter wordt gebruikt.

    Returns:
    None
    """
    data_adapter.output(output=output, df=pd.DataFrame())
