"""
Start script voor Continu Inzicht Viewer
"""

from datetime import datetime, timedelta, timezone
from typing import Optional
from typing import Tuple
from toolbox_continu_inzicht import DataAdapter

import pandas as pd


def calculation_start(
    data_adapter: DataAdapter, output: str, calc_time: Optional[datetime] = None
) -> Tuple[datetime, datetime]:
    """
    Start het berekeningsproces door de berekeningstijd in te stellen en de momenten te verwerken.

    Deze functie neemt een DataAdapter-instantie, een uitvoerstring en een optionele berekeningstijd als parameters.
    Het stelt de globale berekeningstijd in de adapter in en berekent de minimale en maximale datums en tijden op basis van de opgegeven momenten.
    De resultaten worden vervolgens uitgevoerd via de DataAdapter.

    Parameters:
    data_adapter (DataAdapter): Een instantie van DataAdapter die wordt gebruikt om de uitvoer te verwerken en globale variabelen in te stellen.
    output (str): De uitvoerstring die door de DataAdapter wordt gebruikt.
    calc_time (Optional[datetime]): De optionele berekeningstijd. Als deze niet is opgegeven, wordt de huidige tijd gebruikt.

    Result:
    Tuple[datetime, datetime]: Een tuple met de minimale en maximale datums en tijden die zijn berekend.
    """

    # bepaal tijd van berekening en zet deze globaal in de adapter
    if calc_time is None:
        date_time_now = datetime.now(timezone.utc)
        calc_time = datetime(
            date_time_now.year,
            date_time_now.month,
            date_time_now.day,
            date_time_now.hour,
            0,
            0,
        ).replace(tzinfo=date_time_now.tzinfo)

    # zet de huidige datum/ tijd via de adapter
    data_adapter.set_global_variable(key="calc_time", value=calc_time)

    min_date_time = calc_time
    max_date_time = calc_time

    calc_time = data_adapter.config.global_variables["calc_time"]
    moments = data_adapter.config.global_variables["moments"]

    records = []

    if calc_time is not None:
        for moment in moments:
            moment_time = calc_time + timedelta(hours=moment)
            min_date_time = min(min_date_time, moment_time)
            max_date_time = max(max_date_time, moment_time)

            record = {
                "moment_id": moment,
                "date_time": moment_time,
                "calc_time": moment_time,
            }
            records.append(record)

            # update calc_time
            df_moment_table = pd.DataFrame.from_records(records)
            data_adapter.output(output=output, df=df_moment_table)

    return min_date_time, max_date_time
