from datetime import datetime, timezone
from toolbox_continu_inzicht.utils.datetime_functions import (
    epoch_from_datetime,
    datetime_from_string,
)


def test_epoch_from_datetime():
    """
    Test de werking van epoch naar datum/ tijd functie
    """
    utc_dt = datetime(2024, 10, 16, 15, 00).replace(tzinfo=timezone.utc)
    epoch = epoch_from_datetime(utc_dt=utc_dt)
    assert epoch == 1729090800000.0


def test_datetime_from_string():
    """
    Test de werking van datum/ tijd naar epoch functie
    """
    utc_dt = datetime(2024, 10, 16, 15, 00).replace(tzinfo=timezone.utc)
    datestr = "2024-10-16T15:00:00Z"
    utc_dt_from_string = datetime_from_string(datestr, "%Y-%m-%dT%H:%M:%SZ")

    assert utc_dt_from_string == utc_dt
