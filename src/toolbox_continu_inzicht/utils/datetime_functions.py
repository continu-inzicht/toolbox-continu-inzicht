from datetime import datetime, timezone


def epoch_from_datetime(utc_dt: datetime) -> float:
    """
    Converteer datum/ tijd in UTC naar epoch in milliseconds

    Args:
        utc_dt (datetime): datum/ tijd in UTC

    Returns:
        float: epoch tijd in milliseconds
    """
    epoch = datetime.fromtimestamp(0, timezone.utc)
    return (utc_dt - epoch).total_seconds() * 1000.0


def datetime_from_string(dt_str: str, fmt: str) -> datetime:
    """
    Converteer epoch in milliseconds naar datum/ tijd in UTC

    Args:
        dt_str (str): string met datum/ tijd (v.b.: 2024-10-16)
        fmt (str): Formaat van de datum/ tijd string (v.b.: %Y-%m-%d)

    Returns:
        datetime: datum/ tijd in UTC
    """
    dtobj_utc = datetime.strptime(dt_str, fmt).replace(tzinfo=timezone.utc)
    return dtobj_utc


def datetime_from_epoch(epoch: float) -> datetime:
    """
    Converteer datum/ tijd in UTC van epoch in milliseconds naar datetime

    Args:
        epoch: float
               epoch tijd in milliseconds

    Returns:
        utc_dt: datetime
            datum/ tijd in UTC
    """
    epoch = epoch / 1000.0
    return datetime.fromtimestamp(epoch, timezone.utc)
