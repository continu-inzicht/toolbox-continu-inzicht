import json
from pathlib import Path

allias_dict = {
    "waterlevel": "WATHTE",
    "water height": "WATHTE",
    "WATHTEVERWACHT": "WATHTE",
    "waterhoogte": "WATHTE",
    "wind": "WINDSHD",
    "golfhoogte": "GOLFHTE",
    "watertemperatuur": "TWAT",
    "luchttemperatuur": "TLCHT",
    "astronomische-getij": "GETIJ",
    "stroming": "STROOMSHD",
    "waterafvoer": "Q",
    "zouten": "SALNTT",
}


def read_aquo(allias: str) -> tuple[str, dict]:
    """Leest de aquo grootheid uit de aquo_grootheid.json file
    geeft een tuple van de bijbehoordende code en een dict met benodigde informatie terug."""
    path = Path(__file__).parent / "aquo_meta_data" / "aquo_grootheid.json"
    aquo_grootheid_dict = json.load(open(path))

    if allias in allias_dict:
        aquo_value = allias_dict[allias]
        return aquo_value, aquo_grootheid_dict[aquo_value]

    elif allias in aquo_grootheid_dict:
        return allias, aquo_grootheid_dict[allias]

    else:
        raise UserWarning(f"{allias=} niet gevonden in de allias_dict")
