import json
from pathlib import Path

# veel bronnen verzinnen eigen namen voor aquo grootheden, hier worden ze vertaald naar de juiste aquo grootheid
# gebruiker kan dit ook toevoegen in de global_variables met de key "aquo_alias"
alias_dict = {
    "waterlevel": "WATHTE",
    "water height": "WATHTE",
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


def read_aquo(alias: str, global_variables: dict) -> tuple[str, dict]:
    """
    Leest de Aquo-grootheid uit de aquo_grootheid.json file
    geeft een tuple van de bij behoorde code en een dict met benodigde informatie terug.
    """
    path = Path(__file__).parent / "aquo_meta_data" / "aquo_grootheid.json"
    aquo_grootheid_dict = json.load(open(path))

    user_aquo_aliases = global_variables.get("aquo_alias", {})

    # door gebruiker opgegeven aliases zijn leidend.
    alias_dict.update(user_aquo_aliases)

    if alias in alias_dict:
        aquo_value = alias_dict[alias]
        return aquo_value, aquo_grootheid_dict[aquo_value]

    elif alias in aquo_grootheid_dict:
        return alias, aquo_grootheid_dict[alias]

    else:
        raise UserWarning(f"{alias=} niet gevonden in de alias_dict")
