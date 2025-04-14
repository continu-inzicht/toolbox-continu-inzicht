import json
from pathlib import Path

# veel bronnen verzinnen eigen namen voor aquo grootheden, hier worden ze vertaald naar de juiste aquo grootheid
# gebruiker kan dit ook toeveogen in de global_variables met de key "aquo_allias"
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


def read_aquo(allias: str, global_variables: dict) -> tuple[str, dict]:
    """
    Leest de Aquo-grootheid uit de aquo_grootheid.json file
    geeft een tuple van de bij behoorde code en een dict met benodigde informatie terug.
    """
    path = Path(__file__).parent / "aquo_meta_data" / "aquo_grootheid.json"
    aquo_grootheid_dict = json.load(open(path))

    user_aquo_alliases = global_variables.get("aquo_allias", {})

    # door gebruiker opgegeven aliases zijn leidend.
    allias_dict.update(user_aquo_alliases)

    if allias in allias_dict:
        aquo_value = allias_dict[allias]
        return aquo_value, aquo_grootheid_dict[aquo_value]

    elif allias in aquo_grootheid_dict:
        return allias, aquo_grootheid_dict[allias]

    else:
        raise UserWarning(f"{allias=} niet gevonden in de allias_dict")
