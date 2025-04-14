"""Script voor het omzetten van Aquo-grootheden CSV- naar JSON-formaat"""

if __name__ == "__main__":
    import pandas as pd
    from pathlib import Path
    import json

    path = Path(__file__).parent
    df_aquo = pd.read_csv(path / "AQUO(Parameters).csv", encoding="utf-8")

    df_aquo = df_aquo.dropna(subset="voorkeurslabel")
    df_aquo = df_aquo.fillna("")
    df_aquo["id"] = df_aquo["id"].astype(int)

    aquo_grootheid = {}
    for index, col in df_aquo.iterrows():
        row_dict = dict(zip(df_aquo.columns, col.values))
        grootheid = row_dict["grootheid"]
        del row_dict["grootheid"]
        aquo_grootheid[grootheid] = row_dict

    with open(path / "aquo_grootheid.json", "w") as f_out:
        json.dump(aquo_grootheid, f_out, indent=4)
