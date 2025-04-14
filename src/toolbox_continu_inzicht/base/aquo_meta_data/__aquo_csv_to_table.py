"""Script voor het omzetten van Aquo-grootheden CSV- naar tabel voor de documentatie"""

if __name__ == "__main__":
    import pandas as pd
    from pathlib import Path
    from tabulate import tabulate

    path = Path(__file__).parent
    df_aquo = pd.read_csv(path / "AQUO(Parameters).csv", encoding="utf-8")

    df_aquo = df_aquo.dropna(subset="voorkeurslabel")
    df_aquo = df_aquo.fillna("")

    # replace the link column with a link to the documentation
    df_aquo["id"] = df_aquo.apply(
        lambda x: f"[{x['id']}]({x['aquo_url']})"
        if (isinstance(x["aquo_url"], str) and len(x["aquo_url"]) > 10)
        else f"{x['id']}",
        axis=1,
    )
    df_aquo = df_aquo.drop(columns=["aquo_url"])
    aquo_grootheid = """
---
title: "Aquo grootheden"
---

Bij het inladen van belastingen wordt rekening gehouden met de Aquo standaard.
Voor toolbox continu inzicht kan het soms lastig zijn omdat de termen binnen de Aquo standaard valt.
Binnen de toolbox wordt de volgende meta data gebruikt:\n\n"""
    aquo_grootheid += tabulate(
        df_aquo, headers="keys", tablefmt="pipe", showindex=False
    )

    aquo_grootheid += """\n\n _Dit is een automatisch gegenereerde tabel, zie de [bron code voor meer informatie](https://github.com/continu-inzicht/toolbox-continu-inzicht/tree/main/src/toolbox_continu_inzicht/base/aquo_meta_data)_"""
    with open(path / "aquo_grootheid.qmd", "w", encoding="utf-8") as f_out:
        f_out.writelines(aquo_grootheid)
