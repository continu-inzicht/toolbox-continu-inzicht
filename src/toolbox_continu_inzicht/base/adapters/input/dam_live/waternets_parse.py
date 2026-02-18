from toolbox_continu_inzicht.base.adapters.input.dam_live.json_folder import input_json_folder
import pandas as pd

def input_waternets(input_config: dict) -> pd.DataFrame:
    rows = []

    for item in input_json_folder(input_config):
        data = item["data"]
        waternet_id = data.get("Id")
        content_version = data.get("ContentVersion")

        # HeadLines
        for head in data.get("HeadLines", []):
            line_id = head.get("Id")
            line_label = head.get("Label")
            for p in head.get("Points", []):
                rows.append(
                    {
                        "waternet_id": waternet_id,
                        "line_type": "Head",
                        "line_id": line_id,
                        "line_label": line_label,
                        "x": p.get("X"),
                        "z": p.get("Z"),
                        "top_headline_id": None,
                        "bottom_headline_id": None,
                        "content_version": content_version,
                    }
                )

        # ReferenceLines
        for ref in data.get("ReferenceLines", []):
            line_id = ref.get("Id")
            line_label = ref.get("Label")
            top_id = ref.get("TopHeadLineId")
            bottom_id = ref.get("BottomHeadLineId")
            for p in ref.get("Points", []):
                rows.append(
                    {
                        "waternet_id": waternet_id,
                        "line_type": "Reference",
                        "line_id": line_id,
                        "line_label": line_label,
                        "x": p.get("X"),
                        "z": p.get("Z"),
                        "top_headline_id": top_id,
                        "bottom_headline_id": bottom_id,
                        "content_version": content_version,
                    }
                )

    return pd.DataFrame(rows)
