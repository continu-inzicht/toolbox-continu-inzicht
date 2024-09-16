from pydantic import BaseModel as PydanticBaseModel
from continu_inzicht_toolbox.base import DataAdapter


class WaardesDelenTwee(PydanticBaseModel):
    """
    Voorbeeld class die laat zien hoe de arcitectuur werkt

    """

    data_adapter: DataAdapter

    def run(self):
        """Runt de funcies en stuur de df terug"""
        postgresql_kwargs = {"schema": "citoolbox_schema", "table": "data"}
        csv_kwargs = {}
        kwargs = {"postgresql_database": postgresql_kwargs, "csv": csv_kwargs}
        df = self.data_adapter.input("WaardesDelenTwee", **kwargs)

        # check of de data klopt
        cols = ["objectid", "objecttype", "parameterid", "datetime", "value"]
        list_bool_cols = [col in df.columns for col in cols]
        if all(list_bool_cols):
            df = self.delen_door_twee(df)
            self.data_adapter.output("WaardesDelenTwee", df, **kwargs)

        else:
            raise UserWarning(
                f"Data moet de {len(cols)} kollom(en) {cols} hebben, maar heeft {df.columns}"
            )

    # Maar dan een hoop risico berekeningen
    @staticmethod
    def delen_door_twee(df):
        """Vermenigvuldigd de meetstation waardes met 2 als voorbeeld"""
        df["value"] = df["value"] / 2
        return df


class WaardesKeerTwee(PydanticBaseModel):
    """
    Voorbeeld class die laat zien hoe de arcitectuur werkt

    """

    data_adapter: DataAdapter

    def run(self):
        """Runt de funcies en stuur de df terug"""
        postgresql_kwargs = {"schema": "citoolbox_schema", "table": "data"}
        csv_kwargs = {}
        kwargs = {"postgresql_database": postgresql_kwargs, "csv": csv_kwargs}

        df = self.data_adapter.input("WaardesKeerTwee", **kwargs)

        # check of de data klopt
        cols = ["objectid", "objecttype", "parameterid", "datetime", "value"]
        list_bool_cols = [col in df.columns for col in cols]
        if all(list_bool_cols):
            df = self.keer_twee(df)
            self.data_adapter.output("WaardesKeerTwee", df, **kwargs)

        else:
            raise UserWarning(
                f"Data moet de {len(cols)} kollom(en) {cols} hebben, maar heeft {df.columns}"
            )

    # Maar dan een hoop risico berekeningen
    @staticmethod
    def keer_twee(df):
        """Deelt de meetstation waardes door 2 als voorbeeld"""
        df["value"] = df["value"] * 2
        return df
