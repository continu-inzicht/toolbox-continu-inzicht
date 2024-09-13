from pydantic import BaseModel as PydanticBaseModel
from continu_inzicht_toolbox.base import DataAdapter


class WaardesDelenTwee(PydanticBaseModel):
    """
    Voorbeel class die laat zien hoe de arcitectuur werkt

    """

    data_adapter: DataAdapter

    def run(self):
        """Runt de funcies en stuur de df terug"""
        df = self.data_adapter.input("waardes_keer_twee")

        # check of de data klopt
        cols = ["dijkring_1", "drijkring_2"]
        if cols in df.columns:
            df = self.keer_twee(df)
            return df

        else:
            raise UserWarning(f"Data moet de {len(cols)} kollom(en) {cols} hebben")

    # Maar dan een hoop risico berekeningen
    @staticmethod
    def keer_twee(df):
        """Deelt waardes van drijkring2 door 2"""
        df["dijkring_2"] = df["dijkring_2"] * 2
        return df
