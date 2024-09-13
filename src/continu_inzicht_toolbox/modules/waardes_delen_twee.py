from pydantic import BaseModel as PydanticBaseModel
from continu_inzicht_toolbox.base import DataAdapter


class WaardesDelenTwee(PydanticBaseModel):
    """
    Voorbeel class die laat zien hoe de arcitectuur werkt

    """

    data_adapter: DataAdapter

    def run(self):
        """Runt de funcies en stuur de df terug"""
        df = self.data_adapter.input("waardes_delen_twee")
        df = self.delen_door_twee(df)
        return df

    # Maar dan een hoop risico berekeningen
    @staticmethod
    def delen_door_twee(df):
        """Deelt waardes van drijkring2 door 2"""
        df["dijkring_2"] = df["dijkring_2"] / 2
        return df
