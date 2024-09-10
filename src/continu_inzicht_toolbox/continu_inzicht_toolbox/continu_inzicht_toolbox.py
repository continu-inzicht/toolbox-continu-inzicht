from pydantic import BaseModel


class PlaceHolder(BaseModel):
    """
    Placeholder class to show how pydantic would work

    """

    place: str
    holder: str

    def print_placeholders(self, n: int):
        """
        Prints the placeholders of the class

        Parameters
        ----------

        N: Number of times to print


        Returns
        -------
        None
        """

        for i in range(n):
            print(f"{i}:{self.place} {self.holder}")


if __name__ == "__main__":
    c = PlaceHolder(place="a", holder="b")
    c.print_placeholders(2)
