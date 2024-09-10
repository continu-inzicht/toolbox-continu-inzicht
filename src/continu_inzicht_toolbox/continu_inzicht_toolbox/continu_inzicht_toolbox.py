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
        n : int
            Number of times to print statement


        Returns
        -------
        None
        """

        for i in range(n):
            print(f"{i}:{self.place} {self.holder}")

    def count_placeholders_length(self):
        """
        Counts the length of the placeholders of the class

        Parameters
        ----------
        None


        Returns
        -------
        int
            Number of charaters placeholders are.
        """

        return int(len(self.place) + len(self.holder))


if __name__ == "__main__":
    c = PlaceHolder(place="a", holder="b")
    c.print_placeholders(2)
    c.count_placeholders_length()
