from typing import Optional


class StringSet(set):
    """Set that only allows adding str objects."""

    def __init__(
        self,
        *args: tuple[object],
        force_upper_case: Optional[bool] = False,
        **kwargs: dict[str, object],
    ) -> None:
        """Build an unordered collection of unique elements of type str.

        StringSet() -> new empty StringSet object
        StringSet(iterable) -> new StringSet object
        """
        self.upper_case = force_upper_case

        # Nuevo código para verificar tipos en la inicialización
        if args:
            iterable = args[0]
            new_iterable = []
            for item in iterable:
                if not isinstance(item, str):
                    raise ValueError(f"El elemento '{item}' no es una cadena")
                if self.upper_case:
                    item = item.upper()
                new_iterable.append(item)
            super().__init__(new_iterable, **kwargs)
        else:
            super().__init__(**kwargs)

    def add(self, item: str) -> None:
        """Add an element to a set. Checks the element type to be a str."""
        if not isinstance(item, str):
            raise ValueError(f"El elemento '{item}' no es una cadena")

        if self.upper_case:
            item = item.upper()

        return super().add(item)

    def __contains__(self, o: object) -> bool:
        """Overwrite the `in` operator.

        x.__contains__(y) <==> y in x.
        """
        if not isinstance(o, str):
            o = str(o)

        if self.upper_case:
            o = o.upper()

        return super().__contains__(o)
