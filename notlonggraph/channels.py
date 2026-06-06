# Channels: merge rule for a state key (reducers)

from typing import Any, Callable

from notlonggraph.errors import InvalidUpdateError, EmptyChannelError


class Channel:
    """base contract: every channel must provide update / get / fresh_copy"""
    _NOTHING = object()

    def update(self, news: list) -> None:
        raise NotImplementedError()

    def get(self) -> Any:
        raise NotImplementedError()

    def fresh_copy(self) -> "Channel":
        raise NotImplementedError()


class LastValue(Channel):
    def __init__(self, initial_value: Any = Channel._NOTHING) -> None:
        self.value = initial_value

    def update(self, news: list) -> None:
        nb_elts = len(news)
        if nb_elts == 0:
            return
        if nb_elts == 1:
            self.value = news[0]
        else:
            raise InvalidUpdateError(
                f"LastValue cannot take {nb_elts} writes in a single step"
            )

    def get(self) -> Any:
        if self.value is self._NOTHING:
            raise EmptyChannelError("LastValue has never been written")
        return self.value

    def fresh_copy(self) -> "LastValue":
        return LastValue()


class BinaryOperatorAggregate(Channel):
    def __init__(
        self,
        operator: Callable[[Any, Any], Any],
        initial_value: Any = Channel._NOTHING,
    ) -> None:
        self.operator = operator
        self.value = initial_value

    def update(self, news: list) -> None:
        if not news:
            return
        if self.value is self._NOTHING:
            self.value = news[0]
            news = news[1:]
        for n in news:
            self.value = self.operator(self.value, n)

    def get(self) -> Any:
        if self.value is self._NOTHING:
            raise EmptyChannelError("BinaryOperatorAggregate has never been written")
        return self.value

    def fresh_copy(self) -> "BinaryOperatorAggregate":
        return BinaryOperatorAggregate(self.operator)


if __name__ == "__main__":
    from tests.test_channels import test_channels

    test_channels()
    print("all tests pass")
