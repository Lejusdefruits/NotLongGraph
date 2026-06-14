# Channels: merge rule for a state key (reducers)
import warnings
from typing import Any, Callable

from notlonggraph.errors import InvalidUpdateError, EmptyChannelError


class Channel:
    """base contract: every channel must provide update / get / fresh_copy"""
    _NOTHING = object()
    _checkpointable = True

    def update(self, news: list) -> None:
        raise NotImplementedError()

    def get(self) -> Any:
        raise NotImplementedError()

    def fresh_copy(self) -> "Channel":
        raise NotImplementedError()

    def on_step_end(self) -> None:
        pass

    def __repr__(self):
        try:
            value = self.get()
        except EmptyChannelError:
            value = "<empty>"
        return f"{self.__class__.__name__}({value})"


class LastValue(Channel):
    def __init__(self, initial_value: Any = Channel._NOTHING) -> None:
        self.value = initial_value

    def update(self, news: list) -> None:
        nb_elts = len(news)
        if nb_elts == 0:
            warnings.warn("LastValue received an empty update, ignoring it", stacklevel=2)
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
            warnings.warn("BinaryOperatorAggregate received an empty update, ignoring it", stacklevel=2)
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

class Topic(Channel):
    def __init__(self, accumulate: bool = False) -> None:
        self.accumulate = accumulate
        self.value = []

    def update(self, news: list) -> None:
        if self.accumulate:
            self.value.extend(news)
        else:
            self.value = news.copy()

    def get(self) -> list:
        return self.value

    def fresh_copy(self) -> "Topic":
        return Topic(self.accumulate)

class WindowedValue(Channel):
    def __init__(self, maxlen: int) -> None:
        self.maxlen = maxlen
        self.value = []

    def update(self, news: list) -> None:
        self.value.extend(news)
        if len(self.value) > self.maxlen:
            self.value = self.value[-self.maxlen:]

    def get(self) -> list:
        return self.value

    def fresh_copy(self) -> "WindowedValue":
        return WindowedValue(self.maxlen)

class EphemeralValue(Channel):
    def __init__(self) -> None:
        self.value = self._NOTHING
        self.is_new = False

    def update(self, news: list) -> None:
        nb_elts = len(news)
        if nb_elts == 0:
            warnings.warn("EphemeralValue received an empty update, ignoring it", stacklevel=2)
            return
        if nb_elts == 1:
            self.value = news[0]
        else:
            raise InvalidUpdateError(
                f"EphemeralValue cannot take {nb_elts} writes in a single step"
            )
        self.is_new = True

    def get(self) -> Any:
        if self.value is self._NOTHING:
            raise EmptyChannelError("EphemeralValue has never been written")
        return self.value

    def on_step_end(self) -> None:
        if self.is_new:
            self.is_new = False
        else:
            self.value = self._NOTHING

    def fresh_copy(self) -> "EphemeralValue":
        return EphemeralValue()

class NamedBarrierValue(Channel):
    def __init__(self, names: set) -> None:
        self.names = names
        self.value = self._NOTHING
        self.seen = set()

    def update(self, news: list) -> None:
        self.seen.update(news)

    def get(self) -> Any:
        if self.seen >= self.names:
            return None  # or some other value, the point is that it's not empty
        else:
            raise EmptyChannelError(
                f"NamedBarrierValue has not yet received all expected updates: {self.seen} / {self.names}"
            )

    def fresh_copy(self) -> "NamedBarrierValue":
        return NamedBarrierValue(self.names)

class ValidatedValue(Channel):
    def __init__(self, validate: Callable[[Any], bool]) -> None:
        self.validate = validate
        self.value = self._NOTHING

    def update(self, news: list) -> None:
        nb_elts = len(news)
        if nb_elts == 0:
            warnings.warn("ValidatedValue received an empty update, ignoring it", stacklevel=2)
            return
        if nb_elts != 1:
            raise InvalidUpdateError(
                    f"ValidatedValue cannot take {nb_elts} writes in a single step"
                )
        if nb_elts == 1 and self.validate(news[0]):
            self.value = news[0]
        else:
            raise InvalidUpdateError("ValidatedValue cannot take a rejected write")

    def get(self) -> Any:
        if self.value is self._NOTHING:
            raise EmptyChannelError("ValidatedValue has never been written")
        return self.value

    def fresh_copy(self) -> "ValidatedValue":
        return ValidatedValue(self.validate)


class AnyValue(Channel):
    # comme LastValue mais accepte plusieurs writes/vague sans lever
    def __init__(self, initial_value: Any = Channel._NOTHING) -> None:
        self.value = initial_value

    def update(self, news: list) -> None:
        nb_elts = len(news)
        if nb_elts == 0:
            warnings.warn("AnyValue received an empty update, ignoring it", stacklevel=2)
            return
        self.value = news[0]

    def get(self) -> Any:
        if self.value is self._NOTHING:
            raise EmptyChannelError("AnyValue has never been written")
        return self.value

    def fresh_copy(self) -> "AnyValue":
        return AnyValue()

class UntrackedValue(LastValue):
    _checkpointable = False
    def fresh_copy(self) -> "UntrackedValue":
        return UntrackedValue()
class DynamicBarrierValue(Channel):
    def __init__(self) -> None:
        self.names = None
        self.seen = set()

    def update(self, news: list) -> None:
        ...

    def get(self) -> Any:
        ...

    def fresh_copy(self) -> "DynamicBarrierValue":
        ...

class HistoryValue(Channel):
    # garde tout l'historique, pas juste la valeur courante (base du time-travel)
    def __init__(self) -> None:
        ...

    def update(self, news: list) -> None:
        ...

    def get(self) -> Any:
        ...

    def fresh_copy(self) -> "HistoryValue":
        ...

class ConsensusValue(Channel):
    # vote majoritaire entre les writes d'une même vague
    def __init__(self) -> None:
        ...

    def update(self, news: list) -> None:
        ...

    def get(self) -> Any:
        ...

    def fresh_copy(self) -> "ConsensusValue":
        ...

class ExpiringValue(Channel):
    # visible ttl vagues puis effacée (EphemeralValue = ttl 1)
    def __init__(self, ttl: int) -> None:
        ...

    def update(self, news: list) -> None:
        ...

    def get(self) -> Any:
        ...

    def on_step_end(self) -> None:
        ...

    def fresh_copy(self) -> "ExpiringValue":
        ...

class WriteOnceValue(Channel):
    # seule la première écriture compte, le reste est ignoré
    def __init__(self) -> None:
        ...

    def update(self, news: list) -> None:
        ...

    def get(self) -> Any:
        ...

    def fresh_copy(self) -> "WriteOnceValue":
        ...

class DefaultValue(Channel):
    # LastValue qui renvoie un défaut au lieu de lever si vide
    def __init__(self, default: Any) -> None:
        ...

    def update(self, news: list) -> None:
        ...

    def get(self) -> Any:
        ...

    def fresh_copy(self) -> "DefaultValue":
        ...

class RateLimitedValue(Channel):
    # au plus un write accepté toutes les every vagues, le reste throttle
    def __init__(self, every: int) -> None:
        ...

    def update(self, news: list) -> None:
        ...

    def get(self) -> Any:
        ...

    def on_step_end(self) -> None:
        ...

    def fresh_copy(self) -> "RateLimitedValue":
        ...

if __name__ == "__main__":
    from tests.test_channels import test_channels

    test_channels()
    print("all tests pass")
