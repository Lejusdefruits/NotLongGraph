# Channels: merge rule for a state key (reducers)
import collections
import copy
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

    def consume(self) -> bool:
        """checkpoint"""
        return False

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

class WaitForNames:
    def __init__(self, names) -> None:
        self.names = set(names)

class DynamicBarrierValue(Channel):
    def __init__(self) -> None:
        self.names = None
        self.seen = set()

    def consume(self) -> bool:
        if self.names is None:
            return False
        if self.seen >= self.names:
            self.seen = set()
            return True
        else:
            return False

    def update(self, news: list) -> None:
        for msg in news:
            if isinstance(msg, WaitForNames):
                self.names = msg.names
            else:
                self.seen.add(msg)

    def get(self) -> Any:
        if self.names is None:
            raise EmptyChannelError("DynamicBarrierValue has not yet received a WaitForNames message")
        if self.seen >= self.names:
            return None
        else:
            raise EmptyChannelError(
                f"DynamicBarrierValue has not yet received all expected updates: {self.seen} / {self.names}"
            )

    def fresh_copy(self) -> "DynamicBarrierValue":
        return DynamicBarrierValue()

class HistoryValue(Channel):
    def __init__(self) -> None:
        self.history = []

    def snapshot(self) -> list:
        return copy.deepcopy(self.history)

    def update(self, news: list) -> None:
        if len(news) == 0:
            warnings.warn("HistoryValue received an empty update, ignoring it", stacklevel=2)
            return
        self.history.append(news[-1])

    def get(self) -> Any:
        if not self.history:
            raise EmptyChannelError("HistoryValue has never been written")
        return self.history[-1]

    def fresh_copy(self) -> "HistoryValue":
        return HistoryValue()

class ConsensusValue(Channel):
    def __init__(self) -> None:
        self.consensus = self._NOTHING

    def update(self, news: list) -> None:
        if len(news) == 0:
            warnings.warn("ConsensusValue received an empty update, ignoring it", stacklevel=2)
            return
        self.consensus = collections.Counter(news).most_common(1)[0][0]

    def get(self) -> Any:
        if self.consensus is self._NOTHING:
            raise EmptyChannelError("ConsensusValue has never been written")
        return self.consensus

    def fresh_copy(self) -> "ConsensusValue":
        return ConsensusValue()

class ExpiringValue(Channel):
    def __init__(self, ttl: int) -> None:
        self.ttl = ttl
        self.value = self._NOTHING
        self.remaining_ttl = ttl
        self.is_expired = False

    def update(self, news: list) -> None:
        if len(news) == 0:
            warnings.warn("ExpiringValue received an empty update, ignoring it", stacklevel=2)
            return
        self.value = news[0]
        self.remaining_ttl = self.ttl
        self.is_expired = False

    def get(self) -> Any:
        if self.value is self._NOTHING:
            raise EmptyChannelError("ExpiringValue has never been written")
        return self.value

    def on_step_end(self) -> None:
        if self.is_expired:
            self.is_expired = False
            self.value = self._NOTHING
        if self.remaining_ttl > 0:
            self.remaining_ttl -= 1
        if self.remaining_ttl == 0:
            self.is_expired = True

    def fresh_copy(self) -> "ExpiringValue":
        return ExpiringValue(self.ttl)

class WriteOnceValue(Channel):
    def __init__(self) -> None:
        self.value = self._NOTHING

    def update(self, news: list) -> None:
        if self.value is self._NOTHING:
            if len(news) == 0:
                warnings.warn("WriteOnceValue received an empty update, ignoring it", stacklevel=2)
                return
            self.value = news[0]
        else:
            warnings.warn("WriteOnceValue received a second update, ignoring it", stacklevel=2)

    def get(self) -> Any:
        if self.value is self._NOTHING:
            raise EmptyChannelError("WriteOnceValue has never been written")
        return self.value

    def fresh_copy(self) -> "WriteOnceValue":
        return WriteOnceValue()

class DefaultValue(Channel):
    def __init__(self, default: Any) -> None:
        self.default = default
        self.value = self._NOTHING

    def update(self, news: list) -> None:
        nb_elts = len(news)
        if nb_elts == 0:
            warnings.warn("DefaultValue received an empty update, ignoring it", stacklevel=2)
            return
        if nb_elts == 1:
            self.value = news[0]
        else:
            raise InvalidUpdateError(
                f"DefaultValue cannot take {nb_elts} writes in a single step"
            )

    def get(self) -> Any:
        if self.value is self._NOTHING:
            return self.default
        return self.value

    def fresh_copy(self) -> "DefaultValue":
        return DefaultValue(self.default)

class RateLimitedValue(Channel):
    def __init__(self, every: int) -> None:
        self.every = every
        self.value = self._NOTHING
        self.counter = every

    def update(self, news: list) -> None:
        if self.counter >= self.every:
            if len(news) >= 1:
                self.value = news[0]
                self.counter = 0
            else:
                warnings.warn("RateLimitedValue received an empty update", stacklevel=2)
        else:
            warnings.warn("RateLimitedValue received an update during cooldown", stacklevel=2)

    def get(self) -> Any:
        if self.value is self._NOTHING:
            raise EmptyChannelError("RateLimitedValue has never been written")
        return self.value

    def on_step_end(self) -> None:
        self.counter += 1

    def fresh_copy(self) -> "RateLimitedValue":
        return RateLimitedValue(self.every)

if __name__ == "__main__":
    from tests.test_channels import test_channels

    test_channels()
    print("all tests pass")
