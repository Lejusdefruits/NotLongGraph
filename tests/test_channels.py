import operator

from notlonggraph.channels import LastValue, BinaryOperatorAggregate
from notlonggraph.errors import InvalidUpdateError, EmptyChannelError


def test_channels() -> None:
    # LastValue: overwrites
    c = LastValue()
    c.update([42])                       # single write
    assert c.get() == 42, "LastValue should store 42"

    c.update([99])                       # new write -> overwrites
    assert c.get() == 99, "LastValue should have been overwritten by 99"

    c.update([])                         # empty writes -> no change
    assert c.get() == 99, "empty writes should change nothing"

    # fresh_copy: a brand new, independent channel
    fresh = c.fresh_copy()
    assert isinstance(fresh, LastValue), "fresh_copy must return a LastValue"
    fresh.update([7])
    assert fresh.get() == 7, "the fresh channel stores 7"
    assert c.get() == 99, "the original must NOT be affected by the fresh copy"

    # conflict: two writes on a LastValue in one step -> error
    try:
        LastValue().update([1, 2])
        raise AssertionError("a LastValue conflict should raise InvalidUpdateError")
    except InvalidUpdateError:
        pass

    # reading a channel that was never written -> error
    try:
        LastValue().get()
        raise AssertionError("reading an empty channel should raise EmptyChannelError")
    except EmptyChannelError:
        pass

    # BinaryOperatorAggregate: accumulates with '+'
    agg = BinaryOperatorAggregate(operator.add)
    agg.update([["a"]])                  # first write becomes the base
    assert agg.get() == ["a"], "agg should be ['a']"

    agg.update([["b"], ["c"]])           # two writes in the same step
    assert agg.get() == ["a", "b", "c"], "agg should accumulate -> ['a','b','c']"

    # BinaryOp with number addition
    s = BinaryOperatorAggregate(operator.add)
    s.update([1, 2, 3])                  # 1 + 2 + 3
    assert s.get() == 6, "sum should be 6"


if __name__ == "__main__":
    test_channels()
    print("all tests pass")
