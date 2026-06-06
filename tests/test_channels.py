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


def test_topic() -> None:
    from notlonggraph.channels import Topic

    t = Topic()
    t.update(["a", "b"])
    assert t.get() == ["a", "b"]
    t.update(["c"])
    assert t.get() == ["c"]

    assert Topic().get() == []

    acc = Topic(accumulate=True)
    acc.update(["a"])
    acc.update(["b", "c"])
    assert acc.get() == ["a", "b", "c"]

    fresh = acc.fresh_copy()
    assert isinstance(fresh, Topic)
    assert fresh.get() == []
    fresh.update(["z"])
    assert acc.get() == ["a", "b", "c"]
    fresh.update(["y"])
    assert fresh.get() == ["z", "y"]


def test_windowed_value() -> None:
    from notlonggraph.channels import WindowedValue

    w = WindowedValue(3)
    w.update([1, 2])
    assert w.get() == [1, 2]
    w.update([3, 4])
    assert w.get() == [2, 3, 4]
    w.update([5])
    assert w.get() == [3, 4, 5]

    assert WindowedValue(2).get() == []

    fresh = w.fresh_copy()
    assert isinstance(fresh, WindowedValue)
    assert fresh.get() == []
    fresh.update([1, 2, 3, 4])
    assert fresh.get() == [2, 3, 4]
    assert w.get() == [3, 4, 5]


def test_ephemeral_value() -> None:
    from notlonggraph.channels import EphemeralValue

    e = EphemeralValue()
    e.update(["signal"])
    assert e.get() == "signal"

    e.on_step_end()
    assert e.get() == "signal"

    e.on_step_end()
    try:
        e.get()
        raise AssertionError("an ephemeral value must vanish when not rewritten")
    except EmptyChannelError:
        pass

    e.update(["again"])
    e.on_step_end()
    assert e.get() == "again"

    assert isinstance(e.fresh_copy(), EphemeralValue)


def test_named_barrier() -> None:
    from notlonggraph.channels import NamedBarrierValue

    b = NamedBarrierValue({"search_web", "search_db"})
    try:
        b.get()
        raise AssertionError("a barrier must stay closed until all writers arrive")
    except EmptyChannelError:
        pass

    b.update(["search_web"])
    try:
        b.get()
        raise AssertionError("barrier still closed: only one writer arrived")
    except EmptyChannelError:
        pass

    b.update(["search_db"])
    b.get()

    fresh = b.fresh_copy()
    assert isinstance(fresh, NamedBarrierValue)
    try:
        fresh.get()
        raise AssertionError("a fresh barrier must start closed")
    except EmptyChannelError:
        pass


def test_validated_value() -> None:
    from notlonggraph.channels import ValidatedValue

    v = ValidatedValue(lambda x: isinstance(x, int) and x > 0)
    v.update([5])
    assert v.get() == 5
    v.update([10])
    assert v.get() == 10

    try:
        v.update([-3])
        raise AssertionError("an invalid value must be rejected")
    except InvalidUpdateError:
        pass
    assert v.get() == 10

    fresh = v.fresh_copy()
    assert isinstance(fresh, ValidatedValue)
    try:
        fresh.update([-1])
        raise AssertionError("fresh copy must keep the validator")
    except InvalidUpdateError:
        pass


if __name__ == "__main__":
    test_channels()
    print("basic channels (LastValue, BinaryOperatorAggregate): OK")

    new_channels = [
        ("Topic", test_topic),
        ("WindowedValue", test_windowed_value),
        ("EphemeralValue", test_ephemeral_value),
        ("NamedBarrierValue", test_named_barrier),
        ("ValidatedValue", test_validated_value),
    ]

    pending = []
    for name, fn in new_channels:
        try:
            fn()
        except (ImportError, AttributeError, NotImplementedError) as exc:
            # not implemented yet -> report, keep going. AssertionError (a wrong
            # implementation) is NOT caught here: it must fail loudly.
            pending.append(name)
            print(f"{name}: pending ({type(exc).__name__}: {exc})")
        else:
            print(f"{name}: OK")

    if not pending:
        print("all tests pass")
    else:
        print(f"\nstill to implement: {', '.join(pending)}")
