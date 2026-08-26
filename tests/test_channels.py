import operator

from notlonggraph.channels import BinaryOperatorAggregate, LastValue
from notlonggraph.errors import EmptyChannelError, InvalidUpdateError


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


def test_dynamic_barrier() -> None:
    from notlonggraph.channels import DynamicBarrierValue, WaitForNames

    b = DynamicBarrierValue()
    try:
        b.get()
        raise AssertionError("an unarmed barrier must stay closed")
    except EmptyChannelError:
        pass

    b.update([WaitForNames({"a", "b"})])     # arm with the names to wait for
    try:
        b.get()
        raise AssertionError("armed but no writer yet: still closed")
    except EmptyChannelError:
        pass

    b.update(["a"])
    try:
        b.get()
        raise AssertionError("only one writer arrived: still closed")
    except EmptyChannelError:
        pass

    b.update(["b"])
    assert b.get() is None, "all names seen -> barrier open (non-empty)"

    assert b.consume() is True, "consuming a full barrier clears it and reports a change"
    try:
        b.get()
        raise AssertionError("after consume the barrier closes again")
    except EmptyChannelError:
        pass
    assert b.consume() is False, "nothing left to consume -> no change"

    fresh = b.fresh_copy()
    assert isinstance(fresh, DynamicBarrierValue)
    try:
        fresh.get()
        raise AssertionError("a fresh barrier must start unarmed")
    except EmptyChannelError:
        pass


def test_history_value() -> None:
    from notlonggraph.channels import HistoryValue

    h = HistoryValue()
    try:
        h.get()
        raise AssertionError("reading an empty history should raise EmptyChannelError")
    except EmptyChannelError:
        pass

    h.update(["v1"])
    h.update(["v2"])
    h.update(["v3"])
    assert h.get() == "v3", "get() returns the current (latest) value"
    assert h.snapshot() == ["v1", "v2", "v3"], "snapshot() keeps every past value"

    # snapshot() is a copy: mutating it must not corrupt the channel
    snap = h.snapshot()
    snap.append("tampered")
    assert h.snapshot() == ["v1", "v2", "v3"], "the internal history must stay intact"

    h.update([])                             # empty wave -> ignored
    assert h.snapshot() == ["v1", "v2", "v3"]

    fresh = h.fresh_copy()
    assert isinstance(fresh, HistoryValue)
    assert fresh.snapshot() == [], "a fresh history starts empty"


def test_consensus_value() -> None:
    from notlonggraph.channels import ConsensusValue

    c = ConsensusValue()
    try:
        c.get()
        raise AssertionError("reading an empty consensus should raise EmptyChannelError")
    except EmptyChannelError:
        pass

    c.update(["a", "a", "b"])                # majority vote of one wave
    assert c.get() == "a"

    c.update(["x", "y"])                     # tie -> first value seen wins
    assert c.get() == "x"

    c.update(["z", "z"])                     # each wave is recomputed, no carry-over
    assert c.get() == "z"

    fresh = c.fresh_copy()
    assert isinstance(fresh, ConsensusValue)
    try:
        fresh.get()
        raise AssertionError("a fresh consensus starts empty")
    except EmptyChannelError:
        pass


def test_expiring_value() -> None:
    from notlonggraph.channels import EphemeralValue, ExpiringValue

    # ttl=1 must behave exactly like an EphemeralValue
    e1 = ExpiringValue(1)
    e2 = EphemeralValue()
    e1.update(["x"])
    e2.update(["x"])
    assert e1.get() == "x" and e2.get() == "x"
    e1.on_step_end()
    e2.on_step_end()
    assert e1.get() == "x" and e2.get() == "x", "ttl=1 survives the wave it was written in"
    e1.on_step_end()
    e2.on_step_end()
    for ch in (e1, e2):
        try:
            ch.get()
            raise AssertionError("ttl=1 must vanish on the next wave, like EphemeralValue")
        except EmptyChannelError:
            pass

    # ttl=2 stays visible one wave longer
    e = ExpiringValue(2)
    e.update(["y"])
    e.on_step_end()
    assert e.get() == "y"
    e.on_step_end()
    assert e.get() == "y", "ttl=2 is still visible two waves after the write"
    e.on_step_end()
    try:
        e.get()
        raise AssertionError("ttl=2 must expire eventually")
    except EmptyChannelError:
        pass

    # re-writing revives the value for a full ttl
    e.update(["z"])
    e.on_step_end()
    assert e.get() == "z", "a rewrite must re-arm the time-to-live"

    assert isinstance(e.fresh_copy(), ExpiringValue)


def test_write_once_value() -> None:
    from notlonggraph.channels import WriteOnceValue

    w = WriteOnceValue()
    try:
        w.get()
        raise AssertionError("reading an unwritten WriteOnceValue should raise")
    except EmptyChannelError:
        pass

    w.update(["first"])
    assert w.get() == "first"
    w.update(["second"])                     # locked: ignored
    assert w.get() == "first", "only the first write counts"

    # an empty wave must not lock the channel
    w2 = WriteOnceValue()
    w2.update([])
    w2.update(["real"])
    assert w2.get() == "real"

    assert isinstance(w.fresh_copy(), WriteOnceValue)


def test_default_value() -> None:
    from notlonggraph.channels import DefaultValue

    d = DefaultValue(19)
    assert d.get() == 19, "an unwritten DefaultValue returns its default, never raises"

    d.update([21])
    assert d.get() == 21, "a write takes precedence over the default"

    try:
        DefaultValue(0).update([1, 2])
        raise AssertionError("two writes in one wave should raise InvalidUpdateError")
    except InvalidUpdateError:
        pass

    fresh = d.fresh_copy()
    assert isinstance(fresh, DefaultValue)
    assert fresh.get() == 19, "fresh_copy must keep the default"


def test_rate_limited_value() -> None:
    from notlonggraph.channels import RateLimitedValue

    r = RateLimitedValue(2)                  # at most one write every 2 waves
    r.update(["a"])
    assert r.get() == "a"
    r.on_step_end()
    r.update(["b"])                          # still in cooldown -> throttled
    assert r.get() == "a", "a write during cooldown is dropped"
    r.on_step_end()
    r.update(["c"])                          # cooldown elapsed -> accepted
    assert r.get() == "c"

    # a late write (past the boundary) is still accepted, not throttled forever
    r.on_step_end()
    r.on_step_end()
    r.on_step_end()
    r.update(["d"])
    assert r.get() == "d", "once the cooldown is over, the next write must pass"

    # every=1 never throttles: it behaves like a LastValue
    r1 = RateLimitedValue(1)
    r1.update(["one"])
    r1.on_step_end()
    r1.update(["two"])
    assert r1.get() == "two"

    assert isinstance(r.fresh_copy(), RateLimitedValue)


if __name__ == "__main__":
    test_channels()
    print("basic channels (LastValue, BinaryOperatorAggregate): OK")

    new_channels = [
        ("Topic", test_topic),
        ("WindowedValue", test_windowed_value),
        ("EphemeralValue", test_ephemeral_value),
        ("NamedBarrierValue", test_named_barrier),
        ("ValidatedValue", test_validated_value),
        ("DynamicBarrierValue", test_dynamic_barrier),
        ("HistoryValue", test_history_value),
        ("ConsensusValue", test_consensus_value),
        ("ExpiringValue", test_expiring_value),
        ("WriteOnceValue", test_write_once_value),
        ("DefaultValue", test_default_value),
        ("RateLimitedValue", test_rate_limited_value),
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
