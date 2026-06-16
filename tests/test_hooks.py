import asyncio
import operator
from typing import Annotated, TypedDict

from notlonggraph.graph import StateGraph
from notlonggraph.constants import START, END
from notlonggraph.errors import GraphError
from notlonggraph.hooks import Hook, TimerHook, LogHook


class State(TypedDict):
    value: int
    log: Annotated[list, operator.add]


def double(state):
    return {"value": state["value"] * 2, "log": ["double"]}


async def add_ten(state):
    await asyncio.sleep(0)
    return {"value": state["value"] + 10, "log": ["add_ten"]}


def _build() -> StateGraph:
    g = StateGraph(State)
    g.add_node("double", double)
    g.add_node("add_ten", add_ten)
    g.add_edge(START, "double")
    g.add_edge("double", "add_ten")
    g.add_edge("add_ten", END)
    return g


class RecordingHook(Hook):
    # spy hook: stores every call so the test can inspect the wiring
    def __init__(self) -> None:
        self.events = []

    def before_node(self, node_name, snapshot):
        self.events.append(("before", node_name, snapshot))
        return f"ctx-{node_name}"

    def after_node(self, node_name, output, context):
        self.events.append(("after", node_name, output, context))


def test_base_contract() -> None:
    h = Hook()
    assert h.before_node("n", {}) is None, "the base before_node is a no-op returning None"
    assert h.after_node("n", {}, None) is None, "the base after_node is a no-op"


def test_provided_hooks_run() -> None:
    t = TimerHook()
    start = t.before_node("n", {})
    assert isinstance(start, float), "TimerHook.before_node returns a start time"
    t.after_node("n", {"x": 1}, start)       # prints a duration, must not raise

    log = LogHook()
    assert log.before_node("n", {}) is None
    log.after_node("n", {"x": 1}, None)      # prints, must not raise


def test_hooks_fire_around_each_node() -> None:
    g = _build()
    rec = RecordingHook()
    g.add_hook(rec)
    asyncio.run(g.compile().ainvoke({"value": 3}))

    # before/after fire around every node, in execution order
    phases = [(e[0], e[1]) for e in rec.events]
    assert phases == [
        ("before", "double"),
        ("after", "double"),
        ("before", "add_ten"),
        ("after", "add_ten"),
    ]


def test_context_flows_before_to_after() -> None:
    g = _build()
    rec = RecordingHook()
    g.add_hook(rec)
    asyncio.run(g.compile().ainvoke({"value": 3}))

    for event in rec.events:
        if event[0] == "after":
            _, node_name, _output, context = event
            assert context == f"ctx-{node_name}", "after_node gets the context returned by before_node"


def test_hook_sees_snapshot_and_output() -> None:
    g = _build()
    rec = RecordingHook()
    g.add_hook(rec)
    asyncio.run(g.compile().ainvoke({"value": 3}))

    befores = [e for e in rec.events if e[0] == "before"]
    assert all(isinstance(e[2], dict) for e in befores), "before_node receives the state snapshot"

    outputs = {e[1]: e[2] for e in rec.events if e[0] == "after"}
    assert outputs["double"] == {"value": 6, "log": ["double"]}
    assert outputs["add_ten"]["value"] == 16


def test_multiple_hooks_compose_in_order() -> None:
    g = _build()
    first, second = RecordingHook(), RecordingHook()
    g.add_hook(first)
    g.add_hook(second)
    asyncio.run(g.compile().ainvoke({"value": 3}))

    # both hooks saw the same sequence of nodes
    assert [e[1] for e in first.events] == [e[1] for e in second.events]
    assert len(first.events) == 4 and len(second.events) == 4


def test_profiler_isolates_parallel_nodes() -> None:
    from notlonggraph.hooks import ProfilerHook

    def fast(state):
        return {"log": ["fast"]}

    async def slow(state):
        await asyncio.sleep(0.05)
        return {"log": ["slow"]}

    g = StateGraph(State)
    g.add_node("fast", fast)
    g.add_node("slow", slow)
    g.add_edge(START, "fast")        # fast and slow fan out in the same wave
    g.add_edge(START, "slow")
    g.add_edge("fast", END)
    g.add_edge("slow", END)
    prof = ProfilerHook()
    g.add_hook(prof)
    asyncio.run(g.compile().ainvoke({"value": 1}))

    # each node is timed from its OWN start (context), not a shared attribute
    assert prof.timings["slow"] > prof.timings["fast"]
    assert prof.timings["slow"] >= 0.04, "slow node must keep its real ~0.05s duration"
    assert prof.timings["fast"] < 0.02, "fast node must not inherit the slow node's start"


def test_cache_hook_skips_repeated_inputs() -> None:
    from notlonggraph.hooks import CacheHook

    runs = {"double": 0}

    def counting_double(state):
        runs["double"] += 1
        return {"value": state["value"] * 2, "log": ["double"]}

    g = StateGraph(State)
    g.add_node("double", counting_double)
    g.add_edge(START, "double")
    g.add_edge("double", END)
    g.add_hook(CacheHook())
    app = g.compile()

    first = asyncio.run(app.ainvoke({"value": 3}))
    second = asyncio.run(app.ainvoke({"value": 3}))     # same input -> cache hit

    assert first == second, "the cached output must match the computed one"
    assert runs["double"] == 1, "the node must run once, then be served from cache"

    asyncio.run(app.ainvoke({"value": 5}))              # different input -> miss
    assert runs["double"] == 2, "a new input must re-run the node"


def test_add_hook_rejects_duplicate() -> None:
    g = _build()
    h = TimerHook()
    g.add_hook(h)
    try:
        g.add_hook(h)
        raise AssertionError("adding the same hook twice should raise GraphError")
    except GraphError:
        pass


def test_no_hooks_runs_unchanged() -> None:
    result = asyncio.run(_build().compile().ainvoke({"value": 3}))
    assert result == {"value": 16, "log": ["double", "add_ten"]}


if __name__ == "__main__":
    tests = [
        ("base_contract", test_base_contract),
        ("provided_hooks_run", test_provided_hooks_run),
        ("hooks_fire_around_each_node", test_hooks_fire_around_each_node),
        ("context_flows_before_to_after", test_context_flows_before_to_after),
        ("hook_sees_snapshot_and_output", test_hook_sees_snapshot_and_output),
        ("multiple_hooks_compose_in_order", test_multiple_hooks_compose_in_order),
        ("profiler_isolates_parallel_nodes", test_profiler_isolates_parallel_nodes),
        ("cache_hook_skips_repeated_inputs", test_cache_hook_skips_repeated_inputs),
        ("add_hook_rejects_duplicate", test_add_hook_rejects_duplicate),
        ("no_hooks_runs_unchanged", test_no_hooks_runs_unchanged),
    ]
    for name, fn in tests:
        fn()
        print(f"{name}: OK")
    print("all hook tests pass")
