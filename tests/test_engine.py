import asyncio
import operator
from typing import Annotated, TypedDict

from notlonggraph.constants import END, START
from notlonggraph.graph import StateGraph


class LinearState(TypedDict):
    value: int
    log: Annotated[list, operator.add]


def _double(state):
    return {"value": state["value"] * 2, "log": ["double"]}


async def _add_ten(state):
    await asyncio.sleep(0)
    return {"value": state["value"] + 10, "log": ["add_ten"]}


def test_linear() -> None:
    g = StateGraph(LinearState)
    g.add_node("double", _double)
    g.add_node("add_ten", _add_ten)
    g.add_edge(START, "double")
    g.add_edge("double", "add_ten")
    g.add_edge("add_ten", END)
    app = g.compile()

    result = asyncio.run(app.ainvoke({"value": 3}))
    assert result == {"value": 16, "log": ["double", "add_ten"]}


class LoopState(TypedDict):
    counter: int


def _inc(state):
    return {"counter": state["counter"] + 1}


def _should_continue(state):
    return "loop" if state["counter"] < 3 else "done"


def test_conditional_loop() -> None:
    g = StateGraph(LoopState)
    g.add_node("inc", _inc)
    g.add_edge(START, "inc")
    g.add_conditional_edge("inc", _should_continue, {"loop": "inc", "done": END})
    app = g.compile()

    result = asyncio.run(app.ainvoke({"counter": 0}))
    assert result == {"counter": 3}


def _router_to_node(state):
    return "inc" if state["counter"] < 2 else END


def test_conditional_without_path_map() -> None:
    g = StateGraph(LoopState)
    g.add_node("inc", _inc)
    g.add_edge(START, "inc")
    g.add_conditional_edge("inc", _router_to_node)
    app = g.compile()

    result = asyncio.run(app.ainvoke({"counter": 0}))
    assert result == {"counter": 2}


def _always_loop(state):
    return "inc"


def test_recursion_limit() -> None:
    g = StateGraph(LoopState)
    g.add_node("inc", _inc)
    g.add_edge(START, "inc")
    g.add_conditional_edge("inc", _always_loop, {"inc": "inc"})
    app = g.compile()

    try:
        asyncio.run(app.ainvoke({"counter": 0}))
        raise AssertionError("expected RecursionError")
    except RecursionError:
        pass


class DiamondState(TypedDict):
    log: Annotated[list, operator.add]


def _diamond_double(state):
    return {"log": ["double"]}


async def _diamond_a(state):
    await asyncio.sleep(0)
    return {"log": ["a"]}


async def _diamond_b(state):
    await asyncio.sleep(0)
    return {"log": ["b"]}


def _diamond_join(state):
    return {"log": ["join"]}


def test_diamond_fan_in() -> None:
    g = StateGraph(DiamondState)
    g.add_node("double", _diamond_double)
    g.add_node("a", _diamond_a)
    g.add_node("b", _diamond_b)
    g.add_node("join", _diamond_join)
    g.add_edge(START, "double")
    g.add_edge("double", "a")
    g.add_edge("double", "b")
    g.add_edge("a", "join")
    g.add_edge("b", "join")
    g.add_edge("join", END)
    app = g.compile()

    result = asyncio.run(app.ainvoke({"log": []}))

    assert result["log"].count("join") == 1
    assert "a" in result["log"] and "b" in result["log"]
    assert result["log"] == ["double", "a", "b", "join"]


def _boom(state):
    raise ValueError("node exploded")


def test_node_raises() -> None:
    g = StateGraph(LoopState)
    g.add_node("boom", _boom)
    g.add_edge(START, "boom")
    g.add_edge("boom", END)
    app = g.compile()

    try:
        asyncio.run(app.ainvoke({"counter": 0}))
        raise AssertionError("expected the error to propagate")
    except ValueError as exc:
        assert str(exc) == "node exploded"


_sibling_side_effects = []


async def _slow_sibling(state):
    await asyncio.sleep(0.2)
    _sibling_side_effects.append("done")
    return {"counter": 1}


def test_sibling_cancelled_on_error() -> None:
    _sibling_side_effects.clear()

    async def scenario():
        g = StateGraph(LoopState)
        g.add_node("boom", _boom)
        g.add_node("slow", _slow_sibling)
        g.add_edge(START, "boom")
        g.add_edge(START, "slow")
        g.add_edge("boom", END)
        g.add_edge("slow", END)
        app = g.compile()
        try:
            await app.ainvoke({"counter": 0})
        except ValueError:
            pass
        await asyncio.sleep(0.4)  # laisse le temps a un eventuel zombie de finir

    asyncio.run(scenario())
    assert _sibling_side_effects == []


def test_astream_yields_each_wave() -> None:
    g = StateGraph(LinearState)
    g.add_node("double", _double)
    g.add_node("add_ten", _add_ten)
    g.add_edge(START, "double")
    g.add_edge("double", "add_ten")
    g.add_edge("add_ten", END)
    app = g.compile()

    async def collect_stream():
        states = []
        async for state in app.astream({"value": 3}):
            states.append(state)
        return states

    states = asyncio.run(collect_stream())

    assert states[0]["value"] == 3
    assert states[-1]["value"] == 16
    final = asyncio.run(app.ainvoke({"value": 3}))
    assert states[-1] == final


def test_all() -> None:
    test_linear()
    test_conditional_loop()
    test_conditional_without_path_map()
    test_recursion_limit()
    test_diamond_fan_in()
    test_node_raises()
    test_sibling_cancelled_on_error()
    test_astream_yields_each_wave()


if __name__ == "__main__":
    test_all()
    print("all tests pass")
