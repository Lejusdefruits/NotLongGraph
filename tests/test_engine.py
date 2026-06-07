import asyncio
import operator
from typing import Annotated, TypedDict

from notlonggraph.graph import StateGraph
from notlonggraph.constants import START, END


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


def test_all() -> None:
    test_linear()
    test_conditional_loop()
    test_conditional_without_path_map()
    test_recursion_limit()


if __name__ == "__main__":
    test_all()
    print("all tests pass")
