import operator
from typing import Annotated, TypedDict

from notlonggraph.channels import BinaryOperatorAggregate, LastValue
from notlonggraph.constants import END, START
from notlonggraph.errors import GraphError
from notlonggraph.graph import StateGraph


class State(TypedDict):
    messages: Annotated[list, operator.add]
    counter: int


def _node(state):
    return state


def _build_valid() -> StateGraph:
    g = StateGraph(State)
    g.add_node("a", _node)
    g.add_node("b", _node)
    g.add_edge(START, "a")
    g.add_edge("a", "b")
    g.add_edge("b", END)
    return g


def test_compile_ok() -> None:
    compiled = _build_valid().compile()
    assert isinstance(compiled.channels["messages"], BinaryOperatorAggregate)
    assert isinstance(compiled.channels["counter"], LastValue)


def test_edge_to_unknown_node() -> None:
    g = StateGraph(State)
    g.add_node("a", _node)
    g.add_edge(START, "a")
    g.add_edge("a", "ghost")
    try:
        g.compile()
        raise AssertionError("expected GraphError")
    except GraphError:
        pass


def test_no_entry_point() -> None:
    g = StateGraph(State)
    g.add_node("a", _node)
    g.add_edge("a", END)
    try:
        g.compile()
        raise AssertionError("expected GraphError")
    except GraphError:
        pass


def test_all() -> None:
    test_compile_ok()
    test_edge_to_unknown_node()
    test_no_entry_point()


if __name__ == "__main__":
    test_all()
    print("all tests pass")
