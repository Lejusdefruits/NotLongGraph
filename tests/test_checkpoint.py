import asyncio
import operator
from typing import Annotated, TypedDict

from notlonggraph.constants import END, START
from notlonggraph.engine import read_state
from notlonggraph.graph import StateGraph


class LinearState(TypedDict):
    value: int
    log: Annotated[list, operator.add]


def _double(state):
    return {"value": state["value"] * 2, "log": ["double"]}


async def _add_ten(state):
    await asyncio.sleep(0)
    return {"value": state["value"] + 10, "log": ["add_ten"]}


def _build_linear():
    g = StateGraph(LinearState)
    g.add_node("double", _double)
    g.add_node("a", _add_ten)
    g.add_node("b", _add_ten)
    g.add_edge(START, "double")
    g.add_edge("double", "a")
    g.add_edge("a", "b")
    g.add_edge("b", END)
    return g.compile()


def test_history_records_every_super_step() -> None:
    app = _build_linear()
    asyncio.run(app.ainvoke({"value": 3}))
    hist = app.checkpointer.history
    assert [cp.step for cp in hist] == [0, 1, 2, 3]
    assert read_state(hist[0].channels) == {"value": 3}
    assert hist[0].active == ["double"]
    assert read_state(hist[-1].channels)["value"] == 26
    assert hist[-1].active == []


def test_get_state_history() -> None:
    app = _build_linear()
    asyncio.run(app.ainvoke({"value": 3}))
    states = app.checkpointer.get_state_history()
    assert states[0] == {"value": 3}
    assert states[-1] == {"value": 26, "log": ["double", "add_ten", "add_ten"]}


def test_rewind_matches_full_run() -> None:
    app = _build_linear()
    full = asyncio.run(app.ainvoke({"value": 3}))

    async def resume():
        final = None
        async for state in app.rewind(1):  # juste apres 'double' (value=6)
            final = state
        return final

    assert asyncio.run(resume()) == full


def test_rewind_does_not_mutate_history() -> None:
    app = _build_linear()
    asyncio.run(app.ainvoke({"value": 3}))
    before = app.checkpointer.get_state_history()

    async def resume():
        async for _ in app.rewind(1):
            pass

    asyncio.run(resume())
    assert app.checkpointer.get_state_history() == before


def test_fork_injects_new_input() -> None:
    app = _build_linear()
    asyncio.run(app.ainvoke({"value": 3}))

    async def branch():
        final = None
        async for state in app.fork(1, {"value": 100}):  # patch value=100 apres 'double'
            final = state
        return final

    # depuis value=100 : a (+10) -> b (+10) = 120
    assert asyncio.run(branch())["value"] == 120


class CowState(TypedDict):
    config: int
    log: Annotated[list, operator.add]


def _emit(state):
    return {"log": ["x"]}


def test_cow_shares_unchanged_channel() -> None:
    g = StateGraph(CowState)
    g.add_node("a", _emit)
    g.add_node("b", _emit)
    g.add_edge(START, "a")
    g.add_edge("a", "b")
    g.add_edge("b", END)
    app = g.compile()
    asyncio.run(app.ainvoke({"config": 42, "log": []}))
    hist = app.checkpointer.history
    # 'config' n'est jamais reecrit -> meme objet channel partage entre checkpoints
    assert hist[1].channels["config"] is hist[2].channels["config"]
    # 'log' est ecrit a chaque vague -> objets distincts
    assert hist[1].channels["log"] is not hist[2].channels["log"]


def test_all() -> None:
    test_history_records_every_super_step()
    test_get_state_history()
    test_rewind_matches_full_run()
    test_rewind_does_not_mutate_history()
    test_fork_injects_new_input()
    test_cow_shares_unchanged_channel()


if __name__ == "__main__":
    test_all()
    print("all tests pass")
