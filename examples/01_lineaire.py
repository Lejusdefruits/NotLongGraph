import asyncio
import operator
from typing import Annotated, TypedDict

from notlonggraph.graph import StateGraph
from notlonggraph.constants import START, END


class State(TypedDict):
    value: int
    log: Annotated[list, operator.add]


def double(state):
    return {"value": state["value"] * 2, "log": ["double"]}


async def add_ten(state):
    await asyncio.sleep(0)
    return {"value": state["value"] + 10, "log": ["add_ten"]}


def build():
    g = StateGraph(State)
    g.add_node("double", double)
    g.add_node("add_ten", add_ten)
    g.add_edge(START, "double")
    g.add_edge("double", "add_ten")
    g.add_edge("add_ten", END)
    return g.compile()


async def main():
    app = build()
    result = await app.ainvoke({"value": 3})
    print(result)


if __name__ == "__main__":
    asyncio.run(main())
