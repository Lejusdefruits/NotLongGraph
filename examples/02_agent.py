"""Agent loop with fan-out / fan-in / conditional loop, rendered to SVG.

START -> plan -> {search_web || search_docs} -> synth
synth --need_more--> plan   (loop)
synth --done------> answer -> END
"""
import asyncio
import operator
from typing import Annotated, TypedDict

from notlonggraph.graph import StateGraph
from notlonggraph.constants import START, END
from notlonggraph.checkpoint import MemoryCheckpointer
from notlonggraph.checkpoint import MemoryCheckpointer


class State(TypedDict):
    iter: int
    notes: Annotated[list, operator.add]


def plan(state):
    return {"notes": ["plan"]}


async def search_web(state):
    await asyncio.sleep(0)
    return {"notes": ["web"]}


async def search_docs(state):
    await asyncio.sleep(0)
    return {"notes": ["docs"]}


def synth(state):
    return {"iter": state["iter"] + 1, "notes": ["synth"]}


def answer(state):
    return {"notes": ["answer"]}


def route(state):
    return "need_more" if state["iter"] < 2 else "done"


def build():
    g = StateGraph(State)
    for name, fn in [("plan", plan), ("search_web", search_web),
                     ("search_docs", search_docs), ("synth", synth), ("answer", answer)]:
        g.add_node(name, fn)
    g.add_edge(START, "plan")
    g.add_edge("plan", "search_web")
    g.add_edge("plan", "search_docs")
    g.add_edge("search_web", "synth")
    g.add_edge("search_docs", "synth")
    g.add_conditional_edge("synth", route, {"need_more": "plan", "done": "answer"})
    g.add_edge("answer", END)
    return g.compile()


async def main():
    app = build()
    final_state = await app.ainvoke({"iter": 0, "notes": []})
    print("Final state:", final_state)

if __name__ == "__main__":
    asyncio.run(main())
