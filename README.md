# NotLongGraph

A from-scratch reimplementation of [LangGraph](https://github.com/langchain-ai/langgraph)'s
execution core — built to understand, from the inside out, how stateful agent
orchestration actually works. Async-first, Python 3.12.

## The model

LangGraph is a **state machine whose execution follows a graph**. NotLongGraph
rebuilds its four primitives and the engine that drives them:

- **State** — a shared dict that flows through the graph.
- **Node** — a (sync or async) function `state -> partial update`.
- **Edge** — a transition to the next node(s); fixed or conditional.
- **Channel** — the reducer that merges updates for one state key (overwrite,
  list-append, ...).

The engine runs on the **Pregel / BSP** (Bulk Synchronous Parallel) model, in
*super-steps*. Each step:

1. all active nodes read the **same snapshot** of the state;
2. they run **in parallel** (`asyncio.gather` / `TaskGroup`);
3. every output is merged through the channels;
4. the next wave of active nodes is computed.

Writes from one step are only visible at the next — that's what makes the
parallelism deterministic.

## Usage

```python
import asyncio, operator
from typing import Annotated, TypedDict
from notlonggraph.graph import StateGraph
from notlonggraph.constants import START, END

class State(TypedDict):
    value: int
    log: Annotated[list, operator.add]   # reducer: append instead of overwrite

def double(state):           return {"value": state["value"] * 2, "log": ["double"]}
async def add_ten(state):    return {"value": state["value"] + 10, "log": ["add_ten"]}

g = StateGraph(State)
g.add_node("double", double)
g.add_node("add_ten", add_ten)
g.add_edge(START, "double")
g.add_edge("double", "add_ten")
g.add_edge("add_ten", END)
app = g.compile()

print(asyncio.run(app.ainvoke({"value": 3})))   # {'value': 16, 'log': ['double', 'add_ten']}
```

`add_conditional_edge(src, router, path_map)` routes on the current state, and
`astream` yields the state after each super-step.

## What's implemented

- Typed-schema → channels (`Annotated[..., reducer]`), `LastValue` and reducer channels.
- Async engine: snapshot reads, parallel node execution, write collection and merge.
- Fixed and conditional edges, fan-in deduplication, recursion limit.
- `ainvoke` / `astream`, error propagation and cancellation.

## Run the tests

```bash
uv sync
uv run pytest
```
