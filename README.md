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

## Channels

A channel is the reducer for one state key: it decides how the writes collected
during a super-step are merged into a single value. Each one implements the same
contract — `update(news)`, `get()`, `fresh_copy()` — plus two optional lifecycle
hooks, `on_step_end()` (called on every channel after each step) and `consume()`
(called when the engine has acted on the value). `get()` raises
`EmptyChannelError` when the key has never been written, and `update` raises
`InvalidUpdateError` on an illegal write.

**Core**

- `LastValue` — keeps the last write; conflicts (two writes in one step) raise.
- `BinaryOperatorAggregate` — folds the writes with a binary operator (`add`, ...).
- `Topic` — collects the writes as a list; `accumulate=True` keeps history.
- `WindowedValue` — like an accumulating list, capped to the last `maxlen` items.
- `AnyValue` — like `LastValue` but tolerates several writes per step without raising.
- `UntrackedValue` — a `LastValue` excluded from checkpoints (`_checkpointable = False`).

**Lifecycle (time-aware)**

- `EphemeralValue` — visible only for the step it is written, then cleared.
- `ExpiringValue` — visible for `ttl` steps, then cleared (`EphemeralValue` is `ttl=1`).
- `RateLimitedValue` — accepts at most one write every `every` steps, throttles the rest.

**Barriers (fan-in synchronization)**

- `NamedBarrierValue` — stays empty until every expected writer (fixed set) has written.
- `DynamicBarrierValue` — same, but the expected set is sent at runtime via a
  `WaitForNames` message; `consume()` re-arms it once released.

**Validation / policy**

- `ValidatedValue` — accepts a write only if it passes a predicate, else raises.
- `WriteOnceValue` — only the first write counts; later writes are ignored.
- `DefaultValue` — a `LastValue` that returns a fallback instead of raising when empty.
- `ConsensusValue` — majority vote among the writes of a single step.

**Time-travel**

- `HistoryValue` — keeps every past value (via `snapshot()`), not just the current one.

## Hooks

A hook is code wrapped around each node, registered with `g.add_hook(...)` and
run by the engine *without touching the node itself*. Each hook implements
`before_node(node_name, snapshot)` and `after_node(node_name, output, context)`;
whatever `before_node` returns is handed back to the matching `after_node` as
`context` (e.g. a start time, a cache key). Both default to no-ops, so a hook
overrides only what it needs.

```python
g.add_hook(TimerHook())
g.add_hook(CacheHook())
```

**Observers**

- `TimerHook` — prints each node's duration.
- `LogHook` — records a timeline of entries/exits into `.logs`.
- `CountHook` — counts how many times each node runs (`.counts`).
- `ProfilerHook` — accumulates the total time per node (`.timings`).
- `SlowNodeHook(threshold_ms)` — warns only when a node exceeds a threshold.

**Interceptor**

- `CacheHook` — memoizes node outputs by input; on a repeated input its
  `before_node` returns a `Skip(value)` and the engine short-circuits the node
  instead of running it.

Returning `Skip(value)` from any `before_node` tells the engine to skip the node
and use `value` as its output — the basis for caching and any short-circuit hook.

## Run the tests

```bash
uv sync
uv run pytest
```
