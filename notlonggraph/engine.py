import asyncio

from notlonggraph.constants import START, END
from notlonggraph.errors import EmptyChannelError
from notlonggraph.hooks import Skip

def read_state(channels):
    state = {}
    for key, channel in channels.items():
        try:
             tmp = channel.get()
        except EmptyChannelError:
            continue
        state[key] = tmp
    return state

def collect(outputs):
    collected_outputs = {}
    for curr_dict in outputs:
        for key, value in curr_dict.items():
            if key not in collected_outputs:
                collected_outputs[key] = []
            collected_outputs[key].append(value)
    return collected_outputs

def apply_writes(channels, grouped):
    for key, value in grouped.items():
        channels[key].update(value)

def end_step(channels):
    for channel in channels.values():
        channel.on_step_end()

def successors(edges, done, conditional_edges, state):
    next_nodes = []
    for src, dst in edges:
        if src in done and dst is not END:
            next_nodes.append(dst)
    for src, (router, path_map) in conditional_edges.items():
        if src in done:
            dst = router(state)
            if path_map is not None:
                dst = path_map[dst]
            if dst is not END:
                next_nodes.append(dst)
    return list(dict.fromkeys(next_nodes))

async def run_node(fn, snapshot, node_name, hooks):
    contexts = [h.before_node(node_name, snapshot) for h in hooks]

    hit = next((c for c in contexts if isinstance(c, Skip)), None)
    if hit is not None:
        return hit.value
    if asyncio.iscoroutinefunction(fn):
        result = await fn(snapshot)
    else:
        result = await asyncio.to_thread(fn, snapshot)
    for h, ctx in zip(hooks, contexts):
        h.after_node(node_name, result, ctx)
    return result

async def run(nodes, edges, channels, conditional_edges, input, recursion_limit=25, hooks=None):
    hooks = hooks or []
    channels = {k: ch.fresh_copy() for k, ch in channels.items()}
    collected_outputs = collect([input])
    apply_writes(channels, collected_outputs)
    yield read_state(channels)
    active = successors(edges, {START}, conditional_edges, read_state(channels))
    steps = 0
    while active:
        if steps >= recursion_limit:
            raise RecursionError(f"recursion limit of {recursion_limit} exceeded")
        snapshot = read_state(channels)
        try:
            async with asyncio.TaskGroup() as tg:
                tasks = [tg.create_task(run_node(nodes[node], snapshot, node, hooks)) for node in active]
            outputs = [t.result() for t in tasks]
        except* Exception as eg:
            raise eg.exceptions[0]
        grouped_outputs = collect(outputs)
        apply_writes(channels, grouped_outputs)
        end_step(channels)
        active = successors(edges, set(active), conditional_edges, read_state(channels))
        steps += 1
        yield read_state(channels)
