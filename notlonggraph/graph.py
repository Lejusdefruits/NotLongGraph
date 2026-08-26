import copy

from notlonggraph.checkpoint import MemoryCheckpointer
from notlonggraph.constants import END, START
from notlonggraph.errors import GraphError
from notlonggraph.state import channels_from_schema


class CompiledGraph:
    def __init__(self, nodes, edges, channels, conditional_edges, hooks):
        self.nodes = nodes
        self.edges = edges
        self.channels = channels
        self.conditional_edges = conditional_edges
        self.hooks = hooks
        self.checkpointer = MemoryCheckpointer()

    async def astream(self, input):
        from notlonggraph.engine import run
        self.checkpointer = MemoryCheckpointer()
        async for state in run(self.nodes, self.edges, self.channels, self.conditional_edges, input, hooks=self.hooks, checkpointer=self.checkpointer):
            yield state

    async def ainvoke(self, input):
        from notlonggraph.engine import run
        self.checkpointer = MemoryCheckpointer()
        async for state in run(self.nodes, self.edges, self.channels, self.conditional_edges, input, hooks=self.hooks, checkpointer=self.checkpointer):
            final = state
        return final

    async def rewind(self, k, checkpointer=None):
        from notlonggraph.engine import run
        cp = self.checkpointer.history[k]
        channels = copy.deepcopy(cp.channels)
        async for state in run(
            self.nodes, self.edges, self.channels, self.conditional_edges,
            input=None, hooks=self.hooks, checkpointer=checkpointer,
            start_channels=channels, start_active=cp.active, start_steps=cp.step,
        ):
            yield state

    async def fork(self, k, new_input, checkpointer=None):
        from notlonggraph.engine import apply_writes, collect, run
        cp = self.checkpointer.history[k]
        channels = copy.deepcopy(cp.channels)
        apply_writes(channels, collect([new_input]))
        async for state in run(
            self.nodes, self.edges, self.channels, self.conditional_edges,
            input=None, hooks=self.hooks, checkpointer=checkpointer,
            start_channels=channels, start_active=cp.active, start_steps=cp.step,
        ):
            yield state

    def __repr__(self):
        return f"CompiledGraph(nodes={list(self.nodes.keys())}, edges={self.edges}, channels={self.channels}, conditional_edges={self.conditional_edges}, hooks={self.hooks})"

class StateGraph:
    def __init__(self, schema):
        self.schema = schema
        self.nodes = {}
        self.edges = []
        self.conditional_edges = {}
        self.hooks = []

    def add_node(self, name, fn):
        if name in self.nodes or name is START or name is END:
            raise GraphError(f"node {name} already exists")
        self.nodes[name] = fn

    def add_edge(self, src, dst):
        self.edges.append((src, dst))

    def add_conditional_edge(self, src, router, path_map=None):
        self.conditional_edges[src] = (router, path_map)

    def add_hook(self, hook):
        if hook in self.hooks:
            raise GraphError(f"hook {hook} already exists")
        self.hooks.append(hook)

    def compile(self):
        for src, dst in self.edges:
            if src is not START and src not in self.nodes:
                raise GraphError(f"edge source {src} does not exist")
            if dst is not END and dst not in self.nodes:
                raise GraphError(f"edge destination {dst} does not exist")
        if not any(src is START for src, _ in self.edges):
            raise GraphError("start node does not exist")

        for src, (router, path_map) in self.conditional_edges.items():
            if src is not START and src not in self.nodes:
                raise GraphError(f"edge source {src} does not exist")
            if path_map is not None:
                for dst in path_map.values():
                     if dst is not END and dst not in self.nodes:
                        raise GraphError(f"conditional edge destination {dst} does not exist")

        channels = channels_from_schema(self.schema)
        return CompiledGraph(self.nodes, self.edges, channels, self.conditional_edges, self.hooks)

    def __repr__(self):
        return f"StateGraph(nodes={list(self.nodes.keys())}, edges={self.edges}, conditional_edges={self.conditional_edges}, hooks={self.hooks})"


if __name__ == "__main__":
    from tests.test_graph import test_all
    test_all()
    print("all tests pass")