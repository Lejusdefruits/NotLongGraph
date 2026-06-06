from notlonggraph.constants import START, END
from notlonggraph.errors import GraphError
from notlonggraph.state import channels_from_schema

class CompiledGraph:
    def __init__(self, nodes, edges, channels):
        self.nodes = nodes
        self.edges = edges
        self.channels = channels

class StateGraph:
    def __init__(self, schema):
        self.schema = schema
        self.nodes = {}
        self.edges = []

    def add_node(self, name, fn):
        if name in self.nodes or name is START or name is END:
            raise GraphError(f"node {name} already exists")
        self.nodes[name] = fn

    def add_edge(self, src, dst):
        self.edges.append((src, dst))

    def compile(self):
        for src, dst in self.edges:
            if src is not START and src not in self.nodes:
                raise GraphError(f"edge source {src} does not exist")
            if dst is not END and dst not in self.nodes:
                raise GraphError(f"edge destination {dst} does not exist")
        if not any(src is START for src, _ in self.edges):
            raise GraphError("start node does not exist")
        channels = channels_from_schema(self.schema)
        return CompiledGraph(self.nodes, self.edges, channels)


if __name__ == "__main__":
    from tests.test_graph import test_all
    test_all()
    print("all tests pass")