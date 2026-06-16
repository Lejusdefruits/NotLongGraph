# Hooks: code wrapped around each node (before/after), without touching the node
import time


class Skip:
    # signal returned by a before_node to short-circuit a node: the engine
    # skips the node and uses `value` as its output
    def __init__(self, value):
        self.value = value


class Hook:
    # base contract, both methods are no-ops by default
    def before_node(self, node_name, snapshot):
        return None          # the returned value is passed back to after_node as context

    def after_node(self, node_name, output, context):
        pass


class TimerHook(Hook):
    # prints the duration of each node
    def before_node(self, node_name, snapshot):
        return time.perf_counter()

    def after_node(self, node_name, output, context):
        duration = time.perf_counter() - context
        print(f"[timer] {node_name}: {duration * 1000:.1f} ms")


class LogHook(Hook):
    # traces the entry and exit of each node
    def __init__(self) -> None:
        self.logs = []

    def before_node(self, node_name, snapshot):
        self.logs.append({"node": node_name, "phase": "before", "state": snapshot})

    def after_node(self, node_name, output, context):
        self.logs.append({"node": node_name, "phase": "after", "output": output})


class CountHook(Hook):
    # counts how many times each node runs (dict {node_name: n})
    def __init__(self) -> None:
        self.counts = {}

    def after_node(self, node_name, output, context):
        self.counts[node_name] = self.counts.get(node_name, 0) + 1


class ProfilerHook(Hook):
    # accumulates the total time spent in each node over the run
    def __init__(self) -> None:
        self.timings = {}

    def before_node(self, node_name, snapshot):
        return time.perf_counter()

    def after_node(self, node_name, output, context):
        duration = time.perf_counter() - context
        self.timings[node_name] = self.timings.get(node_name, 0) + duration


class SlowNodeHook(Hook):
    # warns only if a node exceeds a threshold (in ms)
    def __init__(self, threshold_ms: float) -> None:
        self.threshold = threshold_ms / 1000.0

    def before_node(self, node_name, snapshot):
        return time.perf_counter()

    def after_node(self, node_name, output, context):
        duration = time.perf_counter() - context
        if duration > self.threshold:
            print(f"[slow] {node_name}: {duration * 1000:.1f} ms")


class CacheHook(Hook):
    # memoizes node outputs: skips a node whose input was already seen (interceptor)
    def __init__(self) -> None:
        self.cache = {}

    def _key(self, node_name, snapshot):
        return (node_name, repr(sorted(snapshot.items())))

    def before_node(self, node_name, snapshot):
        key = self._key(node_name, snapshot)
        if key in self.cache:
            return Skip(self.cache[key])     # hit -> short-circuit the node
        return key                           # miss -> carry the key to after_node

    def after_node(self, node_name, output, context):
        self.cache[context] = output         # context is the key returned on the miss


class TraceHook(Hook):
    # records a timeline of events into a list instead of printing it
    # (useful later for visualizing the super-steps)
    def __init__(self) -> None:
        ...

    def before_node(self, node_name, snapshot):
        ...

    def after_node(self, node_name, output, context):
        ...
