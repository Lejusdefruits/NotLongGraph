import json
import time

from rich.pretty import Pretty
from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.reactive import reactive
from textual.widgets import DataTable, Footer, Header, Input, Label, OptionList, Static

from notlonggraph.checkpoint import MemoryCheckpointer
from notlonggraph.constants import END, START
from notlonggraph.engine import read_state
from notlonggraph.hooks import Hook


class TuiTimingHook(Hook):
    def __init__(self):
        self.last_duration = {}

    def before_node(self, node_name, snapshot):
        return time.perf_counter()

    def after_node(self, node_name, output, context):
        self.last_duration[node_name] = (time.perf_counter() - context) * 1000


def _name(node):
    if node is START:
        return "START"
    if node is END:
        return "END"
    return str(node)


class StateDisplay(Static):
    state_data = reactive({})

    def watch_state_data(self, state_data: dict) -> None:
        if not state_data:
            self.update("No state available")
            return
        self.update(Pretty(state_data, expand_all=True))


class TuiDebugger(App):
    theme = "textual-dark"

    CSS = """
    #main-container {
        layout: horizontal;
        height: 1fr;
    }
    #timeline {
        width: 15%;
        border-right: solid $primary;
        padding: 1;
    }
    #sidebar {
        width: 25%;
        border-right: solid $primary;
        padding: 1;
    }
    #state-panel {
        width: 60%;
        padding: 1 2;
    }
    .panel-title {
        text-style: bold;
        color: $text-muted;
        margin-bottom: 1;
    }
    #node-table {
        height: 1fr;
        border: solid $panel;
    }
    #timeline-list {
        height: 1fr;
        border: solid $panel;
    }
    #control-bar {
        height: auto;
        padding: 1 2;
        background: $surface;
        align: left middle;
    }
    #fork-input {
        width: 1fr;
        margin-left: 2;
    }
    .control-label {
        margin-top: 1;
    }
    """

    BINDINGS = [
        Binding("left", "prev_step", "Previous Step", show=True),
        Binding("right", "next_step", "Next Step", show=True),
        Binding("q", "quit", "Quit", show=True),
        Binding("d", "", "", show=False),
    ]

    def __init__(self, compiled, history, timing_hook, **kwargs):
        super().__init__(**kwargs)
        self.compiled = compiled
        self.history = history
        self.timing_hook = timing_hook
        self.current_step_index = 0

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="main-container"):
            with Vertical(id="timeline"):
                yield Label("Timeline", classes="panel-title")
                yield OptionList(id="timeline-list")
            with Vertical(id="sidebar"):
                yield Label("Nodes", classes="panel-title")
                yield DataTable(id="node-table")
            with Vertical(id="state-panel"):
                yield Label("Super-Step: 0", id="step-label", classes="panel-title")
                yield Container(StateDisplay(id="state-display"))
        
        with Horizontal(id="control-bar"):
            yield Label("Fork JSON:", classes="control-label")
            yield Input(placeholder='{"iter": 5}', id="fork-input")
            
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#node-table", DataTable)
        table.add_columns("Node", "Time (ms)")
        
        self._build_timeline()
        self.update_view()

    def _build_timeline(self):
        timeline = self.query_one("#timeline-list", OptionList)
        timeline.clear_options()
        for i, cp in enumerate(self.history):
            timeline.add_option(f"Step {cp.step}")
        if self.history:
            timeline.highlighted = self.current_step_index

    @on(OptionList.OptionHighlighted, "#timeline-list")
    def on_timeline_selected(self, event: OptionList.OptionHighlighted) -> None:
        if event.option_index is not None and event.option_index != self.current_step_index:
            self.current_step_index = event.option_index
            self.update_view()

    def action_prev_step(self) -> None:
        if self.current_step_index > 0:
            self.current_step_index -= 1
            self.update_view()

    def action_next_step(self) -> None:
        if self.current_step_index < len(self.history) - 1:
            self.current_step_index += 1
            self.update_view()

    def update_view(self) -> None:
        if not self.history:
            return

        timeline = self.query_one("#timeline-list", OptionList)
        with timeline.prevent(OptionList.OptionHighlighted):
            timeline.highlighted = self.current_step_index
            timeline.scroll_to_highlight()

        cp = self.history[self.current_step_index]
        self.query_one("#step-label", Label).update(
            f"Super-Step: {cp.step} ({self.current_step_index + 1}/{len(self.history)})"
        )

        state_data = read_state(cp.channels)
        self.query_one("#state-display", StateDisplay).state_data = state_data

        table = self.query_one("#node-table", DataTable)
        table.clear()

        active_names = {str(n) for n in cp.active}

        def _add_row(name, sentinel=False):
            is_active = name in active_names
            prefix = "> " if is_active else "  "
            
            timing = ""
            if not sentinel and name in self.timing_hook.last_duration:
                timing = f"{self.timing_hook.last_duration[name]:.1f}"

            from rich.text import Text
            node_text = Text(prefix + name)
            time_text = Text(timing)
            
            if is_active:
                node_text.stylize("bold green")
                time_text.stylize("bold green")
            elif sentinel:
                node_text.stylize("dim")
            
            table.add_row(node_text, time_text)

        _add_row(_name(START), sentinel=True)
        for node in self.compiled.nodes:
            _add_row(_name(node))
        _add_row(_name(END), sentinel=True)

    @on(Input.Submitted, "#fork-input")
    def submit_fork(self, event: Input.Submitted) -> None:
        patch_str = event.value.strip()
        if not patch_str:
            return
            
        try:
            patch = json.loads(patch_str)
            if not isinstance(patch, dict):
                raise ValueError("Must be a JSON object")
        except Exception as e:
            self.notify(f"Invalid JSON: {e}", severity="error")
            return
            
        self.drive_fork(patch)

    @work
    async def drive_fork(self, patch: dict):
        branch_cp = MemoryCheckpointer()
        try:
            async for _ in self.compiled.fork(self.current_step_index, patch, checkpointer=branch_cp):
                pass
            self.history = branch_cp.history
            self.current_step_index = 0
            self._build_timeline()
            self.update_view()
            self.query_one("#fork-input").value = ""
            self.notify("Fork successful! Switched to new timeline.")
        except Exception as e:
            self.notify(f"Fork failed: {e}", severity="error")


def serve(compiled, initial_input):
    import asyncio
    
    timing_hook = TuiTimingHook()
    compiled.hooks.append(timing_hook)
    
    asyncio.run(compiled.ainvoke(initial_input))
    
    app = TuiDebugger(compiled, compiled.checkpointer.history, timing_hook)
    app.run()
