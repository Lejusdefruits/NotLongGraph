import asyncio
import operator
from typing import Annotated, TypedDict

from notlonggraph.constants import END, START
from notlonggraph.debugger import serve
from notlonggraph.graph import StateGraph


class State(TypedDict):
    iteration: int
    test_fails: int
    review_fails: int
    code: str
    log: Annotated[list, operator.add]


async def spec(state):
    await asyncio.sleep(0.1)
    return {"iteration": 0, "test_fails": 0, "review_fails": 0, "code": "", "log": ["drafted spec"]}


async def write_code(state):
    await asyncio.sleep(0.2)
    return {"iteration": state["iteration"] + 1, "code": f"v{state['iteration']+1}", "log": ["wrote code"]}


async def run_tests(state):
    await asyncio.sleep(0.15)
    return {"test_fails": state["test_fails"] + 1, "log": ["ran tests"]}


def route_tests(state):
    if state["test_fails"] <= 2:
        return "fail"
    return "pass"


async def debug(state):
    await asyncio.sleep(0.1)
    return {"log": ["debugged code"]}


async def review_code(state):
    await asyncio.sleep(0.3)
    return {"review_fails": state["review_fails"] + 1, "log": ["code reviewed"]}


def route_review(state):
    if state["review_fails"] <= 1:
        return "reject"
    return "approve"


async def rewrite(state):
    await asyncio.sleep(0.1)
    return {"log": ["addressed review comments"]}


async def deploy(state):
    await asyncio.sleep(0.05)
    return {"log": ["deployed to production!"]}


def build():
    g = StateGraph(State)
    
    nodes = [
        ("spec", spec),
        ("write_code", write_code),
        ("run_tests", run_tests),
        ("debug", debug),
        ("review_code", review_code),
        ("rewrite", rewrite),
        ("deploy", deploy),
    ]
    for name, fn in nodes:
        g.add_node(name, fn)
        
    g.add_edge(START, "spec")
    g.add_edge("spec", "write_code")
    g.add_edge("write_code", "run_tests")
    
    g.add_conditional_edge("run_tests", route_tests, {"fail": "debug", "pass": "review_code"})
    g.add_edge("debug", "write_code")
    
    g.add_conditional_edge("review_code", route_review, {"reject": "rewrite", "approve": "deploy"})
    g.add_edge("rewrite", "write_code")
    
    g.add_edge("deploy", END)
    
    return g.compile()


if __name__ == "__main__":
    serve(build(), {"iteration": 0, "test_fails": 0, "review_fails": 0, "code": "", "log": []})
