import operator
from typing import Annotated, TypedDict

from notlonggraph.channels import BinaryOperatorAggregate, LastValue
from notlonggraph.state import channels_from_schema


class State(TypedDict):
    messages: Annotated[list, operator.add]
    counter: int
    name: str


def test_state() -> None:
    channels = channels_from_schema(State)

    assert set(channels) == {"messages", "counter", "name"}

    assert isinstance(channels["messages"], BinaryOperatorAggregate)
    assert channels["messages"].operator is operator.add

    assert isinstance(channels["counter"], LastValue)
    assert isinstance(channels["name"], LastValue)

    again = channels_from_schema(State)
    assert again["messages"] is not channels["messages"]


if __name__ == "__main__":
    test_state()
    print("all tests pass")
