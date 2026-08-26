from typing import Annotated, get_args, get_origin, get_type_hints

from notlonggraph.channels import BinaryOperatorAggregate, LastValue


def channels_from_schema(schema):
    hints = get_type_hints(schema, include_extras=True)
    table = {}
    for k, v in hints.items():
        if get_origin(v) is Annotated:
            table[k] = BinaryOperatorAggregate(*get_args(v)[1:])
        else:
            table[k] = LastValue()
    return table


if __name__ == "__main__":
    from tests.test_state import test_state
    test_state()
    print("all tests pass")
