from typing import get_type_hints, get_origin, get_args, Annotated
from notlonggraph.channels import LastValue, BinaryOperatorAggregate

def channels_from_schema(schema):
    hints = get_type_hints(schema, include_extras=True)
    table = {}
    for k, v in hints.items():
        if get_origin(v) is Annotated:
            table[k] = BinaryOperatorAggregate(*get_args(v)[1:])
        else:
            table[k] = LastValue()
    return table