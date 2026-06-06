class NotLongGraphError(Exception):
    """base class for all framework exceptions"""


class GraphError(NotLongGraphError):
    """raised when the graph is invalid (at compile time)"""


class InvalidUpdateError(NotLongGraphError):
    """raised when a channel receives writes it cannot merge
    (ex: several concurrent writes on a LastValue during one step)"""


class EmptyChannelError(NotLongGraphError):
    """raised when reading a channel that has never been written"""
