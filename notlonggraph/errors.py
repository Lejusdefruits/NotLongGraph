class NotLongGraphError(Exception):
    """raised when the graph is invalid"""
    pass

class GraphError(NotLongGraphError):
    """base class for graph exceptions"""
    pass