class Sentinel:
    _instances = {}
    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__new__(cls)
        return cls._instances[cls]

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return self.name

class _END(Sentinel):

    def __init__(self):
        super().__init__("END")

class _START(Sentinel):
    def __init__(self):
        super().__init__("START")

END = _END()
START = _START()