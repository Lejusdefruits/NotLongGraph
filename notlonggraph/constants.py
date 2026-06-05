class Sentinel:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return self.name

class _END(Sentinel):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super(_END, cls).__new__(cls)
        return cls._instance
    def __init__(self):
        super().__init__("END")

class _START(Sentinel):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "_instance"):
            cls._instance = super(_START, cls).__new__(cls)
        return cls._instance
    def __init__(self):
        super().__init__("START")

END = _END()
START = _START()