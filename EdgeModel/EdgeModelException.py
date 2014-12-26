

class EdgeModelException(Exception):

    def __init__(self, message, idx=None):
        self.message = message
        self.idx = idx

