from .base import BaseOperation
class WHERE(BaseOperation):
    def __init__(self, args):
        super().__init__(args)

    def validate(self):
        return True

    def evaluate(self, input):
        key = list(input.keys())[0]
        return input[key]

    def merge(self, current_state, delta, return_delta = False):
        return delta