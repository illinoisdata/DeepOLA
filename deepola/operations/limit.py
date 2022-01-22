from .base import BaseOperation
import polars as pl

class LIMIT(BaseOperation):
    def __init__(self, args):
        super().__init__(args)

    def validate(self):
        assert('k' in self.args)
        return True

    def evaluate(self, state, input):
        key = list(input.keys())[0]
        df = input[key]
        k = self.args['k']
        return df[:k]

    def merge(self, current_state, delta, return_delta = False):
        assert(return_delta == False)
        if current_state['result'] is not None:
            output = pl.concat([current_state['result'],delta])
            current_state['result'] = self.evaluate(output)
        else:
            current_state['result'] = self.evaluate(delta)
        return current_state, current_state['result']