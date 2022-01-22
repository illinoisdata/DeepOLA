from polars import col
from .base import BaseOperation
import polars as pl

class SELECT(BaseOperation):
    def __init__(self, args):
        super().__init__(args)

    def validate(self):
        assert('columns' in self.args)
        return True

    def evaluate(self, state, input):
        key = list(input.keys())[0]
        df = input[key]
        columns = self.args['columns']
        if len(columns) == 1 and columns[0] == '*':
            return df
        else:
            return df[columns]

    def merge(self, current_state, delta, return_delta = False):
        output = self.evaluate(delta)
        if current_state['result'] is not None:
            current_state['result'] = pl.concat([current_state['result'],output])
        else:
            current_state['result'] = output
        if return_delta:
            return current_state, output
        else:
            return current_state, current_state['result']
