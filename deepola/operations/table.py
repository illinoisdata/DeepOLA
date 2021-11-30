from .base import BaseOperation
import polars as pl

class TABLE(BaseOperation):
    def __init__(self, args):
        super().__init__(args)

    def validate(self):
        return True

    def evaluate(self, input):
        key = list(input.keys())[0]
        return input[key]

    def merge(self, current_state, delta, return_delta = False):
        output_df = self.evaluate(delta)
        if current_state['result'] is not None:
            current_state['result'] = pl.concat([current_state['result'],output_df])
        else:
            current_state['result'] = output_df

        if return_delta:
            return current_state, output_df
        else:
            return current_state, current_state['result']