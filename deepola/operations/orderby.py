from .base import BaseOperation
import polars as pl

class ORDERBY(BaseOperation):
    def __init__(self, args):
        super().__init__(args)

    def validate(self):
        for arg in self.args:
            assert('column' in arg)
            if 'order' in arg:
                assert(arg['order'].lower() in ['asc','desc'])
        return True

    def evaluate(self, input):
        key = list(input.keys())[0]
        df = input[key]
        reverse = []
        columns = []
        for arg in self.args:
            columns.append(arg['column'])
            if 'order' in arg:
                if arg['order'].lower() == 'desc':
                    reverse.append(True) ## True for DESC
                else:
                    reverse.append(False)
            else:
                reverse.append(False) ## False for ASC
        return df.sort(by = columns, reverse = reverse)

    def merge(self, current_state, delta, return_delta = False):
        assert(return_delta == False)
        if current_state['result'] is not None:
            output = pl.concat([current_state['result'],delta])
            current_state['result'] = self.evaluate(output)
        else:
            current_state['result'] = self.evaluate(delta)
        return current_state, current_state['result']