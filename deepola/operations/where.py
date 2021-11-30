from .base import BaseOperation
import polars as pl
from polars import col
from datetime import datetime

class WHERE(BaseOperation):
    def __init__(self, args):
        """
        args = {
            'predicates': [
                [
                    {
                        'left': the left side operator,
                        'op': the operation,
                        'right': the right side operator
                    },
                ]
            ],
            'form': 'DNF'/'CNF'
        }
        """
        super().__init__(args)

    def validate(self):
        """
        What should WHERE args look like?
        """
        assert('predicates' in self.args)
        return True

    def evaluate(self, input):
        key = list(input.keys())[0]
        df = input[key]
        for predicate in self.args['predicates']:
            num_expressions = len(predicate)
            ### OR across all these predicates.
            combined_expressions = []
            for i in range(num_expressions):
                expression = predicate[i]
                left = expression['left']
                op = expression['op']
                right = expression['right']
                if df[left].dtype.__name__ == "Date":
                    right = f'pl.lit(datetime.strptime("{right}","%Y-%m-%d"))'
                elif type(right) == str:
                    right = f'"{right}"'
                combined_expressions.append(f'col("{left}") {op} {right}')
            combined_expression = '|'.join(combined_expressions)
            combined_expression = f"df.filter({combined_expression})"
            df = eval(combined_expression)
        return df

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