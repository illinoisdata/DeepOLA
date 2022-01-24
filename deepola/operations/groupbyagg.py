from .base import BaseOperation
import polars as pl
from polars import col

class GROUPBYAGG(BaseOperation):
    def __init__(self, args):
        """
        args : {
            'groupby_key': [list of columns],
            'aggregates': [
                {
                    'op': 'sum/count',
                    'col': 'col/expression',
                    'alias': 
                }
            ]
        }
        """
        super().__init__(args)

    def validate(self):
        assert('groupby_key' in self.args)
        assert('aggregates' in self.args)
        return True

    def evaluate(self, state, input):
        """
        Right now, we do not support complex expressions. Need to check how we can support those.
        """
        key = list(input.keys())[0]
        df = input[key]
        groupby_key = self.args['groupby_key']
        if len(groupby_key) == 0:
            df = df.with_column(pl.lit(1))
            groupby_df = df.groupby(["literal"])
        else:
            groupby_df = df.groupby(groupby_key)
        aggregates = self.args['aggregates']
        pl_aggregates = []
        for aggregate in aggregates:
            op = aggregate['op']
            column = aggregate['col']
            alias = aggregate['alias']
            if column == '*':
                column = f'pl.lit("*")'
            else:
                for column_name in df.columns:
                    if column_name in column:
                        column = column.replace(column_name, f'col("{column_name}")')
            if op == 'sum':
                pl_aggregates.append((eval(column)).sum().alias(alias))
            elif op == 'count':
                pl_aggregates.append((eval(column)).count().alias(alias))
            else:
                raise NotImplementedError
        return groupby_df.agg(pl_aggregates)

    def merge(self, current_state, delta, return_delta = False):
        output = self.evaluate(current_state, delta)
        if 'result' in current_state and current_state['result'] is not None:
            groupby_key = self.args['groupby_key']
            if len(groupby_key) == 0:
                groupby_key = ["literal"]
            agg_cols_alias = [x['alias'] for x in self.args['aggregates']]
            ## Rename columns to remove multiple _sum being added.
            agg_col_map = {f'{x}_sum': f'{x}' for x in agg_cols_alias}
            current_state['result'] = pl.concat([current_state['result'], output]).groupby(groupby_key).sum().rename(agg_col_map)
        else:
            current_state['result'] = output
        if return_delta:
            return output
        else:
            return current_state['result']