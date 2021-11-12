from .base import BaseOperation
import polars as pl

class GROUPBYAGG(BaseOperation):
    def __init__(self, args):
        super().__init__(args)

    def validate(self):
        assert('groupby_key' in self.args)
        assert('aggregates' in self.args)
        return True

    def evaluate(self, input):
        """
        Right now, we do not support complex expressions. Need to check how we can support those.
        """
        key = list(input.keys())[0]
        df = input[key]
        groupby_key = self.args['groupby_key']
        groupby_df = df.groupby(groupby_key)
        aggregates = self.args['aggregates']
        pl_aggregates = []
        for aggregate in aggregates:
            op = aggregate['op']
            col = aggregate['col']
            alias = aggregate['alias']
            if op == 'sum':
                pl_aggregates.append(pl.sum(col).alias(alias))
            elif op == 'count':
                pl_aggregates.append(pl.count(col).alias(alias))
            else:
                raise NotImplementedError
        return groupby_df.agg(pl_aggregates)

    def merge(self, current_state, delta, return_delta = False):
        output = self.evaluate(delta)
        if current_state['result'] is not None:
            groupby_cols = self.args['groupby_key']
            agg_cols_alias = [x['alias'] for x in self.args['aggregates']]
            ## Rename columns to remove multiple _sum being added.
            agg_col_map = {f'{x}_sum': f'{x}' for x in agg_cols_alias}
            current_state['result'] = pl.concat([current_state['result'], output]).groupby(groupby_cols).sum().rename(agg_col_map)
        else:
            current_state['result'] = output
        if return_delta:
            return current_state, output
        else:
            return current_state, current_state['result']