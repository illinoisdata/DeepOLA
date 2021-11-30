from .base import BaseOperation
import polars as pl


class JOIN(BaseOperation):

    def __init__(self, args):
        # https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.DataFrame.join.html#polars.DataFrame.join
        """
        args = {
            'on': Name(s) of the join columns in both DataFrames.
            'how': Join strategy. Inner by default.
        }
        """
        super().__init__(args)

    def validate(self):
        assert('on' in self.args)
        return True

    def evaluate(self, inputs):
        print(inputs.keys())
        # if there is only 1 input, return an empty dataframe
        if len(inputs) < 2:
            return pl.DataFrame()

        how = self.args['how'] if 'how' in self.args else 'inner'

        # joining first two inputs for now
        key_1, key_2 = list(inputs.keys())[:2]
        return inputs[key_1].join(inputs[key_2], on=self.args['on'], how=how)

    def merge(self, current_state, delta, return_delta=False):
        """
        Args:
            current_state (dict): {'result': df, 'metadata': {inputs}}
            delta (dict): {input0: df}
        """
        delta_key = list(delta.keys())[0]
        poss_keys = [key for key in current_state['metadata'].keys()
                     if key != delta_key]

        if current_state['result'] is not None and len(poss_keys) > 0:
            # join delta with the other table stored in current state's metadata
            other_key = poss_keys[0]
            joined_delta = self.evaluate(
                current_state['metadata'][other_key], delta[delta_key])

            if current_state['result'].is_empty():
                output = joined_delta
            else:
                output = pl.concat([current_state['result'], joined_delta])
            current_state['result'] = output

        else:
            current_state['result'] = self.evaluate(delta)

        # add delta to same table in current state's metadata
        if delta_key in current_state['metadata']: 
            added_df = pl.concat(
                [current_state['metadata'][delta_key], delta[delta_key]])
            current_state['metadata'][delta_key] = added_df
        else:
            current_state['metadata'] = delta

        return current_state, current_state['result']
