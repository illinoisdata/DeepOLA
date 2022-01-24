from .base import BaseOperation
import polars as pl
import logging
logger = logging.getLogger()

class INNERJOIN(BaseOperation):
    def __init__(self, args):
        super().__init__(args)

    def validate(self):
        assert('left_on' in self.args)
        assert('right_on' in self.args)
        return True

    @property
    def stateful_inputs(self):
        """
        Need to override this property since JOIN has to store input's states.
        """
        return True

    def evaluate(self, state, input):
        logger.debug('func:start:INNERJOIN_evaluate')
        if 'input' not in state:
            state['input'] = {'input0': [], 'input1':[]}

        if 'input0' in input:
            left_df = [input['input0']]
            right_df = state['input']['input1']
            state['input']['input0'].append(input['input0'])
        else:
            left_df = state['input']['input0']
            right_df = [input['input1']]
            state['input']['input1'].append(input['input1'])

        if len(right_df) == 0 or len(left_df) == 0:
            return None

        join_dfs = []
        for left in left_df:
            for right in right_df:
                logger.debug('func:start:INNERJOIN_evaluate_join')
                df = left.join(right, how = "inner", left_on = self.args['left_on'], right_on = self.args['right_on'])
                logger.debug('func:end:INNERJOIN_evaluate_join')
                join_dfs.append(df)
        result_df = pl.concat(join_dfs)
        if 'result' not in state:
            state['result'] = []
        state['result'].append(result_df)
        logger.debug('func:end:INNERJOIN_evaluate')
        return result_df

    def merge(self, current_state, delta, return_delta = False):
        logger.debug('func:start:INNERJOIN_merge')
        output = self.evaluate(current_state,delta)
        logger.debug('func:start:INNERJOIN_input_merge')
        if return_delta:
            return output
        else:
            return pl.concat(current_state['result'])