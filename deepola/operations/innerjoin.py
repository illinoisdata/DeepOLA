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

    def evaluate(self, input):
        logger.debug('func:start:INNERJOIN_evaluate')
        assert(len(input.keys()) == 2)
        keys = list(input.keys())
        left_df = input[keys[0]]
        right_df = input[keys[1]]
        if left_df is None or right_df is None:
            df = None
        else:
            logger.debug('func:start:INNERJOIN_evaluate_join')
            df = left_df.join(right_df, how = "inner", left_on = self.args['left_on'], right_on = self.args['right_on'])
            logger.debug('func:end:INNERJOIN_evaluate_join')
        logger.debug('func:end:INNERJOIN_evaluate')
        return df

    def merge(self, current_state, delta, return_delta = False):
        logger.debug('func:start:INNERJOIN_merge')
        output = self.evaluate(delta)
        logger.debug('func:start:INNERJOIN_input_merge')
        if delta['input0'] is not None:
            if current_state['inputs']['input0'] is None:
                current_state['inputs']['input0'] = delta['input0']
            else:
                current_state['inputs']['input0'] = current_state['inputs']['input0'].vstack(delta['input0']) #, in_place=True) # = pl.concat([current_state['inputs']['input0'], delta['input0']])
        if delta['input1'] is not None:
            if current_state['inputs']['input1'] is None:
                current_state['inputs']['input1'] = delta['input1']
            else:
                current_state['inputs']['input1'] = current_state['inputs']['input0'].vstack(delta['input1']) #.vstack(delta['input1'], in_place=True) # = pl.concat([current_state['inputs']['input1'], delta['input1']])
        logger.debug('func:end:INNERJOIN_input_merge')
        logger.debug('func:start:INNERJOIN_result_merge')
        if current_state['result'] is not None:
            current_state['result'] = current_state['result'].vstack(output) #, in_place=True) # = pl.concat([current_state['result'],output])
        else:
            current_state['result'] = output
        logger.debug('func:end:INNERJOIN_result_merge')
        logger.debug('func:end:INNERJOIN_merge')
        if return_delta:
            return current_state, output
        else:
            return current_state, current_state['result']
            logger.debug('func:start:INNERJOIN_evaluate_join')
