class BaseOperation:
    def __init__(self, args):
        self.args = args
        self.validate()

    @property
    def stateful_inputs(self):
        return False

    def validate(self):
        """
        Define validation functions on the operation arguments.
        """
        raise NotImplementedError

    def evaluate(self, inputs):
        """
        @param inputs: A dict of dataframes.
        Evaluates the operation on the given list of input dataframes. Returns an output dataframe.
        """
        raise NotImplementedError

    def merge(self, current_state, delta, return_delta = True):
        """
        @param current_state -> includes result and input (dict)
        @param delta
        This function merges the current_state with the result on input delta to give the result at this node with the merged data.
        """
        raise NotImplementedError
        