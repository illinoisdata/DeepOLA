import logging
import polars as pl
import time
import sys
logger = logging.getLogger()

class QuerySession:
    def __init__(self, query):
        """
        @param query: The query we want to evaluate in the current session.
        """
        logger.debug(f"func:start:QuerySession__init__")
        self.query = query
        self.task_queue = []
        self.current_state = {}
        for node in self.query.nodes:
            self.current_state[node] = {} ## State is a node-level session-level key-value store
        logger.debug(f"func:end:QuerySession__init__")

    def run(self,  eval_node, input_nodes):
        """
        @param eval_node: A node that we want to evaluate.
        @param input_nodes: A dict containing the input nodes and the corresponding data.
        This function doesn't need to store the state of the operations. It directly evaluates all the operations on the current input data assuming this is only the input data that you want to evaluate your query on.
        """
        raise NotImplementedError

    def run_incremental(self, eval_node, input_nodes):
        """
        @param eval_node: A node that we want to evaluate.
        @param input_nodes: A dict containing the input nodes and the corresponding delta dict. The delta dict contains the input index and change to that input.
        """
        logger.debug(f"func:start:run_incremental args:{eval_node}")
        if eval_node not in self.query.output_nodes:
            raise Exception("Invalid eval_node")

        ## Create initial tasks - IE for each table.
        for node in input_nodes:
            assert(len(self.query.nodes[node]['child']) == 0) ## No child nodes for the input nodes.
            self.task_queue.append({'node': node, 'input': input_nodes[node], 'type':'incremental_evaluate'})

        ## Process the queue of tasks
        while len(self.task_queue) > 0:
            task = self.task_queue.pop(0) ## Pop the front of the queue.
            current_node = task['node']
            task_type = task['type']
            input_data = task['input']

            start_time = time.time()
            logger.debug(f"func:start:TaskEvaluate args:{task_type},{current_node},{self.query.nodes[current_node]['operation']}")
            # current_state = self.current_state[current_node]
            parent_nodes = self.query.nodes[current_node]['parent']
            # missing_data = self._missing_data(current_state, input_data) ## Either current_state is None; else current_state should have inputs available

            if task_type == "incremental_evaluate":
                output = self.query.nodes[current_node]['operation'].evaluate(self.current_state[current_node],input_data)
                if output is not None:
                    for node in parent_nodes:
                        node_index = self.query.nodes[node]['child'].index(current_node)
                        if self.query.nodes[node]['type'] == "DM" or node == eval_node:
                            self.task_queue.append({'node':node, 'input': {f'input{node_index}':output}, 'type': 'merge_result'})
                        else:
                            self.task_queue.append({'node':node, 'input': {f'input{node_index}':output}, 'type': 'incremental_evaluate'})
            elif task_type == "merge_result":
                output = self.query.nodes[current_node]['operation'].merge(self.current_state[current_node],input_data)
                if output is not None:
                    for node in parent_nodes:
                        node_index = self.query.nodes[node]['child'].index(current_node)
                        self.task_queue.append({'node':node, 'input': {f'input{node_index}':output}, 'type': 'evaluate'})
            elif task_type == "evaluate":
                output = self.query.nodes[current_node]['operation'].evaluate(self.current_state[current_node],input_data)
                if output is not None:
                    for node in parent_nodes:
                        node_index = self.query.nodes[node]['child'].index(current_node)
                        self.task_queue.append({'node':node, 'input': {f'input{node_index}':output}, 'type': 'evaluate'})
            else:
                raise NotImplementedError
            end_time = time.time()
            logger.debug(f"func:end:TaskEvaluate Time taken:{end_time-start_time}")
        logger.debug(f"func:end:run_incremental")
        if output is None:
            return pl.DataFrame()
        return output
