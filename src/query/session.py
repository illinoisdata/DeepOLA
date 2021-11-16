import logging
import polars as pl
import time

class QuerySession:
    def __init__(self, query):
        """
        @param query: The query we want to evaluate in the current session.
        """
        self.query = query
        self.task_queue = []
        self.current_state = {}
        for node in self.query.nodes:
            if self.query.need_state(node):
                num_inputs = len(self.query.nodes[node]['child'])
                self.current_state[node] = {'result': None, 'inputs':{} }
                for i in range(num_inputs):
                    self.current_state[node]['inputs'][f'input{i}'] = None
            else:
                self.current_state[node] = None

    def run(self,  eval_node, input_nodes):
        """
        @param eval_node: A node that we want to evaluate.
        @param input_nodes: A dict containing the input nodes and the corresponding data.
        This function doesn't need to store the state of the operations. It directly evaluates all the operations on the current input data assuming this is only the input data that you want to evaluate your query on.
        """
        raise NotImplementedError

    def _missing_data(self, state, delta):
        if state is None:
            if len(delta) > 0:
                return False
            return True
        else:
            if len(state['inputs']) > 1:
                for key in state['inputs']:
                    if state['inputs'][key] is None and (key not in delta or delta[key] is None):
                        return True
                return False
            else:
                return False

    def _build_input_data(self, state, node_index, delta):
        new_input = {}
        if state is not None:
            if 'inputs' in state and len(state['inputs']) > 1:
                for key in state['inputs']:
                    new_input[key] = state['inputs'][key]
        new_input[f'input{node_index}'] = delta
        return new_input

    def run_incremental(self, eval_node, input_nodes):
        """
        @param eval_node: A node that we want to evaluate.
        @param input_nodes: A dict containing the input nodes and the corresponding delta dict. The delta dict contains the input index and change to that input.
        """
        if eval_node not in self.query.output_nodes:
            raise Exception("Invalid eval_node")

        ## Create initial tasks - IE for each table.
        for node in input_nodes:
            assert(len(self.query.nodes[node]['child']) == 0) ## No child nodes for the input nodes.
            self.task_queue.append({'node': node, 'input':input_nodes[node], 'type':'incremental_evaluate'})

        ## Process the queue of tasks
        while len(self.task_queue) > 0:
            task = self.task_queue.pop(0) ## Pop the front of the queue.
            current_node = task['node']
            task_type = task['type']
            input_data = task['input']
            logging.debug(f"Evaluating task {task_type} on {current_node}")

            start_time = time.time()
            current_state = self.current_state[current_node]
            parent_nodes = self.query.nodes[current_node]['parent']
            missing_data = self._missing_data(current_state, input_data) ## Either current_state is None; else current_state should have inputs available

            if task_type == "incremental_evaluate":
                output = self.query.nodes[current_node]['operation'].evaluate(input_data)
                if not missing_data:
                    for node in parent_nodes:
                        node_index = self.query.nodes[node]['child'].index(current_node)
                        node_input = self._build_input_data(self.current_state[node], node_index, output)
                        if self.query.nodes[node]['type'] == "DM" or node == eval_node:
                            self.task_queue.append({'node':node, 'input': node_input, 'type': 'merge_result'})
                        elif self.query.nodes[node]['operation'].stateful_inputs:
                            self.task_queue.append({'node':node, 'input': node_input, 'type': 'merge_stateful'})
                        else:
                            self.task_queue.append({'node':node, 'input': node_input, 'type': 'incremental_evaluate'})
            elif task_type == "merge_stateful":
                ## Merge called because stateful inputs.
                self.current_state[current_node],output = self.query.nodes[current_node]['operation'].merge(current_state, input_data, return_delta = True)
                if not missing_data:
                    for node in parent_nodes:
                        node_index = self.query.nodes[node]['child'].index(current_node)
                        node_input = self._build_input_data(self.current_state[node], node_index, output)
                        if self.query.nodes[node]['type'] == "DM" or node == eval_node:
                            self.task_queue.append({'node':node, 'input': node_input, 'type': 'merge_result'})
                        elif self.query.nodes[node]['operation'].stateful_inputs:
                            self.task_queue.append({'node':node, 'input': node_input, 'type': 'merge_stateful'})
                        else:
                            self.task_queue.append({'node':node, 'input': node_input, 'type': 'incremental_evaluate'})
            elif task_type == "merge_result":
                ## Merge can be called because you found a DM or eval_node.
                self.current_state[current_node],output = self.query.nodes[current_node]['operation'].merge(current_state, input_data)
                if not missing_data:
                    for node in parent_nodes:
                        node_index = self.query.nodes[node]['child'].index(current_node)
                        node_input = self._build_input_data(self.current_state[node], node_index, output)
                        self.task_queue.append({'node':node, 'input': node_input, 'type': 'evaluate'})
            elif task_type == "evaluate":
                output = self.query.nodes[current_node]['operation'].evaluate(input_data)
                if not missing_data:
                    for node in parent_nodes:
                        node_index = self.query.nodes[node]['child'].index(current_node)
                        node_input = self._build_input_data(node_input, node_index, output)
                        self.task_queue.append({'node':node, 'input': node_input, 'type': 'evaluate'})

            end_time = time.time()
            logging.debug(f"Time taken: {end_time - start_time}")

        if output is None:
            return pl.DataFrame()
        return output