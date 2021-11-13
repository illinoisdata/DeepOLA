import logging

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
                self.current_state[node] = {'result': None}

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
        if eval_node not in self.query.output_nodes:
            raise Exception("Invalid eval_node")

        ## Create initial tasks
        for node in input_nodes:
            if self.query.nodes[node]['type'] == "DM" or node == eval_node:
                self.task_queue.append({'node': node, 'input':input_nodes[node], 'type':'merge'})
            else:
                self.task_queue.append({'node': node, 'input':input_nodes[node], 'type':'incremental_evaluate'})

        ## Process the queue of tasks
        while len(self.task_queue) > 0:
            task = self.task_queue.pop(0) ## Pop the front of the queue.
            current_node = task['node']
            task_type = task['type']
            input_data = task['input']
            logging.debug(f"Evaluating task {task_type} on {current_node}")

            ## Checking if missing data.
            current_state = self.current_state.get(current_node,None)
            parent_nodes = self.query.nodes[current_node]['parent']
            child_nodes = self.query.nodes[current_node]['child']
            missing_data = False
            if len(child_nodes) > 1:
                keys = set()
                for key in current_state:
                    if key.startswith('input'):
                        keys.insert(key)
                for key in input_data:
                    keys.insert(key)
                if(keys.size() != len(child_nodes)):
                    missing_data = True
            else:
                missing_data = False
            if task_type == "merge":
                if self.query.nodes[current_node]['type'] == "DM" or current_node == eval_node:
                    ## Update the current state.
                    new_state,output = self.query.nodes[current_node]['operation'].merge(current_state, input_data)
                    self.current_state[current_node] = new_state
                    if not missing_data:
                        for node in parent_nodes:
                            node_index = self.query.nodes[node]['child'].index(current_node)
                            self.task_queue.append({'node':node, 'input': {f'input{node_index}':output}, 'type': 'evaluate'})
                else:
                    ## Merged because stateful_inputs True
                    new_state,output = self.query.nodes[current_node]['operation'].merge(current_state, input_data, return_delta = True)
                    if not missing_data:
                        for node in parent_nodes:
                            node_index = self.query.nodes[node]['child'].index(current_node)
                            self.task_queue.append({'node':node, 'input': {f'input{node_index}':output}, 'type': 'incremental_evaluate'})
            elif task_type == "evaluate":
                ## Since already merged, need to just evaluate the remaining operations
                output = self.query.nodes[current_node]['operation'].evaluate(input_data)
                if not missing_data:
                    for node in parent_nodes:
                        node_index = self.query.nodes[node]['child'].index(current_node)
                        self.task_queue.append({'node':node, 'input': {f'input{node_index}':output}, 'type': 'evaluate'})
            elif task_type == "incremental_evaluate":
                ## Incrementally evaluate the operation
                output = self.query.nodes[current_node]['operation'].evaluate(input_data)
                if not missing_data:
                    for node in parent_nodes:
                        node_index = self.query.nodes[node]['child'].index(current_node)
                        if self.query.nodes[node]['type'] == "DM" or node == eval_node:
                            self.task_queue.append({'node':node, 'input': {f'input{node_index}':output}, 'type': 'merge'})
                        else:
                            self.task_queue.append({'node':node, 'input': {f'input{node_index}':output}, 'type': 'incremental_evaluate'})
        return output
