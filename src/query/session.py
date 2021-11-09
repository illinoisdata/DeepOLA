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
                self.current_state[node] = {'result': None, 'metadata': {}}

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

            node = task['node']
            task_type = task['type']
            input_data = task['input']

            parent_nodes = self.query.nodes[node]['parent']
            if task_type == "merge":
                current_state = self.current_state.get(node,None)
                if self.query.nodes[node]['type'] == "DM" or node == eval_node:
                    new_state,output = self.query.nodes[node]['operation'].merge(current_state, input_data)
                    self.current_state[node] = new_state
                    for node in parent_nodes:
                        self.task_queue.append({'node':node, 'input': {'input0':output}, 'type': 'evaluate'})
                else:
                    ## If Parent has stateful_inputs True
                    new_state,output = self.query.nodes[node]['operation'].merge(current_state, input_data, return_delta = True)
                    self.current_state[node] = new_state
                    for node in parent_nodes:
                        self.task_queue.append({'node':node, 'input': {'input0':output}, 'type': 'incremental_evaluate'})
            elif task_type == "evaluate":
                output = self.query.nodes[node]['operation'].evaluate(input_data)
                for node in parent_nodes:
                    self.task_queue.append({'node':node, 'input': {'input0':output}, 'type': 'evaluate'})
            elif task_type == "incremental_evaluate":
                output = self.query.nodes[node]['operation'].evaluate(input_data)
                for node in parent_nodes:
                    if self.query.nodes[node]['type'] == "DM" or node == eval_node:
                        self.task_queue.append({'node':node, 'input': {'input0':output}, 'type': 'merge'})
                    else:
                        self.task_queue.append({'node':node, 'input': {'input0':output}, 'type': 'incremental_evaluate'})
        return output