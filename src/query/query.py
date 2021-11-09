class Query:
    def __init__(self):
        self.nodes = {} ## Node name mapped to a dict of node type and operation
        self.output_nodes = [] ## List of nodes which can be an output node and hence need to store the current_state.

    def add_operation(self, name, operation, node_type = "DA", output = False):
        """
        @param name: The name of the input operation
        @param operation: The Operation class object.
        Adds the input operation as a node to the Query graph.
        """
        if name in self.nodes:
            raise Exception("Duplicate Operation")
        self.nodes[name] = {
            'type': node_type,
            'operation': operation,
            'parent': [],
            'child': []
        }
        if output:
            self.output_nodes.append(name)

    def add_edge(self, source, destination):
        """
        @param source: An operation name
        @param destination: An operation name
        Adds an edge from the source operation to the destination operation.
        """
        if source not in self.nodes or destination not in self.nodes:
            raise Exception("Undefined Operation")
        self.nodes[source]['parent'].append(destination)
        self.nodes[destination]['child'].append(source)
    
    def need_state(self,node_name):
        if node_name not in self.nodes:
            raise Exception("Undefined Operation")
        if node_name in self.output_nodes: ## If its an output node.
            return True
        node = self.nodes[node_name]
        if node['type'] == "DM": ## If its a DM type of node
            return True
        else:
            for operation in node['parent']: ## If any of its parent require stateful inputs.
                if self.nodes[operation]['operation'].stateful_inputs:
                    return True
            return False

    def compile(self):
        """
        Compiles the query and makes sure the edges do not have any cyclic dependencies.
        It also processes the DAG to identify the nodes beyond which we need to merge the query result.
        This also compresses multiple operation together into a Chained Operation to reduce separate operation computations.
        The edges cannot be added once the query is compiled.
        """
        pass
        
    def display(self):
        """
        Generates a graph plot for the input query.
        """
        pass

    def save(self, outfile):
        """
        @param outfile: The output file name.
        Saves the query object as a JSON to the output file name.
        To jsonify the query, it needs to jsonify each operation.
        """
        pass
    
    def load(self, infile):
        """
        @param infile: The input file name. 
        Loads the query object as a JSON from the input file name.
        Creates operation object for each of the query operation.
        """
        pass