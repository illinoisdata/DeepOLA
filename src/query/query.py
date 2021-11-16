import graphviz
import json
from operations import *

class Query:
    def __init__(self):
        self.nodes = {} ## Node name mapped to a dict of node type and operation
        self.output_nodes = [] ## List of nodes which can be an output node and hence need to store the current_state.
        self.edges = []

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
            'child': [],
            'relation': type(operation).__name__,
            'output': output
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
        self.edges.append((source,destination))
    
    def need_state(self,node_name):
        if node_name not in self.nodes:
            raise Exception("Undefined Operation")
        if node_name in self.output_nodes: ## If its an output node.
            return True
        node = self.nodes[node_name]
        if node['type'] == "DM": ## If its a DM type of node
            return True
        elif self.nodes[node_name]['operation'].stateful_inputs:
            return True
        else:
            return False

    def compile(self):
        """
        Compiles the query and makes sure the edges do not have any cyclic dependencies.
        It also processes the DAG to identify the nodes beyond which we need to merge the query result.
        This also compresses multiple operation together into a Chained Operation to reduce separate operation computations.
        The edges cannot be added once the query is compiled.
        """
        pass
        
    def display(self, outfile = None):
        """
        Generates a graph plot for the input query.
        """
        dot = graphviz.Digraph()
        for node in self.nodes:
            dot.node(node)
        dot.edges(self.edges)
        dot.render(outfile, view=True)

    def save(self, path):
        """
        @param path: The output file name.
        Saves the query object as a JSON to the output file name.
        To jsonify the query, it needs to jsonify each operation.
        """
        with open(path, 'w') as outfile:
            json.dump(self, outfile, default=lambda o: o.__dict__,
                       sort_keys=True, indent=4)
    
    def load(self, path):
        """
        @param path: The input file name. 
        Loads the query object as a JSON from the input file name.
        Creates operation object for each of the query operation.
        """
        with open(path, 'r') as infile:
            data = json.load(infile)
            for name, node_data in data['nodes'].items():
                node_type = node_data['type']
                relation = node_data['relation']
                output = node_data['output']
                operation = globals()[relation](node_data['operation']['args'])

                self.add_operation(
                    name=name,
                    operation=operation,
                    node_type=node_type,
                    output=output
                )
            
            for edge1, edge2 in data['edges']:
                self.add_edge(edge1, edge2)
