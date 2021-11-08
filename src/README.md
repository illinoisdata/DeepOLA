# Code Organization

## Classes
There are primarily three classes:
- Query: Represents an input query in the form of a DAG.
- Operations: Represents a basic unit of SQL task.
- QuerySession: Represents a session corresponding to a query evaluation.

#### Query
```python
class Query(Object):
    def __init__(self):
        self.nodes = []
        self.parent_nodes = {} ## Given a node, the parent nodes.
        self.dependencies = {} ## Given a node, the child nodes.

    def add_operation(self, name, operation):
        """
        @param name: The name of the input operation
        @param operation: The Operation class object.
        Adds the input operation as a node to the Query graph.
        """
        if name in self.nodes:
            raise Exception("Duplicate Operation")
        self.nodes[name] = operation

    def add_edge(self, source, destination):
        """
        @param source: An operation name
        @param destination: An operation name
        Adds an edge from the source operation to the destination operation.
        """
        if source not in self.nodes or destination not in self.nodes:
            raise Exception("Undefined Operation")
        if source not in self.parent_nodes:
            self.parent_nodes[source] = []
        if destination not in self.dependencies:
            self.dependencies[destination] = []
        self.parent_nodes[source].append(destination)
        self.dependencies[destination].append(source)

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
    
    def save(self, outfile):
        """
        @param outfile: The output file name.
        Saves the query object as a JSON to the output file name.
        To jsonify the query, it needs to jsonify each operation.
        """
    
    def load(self, infile):
        """
        @param infile: The input file name. 
        Loads the query object as a JSON from the input file name.
        Creates operation object for each of the query operation.
        """
```

#### Operation
```python
class BaseOperation(Object):
    def __init__(self, args):
        self.args = args
        self.validate()

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
    
    def incremental_evaluate(self, current_state, delta):
        """
        @param current_state: The current state of the node including the overall data evaluated and the result obtained.
        @param delta: A dict of input delta. A delta of None or missing key indicate there is no change to that table. The default keys are `input0`, `input1` etc. corresponding to input at index 0, 1, etc. for the query
        This function computes the operation result on the input delta and updates the state appropriately.
        """
        raise NotImplementedError
    
    def merge(self, current_state, delta):
        """
        @param current_state
        @param delta
        This function merges the current_state with the result on input delta to give the result at this node with the merged data.
        """
        
```

#### QuerySession
```python
class QuerySession(Object):
    def __init__(self, query):
        """
        @param query: The query we want to evaluate in the current session.
        """
        self.query = query
        self.task_queue = []
        self.current_state = {}
        for node in self.query.nodes:
            self.current_state[node] = {} ## Maintain the state of the input nodes.

    def run(self,  eval_node, input_nodes):
        """
        @param eval_node: A node that we want to evaluate.
        @param input_nodes: A dict containing the input nodes and the corresponding data.
        This function doesn't need to store the state of the operations. It directly evaluates all the operations on the current input data assuming this is only the input data that you want to evaluate your query on.
        """

    def run_incremental(self, eval_node, input_nodes):
        """
        @param eval_node: A node that we want to evaluate.
        @param input_nodes: A dict containing the input nodes and the corresponding delta dict. The delta dict contains the input index and change to that input.
        """
        if eval_node not in self.query.nodes:
            raise Exception("Invalid eval_node")
        output = self.current_state[eval_node]

        ## Create initial tasks
        for node in input_nodes:
            self.task_queue.append({'node': node, 'input_delta':input_nodes[node]})

        ## Process the queue of tasks
        while !self.task_queue.empty():
            task = self.task_queue.pop()

            node = task['node']
            task_type = task['type']
            input_data = task['input']

            current_state = self.current_state[node]
            parent_nodes = self.query.parent_nodes[node]
            if type == "evaluate" or self.query.node_type[node] == "DM" or node == eval_node:
              output,new_state = self.query.nodes[node].merge(current_state, input_data)
              self.current_state[node] = new_state
              for node in parent_nodes:
                  self.task_queue.append({'node':node, 'input': output, 'type': 'evaluate'})
            else:
              output,new_state = self.query.nodes[node].incremental_evaluate(current_state, input_data)
              self.current_state[node] = new_state
              for node in parent_nodes:
                  self.task_queue.append({'node':node, 'input': output, 'type': 'incremental_evaluate'})
        return output
```

## Example
```python
q = Query()
q.add_operation(name='table_lineitem',operation=operations.TABLE(args={'table': 'lineitem'}))
q.add_operation(name='where_operation',operation=operations.WHERE(args={'predicate': 'id > 5'}))
q.add_operation(name='select_operation',operation=operations.SELECT(args={'columns': '*'}))
q.add_edge('table_lineitem','where_operation')
q.add_edge('where_operation','select_operation')
q.compile() ## Represents the SQL query: SELECT * FROM lineitem WHERE id > 5;

partitioned_dfs = [df1,df2,df3,df4]
with QuerySession(q) as qs:
    for df in partitioned_dfs:
        qs.run_incremental('select_operation', {'table_lineitem': {'input0':df} })
```

## Questions
1. What exactly should "state" of a node contain?
