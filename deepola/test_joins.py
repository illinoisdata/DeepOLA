import operations
from query.query import Query
from query.session import QuerySession
from utils import load_table
import time

start_time = time.time()

## Ideally we want to be able to load the query from JSON
q = Query()
q.add_operation(name='table_customer_1',
                operation=operations.TABLE(args={'table': 'customer'}))
q.add_operation(name='table_customer_2',
                operation=operations.TABLE(args={'table': 'customer'}))
q.add_operation(name='join_operation', operation=operations.JOIN(
    args={'on':'c_name'}), output=True)

q.add_edge('table_customer_1', 'join_operation')
# q.add_edge('table_customer_2', 'join_operation')
q.compile()

session = QuerySession(q)
variation = 'run_incremental'
if variation == 'run_incremental':
    partitioned_dfs = []
    num_partitions = 3
    for partition in range(1, num_partitions+1):
        df_1 = load_table('customer', partition)
        df_2 = load_table('customer', partition+3)
        result = session.run_incremental(eval_node='join_operation', input_nodes={
                                         'table_customer_1': {'input0': df_1, 'input1':df_2}})
        print(result)
else:
    df_1 = load_table('customer', 1, 3)
    df_2 = load_table('customer', 4, 6)
    result = session.run_incremental(eval_node='join_operation', input_nodes={
                                     'table_customer_1': {'input0': df_1,
                                                        'input1': df_2}})
    print(result)

end_time = time.time()
print(f"Time taken for evaluation: {end_time-start_time} seconds")
