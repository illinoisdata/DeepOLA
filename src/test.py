import operations
from query.query import Query
from query.session import QuerySession
from utils import load_table

q = Query()
q.add_operation(name='table_lineitem',operation=operations.TABLE(args={'table': 'lineitem'}))
q.add_operation(name='where_operation',operation=operations.WHERE(args={'predicate': 'l_discount >= 0.10'}))
q.add_operation(name='select_operation',operation=operations.SELECT(args={'columns': '*'}),output=True)
q.add_edge('table_lineitem','where_operation')
q.add_edge('where_operation','select_operation')
q.compile()

session = QuerySession(q)

partitioned_dfs = []
num_partitions = 5
for partition in range(1,num_partitions+1):
    df = load_table('lineitem',partition)
    result = session.run_incremental(eval_node='select_operation',input_nodes={'table_lineitem':{'input0':df}})
    print(len(result))