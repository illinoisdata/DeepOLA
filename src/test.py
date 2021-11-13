import operations
from query.query import Query
from query.session import QuerySession
from utils import load_table
import time

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

start_time = time.time()

## Ideally we want to be able to load the query from JSON
q = Query()
q.add_operation(name='table_lineitem',operation=operations.TABLE(args={'table': 'lineitem'}))
q.add_operation(name='where_operation',operation=operations.WHERE(args={'predicates': [ [{'left':'l_discount','op':'>=','right': '0.10'}] ]}))
q.add_operation(node_type="DM",name='groupbyagg_operation',operation=operations.GROUPBYAGG(args={'groupby_key':['l_shipmode'],'aggregates':[{'op':'count','col':'l_quantity','alias':'ct_qty'},{'op':'sum','col':'l_quantity','alias':'sum_qty'}]}))
q.add_operation(node_type="DM",name='orderby_operation',operation=operations.ORDERBY(args=[{'column':'ct_qty'}]))
q.add_operation(node_type="DM",name='limit_operation',operation=operations.LIMIT(args={'k':3}))
q.add_operation(name='select_operation',operation=operations.SELECT(args={'columns': '*'}),output=True)

q.add_edge('table_lineitem','where_operation')
q.add_edge('where_operation','groupbyagg_operation')
q.add_edge('groupbyagg_operation','orderby_operation')
q.add_edge('orderby_operation','limit_operation')
q.add_edge('limit_operation','select_operation')
q.compile()

session = QuerySession(q)
variation = 'run_incremental'
if variation == 'run_incremental':
    partitioned_dfs = []
    num_partitions = 5
    for partition in range(1,num_partitions+1):
        df = load_table('lineitem',partition)
        result = session.run_incremental(eval_node='select_operation',input_nodes={'table_lineitem':{'input0':df}})
        print(result)
else:
    df = load_table('lineitem',1,5)
    result = session.run_incremental(eval_node='select_operation',input_nodes={'table_lineitem':{'input0':df}})
    print(result)    

end_time = time.time()
print(f"Time taken for evaluation: {end_time-start_time} seconds")
