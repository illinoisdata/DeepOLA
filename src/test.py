from numpy import VisibleDeprecationWarning
from operations import *
from query.query import Query
from query.session import QuerySession
from utils import load_table
import time

import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')

start_time = time.time()

## Ideally we want to be able to load the query from JSON
# q = Query()
# q.add_operation(name='table_lineitem',operation=operations.TABLE(args={'table': 'lineitem'}))
# q.add_operation(name='where_operation',operation=operations.WHERE(args={'predicates': [ [{'left':'l_discount','op':'>=','right': '0.10'}] ]}))
# q.add_operation(node_type="DM",name='groupbyagg_operation',operation=operations.GROUPBYAGG(args={'groupby_key':['l_shipmode'],'aggregates':[{'op':'count','col':'l_quantity','alias':'ct_qty'},{'op':'sum','col':'l_quantity','alias':'sum_qty'}]}))
# q.add_operation(node_type="DM",name='orderby_operation',operation=operations.ORDERBY(args=[{'column':'ct_qty'}]))
# q.add_operation(node_type="DM",name='limit_operation',operation=operations.LIMIT(args={'k':3}))
# q.add_operation(name='select_operation',operation=operations.SELECT(args={'columns': '*'}),output=True)
# q.add_edge('table_lineitem','where_operation')
# q.add_edge('where_operation','groupbyagg_operation')
# q.add_edge('groupbyagg_operation','orderby_operation')
# q.add_edge('orderby_operation','limit_operation')
# q.add_edge('limit_operation','select_operation')
# q.compile()

q = Query()
q.add_operation(name='table_lineitem',operation=TABLE(args={'table': 'lineitem'}))
q.add_operation(name='table_customer',operation=TABLE(args={'table': 'customer'}))
q.add_operation(name='table_orders',operation=TABLE(args={'table': 'orders'}))
q.add_operation(name='where_c_mktsegment',operation=WHERE(args={'form':'DNF','predicates': [[{'left':'c_mktsegment','op':'==','right':'BUILDING'}]]}))
# q.add_operation(name='where_o_orderdate',operation=WHERE(args={'form':'DNF','predicates': [[{'left':'o_orderdate','op':'<','right':'1995-03-15'}]]}))
# q.add_operation(name='where_l_shipdate',operation=WHERE(args={'form':'DNF','predicates': [[{'left':'l_shipdate','op':'>','right':'1995-03-15'}]]}))
q.add_operation(name='join_customer_order',operation=INNERJOIN(args={'left_on':['c_custkey'],'right_on':['o_custkey']}))
q.add_operation(name='join_lineitem_order',operation=INNERJOIN(args={'left_on':['l_orderkey'],'right_on':['o_orderkey']}))
q.add_operation(node_type="DM",name='groupby_operation',operation=GROUPBYAGG(args={'groupby_key':['l_orderkey','o_orderdate','o_shippriority'],'aggregates':[{'op':'sum','col':'l_extendedprice*(1-l_discount)','alias':'revenue'}]}))
q.add_operation(node_type="DM",name='orderby_operation',operation=ORDERBY(args=[{'column':'revenue','order':'desc'},{'column':'o_orderdate'}]))
q.add_operation(node_type="DM",name='limit_operation',operation=LIMIT(args={'k':20}))
q.add_operation(output=True,name='select_operation',operation=SELECT(args={'columns':'*'}))

q.add_edge('table_customer','where_c_mktsegment')
q.add_edge('where_c_mktsegment','join_customer_order')
q.add_edge('table_orders','join_customer_order')
q.add_edge('table_lineitem','join_lineitem_order')
q.add_edge('join_customer_order','join_lineitem_order')
q.add_edge('join_lineitem_order','groupby_operation')
q.add_edge('groupby_operation','orderby_operation')
q.add_edge('orderby_operation','limit_operation')
q.add_edge('limit_operation','select_operation')
q.compile()
# q.display()

session = QuerySession(q)
variation = 'run_incremental'
variation = 'run_total'
tables = ['customer','orders','lineitem']
data_dir = '../data'
if variation == 'run_incremental':
    partitioned_dfs = []
    num_partitions = 5
    for partition in range(1,num_partitions+1):
        input_nodes = {}
        for table in tables:
            df = load_table(table,partition,directory=data_dir)
            input_nodes[f'table_{table}'] = {'input0':df}
        result = session.run_incremental(eval_node='select_operation',input_nodes=input_nodes)
        print(result)
else:
    for table in tables:
        df = load_table(table,1,5,directory=data_dir)
        result = session.run_incremental(eval_node='select_operation',input_nodes={f'table_{table}':{'input0':df}})
        print(result)
end_time = time.time()
print(f"Time taken for evaluation: {end_time-start_time} seconds")
