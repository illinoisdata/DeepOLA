from operations import *
from query.query import Query

# SELECT
#     l_orderkey,
#     sum(l_extendedprice * (1 - l_discount)) as revenue,
#     sum(l_extendedprice) as revenue,
#     o_orderdate,
#     o_shippriority
# FROM
#     customer,
#     orders,
#     lineitem
# WHERE
#     c_mktsegment = 'BUILDING'
#     AND c_custkey = o_custkey
#     AND l_orderkey = o_orderkey
#     AND o_orderdate < date '1995-03-15'
#     AND l_shipdate > date '1995-03-15'
# GROUP BY
#     l_orderkey,
#     o_orderdate,
#     o_shippriority
# ORDER BY
#     revenue desc,
#     o_orderdate
# LIMIT 20;

q = Query()
q.add_operation(name='table_lineitem',operation=TABLE(args={'table': 'lineitem'}))
q.add_operation(name='table_customer',operation=TABLE(args={'table': 'customer'}))
q.add_operation(name='table_orders',operation=TABLE(args={'table': 'orders'}))
q.add_operation(name='where_c_mktsegment',operation=WHERE(args={'predicates': [{'left':'c_mktsegment','op':'=','right':'BUILDING'}]}))
q.add_operation(name='where_o_orderdate',operation=WHERE(args={'predicates': [{'left':'o_orderdate','op':'<','right':'1995-03-15'}]}))
q.add_operation(name='where_l_shipdate',operation=WHERE(args={'predicates': [{'left':'l_shipdate','op':'>','right':'1995-03-15'}]}))
q.add_operation(name='join_customer_order',operation=INNERJOIN(args={'left_on':['c_custkey'],'right_on':['o_custkey']}))
q.add_operation(name='join_lineitem_order',operation=INNERJOIN(args={'left_on':['l_orderkey'],'right_on':['o_orderkey']}))
q.add_operation(node_type="DM",name='groupby_operation',operation=GROUPBYAGG(args={'groupby_key':['l_orderkey','o_orderdate','o_shippriority'],'aggregates':[{'op':'sum','col':'l_extendedprice','alias':'revenue'}]}))
q.add_operation(node_type="DM",name='orderby_operation',operation=ORDERBY(args=[{'column':'revenue','order':'desc'},{'column':'o_orderdate'}]))
q.add_operation(node_type="DM",name='limit_operation',operation=LIMIT(args={'k':20}))
q.add_operation(output=True,name='select_operation',operation=SELECT(args={'columns':'*'}))

q.add_edge('table_lineitem','where_l_shipdate')
q.add_edge('table_customer','where_c_mktsegment')
q.add_edge('table_orders','where_o_orderdate')
q.add_edge('where_c_mktsegment','join_customer_order')
q.add_edge('where_o_orderdate','join_customer_order')
q.add_edge('join_customer_order','join_lineitem_order')
q.add_edge('where_l_shipdate','join_lineitem_order')
q.add_edge('join_lineitem_order','groupby_operation')
q.add_edge('groupby_operation','orderby_operation')
q.add_edge('orderby_operation','limit_operation')
q.add_edge('limit_operation','select_operation')
q.compile()

q.display('outputs/test-query.gv')