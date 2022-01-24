from deepola.operations import *
from deepola.query.query import Query

# select
# 	customer.c_custkey,
# 	customer.c_name,
# 	sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) as revenue,
# 	customer.c_acctbal,
# 	nation.n_name,
# 	customer.c_address,
# 	customer.c_phone,
# 	customer.c_comment
# from
# 	customer,
# 	orders,
# 	lineitem,
# 	nation
# where
# 	customer.c_custkey = orders.o_custkey
# 	and lineitem.l_orderkey = orders.o_orderkey
# 	and orders.o_orderdate > date '1994-03-15'
# 	and orders.o_orderdate < date '1994-06-15'
# 	and lineitem.l_returnflag = 'R'
# 	and customer.c_nationkey = nation.n_nationkey
# group by
# 	customer.c_custkey,
# 	customer.c_name,
# 	customer.c_acctbal,
# 	customer.c_phone,
# 	nation.n_name,
# 	customer.c_address,
# 	customer.c_comment
# order by
# 	revenue desc;

q = Query()
q.add_operation(name='table_customer',operation=TABLE(args={'table': 'customer'}))
q.add_operation(name='table_orders',operation=TABLE(args={'table': 'orders'}))
q.add_operation(name='table_lineitem',operation=TABLE(args={'table': 'lineitem'}))
q.add_operation(name='table_nation',operation=TABLE(args={'table': 'nation'}))

q.add_operation(name='where_o_orderdate_1',operation=WHERE(args={'form':'DNF','predicates': [[{'left':'o_orderdate','op':'>','right':'1994-03-15'}]]}))
q.add_operation(name='where_o_orderdate_2',operation=WHERE(args={'form':'DNF','predicates': [[{'left':'o_orderdate','op':'<','right':'1994-06-15'}]]}))
q.add_operation(name='where_l_returnflag',operation=WHERE(args={'form':'DNF','predicates': [[{'left':'l_returnflag','op':'==','right':'R'}]]}))

q.add_operation(name='join_customer_nation',operation=INNERJOIN(args={'left_on':['c_nationkey'],'right_on':['n_nationkey']}))
q.add_operation(name='join_customer_order',operation=INNERJOIN(args={'left_on':['c_custkey'],'right_on':['o_custkey']}))
q.add_operation(name='join_lineitem_order',operation=INNERJOIN(args={'left_on':['l_orderkey'],'right_on':['o_orderkey']}))

q.add_operation(node_type="DM",name='groupby_operation',operation=GROUPBYAGG(args={'groupby_key':['c_custkey','c_name','c_acctbal','c_phone','n_name','c_address','c_comment'],'aggregates':[{'op':'sum','col':'l_extendedprice*(1-l_discount)','alias':'revenue'}]}))
q.add_operation(node_type="DM",name='orderby_operation',operation=ORDERBY(args=[{'column':'revenue','order':'desc'}]))

q.add_operation(output=True,name='select_operation',operation=SELECT(args={'columns':['*']}))

q.add_edge('table_orders','where_o_orderdate_1')
q.add_edge('where_o_orderdate_1','where_o_orderdate_2')
q.add_edge('table_lineitem','where_l_returnflag')

q.add_edge('table_customer','join_customer_nation')
q.add_edge('table_nation','join_customer_nation')

q.add_edge('join_customer_nation','join_customer_order')
q.add_edge('where_o_orderdate_2','join_customer_order')

q.add_edge('where_l_returnflag','join_lineitem_order')
q.add_edge('join_customer_order','join_lineitem_order')

q.add_edge('join_lineitem_order','groupby_operation')
q.add_edge('groupby_operation','orderby_operation')
q.add_edge('orderby_operation','select_operation')

q.compile()
# q.display()