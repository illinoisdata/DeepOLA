from deepola.operations import *
from deepola.query.query import Query

sql_query = """
select
	lineitem.l_orderkey,
	sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	customer.c_mktsegment = 'BUILDING'
	and customer.c_custkey = orders.o_custkey
	and lineitem.l_orderkey = orders.o_orderkey
	and orders.o_orderdate < date '1995-03-15'
	and lineitem.l_shipdate > date '1995-03-15'
group by
	lineitem.l_orderkey,
	orders.o_orderdate,
	orders.o_shippriority
order by
	revenue desc,
	orders.o_orderdate;
"""

q = Query()
q.add_operation(name='table_lineitem',operation=TABLE(args={'table': 'lineitem'}))
q.add_operation(name='table_customer',operation=TABLE(args={'table': 'customer'}))
q.add_operation(name='table_orders',operation=TABLE(args={'table': 'orders'}))
q.add_operation(name='where_c_mktsegment',operation=WHERE(args={'form':'DNF','predicates': [[{'left':'c_mktsegment','op':'==','right':'BUILDING'}]]}))
q.add_operation(name='where_o_orderdate',operation=WHERE(args={'form':'DNF','predicates': [[{'left':'o_orderdate','op':'<','right':'1995-03-15'}]]}))
q.add_operation(name='where_l_shipdate',operation=WHERE(args={'form':'DNF','predicates': [[{'left':'l_shipdate','op':'>','right':'1995-03-15'}]]}))
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