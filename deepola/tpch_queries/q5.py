from deepola.operations import *
from deepola.query.query import Query

sql_query = """
select
	nation.n_name,
	sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	customer.c_custkey = orders.o_custkey
	and lineitem.l_orderkey = orders.o_orderkey
	and lineitem.l_suppkey = supplier.s_suppkey
	and customer.c_nationkey = supplier.s_nationkey
	and supplier.s_nationkey = nation.n_nationkey
	and nation.n_regionkey = r_regionkey
	and region.r_name = 'ASIA'
	and orders.o_orderdate > date '1994-03-15'
	and orders.o_orderdate < date '1995-03-15'
group by
	nation.n_name
order by
	revenue desc;
"""

q = Query()
q.add_operation(name='table_customer',operation=TABLE(args={'table': 'customer'}))
q.add_operation(name='table_orders',operation=TABLE(args={'table': 'orders'}))
q.add_operation(name='table_lineitem',operation=TABLE(args={'table': 'lineitem'}))
q.add_operation(name='table_supplier',operation=TABLE(args={'table': 'supplier'}))
q.add_operation(name='table_nation',operation=TABLE(args={'table': 'nation'}))
q.add_operation(name='table_region',operation=TABLE(args={'table': 'region'}))

q.add_operation(name='where_o_orderdate_1',operation=WHERE(args={'form':'DNF','predicates': [[{'left':'o_orderdate','op':'>','right':'1994-03-15'}]]}))
q.add_operation(name='where_o_orderdate_2',operation=WHERE(args={'form':'DNF','predicates': [[{'left':'o_orderdate','op':'<','right':'1995-03-15'}]]}))
q.add_operation(name='where_r_name',operation=WHERE(args={'form':'DNF','predicates': [[{'left':'r_name','op':'==','right':'ASIA'}]]}))

q.add_operation(name='join_nation_region',operation=INNERJOIN(args={'left_on':['n_regionkey'],'right_on':['r_regionkey']}))
q.add_operation(name='join_supplier_nation',operation=INNERJOIN(args={'left_on':['s_nationkey'],'right_on':['n_nationkey']}))
q.add_operation(name='join_customer_supplier',operation=INNERJOIN(args={'left_on':['c_nationkey'],'right_on':['s_nationkey']}))
q.add_operation(name='join_customer_order',operation=INNERJOIN(args={'left_on':['c_custkey'],'right_on':['o_custkey']}))
q.add_operation(name='join_lineitem_order',operation=INNERJOIN(args={'left_on':['l_orderkey'],'right_on':['o_orderkey']}))
q.add_operation(name='join_lineitem_supplier',operation=INNERJOIN(args={'left_on':['l_suppkey'],'right_on':['s_suppkey']}))

q.add_operation(node_type="DM",name='groupby_operation',operation=GROUPBYAGG(args={'groupby_key':['n_name'],'aggregates':[{'op':'sum','col':'l_extendedprice*(1-l_discount)','alias':'revenue'}]}))
q.add_operation(node_type="DM",name='orderby_operation',operation=ORDERBY(args=[{'column':'revenue','order':'desc'}]))

q.add_operation(output=True,name='select_operation',operation=SELECT(args={'columns':['n_name','revenue']}))

q.add_edge('table_orders','where_o_orderdate_1')
q.add_edge('where_o_orderdate_1','where_o_orderdate_2')
q.add_edge('table_region','where_r_name')

q.add_edge('table_nation','join_nation_region')
q.add_edge('where_r_name','join_nation_region')

q.add_edge('table_supplier','join_supplier_nation')
q.add_edge('join_nation_region','join_supplier_nation')

q.add_edge('table_customer','join_customer_supplier')
q.add_edge('join_supplier_nation','join_customer_supplier')

q.add_edge('join_customer_supplier','join_customer_order')
q.add_edge('where_o_orderdate_2','join_customer_order')

q.add_edge('table_lineitem','join_lineitem_order')
q.add_edge('join_customer_order','join_lineitem_order')

q.add_edge('join_lineitem_order','join_lineitem_supplier')
q.add_edge('join_supplier_nation','join_lineitem_supplier')

q.add_edge('join_lineitem_supplier','groupby_operation')
q.add_edge('groupby_operation','orderby_operation')
q.add_edge('orderby_operation','select_operation')
q.compile()
# q.display()