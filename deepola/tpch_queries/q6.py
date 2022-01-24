from deepola.operations import *
from deepola.query.query import Query

sql_query = """
select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date '1994-03-15'
	and l_shipdate < date '1995-03-15'
	and l_discount between 0.04 and 0.06
	and l_quantity < 20;
"""

q = Query()
q.add_operation(name='table_lineitem',operation=TABLE(args={'table': 'lineitem'}))

q.add_operation(name='where_l_shipdate_1',operation=WHERE(args={'form':'DNF','predicates': [[{'left':'l_shipdate','op':'>','right':'1994-03-15'}]]}))
q.add_operation(name='where_l_shipdate_2',operation=WHERE(args={'form':'DNF','predicates': [[{'left':'l_shipdate','op':'<','right':'1995-03-15'}]]}))
q.add_operation(name='where_l_discount_1',operation=WHERE(args={'form':'DNF','predicates': [[{'left':'l_discount','op':'<','right':'0.06'}]]}))
q.add_operation(name='where_l_discount_2',operation=WHERE(args={'form':'DNF','predicates': [[{'left':'l_discount','op':'<','right':'0.04'}]]}))
q.add_operation(name='where_l_quantity',operation=WHERE(args={'form':'DNF','predicates': [[{'left':'l_quantity','op':'<','right':'20'}]]}))

q.add_operation(node_type="DM",name='groupby_operation',operation=GROUPBYAGG(args={'groupby_key':[],'aggregates':[{'op':'sum','col':'l_extendedprice*(1-l_discount)','alias':'revenue'}]}))
q.add_operation(output=True,name='select_operation',operation=SELECT(args={'columns':['revenue']}))

q.add_edge('table_lineitem','where_l_shipdate_1')
q.add_edge('where_l_shipdate_1','where_l_shipdate_2')
q.add_edge('where_l_shipdate_2','where_l_discount_1')
q.add_edge('where_l_discount_1','where_l_discount_2')
q.add_edge('where_l_discount_2','where_l_quantity')
q.add_edge('where_l_quantity','groupby_operation')
q.add_edge('groupby_operation','select_operation')
q.compile()
# q.display()