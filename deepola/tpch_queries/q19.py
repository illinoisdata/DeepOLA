from deepola.operations import *
from deepola.query.query import Query

# select
# 	sum(lineitem.l_extendedprice* (1 - lineitem.l_discount)) as revenue
# from
# 	lineitem,
# 	part
# where
# 	(
# 		part.p_partkey = lineitem.l_partkey
# 		and part.p_brand = 'Brand#5'
# 		and part.p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
# 		and lineitem.l_quantity >= 1 and lineitem.l_quantity <= 11
# 		and part.p_size between 1 and 5
# 		and lineitem.l_shipmode in ('AIR', 'AIR REG')
# 		and lineitem.l_shipinstruct = 'DELIVER IN PERSON'
# 	)
# 	or
# 	(
# 		part.p_partkey = lineitem.l_partkey
# 		and part.p_brand = 'Brand#10'
# 		and part.p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
# 		and lineitem.l_quantity >= 5 and lineitem.l_quantity <= 15
# 		and part.p_size between 1 and 10
# 		and lineitem.l_shipmode in ('AIR', 'AIR REG')
# 		and lineitem.l_shipinstruct = 'DELIVER IN PERSON'
# 	)
# 	or
# 	(
# 		part.p_partkey = lineitem.l_partkey
# 		and part.p_brand = 'Brand#15'
# 		and part.p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
# 		and lineitem.l_quantity >= 10 and lineitem.l_quantity <= 20
# 		and part.p_size between 1 and 15
# 		and lineitem.l_shipmode in ('AIR', 'AIR REG')
# 		and lineitem.l_shipinstruct = 'DELIVER IN PERSON'
# 	);

q = Query()
q.add_operation(name='table_lineitem',operation=TABLE(args={'table': 'lineitem'}))
q.add_operation(name='table_part',operation=TABLE(args={'table': 'part'}))

q.add_operation(name='join_part_lineitem',operation=INNERJOIN(args={'left_on':['p_partkey'],'right_on':['l_partkey']}))

q.add_operation(name='where_big',operation=WHERE(args={'form':'CNF','predicates': [
        [
            {'left':'p_brand','op':'==','right':'Brand#5'},
            # {'left':'p_container','op':'IN','right':['SM CASE','SM BOX','SM PACK','SM PKG']},
            {'left':'l_quantity','op':'>=','right':1},
            {'left':'l_quantity','op':'<=','right':11},
            {'left':'p_size','op':'>=','right':1},
            {'left':'l_quantity','op':'<=','right':5},
            # {'left':'l_shipmode','op':'IN','right':['AIR', 'AIR REG']},
            {'left':'l_shipinstruct','op':'==','right':'DELIVER IN PERSON'}
        ], ## OR1
        [
            {'left':'p_brand','op':'==','right':'Brand#10'},
            # {'left':'p_container','op':'IN','right':['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK']},
            {'left':'l_quantity','op':'>=','right':5},
            {'left':'l_quantity','op':'<=','right':15},
            {'left':'p_size','op':'>=','right':1},
            {'left':'l_quantity','op':'<=','right':10},
            # {'left':'l_shipmode','op':'IN','right':['AIR', 'AIR REG']},
            {'left':'l_shipinstruct','op':'==','right':'DELIVER IN PERSON'}
        ], ## OR2
        [
            {'left':'p_brand','op':'==','right':'Brand#15'},
            # {'left':'p_container','op':'IN','right':['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']},
            {'left':'l_quantity','op':'>=','right':10},
            {'left':'l_quantity','op':'<=','right':20},
            {'left':'p_size','op':'>=','right':1},
            {'left':'l_quantity','op':'<=','right':15},
            # {'left':'l_shipmode','op':'IN','right':['AIR', 'AIR REG']},
            {'left':'l_shipinstruct','op':'==','right':'DELIVER IN PERSON'}
        ], ## OR3
    ]
}))

q.add_operation(node_type="DM",name='groupby_operation',operation=GROUPBYAGG(args={'groupby_key':[],'aggregates':[{'op':'sum','col':'l_extendedprice*(1-l_discount)','alias':'revenue'}]}))
q.add_operation(output=True,name='select_operation',operation=SELECT(args={'columns':['revenue']}))

q.add_edge('table_part','join_part_lineitem')
q.add_edge('table_lineitem','join_part_lineitem')
q.add_edge('join_part_lineitem','where_big')
q.add_edge('where_big','groupby_operation')
q.add_edge('groupby_operation','select_operation')
q.compile()
# q.display()