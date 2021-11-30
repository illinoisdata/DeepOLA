from deepola.operations import *
from deepola.query.query import Query

# SELECT
#     l_returnflag,
#     l_linestatus,
#     sum(l_quantity) as sum_qty,
#     sum(l_extendedprice) as sum_base_price,
#     sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
#     sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
#     avg(l_quantity) as avg_qty,
#     avg(l_extendedprice) as avg_price,
#     avg(l_discount) as avg_disc,
#     count(*) as count_order
# FROM
#     lineitem
# WHERE
#     l_shipdate <= date '1998-12-01'
# GROUP BY
#     l_returnflag,
#     l_linestatus
# ORDER BY
#     l_returnflag,
#     l_linestatus;

q = Query()
q.add_operation(name='table_lineitem',operation=TABLE(args={'table': 'lineitem'}))
q.add_operation(name='where_l_shipdate',operation=WHERE(args={'predicates': [[{'left':'l_shipdate','op':'<=','right':'1998-12-01'}]]}))
q.add_operation(
    node_type="DM",
    name='groupby_operation',
    operation=GROUPBYAGG(
        args={
            'groupby_key':['l_returnflag','l_linestatus'],
            'aggregates':[
                {'op':'sum','col':'l_quantity','alias':'sum_qty'},
                {'op':'sum','col':'l_extendedprice','alias':'sum_base_price'},
                {'op':'sum','col':'l_extendedprice * (1 - l_discount)','alias':'sum_disc_price'},
                {'op':'sum','col':'l_extendedprice * (1 - l_discount) * (1 + l_tax)','alias':'sum_charge'},
                {'op':'count','col':'l_quantity','alias':'count_qty'},
                {'op':'count','col':'l_extendedprice','alias':'count_price'},
                {'op':'sum','col':'l_discount','alias':'sum_disc'},
                {'op':'count','col':'l_discount','alias':'count_disc'},
                {'op':'count','col':'*','alias':'count_order'},
            ]
        }
    )
)
q.add_operation(node_type="DM",name='orderby_operation',operation=ORDERBY(args=[{'column':'l_returnflag'},{'column':'l_linestatus'}]))
q.add_operation(output=True,name='select_operation',operation=SELECT(args={'columns':'*'}))

q.add_edge('table_lineitem','where_l_shipdate')
q.add_edge('where_l_shipdate','groupby_operation')
q.add_edge('groupby_operation','orderby_operation')
q.add_edge('orderby_operation','select_operation')
q.compile()

if __name__ == "__main__":
    q.save('q1.json')