"""
Generate deep group-by query

Example: python ../scripts/deep_query_gen.py 10
"""

# INDENT_CHAR = '\t'
INDENT_CHAR = '  '
TABLE_NAME = 'numbertable'


def indent(query):
    return '\n'.join([INDENT_CHAR + line for line in query.split('\n')])


def template_at_depth(depth, gb_columns, value_column, agg_cycle):
    assert len(gb_columns) >= depth
    agg = agg_cycle[depth % len(agg_cycle)]
    gb = ', '.join(gb_columns[:depth])
    query = f'SELECT {agg}(t.{value_column}) as {value_column}\n'
    if len(gb_columns) == depth:
        query += 'FROM {} as t\n'
    else:
        query += 'FROM (\n'
        query += '{}\n'
        query += ') AS t\n'
    if depth > 0:
        query += f'GROUP BY {gb}'
    return query


def generate_query(num_depths, gb_columns, value_column, agg_cycle):
    query = TABLE_NAME
    for depth in range(num_depths, -1, -1):
        template_depth = template_at_depth(depth, gb_columns, value_column, agg_cycle)
        query = indent(query)
        query = template_depth.format(query)
    return query


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Generate deep query')
    parser.add_argument('num_depths', type=int,
                        help='Query depths')
    parser.add_argument('--gb_columns', type=str, default=None,
                        help='List of group-by column names')
    parser.add_argument('--value_column', type=str, default="x",
                        help='Value column name')
    parser.add_argument('--agg_cycle', type=list, default=['sum', 'max'],
                        help='List of aggregates to spin through')
    args = parser.parse_args()

    # Fill in args
    num_depths = args.num_depths
    if args.gb_columns is None:
        gb_columns = ["c" + "i" * idx for idx in range(num_depths)]
    else:
        assert len(args.gb_columns) == args.num_depths
        gb_columns = args.gb_columns
    value_column = args.value_column
    agg_cycle = args.agg_cycle

    # Generate query
    print(generate_query(num_depths, gb_columns, value_column, agg_cycle))
