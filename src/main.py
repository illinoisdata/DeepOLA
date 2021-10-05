import pandas as pd
import time
from query.query import Query, QueryOLA
from operations import *
import utils
import json
import ipdb
from sqlite3 import connect
import copy

def test_query(file_path):
	### Get tables to be loaded.
	operations = utils.load_query_json(file_path)
	query = QueryOLA(operations)

	tables = ['customer','orders','lineitem']
	input_tables = {}
	for table in tables:
		if table == 'orders':
			input_tables[table] = utils.load_table(table,1,2)
		else:
			input_tables[table] = utils.load_table(table,1)
	input_tables_cache = copy.deepcopy(input_tables)
	for table in input_tables:
		print(table, input_tables[table].size)
	result = query.evaluate(input_tables)
	print(result)

	### Part 2 Orders.
	new_input_tables = input_tables_cache
	new_input_tables['lineitem'] = utils.load_table('lineitem',2)
	result_updated = query.online_evaluate(result, new_input_tables)
	print(result_updated)

def test():
	lineitem_names = ['ORDERKEY','PARTKEY','SUPPKEY','LINENUMBER','QUANTITY','EXTENDEDPRICE',
	'DISCOUNT','TAX','RETURNFLAG','LINESTATUS','SHIPDATE','COMMITDATE','RECEIPTDATE',
	'SHIPINSTRUCT','SHIPMODE','COMMENT']
	df1 = pd.read_csv('../data/lineitem.tbl.1',sep='|', names = lineitem_names)
	df2 = pd.read_csv('../data/lineitem.tbl.2',sep='|', names = lineitem_names)

	## Example Query: SELECT COUNT(ORDERKEY) FROM LINEITEM WHERE QUANTITY > 30
	where_condition = WHERE({'left': 'QUANTITY','op': '>', 'right': 30})
	agg_condition = AGG({'key':None, 'op': 'count', 'column': 'ORDERKEY'})
	operations = [where_condition, agg_condition]

	### Concatenating the results and then evaluating.
	query1 = Query(operations)
	merged_df = pd.concat([df1,df2])
	start_time = time.time()
	result = query1.evaluate(merged_df)
	end_time = time.time()
	print("Time taken: %.6f"%(end_time - start_time))
	print("Result: ",result)

	### Online Aggregation by merging results from the individual data frames.
	query2 = QueryOLA(operations)
	start_time = time.time()
	res1 = query2.evaluate(df1)
	result = query2.online_evaluate(res1, df2)
	end_time = time.time()
	print("Time taken: %.6f"%(end_time - start_time))
	print("Result: ",result)

def test_pandas_sql_query(query):
	conn = connect(':memory:')
	customer = utils.load_table('customer',1)
	customer.to_sql('customer',conn)

	lineitem = utils.load_table('lineitem',1,2)
	lineitem.to_sql('lineitem',conn)

	orders = utils.load_table('orders',1,2)
	orders.to_sql('orders',conn)

	result = pd.read_sql(query, conn)
	print(result)
	return result

if __name__ == "__main__":
	# test_query('../queries/tpch-1.json')
	sql_query = open('../queries/tpch-1.sql','r').read()
	test_pandas_sql_query(sql_query)