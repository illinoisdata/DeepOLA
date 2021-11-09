import polars as pl
from operations import *
import json

def load_query_json(file_path):
	current_relation = json.loads(open(file_path,'r').read())
	operations = []
	while(True):
		relation_class = globals()[current_relation["relation"]]
		print("RELATION CLASS:",current_relation["relation"])
		relation = relation_class(current_relation["args"])
		operations.append(relation)
		if "next_operation" in current_relation:
			current_relation = current_relation["next_operation"]
		else:
			break
	return operations

header = {
	'lineitem': ['ORDERKEY','PARTKEY','SUPPKEY','LINENUMBER','QUANTITY','EXTENDEDPRICE',
	'DISCOUNT','TAX','RETURNFLAG','LINESTATUS','SHIPDATE','COMMITDATE','RECEIPTDATE',
	'SHIPINSTRUCT','SHIPMODE','COMMENT'],
	'orders': ['ORDERKEY','CUSTKEY','ORDERSTATUS','TOTALPRICE','ORDERDATE','ORDERPRIORITY','CLERK','SHIPPRIORITY','COMMENT'],
	'customer': ['CUSTKEY','NAME','ADDRESS','NATIONKEY','PHONE','ACCTBAL','MKTSEGMENT','COMMENT']
}
table_prefix = {'lineitem': 'l','orders': 'o','customer': 'c' }
header_parsed = {}
for table_name in header.keys():
	header_parsed[table_name] = [table_prefix[table_name]+'_'+x.lower() for x in header[table_name]]

def load_table(table_name, start_part = 1, end_part = None):
	if end_part is None:
		file_path = '../data/{0}.tbl.{1}'.format(table_name,start_part)
		return pl.read_csv(file_path,sep='|', has_headers = False, new_columns = header_parsed[table_name])
	else:
		all_dfs = []
		for part in range(start_part, end_part+1):
			file_path = '../data/{0}.tbl.{1}'.format(table_name,part)
			df = pl.read_csv(file_path,sep='|', has_headers = False, new_columns = header_parsed[table_name])
			all_dfs.append(df)
		return pl.concat(all_dfs)