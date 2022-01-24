import polars as pl
from deepola.operations import *
import os
import json
import logging
logger = logging.getLogger()

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
	'customer': ['CUSTKEY','NAME','ADDRESS','NATIONKEY','PHONE','ACCTBAL','MKTSEGMENT','COMMENT'],
	'supplier': ["suppkey","name","address","nationkey","phone","acctbal","comment"],
	'nation': ["nationkey","name","regionkey","comment"],
	'region': ["regionkey","name","comment"],
	'part': ["partkey","name","mfgr","brand","type","size","container","retailprice","comment"]
}
table_prefix = {'lineitem': 'l','orders': 'o','customer': 'c', 'supplier': 's', 'nation': 'n', 'region': 'r', 'part': 'p'}
header_parsed = {}
for table_name in header.keys():
	header_parsed[table_name] = [table_prefix[table_name]+'_'+x.lower() for x in header[table_name]]

def load_table(table_name, start_part = 1, end_part = None, directory='../data'):
	logger.debug(f"func:start:load_table {table_name} {start_part} {end_part} {directory}")
	if os.path.exists(f'{directory}/{table_name}.tbl'):
		if start_part == 1:
			file_path = f'{directory}/{table_name}.tbl'
			df = pl.read_csv(file_path,sep='|', has_headers = False, new_columns = header_parsed[table_name],use_pyarrow=True)
			logger.debug(f"func:end:load_table {table_name} {start_part} {end_part} {directory}")
			return df
		else:
			return None
	if end_part is None:
		file_path = f'{directory}/{table_name}.tbl.{start_part}'
		df = pl.read_csv(file_path,sep='|', has_headers = False, new_columns = header_parsed[table_name],use_pyarrow=True)
		logger.debug(f"func:end:load_table {table_name} {start_part} {end_part} {directory}")
		return df
	else:
		all_dfs = []
		for part in range(start_part, end_part+1):
			file_path = f'{directory}/{table_name}.tbl.{part}'
			df = pl.read_csv(file_path,sep='|', has_headers = False, new_columns = header_parsed[table_name],use_pyarrow=True)
			all_dfs.append(df)
		concatenated_df = pl.concat(all_dfs)
		logger.debug(f"func:end:load_table {table_name} {start_part} {end_part} {directory}")
		return concatenated_df