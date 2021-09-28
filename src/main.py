import pandas as pd
import time
from query.query import Query, QueryOLA
from operations import WHERE, AGG

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
	print(result)

	### Online Aggregation by merging results from the individual data frames.
	query2 = QueryOLA(operations)
	start_time = time.time()
	res1 = query2.evaluate(df1)
	result = query2.online_evaluate(res1, df2)
	end_time = time.time()
	print("Time taken: %.6f"%(end_time - start_time))
	print(result)

if __name__ == "__main__":
	test()