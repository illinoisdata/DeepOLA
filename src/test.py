from numpy import VisibleDeprecationWarning
from operations import *
from query.query import Query
from query.session import QuerySession
import tpch_queries
from tpch_queries import q1
from utils import load_table
import time
import logging
import json
import os
import importlib

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')

import argparse
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--variation', type=str, required=False, default = 'run_incremental', help='Whether run incremental evaluation or not')
parser.add_argument('--data_dir', type=str, required=False, default = 'data/scale=1', help='Data Directory')
parser.add_argument('--num_partitions', type=int, required=False, default = 1, help='Number of partitions to evaluate on')
parser.add_argument('--query', type=str, required=False, default = 'q1', help='Query to evaluate on')
args = parser.parse_args()

import tpch_queries

num_partitions = args.num_partitions
query_num = args.query
data_dir = args.data_dir
output_dir = f"outputs/{data_dir}/{query_num}/"
os.makedirs(output_dir,exist_ok=True)

query_module = importlib.import_module(f"tpch_queries.{query_num}")
query = query_module.q
session = QuerySession(query)

tables = []
for node in query.nodes:
    cls = type(query.nodes[node]['operation']).__name__
    if cls == "TABLE":
        tables.append(query.nodes[node]['operation'].args['table'])
time_taken = 0
logs = []
if args.variation == 'run_incremental':
    partitioned_dfs = []
    for partition in range(1,num_partitions+1):
        start_time = time.time()
        input_nodes = {}
        for table in tables:
            df = load_table(table,partition,directory=f'../{data_dir}')
            input_nodes[f'table_{table}'] = {'input0':df}
        result = session.run_incremental(eval_node='select_operation',input_nodes=input_nodes)
        end_time = time.time()
        time_taken += (end_time - start_time)
        print("Time taken: ",time_taken)
        logs.append({
            'log_time': time.time(),
            'variation': 'incremental',
            'query': query_num,
            'partition': partition,
            'time_taken': time_taken,
            'file_path': f'{output_dir}partial-{partition}.csv'
        })
        result.to_csv(f'{output_dir}partial-{partition}.csv')
else:
    start_time = time.time()
    input_nodes = {}
    for table in tables:
        df = load_table(table,1,num_partitions,directory=f'../{data_dir}')
        input_nodes[f'table_{table}'] = {'input0':df}
    result = session.run_incremental(eval_node='select_operation',input_nodes=input_nodes)
    time_taken = time.time() - start_time
    print("Time taken: ",time_taken)
    result.to_csv(f'{output_dir}complete-{num_partitions}.csv')
    logs.append({
            'log_time': time.time(),
            'variation': 'complete',
            'query': query_num,
            'partition': num_partitions,
            'time_taken': time_taken,
            'file_path': f'{output_dir}complete-{num_partitions}.csv'
    })
with open(f'{output_dir}run-logs.json.{time.time()}','w') as f:
    f.write(json.dumps(logs,indent=2))