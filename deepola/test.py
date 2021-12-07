from numpy import VisibleDeprecationWarning
from datetime import datetime
import time
import logging
import json
import os
import importlib
import argparse

from deepola.operations import *
from deepola.query.query import Query
from deepola.query.session import QuerySession
from utils import load_table

### Argument Parser
parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--variation', type=str, required=False, default = 'run_incremental', help='Whether run incremental evaluation or not')
parser.add_argument('--data_dir', type=str, required=False, default = '../data/', help='Data Directory')
parser.add_argument('--scale', type=str, required=False, default = 1, help='Scale')
parser.add_argument('--partitions', type=int, required=False, default = 1, help='Number of partitions to evaluate on')
parser.add_argument('--query', type=str, required=False, default = 'q1', help='Query to evaluate on')
args = parser.parse_args()

### Configure Logging
log_dir = f"logs/{args.variation}/scale={args.scale}/partition={args.partitions}/query={args.query}/"
data_variation=f"scale={args.scale}/partition={args.partitions}/"
os.makedirs(log_dir, exist_ok=True)
# current_timestamp = int(datetime.now().replace(microsecond=0).timestamp())
current_timestamp = 'latest'
log_file = f"{log_dir}/{current_timestamp}.log"
logging.basicConfig(filename=log_file, filemode='w', level=logging.DEBUG, format='%(asctime)s.%(msecs)d %(levelname)s:%(message)s',datefmt = '%s')
logger = logging.getLogger()

num_partitions = args.partitions
query_num = args.query
data_dir = args.data_dir
output_dir = f"outputs/{data_variation}/query={query_num}/"
os.makedirs(output_dir,exist_ok=True)

time_taken = 0

### Parsing Query
query_module = importlib.import_module(f"deepola.tpch_queries.{query_num}")
query = query_module.q
tables = []
for node in query.nodes:
    cls = type(query.nodes[node]['operation']).__name__
    if cls == "TABLE":
        tables.append(query.nodes[node]['operation'].args['table'])

### Creating QuerySession
session = QuerySession(query)
logs = []
if args.variation == 'run_incremental':
    logger.debug(f"func:start:QueryProcessing")
    partitioned_dfs = []
    for partition in range(1,num_partitions+1):
        start_time = time.time()
        input_nodes = {}
        for table in tables:
            df = load_table(table,partition,directory=f'{data_dir}/{data_variation}')
            input_nodes[f'table_{table}'] = {'input0':df}
        logger.debug(f"func:start:EvaluatingResults partition:{partition}")
        result = session.run_incremental(eval_node='select_operation',input_nodes=input_nodes)
        logger.debug(f"func:end:EvaluatingResults partition:{partition}")
        end_time = time.time()
        time_taken += (end_time - start_time)
        print("Time taken: ",time_taken)

        logger.debug(f"func:start:SavingResults partition:{partition}")
        result.to_csv(f'{output_dir}partial-{partition}.csv')
        logger.debug(f"func:end:SavingResults partition:{partition}")

        logs.append({
            'log_time': time.time(),
            'variation': 'incremental',
            'query': query_num,
            'partition': partition,
            'time_taken': time_taken,
            'file_path': f'{output_dir}partial-{partition}.csv'
        })
    logger.debug(f"func:end:QueryProcessing")
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
