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
parser.add_argument('--variation', type=str, required=False, default = 'run_incremental', help='Whether run incremental evaluation or not. {run_incremental, evaluate}')
parser.add_argument('--data_dir', type=str, required=False, default = '../data', help='Data Directory. Default: ../data')
parser.add_argument('--scale', type=str, required=False, default = 1, help='Scale. Default: 1')
parser.add_argument('--partitions', type=int, required=False, default = 10, help='Number of Partitions in Data. Default: 1')
parser.add_argument('--eval_partitions', type=int, required=False, default = 10, help='Number of Partitions to evaluate on. Default: 1')
parser.add_argument('--query', type=str, required=False, default = 'q1', help='Query to evaluate on. Default: q1')

args = parser.parse_args()
num_partitions = args.partitions
num_eval_partitions = args.eval_partitions
scale = args.scale
query_num = args.query
data_dir = args.data_dir
eval_variation = args.variation

### Configure Output Directories
data_variation=f"scale={scale}/partition={num_partitions}"
output_variation = f"query={query_num}/eval_partitions={num_eval_partitions}"
output_dir = f"outputs/{eval_variation}/{data_variation}/{output_variation}"
os.makedirs(output_dir,exist_ok=True)

### Configure Logging
log_dir = f"logs/{eval_variation}/{data_variation}/{output_variation}"
os.makedirs(log_dir, exist_ok=True)
# current_timestamp = int(datetime.now().replace(microsecond=0).timestamp())
current_timestamp = 'latest'
log_file = f"{log_dir}/{current_timestamp}.log"
logging.basicConfig(filename=log_file, filemode='w', level=logging.DEBUG, format='%(asctime)s.%(msecs)d %(levelname)s:%(message)s',datefmt = '%s')
logger = logging.getLogger()

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
reading_time = 0
total_time = 0
print("Variation | Partition | Reading Time | Evaluation Time | Total Time")
logs = []
logger.debug(f"func:start:QueryProcessing")
if args.variation == 'run_incremental':
    partitioned_dfs = []
    for partition in range(1,num_eval_partitions+1):
        start_time = time.time()
        input_nodes = {}
        for table in tables:
            reading_start_time = time.time()
            df = load_table(table,partition,directory=f'{data_dir}/{data_variation}')
            reading_end_time = time.time()
            reading_time += (reading_end_time - reading_start_time)
            input_nodes[f'table_{table}'] = {'input0':df}
        logger.debug(f"func:start:EvaluatingResults partition:{partition}")
        result = session.run_incremental(eval_node='select_operation',input_nodes=input_nodes)
        logger.debug(f"func:end:EvaluatingResults partition:{partition}")
        end_time = time.time()
        total_time += (end_time - start_time)
        print("%s | %d | %.6f | %.6f | %.6f" % (eval_variation, partition, reading_time, total_time-reading_time, total_time))

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
else:
    start_time = time.time()
    input_nodes = {}
    for table in tables:
        logger.debug(f"func:start:LoadingData Table:{table} Partitions:{num_eval_partitions}")
        reading_start_time = time.time()
        df = load_table(table,1,num_eval_partitions,directory=f'{data_dir}/{data_variation}')
        reading_end_time = time.time()
        reading_time += (reading_end_time - reading_start_time)
        input_nodes[f'table_{table}'] = {'input0':df}
        logger.debug(f"func:end:LoadingData Table:{table} Partitions:{num_eval_partitions}")
    logger.debug(f"func:start:EvaluatingResults")
    result = session.run_incremental(eval_node='select_operation',input_nodes=input_nodes)
    logger.debug(f"func:end:EvaluatingResults")
    total_time += (time.time() - start_time)
    print("%s | %d | %.6f | %.6f | %.6f" % (eval_variation, num_eval_partitions, reading_time, total_time - reading_time, total_time))
    logger.debug(f"func:start:SavingResults partition:{num_eval_partitions}")
    result.to_csv(f'{output_dir}complete-{num_eval_partitions}.csv')
    logger.debug(f"func:end:SavingResults partition:{num_eval_partitions}")
    logs.append({
            'log_time': time.time(),
            'variation': 'complete',
            'query': query_num,
            'partition': num_partitions,
            'time_taken': time_taken,
            'file_path': f'{output_dir}complete-{num_partitions}.csv'
    })
    logger.debug(f"func:end:QueryProcessing")
with open(f'{output_dir}run-logs.json.{time.time()}','w') as f:
    f.write(json.dumps(logs,indent=2))
