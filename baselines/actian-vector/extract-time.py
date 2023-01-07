import pandas as pd
import numpy as np
import re

output_dir="outputs"
scale=100
start_run=1
num_runs=5

result = []
for query_no in range(1,23):
    print(query_no)
    time_taken_list = []
    for run in range(start_run,start_run+num_runs):
        time_file = f"{output_dir}/scale={scale}/run={run}/{query_no}.log"
        time_log = open(time_file,"r").readlines()[-6]
        time_taken = float(re.findall("\d+\.\d+", time_log)[0])
        time_taken_list.append(time_taken)
    result.append({
        "query_no": query_no,
        "mean": np.mean(time_taken_list),
        "std": np.std(time_taken_list),
        "num_runs": len(time_taken_list),
    })

result_output_file = f"{output_dir}/scale={scale}/combined-results.csv"
pd.DataFrame(result).to_csv(result_output_file,index=False)
print(f"Saved result to {result_output_file}")
