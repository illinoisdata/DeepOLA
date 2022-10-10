import glob
import json
import sys
import pandas as pd

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 extract-results.py <scale> <variation> <num_runs>")
        exit(1)

    scale = int(sys.argv[1])
    variation = sys.argv[2]
    num_runs = int(sys.argv[3])

    queries = ['q1','q6']
    for run in range(1,num_runs+1):
        for query in queries:
            log_file = f"outputs/scale={scale}/{variation}/run={run}/{query}/run.log"
            print(f"Scanning File: {log_file}")
            csv_lines = []
            log_lines = open(log_file,'r').readlines()
            csv_start = False
            for line in log_lines:
                if csv_start and "CSV-ENDS" in line:
                    break
                if csv_start:
                    csv_lines.append(line.strip())
                if not csv_start and "CSV-STARTS" in line:
                    csv_start = True;
            df = pd.DataFrame([line.split(',') for line in csv_lines[1:]], columns=csv_lines[0].split(','))
            df["partition"] = df["partition"].astype('int64')
            df["timestamp"] = pd.to_datetime(df["timestamp"]).astype('int64')
            df["time_taken"] = df["timestamp"].diff()
            df["total_time_taken"] = df["time_taken"].cumsum()
            df = df[df["partition"] != 0]

            ## Create file for each result
            grouped_df = df.groupby("partition")
            meta_data = {"time_measures_ns": [], "progress": []}
            columns_to_remove = ["progress", "timestamp", "time_taken", "total_time_taken"]
            for partition, filtered_df in grouped_df:
                time_taken = filtered_df["total_time_taken"].max()
                progress = filtered_df["progress"].max()
                filtered_df = filtered_df.drop(columns = columns_to_remove)
                # Write output and meta-data
                filtered_df.to_csv(f"outputs/scale={scale}/{variation}/run={run}/{query}/{partition-1}.csv",index=False)
                meta_data["time_measures_ns"].append(time_taken)
                meta_data["progress"].append(progress)

            meta_data["results_dir"] = f"outputs/scale={scale}/{variation}/run={run}/{query}/"
            meta_data["time_measures_ns"].sort()
            meta_data["progress"].sort()
            with open(f"outputs/scale={scale}/{variation}/run={run}/{query}/meta.json",'w') as f:
                f.write(json.dumps(meta_data, indent=2))
