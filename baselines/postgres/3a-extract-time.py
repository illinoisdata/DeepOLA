import pandas as pd
import numpy as np
import sys

if __name__ == "__main__":
    scale = int(sys.argv[1])
    runs = int(sys.argv[2])
    results = []
    for query_no in range(1,23):
        query = f"q{query_no}"
        for run in range(1,runs+1):
            try:
                log_file = open(f"outputs/scale={scale}/{query}-run{run}.log","r").readlines()
            except:
                continue
            total_time = 0
            for l in log_file:
                if "Time:" in l:
                    print(l.strip())
                    time_taken = float(l.strip().split("ms")[0].split(":")[1].strip())
                    total_time += time_taken
            results.append({
                "query": query,
                "run": run,
                "time": np.round(total_time/1000,3)
            })
    df = pd.DataFrame(results)
    print(df.groupby(["query"]).std().round(2))
    print(df.groupby(["query"]).mean().round(2))
    df.to_csv(f"outputs/scale={scale}.csv",index=False)
