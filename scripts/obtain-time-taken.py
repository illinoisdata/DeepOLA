import os
import sys
import pandas as pd

def obtain_time_taken_for_plot(scale, partition, log_level):
    output_dir = f"../deepola/wake/outputs/scale={scale}/partition={partition}"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    result = []
    for query_no in range(1,23):
        try:
            log_file = open(f'../deepola/wake/logs/scale={scale}/partition={partition}/q{query_no}-{log_level}.log','r').readlines()
        except:
            continue
        for l in log_file:
            if "Query Took" in l:
                result.append({
                    'query_no': f'q{query_no}',
                    'time': float(l.strip().split(':')[-1].strip()[:-1])
                })
    df = pd.DataFrame(result)
    print(f"Wrote to file: {output_dir}/result.csv")
    df.to_csv(f"{output_dir}/result.csv", index=False)
    return df

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 obtain-time-taken.py <scale=50> <partition=512M>")
        exit(1)
    scale = sys.argv[1]
    partition = sys.argv[2]
    log_level = sys.argv[3]
    print(obtain_time_taken_for_plot(scale,partition,log_level))
