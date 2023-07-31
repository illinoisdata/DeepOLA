import json
import os
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

def wake_dir(scale, partition, run, qdx):
    return f"/results/wake/scale={scale}/partition={partition}/parquet/run={run}/q{qdx}"

def polars_dir(scale, partition, run, qdx):
    return f"/results/polars/scale={scale}/partition={partition}/"

def postgres_read_time(scale):
    result_file = f"/results/postgres/scale={scale}/timings.csv"
    result_df = pd.read_csv(result_file)
    result = []
    for qdx in range(1, 22 + 1):
        filtered_df = result_df[result_df["query"] == qdx]
        if len(filtered_df) > 0:
            agg_result = filtered_df.groupby("query").agg({ "time": ["mean", "std"] })
            result.append([agg_result.iloc[0][0], agg_result.iloc[0][1]])
        else:
            result.append([0.0, 0.0])
    return np.array(result)

def polars_read_time(scale, partition):
    result_file = f"/results/polars/scale={scale}/partition={partition}/timings.csv"
    result_df = pd.read_csv(result_file)
    result = []
    for qdx in range(1, 22 + 1):
        filtered_df = result_df[result_df["query"] == qdx]
        if len(filtered_df) > 0:
            agg_result = filtered_df.groupby("query").agg({ "time": ["mean", "std"] })
            result.append([agg_result.iloc[0][0], agg_result.iloc[0][1]])
        else:
            result.append([0.0, 0.0])
    return np.array(result)

def wake_read_time(scale, partition, num_runs, pdx):
    avg_stddevs = []
    for qdx in range(1, 22 + 1):
        all_time = []
        for run in range(1, num_runs + 1):
            with open(os.path.join(wake_dir(scale, partition, run, qdx), "meta.json")) as f:
                result = json.load(f)
                if pdx >= 1 and pdx >= len(result["time_measures_ns"]):
                    pdx = -1
                all_time.append(result["time_measures_ns"][pdx] / 1e9)
        avg_stddevs.append((np.mean(all_time), np.std(all_time)))
    return np.array(avg_stddevs)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('scale', type=int)
    parser.add_argument('partition', type=int)
    parser.add_argument('num_runs', type=int)
    args = parser.parse_args()
    
    # TODO: Read other baselines here.
    # Reads results.
    all_results = [
        ("Postgres", postgres_read_time(args.scale)),
        ("Polars", polars_read_time(args.scale, args.partition)),
        ("WAKE-final", wake_read_time(args.scale, args.partition, args.num_runs, -1)),
        ("WAKE-first", wake_read_time(args.scale, args.partition, args.num_runs, 1)),
    ]

    # Plots results.
    # https://matplotlib.org/stable/gallery/lines_bars_and_markers/barchart.html#sphx-glr-gallery-lines-bars-and-markers-barchart-py
    SZ = 4.0
    fig, ax = plt.subplots(figsize=(22 * 0.05 * len(all_results) * SZ, SZ))
    xs = np.arange(1, 22 + 1)
    xticklabels = [f"q{x}" for x in  xs]
    width = 0.1
    multiplier = -len(all_results) / 2 + 0.5
    for label, avg_stddevs in all_results:
        offset = width * multiplier
        multiplier += 1
        ax.bar(xs + offset, avg_stddevs[:, 0], width=width, yerr=avg_stddevs[:, 1], label=label)
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.set_yscale("log")
    ax.set_ylim((0.1, 1e4))
    ax.set_xlabel("TPC-H Query Number (of original queries)")
    ax.set_ylabel("Query Latency (s)")
    ax.set_xticks(xs)
    ax.set_xticklabels(xticklabels)
    fig.tight_layout()
    fig.savefig("/results/viz/fig7_tpch.png")
