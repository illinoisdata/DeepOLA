import json
import os
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

def wake_dir(scale, partition, run, qdx):
    return f"/results/wake/scale={scale}/partition={partition}/parquet/run={run}/q{qdx}"


def wake_read_error(scale, partition, num_runs, qdx):
    all_time_ns = []
    for run in range(1, num_runs + 1):
        with open(os.path.join(wake_dir(scale, partition, run, qdx), "meta.json")) as f:
            result = json.load(f)
            all_time_ns.append(result["time_measures_ns"])
    all_time_s = np.array(all_time_ns) / 1e9
    all_time_s = all_time_s.mean(axis=0)
    with open(os.path.join(wake_dir(scale, partition, 1, qdx), "results.json")) as f:
        result = json.load(f)
        mape_p = result["mape_p"]
        recall_p = result["recall_p"]
    assert len(all_time_s) == len(mape_p)
    assert len(all_time_s) == len(recall_p)
    return all_time_s[2:], np.array(mape_p)[2:], np.array(recall_p)[2:]


def polars_read_time(scale, partition, qdx):
    result_file = f"/results/polars/scale={scale}/partition={partition}/timings.csv"
    result_df = pd.read_csv(result_file)
    result_df = result_df[result_df["query"] == qdx]
    if len(result_df) > 0:
        agg_result = result_df.groupby("query").agg({ "time": ["mean", "std"] })
        return agg_result.values
    else:
        return [[0.0, 0.0]]

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('scale', type=int)
    parser.add_argument('partition', type=int)
    parser.add_argument('num_runs', type=int)
    args = parser.parse_args()
    
    # Read results
    polars_results = [
        polars_read_time(args.scale, args.partition, 8),
        polars_read_time(args.scale, args.partition, 18),
        polars_read_time(args.scale, args.partition, 21),
    ]
    wake_results = [
        wake_read_error(args.scale, args.partition, args.num_runs, 8),
        wake_read_error(args.scale, args.partition, args.num_runs, 18),
        wake_read_error(args.scale, args.partition, args.num_runs, 21),
    ]
    titles = ["Q8", "Q18", "Q21"]

    # Plots results.
    SZ = 3.0
    fig, axs = plt.subplots(ncols=3, figsize=(3 * 1.6 * SZ, SZ))
    for idx, ax in enumerate(axs):
        ## Add Polars time as a vertical line.
        polars_time = polars_results[idx][0][0]
        ax.axvline(polars_time)
        wake_time, wake_mape_p, wake_recall_p = wake_results[idx]
        lns1 = ax.plot(wake_time, wake_mape_p, 'b-', label="MAPE")
        ax.set_ylabel("MAPE (%)")
        if all(wake_mape_p[:-1] > 0):
            ax.set_yscale("log")
        else:
            ax.set_ylim((-5, 105))

        ax_twin = ax.twinx()
        lns2 = ax_twin.plot(wake_time, wake_recall_p, 'g-', label="Recall")
        ax_twin.set_ylim((-5, 105))
        ax_twin.set_ylabel("Recall")

        lns = lns1 + lns2
        labs = [l.get_label() for l in lns]
        ax.legend(lns, labs, loc='lower center', bbox_to_anchor=(0.5, 1.15), ncols=len(lns))
        ax.set_xlabel("Elapsed Time (s)")
        ax.set_title(titles[idx])
    fig.tight_layout()
    fig.savefig("/results/viz/fig8_tpch_error.png")
