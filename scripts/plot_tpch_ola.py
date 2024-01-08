import json
import os
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def wake_dir(scale, partition, run, qdx):
    if qdx == 26 or qdx == 27:
        return f"/results/wake/scale={scale}/partition={partition}/cleaned-parquet/run={run}/q{qdx}"
    else:
        return f"/results/wake/scale={scale}/partition={partition}/parquet/run={run}/q{qdx}"


def wanderjoin_file(scale, partition, run, qdx):
    return f"/results/wanderjoin/scale={scale}/partition={partition}/run={run}/{qdx}.csv"


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
    assert len(all_time_s) == len(mape_p)
    return all_time_s[2:], mape_p[2:]


def wanderjoin_read_error(scale, partition, num_runs, qdx):
    readings = []
    for run in range(1, num_runs + 1):
        result_df = pd.read_csv(wanderjoin_file(scale, partition, run, qdx), skipfooter=1)
        for i, row in result_df.iterrows():
            readings.append(((float(row["time (ms)"])/1000.0), 100.0 * float(row["rel. CI"])))
    combined_df = pd.DataFrame(readings, columns = ["time", "error"])
    result_df = combined_df.groupby("time").agg({ "error": ["mean"] }).sort_values("time", ascending=True).reset_index()
    time = result_df.values[:,0]
    mape = result_df.values[:,1]
    return time, mape


def polars_read_time(scale, partition, qdx):
    result_file = f"/results/polars/scale={scale}/partition={partition}/timings.csv"
    result_df = pd.read_csv(result_file)
    result_df = result_df[result_df["query"] == qdx]
    if len(result_df) > 0:
        agg_result = result_df.groupby("query").agg({ "time": ["mean", "std"] })
        return agg_result.values
    else:
        return [[0.0, 0.0]]


def progressive_read_error(scale, partition, num_runs, qdx):
    return [0.0], [0.0]


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('scale', type=int)
    parser.add_argument('partition', type=int)
    parser.add_argument('num_runs', type=int)
    args = parser.parse_args()
    
    polars_results = [
        polars_read_time(args.scale, args.partition, 26),
        polars_read_time(args.scale, args.partition, 27),
        polars_read_time(args.scale, args.partition, 23),
        polars_read_time(args.scale, args.partition, 24),
        polars_read_time(args.scale, args.partition, 25),
    ]

    wake_results = [
        wake_read_error(args.scale, args.partition, args.num_runs, 26),
        wake_read_error(args.scale, args.partition, args.num_runs, 27),
        wake_read_error(args.scale, args.partition, args.num_runs, 23),
        wake_read_error(args.scale, args.partition, args.num_runs, 24),
        wake_read_error(args.scale, args.partition, args.num_runs, 25),
    ]

    ola_results = [
        progressive_read_error(args.scale, args.partition, args.num_runs, 26),
        progressive_read_error(args.scale, args.partition, args.num_runs, 27),
        wanderjoin_read_error(args.scale, args.partition, args.num_runs, 23),
        wanderjoin_read_error(args.scale, args.partition, args.num_runs, 24),
        wanderjoin_read_error(args.scale, args.partition, args.num_runs, 25),
    ]

    titles = [
        "Modified Q1",
        "Modified Q6",
        "Modified Q3",
        "Modified Q7",
        "Modified Q10",
    ]

    ola_labels = [
        "ProgressiveDB",
        "ProgressiveDB",
        "Wanderjoin",
        "Wanderjoin",
        "Wanderjoin",
    ]

    # Plots results.
    SZ = 2.0
    fig, axs = plt.subplots(ncols=len(ola_labels), figsize=(len(ola_labels) * 1.2 * SZ, SZ))
    for idx, ax in enumerate(axs):
        ## Add Polars time as a vertical line.
        polars_time = polars_results[idx][0][0]
        ax.axvline(polars_time)

        wake_time, wake_mape_p = wake_results[idx]
        ola_time, ola_mape_p = ola_results[idx]

        lns1 = ax.plot(wake_time, wake_mape_p, 'b-x', label="WAKE")
        lns2 = ax.plot(ola_time, ola_mape_p, 'r.-', label=ola_labels[idx])

        lns = lns1 + lns2
        labs = [l.get_label() for l in lns]
        ax.legend(lns, labs, loc='lower center', bbox_to_anchor=(0.5, 1.2), ncols=len(lns))
        ax.set_xlabel("Elapsed Time (s)")
        ax.set_ylabel("MAPE (%)")
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_title(titles[idx])
    fig.tight_layout()
    fig.savefig("/results/viz/fig9_tpch_ola.png")
