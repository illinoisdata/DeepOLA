import json
import os
import matplotlib.pyplot as plt
import numpy as np


def wake_dir(scale, partition, run, qdx):
    return f"/results/wake/scale={scale}/partition={partition}/parquet/run={run}/q{qdx}"


def wanderjoin_dir(scale, partition, run, qdx):
    return f"/results/wanderjoin/scale={scale}/partition={partition}/run={run}/q{qdx}"


def wake_read_error(scale, partition, num_runs, qdx):
    all_time_ns = []
    for run in range(1, num_runs + 1):
        with open(os.path.join(wake_dir(scale, partition, run, qdx), "meta.json")) as f:
            result = json.load(f)
            all_time_ns.append(result["time_measures_ns"])
    all_time_s = np.array(all_time_ns) / 1e9
    all_time_s = all_time_s.mean(axis=0)
    with open(os.path.join(wake_dir(scale, partition, 0, qdx), "results.json")) as f:
        result = json.load(f)
        mape_p = result["mape_p"]
    assert len(all_time_s) == len(mape_p)
    return all_time_s[1:], mape_p[1:]


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('scale', type=int)
    parser.add_argument('partition', type=int)
    parser.add_argument('num_runs', type=int)
    args = parser.parse_args()
    
    # TODO: Read Polars results here.
    # Reads results.
    wake_results = [
        wake_read_error(args.scale, args.partition, args.num_runs, 26),
        wake_read_error(args.scale, args.partition, args.num_runs, 27),
        wake_read_error(args.scale, args.partition, args.num_runs, 23),
        wake_read_error(args.scale, args.partition, args.num_runs, 24),
        wake_read_error(args.scale, args.partition, args.num_runs, 25),
    ]
    titles = [
        "Modified Q1",
        "Modified Q6",
        "Modified Q3",
        "Modified Q7",
        "Modified Q10",
    ]

    # Plots results.
    SZ = 2.0
    fig, axs = plt.subplots(ncols=5, figsize=(5 * 1.2 * SZ, SZ))
    for idx, ax in enumerate(axs):
        # TODO: Get Polars number here and plot as a vertical line in ax.
        wake_time, wake_mape_p = wake_results[idx]

        lns1 = ax.plot(wake_time, wake_mape_p, 'b-', label="WAKE")

        lns = lns1
        labs = [l.get_label() for l in lns]
        ax.legend(lns, labs, loc='lower center', bbox_to_anchor=(0.5, 1.2), ncols=len(lns))
        ax.set_xlabel("Elapsed Time (s)")
        ax.set_ylabel("MAPE (%)")
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_title(titles[idx])
    fig.tight_layout()
    fig.savefig("/results/viz/fig9_tpch_ola.png")
