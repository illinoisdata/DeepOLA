import json
import os
import matplotlib.pyplot as plt
import numpy as np


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
    with open(os.path.join(wake_dir(scale, partition, 0, qdx), "results.json")) as f:
        result = json.load(f)
        mape_p = result["mape_p"]
        recall_p = result["recall_p"]
    assert len(all_time_s) == len(mape_p)
    assert len(all_time_s) == len(recall_p)
    return all_time_s[1:], mape_p[1:], recall_p[1:]


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
        wake_read_error(args.scale, args.partition, args.num_runs, 8),
        wake_read_error(args.scale, args.partition, args.num_runs, 18),
        wake_read_error(args.scale, args.partition, args.num_runs, 21),
    ]
    titles = ["Q8", "Q18", "Q21"]

    # Plots results.
    SZ = 3.0
    fig, axs = plt.subplots(ncols=3, figsize=(3 * 1.6 * SZ, SZ))
    for idx, ax in enumerate(axs):
        # TODO: Get Polars number here and plot as a vertical line in ax.
        wake_time, wake_mape_p, wake_recall_p = wake_results[idx]

        lns1 = ax.plot(wake_time, wake_mape_p, 'b-', label="MAPE")
        ax.set_ylabel("MAPE (%)")
        if all(wake_time > 0):
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
