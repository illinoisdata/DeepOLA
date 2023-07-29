import json
import os
import matplotlib.pyplot as plt
import numpy as np


def wake_dir(depth, run):
    return f"/results/wake/g10_p1m_n100_c4/depth={depth}/run={run}/q30"


def wake_read_depth(num_runs, pdx):
    depth_time = []
    for depth in range(10 + 1):
        all_time = []
        for run in range(1, num_runs + 1):
            with open(os.path.join(wake_dir(depth, run), "meta.json")) as f:
                result = json.load(f)
                all_time.append(result["time_measures_ns"][pdx] / 1e9)
        depth_time.append(np.mean(all_time))
    return depth_time


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('num_runs', type=int)
    args = parser.parse_args()
    
    # TODO: Read Actian Vector results here.
    # Reads results.
    all_results = [
        ("WAKE-100th", wake_read_depth(args.num_runs, 99)),
        ("WAKE-10th", wake_read_depth(args.num_runs, 9)),
        ("WAKE-1st", wake_read_depth(args.num_runs, 0)),
    ]

    # Plots results.
    SZ = 3.0
    fig, ax = plt.subplots(figsize=(1.6 * SZ, SZ))
    xs = np.arange(10 + 1)
    for label, avgs in all_results:
        print(avgs)
        ax.plot(xs, avgs, label=label)
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.set_yscale("log")
    ax.set_xlabel("Depth of Query")
    ax.set_ylabel("Query Latency (s)")

    fig.tight_layout()
    fig.savefig("/results/viz/fig11_depth.png")
