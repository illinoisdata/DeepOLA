import json
import os
import matplotlib.pyplot as plt
import numpy as np


def wake_dir(scale, partition, qdx):
    return f"/results/wake/scale={scale}/partition={partition}/parquet/ci/q{qdx}"


def wake_read_ci(scale, partition, num_runs, qdx):
    with open(os.path.join(wake_dir(scale, partition, qdx), "results.json")) as f:
        results = json.load(f)
        for k in results:
            results[k] = results[k][2:]  # Truncate first two.
        return results


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('scale', type=int)
    parser.add_argument('partition', type=int)
    parser.add_argument('num_runs', type=int)
    args = parser.parse_args()
    
    # TODO: Read Polars results here.
    # Reads results.
    wake_results = wake_read_ci(args.scale, args.partition, args.num_runs, "14c")
    xs = np.arange(len(wake_results["x"])) + 2

    # Plots results.
    SZ = 2.0
    fig, axs = plt.subplots(ncols=2, figsize=(2.5 * 1.2 * SZ, SZ))

    # CI Convergence
    ax = axs[0]
    ax.plot(xs, wake_results["x"], 'b-')
    ax.fill_between(xs, wake_results["x_neg"], wake_results["x_pos"], color='b', alpha=0.1)
    ax.set_xlabel("Number of Partitions")
    ax.set_ylabel("promo_revenue")
    ax.set_title("CI Convergence")

    # CI Correctness
    ax = axs[1]
    ax.plot(xs, wake_results["rel_max"], label="Max")
    ax.plot(xs, wake_results["rel_p95"], label="P95")
    ax.plot(xs, wake_results["rel_avg"], label="Avg")
    ax.plot(xs, np.ones_like(xs), label="P95 Limit")
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.set_xlabel("Number of Partitions")
    ax.set_ylabel("Rel. CI Range")
    ax.set_title("CI Correctness")

    fig.tight_layout()
    fig.savefig("/results/viz/fig10_ci.png")
