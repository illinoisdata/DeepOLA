import os
import glob
import re
import json

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import describe as scipy_descibe

# COUNT (fixing)
# Q_IDX = "1c"
# VALUE_COLUMN = "count_order"
# VAR_COLUMN = "count_order_var"

# SUM
# Q_IDX = "6c"
# VALUE_COLUMN = "disc_price_sum"
# VAR_COLUMN = "disc_price_sum_var"

# AVG
Q_IDX = "14c"
VALUE_COLUMN = "promo_revenue"
VAR_COLUMN = "promo_revenue_var"

CONFIDENCE_LEVEL = 0.95
ALPHA = (1 / (1 - CONFIDENCE_LEVEL)) ** 0.5
# ALPHA = 1.96

def read_result(t, dirpath):
    assert isinstance(t, int)
    return pd.read_csv(f"{dirpath}/{t}.csv")

def write_result(dirpath, results):
    # Write in JSON
    json_path = f"{dirpath}/results.json"
    with open(json_path, "w") as f:
        json.dump(results, f)
    print(f"Written {json_path}")

    # Write in text table
    columns = []
    cells = []
    for key, values in results.items():
        columns.append(key)
        cells.append(values)
        assert len(cells[0]) == len(values)
    columns.append("idx")
    cells.append(list(range(len(cells[0]))))
    cells = np.array(cells).T
    txt_path = f"{dirpath}/results.txt"
    with open(txt_path, "w") as f:
        for column in columns:
            f.write(f"{column}\t")
        f.write("\n")
        for row in cells:
            for cell in row:
                f.write(f"{cell}\t")
            f.write("\n")
    print(f"Written {txt_path}")

def read_all_results(dirpath, nt=None):
    if nt is None:
        # Read all CSVs
        csvs = map(
            lambda path: re.search("\/(\d+)\.csv", path),
            glob.glob(f"{dirpath}/*.csv")
        )
        numbered_csvs = filter(lambda c: c is not None, csvs)
        nt = max(map(lambda c: int(c.group(1)), numbered_csvs)) + 1
    return [read_result(t, dirpath=dirpath) for t in range(nt)]

def read_all_results_runs(dirpath, nruns):
    return [read_all_results(f"{dirpath}/run={r}/q{Q_IDX}") for r in range(1, nruns + 1)]

def check_ci(df, df_ref):
    assert len(df) == 1
    assert len(df_ref) == 1
    diff = (df[VALUE_COLUMN] - df_ref[VALUE_COLUMN])[0]
    stddev = df[VAR_COLUMN][0] ** 0.5
    return abs(diff) <= ALPHA * stddev, \
           ALPHA * stddev, \
           abs(diff), \
           abs(diff) / (ALPHA * stddev), \
           df[VALUE_COLUMN][0], \
           df_ref[VALUE_COLUMN][0], \

def calculate_accuracy_all(q_results_runs):
    acc = []
    acc = []
    for q_results in q_results_runs:
        q_ref = q_results[-1]  # Use last result as the reference.
        acc.append([check_ci(q_result, q_ref) for q_result in q_results])
    acc = np.array(acc)
    print(scipy_descibe(acc[:, 1:, 3].flatten()))
    return {
        "true_avg": list(acc[..., 0].mean(0)),
        "range_avg": list(acc[..., 1].mean(0)),
        "diff_avg": list(acc[..., 2].mean(0)),
        "rel_avg": list(acc[..., 3].mean(0)),
        "rel_p95": list(np.percentile(acc[..., 3], 95.0, axis=0)),
        "rel_max": list(acc[..., 3].max(0)),
        "x": list(acc[..., 4][0]),
        "x_pos": list(acc[..., 4][0] + acc[..., 1][0]),
        "x_neg": list(acc[..., 4][0] - acc[..., 1][0]),
        # "range": acc[..., 1][1],
        # "diff": acc[..., 2][1],
        # "rel": acc[..., 3],
    }

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Extract accuracy from many result')
    parser.add_argument('dirpath', type=str,
                        help='path to directory with qxx subdirectories')
    parser.add_argument('ref_dirpath', type=str,
                        help='path to directory with qxx having intermediate results')
    parser.add_argument('num_runs', type=int,
                        help='number of runs')
    parser.add_argument('--answer', type=str, default=None,
                        help='path to asnwer csv')
    args = parser.parse_args()
    print(f"Args: {args}")
    if args.answer is None:
        print(f"WARNING: using last result as the true answer")
        answer = None
    else:
        print(f"Using {args.answer} as the answer sheet")
        answer = pd.read_csv(args.answer)
        print(answer)

    # Extract result and accuracy
    dirpath = args.dirpath
    ref_dirpath = args.ref_dirpath
    num_runs = args.num_runs
    q_results_runs = read_all_results_runs(ref_dirpath, num_runs)
    q_accuracy = calculate_accuracy_all(q_results_runs)

    # Check
    # print(q_accuracy)
    # N, M, SZ, RS = 1, 3, 5, 1.6
    # fig, axes = plt.subplots(ncols=M, figsize=(M * SZ * RS, N * SZ))
    # axes[0].plot(q_accuracy["true_avg"])
    # axes[1].plot(q_accuracy["range_avg"], label="range_avg")
    # axes[1].plot(q_accuracy["diff_avg"], label="diff_avg")
    # # axes[1].hist(q_accuracy["range"], label="range")
    # # axes[1].hist(q_accuracy["diff"], label="diff")
    # axes[1].legend()
    # axes[2].plot(q_accuracy["rel_avg"])
    # axes[2].plot(q_accuracy["rel_p95"])
    # axes[2].plot(q_accuracy["rel_max"])
    # # axes[2].hist(q_accuracy["rel"].flatten())
    # # axes[2].set_yscale('log')
    # plt.show()

    # Write accuracy
    write_result(dirpath, q_accuracy)

