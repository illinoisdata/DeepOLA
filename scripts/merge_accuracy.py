"""
Example: merging across 10 runs over all 25 queries and name table with prefix fooHundred.

    for qidx in {1..25}; do \
        python scripts/merge_accuracy.py \
            deepola/wake/saved-vanilla/scale\=100/partition\=512M/parquet \
            1 10 ${qidx} ./merged.txt --latex_name fooHundred; \
    done
"""

import os
import glob
import re
import json

import numpy as np
import pandas as pd

from scipy.special import gamma


columns = [
    "mape_p",
    "mae",
    "recall_p",
    "precision_p",
]
write_columns = [
    *columns,
    "time_avg_s",
    "time_stddev_s",
]
write_columns_line = '\t'.join(write_columns)


# https://www.geeksforgeeks.org/python-program-to-convert-integer-to-roman/
def intToRoman(num):
    m = ["", "M", "MM", "MMM"]
    c = ["", "C", "CC", "CCC", "CD", "D",
         "DC", "DCC", "DCCC", "CM "]
    x = ["", "X", "XX", "XXX", "XL", "L",
         "LX", "LXX", "LXXX", "XC"]
    i = ["", "I", "II", "III", "IV", "V",
         "VI", "VII", "VIII", "IX"]
    thousands = m[num // 1000]
    hundreds = c[(num % 1000) // 100]
    tens = x[(num % 100) // 10]
    ones = i[num % 10]
    return thousands + hundreds + tens + ones


def read_dir(q_idx, dirpath):
    assert isinstance(q_idx, int)
    return f"{dirpath}/q{q_idx}"

def read_meta(q_idx, dirpath):
    with open(f"{read_dir(q_idx, dirpath=dirpath)}/meta.json", "r") as f:
        return json.loads(f.readline())

def read_result(q_idx, dirpath, expected_ts):
    # Read from text tables
    results = []
    for column in columns:
        txt_path = f"{read_dir(q_idx, dirpath=dirpath)}/{column}.txt"
        result = np.loadtxt(txt_path, skiprows=1)
        assert all(result[:, 0] == expected_ts)
        assert result.shape[1] == 2
        results.append(result[:, 1])
    return np.array(results)

def read_all_results(q_idx, runs, dirpath):
    all_ts = []
    for run in runs:
        run_dirpath = os.path.join(dirpath, f"run={run}")
        q_meta = read_meta(q_idx, run_dirpath)
        ts = np.array(q_meta['time_measures_ns']) / 1e9
        results = read_result(q_idx, run_dirpath, ts)
        print(f"Read results from run dirpath {run_dirpath}")
        all_ts.append(ts)
    all_ts = np.array(all_ts)
    return np.vstack([results, all_ts.mean(axis=0), all_ts.std(axis=0)]).T

def write_result(output_path, results, q_idx, latex_name):
    with open(output_path, "a") as f:
        if latex_name is not None:
            f.write("\t\\pgfplotstableread{\n")
            f.write(f"\t\t{write_columns_line}\n")
            for row in results:
                row_str = '\t'.join(str(elem) for elem in row)
                f.write(f"\t\t{row_str}\n")
            f.write(f"\t}}\\{latex_name}q{intToRoman(q_idx)}\n")
        else:
            f.write(f"{write_columns_line}\n")
            for row in results:
                row_str = '\t'.join(str(elem) for elem in row)
                f.write(f"{row_str}\n")
    print(f"Written {results.shape} to {output_path}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Extract accuracy from many result')
    parser.add_argument('dirpath', type=str,
                        help='path to directory with qxx subdirectories')
    parser.add_argument('run_l', type=int,
                        help='lower run ID to select [run_l, ..., run_r]')
    parser.add_argument('run_r', type=int,
                        help='higher run ID to select [run_l, ..., run_r]')
    parser.add_argument('q_idx', type=int,
                        help='query number to extract (1, 2, ..., 22)')
    parser.add_argument('output_path', type=str,
                        help='path to output file to append result')
    parser.add_argument('--latex_name', type=str, default=None,
                        help='if set, write output with latex syntax using this name')
    args = parser.parse_args()
    print(f"Args: {args}")
    dirpath = args.dirpath
    run_l = args.run_l
    run_r = args.run_r
    q_idx = args.q_idx
    output_path = args.output_path
    latex_name = args.latex_name

    # Extract accuracy result and summarize durations
    q_results = read_all_results(q_idx, range(run_l, run_r + 1), dirpath)

    # Write accuracy
    write_result(output_path, q_results, q_idx, latex_name)
