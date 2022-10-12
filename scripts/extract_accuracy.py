import os
import glob
import re
import json

import numpy as np
import pandas as pd

from scipy.special import gamma


# Groupby columns for each query
on_dict = {
    1: ["l_returnflag", "l_linestatus"],
    2: ["s_acctbal", "s_name", "n_name", "ps_partkey", "p_mfgr", "s_address", "s_phone", "s_comment"],    # no aggregate
    3: ["l_orderkey", "o_orderdate", "o_shippriority"],
    4: ["o_orderpriority"],
    5: ["n_name"],
    6: [],
    7: ["supp_nation", "cust_nation", "l_year"],
    8: ["o_year"],
    9: ["nation", "o_year"],
    10: ["o_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment"],
    11: ["ps_partkey"],
    12: ["l_shipmode"],
    13: ["o_orderkey_unit_sum"],
    14: [],
    15: ["l_suppkey", "s_name", "s_address", "s_phone"],
    16: ["p_brand", "p_type", "p_size"],
    17: [],
    18: ["c_name", "o_custkey", "l_orderkey", "o_orderdate", "o_totalprice"],
    19: [],
    20: ["s_name", "s_address"],  # no aggregate
    21: ["s_name"],
    22: ["cntrycode"],
}

def read_dir(q_idx, dirpath):
    assert isinstance(q_idx, int)
    return f"{dirpath}/q{q_idx}"

def read_meta(q_idx, dirpath):
    with open(f"{read_dir(q_idx, dirpath=dirpath)}/meta.json", "r") as f:
        return json.loads(f.readline())

def read_result(q_idx, t, dirpath):
    assert isinstance(t, int)
    return pd.read_csv(f"{read_dir(q_idx, dirpath=dirpath)}/{t}.csv")

def write_result(q_idx, dirpath, results, ts):
    # Write in JSON
    json_path = f"{read_dir(q_idx, dirpath=dirpath)}/results.json"
    with open(json_path, "w") as f:
        json.dump(results, f)
    print(f"Written {json_path}")

    # Write in text table
    for key, values in results.items():
        txt_path = f"{read_dir(q_idx, dirpath=dirpath)}/{key}.txt"
        with open(txt_path, "w") as f:
            f.write("time\tvalue\n")
            for t, v in zip(ts, values):
                f.write(f"{t}\t{v}\n")
        print(f"Written {txt_path}")


def read_all_results(q_idx, dirpath, nt=None):
    if nt is None:
        # Read all CSVs
        nt = max(map(
            lambda path: int(re.search("\/(\d+)\.csv", path).group(1)),
            glob.glob(f"{read_dir(q_idx, dirpath=dirpath)}/*.csv")
        )) + 1
        print(f"Detected {nt} CSVs")
    return [read_result(q_idx, t, dirpath=dirpath) for t in range(nt)]

def calculate_pes(df, df_ref, on):
    # Percentage error on inner-join of df onto df_ref
    if len(on) == 0:
        inv_on = df.columns
        pes = np.concatenate([
            ((df[col] - df_ref[col]) / df_ref[col]).values
            for col in inv_on
        ])
    else:
        inv_on = df.columns.difference(on)
        if len(inv_on) == 0:
            return 0
        df_join = df.merge(df_ref, on=on, how="inner", suffixes=('', '_ref'))
        pes = np.concatenate([
            ((df_join[col] - df_join[f"{col}_ref"]) / df_join[f"{col}_ref"]).values
            for col in inv_on
        ])
    return pes

def calculate_mape(df, df_ref, on):
    # Mean absolute percentage error on inner-join of df onto df_ref
    pes = calculate_pes(df, df_ref, on)
    if isinstance(pes, np.ndarray):
        return 100 * (abs(pes)).mean()
    return pes

def calculate_es(df, df_ref, on):
    # Error on inner-join of df onto df_ref
    if len(on) == 0:
        inv_on = df.columns
        es = np.concatenate([
            (df[col] - df_ref[col]).values
            for col in inv_on
        ])
    else:
        inv_on = df.columns.difference(on)
        if len(inv_on) == 0:
            return np.array([0])
        df_join = df.merge(df_ref, on=on, how="inner", suffixes=('', '_ref'))
        es = np.concatenate([
            (df_join[col] - df_join[f"{col}_ref"]).values
            for col in inv_on
        ])
    return es

def calculate_mae(df, df_ref, on):
    es = calculate_es(df, df_ref, on)
    if isinstance(es, np.ndarray):
        return 100 * (abs(es)).mean()
    return es

def calculate_recall(df, df_ref, on):
    # Count number of missing rows in df onto df_ref
    if len(on) == 0:
        return df_ref.shape[0] - df.shape[0]
    expect = df_ref.shape[0]
    recall_correct = df.merge(df_ref, on=on, how="inner").shape[0]
    return 100 * recall_correct / expect

def calculate_precision(df, df_ref, on):
    # Count number of excess rows in df onto df_ref
    if len(on) == 0:
        return df.shape[0] - df_ref.shape[0]
    recall_all = df.shape[0]
    recall_correct = df.merge(df_ref, on=on, how="inner").shape[0]
    return 100 if recall_all == 0 else 100 * recall_correct / recall_all

def calculate_accuracy_all(q_results, on, q_ref=None):
    if q_ref is None:
        # Use last result as the reference.
        q_ref = q_results[-1]
    return {
        'mape_p': [calculate_mape(q_result, q_ref, on) for q_result in q_results],
        'mae': [calculate_mae(q_result, q_ref, on) for q_result in q_results],
        'recall_p': [calculate_recall(q_result, q_ref, on) for q_result in q_results],
        'precision_p': [calculate_precision(q_result, q_ref, on) for q_result in q_results],
    }

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Extract accuracy from many result')
    parser.add_argument('dirpath', type=str,
                        help='path to directory with qxx subdirectories')
    parser.add_argument('q_idx', type=int,
                        help='query number to extract (1, 2, ..., 22)')
    args = parser.parse_args()
    print(f"Args: {args}")
    print(f"WARNING: using last result as the true answer")  # TODO: use manually selected CSV

    # Extract result and accuracy
    dirpath = args.dirpath
    q_idx = args.q_idx
    q_results = read_all_results(q_idx, dirpath)
    q_meta = read_meta(q_idx, dirpath)
    q_accuracy = calculate_accuracy_all(q_results, on=on_dict[q_idx])
    ts = np.array(q_meta['time_measures_ns']) / 1e9

    # Write accuracy
    write_result(q_idx, dirpath, q_accuracy, ts)