import os
import sys
import json
import polars as pl

def get_baseline_df(scale, variation, qdx):
    answers_file = f"/data/DeepOLA/baselines/postgres/scale={scale}/variation={variation}/{qdx}.csv"
    print(f">>> {qdx}, using baseline {answers_file}")
    return pl.read_csv(answers_file)

def get_obtained_df(scale, results_dir, qdx):
    query_results_dir = os.path.join(results_dir, f"q{qdx}")
    results_metadata = os.path.join(query_results_dir, "meta.json")
    meta_data = json.loads(open(results_metadata,'r').read())
    num_results = len(meta_data["time_measures_ns"])
    last_result_file = f"{query_results_dir}/final.csv"
    print(f">>> {qdx}, using obtained {last_result_file}")
    return pl.read_csv(last_result_file)

if __name__ == "__main__":
    if len(sys.argv) <= 2:
        print("Usage: python3 test-correctness.py <scale> <output-base-dir: path to directory containing qX folders> <optional: variation>")
        exit(1)

    scale = sys.argv[1]
    results_dir = sys.argv[2]
    variation = sys.argv[3] if len(sys.argv) >=4 else "data"

    incorrect_queries = []
    baseline_not_available = []
    results_not_available = []
    for qdx in range(1,26):
        try:
            obtained_df = get_obtained_df(scale, results_dir, qdx)
        except:
            results_not_available.append(qdx)
            continue

        try:
            baseline_df = get_baseline_df(scale, variation, qdx)
        except:
            baseline_not_available.append(qdx)
            continue

        # To avoid failure due to incorrect column names
        columns = baseline_df.columns
        baseline_df.columns = columns
        obtained_df.columns = columns

        # Sort the dataframe: When order-by is defined on partial keys,
        # the order of rows when these keys match is non-deterministic.
        # Impacted Queries: Q3, Q10
        obtained_df = obtained_df.sort(obtained_df.columns)
        baseline_df = baseline_df.sort(baseline_df.columns)

        # Strip the string values in the dataframe
        obtained_df = obtained_df.with_columns([pl.col(pl.datatypes.Utf8).str.strip().keep_name()])
        baseline_df = baseline_df.with_columns([pl.col(pl.datatypes.Utf8).str.strip().keep_name()])

        # Compare the two dataframes
        print(f"Comparing {qdx}")
        try:
            result = pl.testing.assert_frame_equal(baseline_df, obtained_df, check_dtype=False)
            print(f"Correct Output on {qdx}")
        except Exception as e:
            print(e)
            print(f"Incorrect Output on {qdx}")
            incorrect_queries.append(qdx)

    print(f"Incorrect Queries: {incorrect_queries}")
    print(f"Baseline not available: {baseline_not_available}")
    print(f"Result not available: {results_not_available}")