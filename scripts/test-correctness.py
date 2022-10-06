import os
import sys
import json
import pandas as pd

def compare_dataframe(left_df_path, right_df_path):
    print(left_df_path)
    print(right_df_path)
    if os.path.exists(left_df_path):
        left_df = pd.read_csv(left_df_path, skiprows = 1, header = None).round(1)
    else:
        print("Invalid left df path")
        exit(1)
    if os.path.exists(right_df_path):
        right_df = pd.read_csv(right_df_path, skiprows = 1, header = None).round(1)
        right_df_str = right_df.select_dtypes(['object'])
        right_df[right_df_str.columns] = right_df_str.apply(lambda x: x.str.strip())
    else:
        print("Invalid right df path")
        exit(1)
    #print(left_df.eq(right_df).all())
    comparison = left_df.equals(right_df)
    print(comparison)

if __name__ == "__main__":
    scale = sys.argv[1]
    q = int(sys.argv[2])
    for query_no in range(1,23):
        try:
            meta_data = json.loads(open(f"../deepola/wake/outputs/q{query_no}/meta.json","r").read())
            num_outputs = len(meta_data["time_measures_ns"]) - 1
            output_df = f"../deepola/wake/outputs/q{query_no}/{num_outputs}.csv"
            expected_df = f"/mnt/DeepOLA/resources/tpc-h/data/scale={scale}/original/{query_no}.csv"
            compare_dataframe(output_df, expected_df)
        except Exception as e:
            print(e)
            print(query_no)
            continue
