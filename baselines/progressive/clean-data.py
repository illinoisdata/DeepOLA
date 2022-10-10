import os
import sys
import glob
import polars as pl
from tqdm import tqdm

lineitem_cols = ['l_orderkey','l_partkey','l_suppkey','l_linenumber','l_quantity','l_extendedprice','l_discount','l_tax','l_returnflag','l_linestatus','l_shipdate','l_commitdate','l_receiptdate','l_shipinstruct','l_shipmode','l_comment']

def clean_dataframe(input_file, output_file, variation = 'data'):
    df = pl.read_csv(input_file, sep = '|', has_header = False, new_columns = lineitem_cols)
    df = df[[s.name for s in df if not (s.null_count() == df.height)]]
    df = df.with_columns([(df["l_discount"] * 100).cast(pl.Int64), (df["l_tax"] * 100).cast(pl.Int64), df["l_extendedprice"].round(0).cast(pl.Int64)])
    df.write_csv(output_file, has_header = False)


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python3 clean-data.py <scale> <partition> <variation>")
        exit(1)
    scale = sys.argv[1]
    partition = sys.argv[2]
    variation = sys.argv[3]
    data_base_directory = f"/data/DeepOLA/resources/tpc-h/{variation}/scale={scale}/partition={partition}/tbl"
    data_output_directory = f"/mnt/DeepOLA/resources/tpc-h/{variation}/scale={scale}/partition={partition}/cleaned-tbl"
    if not os.path.exists(data_output_directory):
        os.makedirs(data_output_directory)
    input_files = glob.glob(f"{data_base_directory}/lineitem.tbl*")
    input_files.sort()
    for input_file in tqdm(input_files):
        output_file = f"{data_output_directory}/{input_file.split('/')[-1]}"
        clean_dataframe(input_file, output_file)
