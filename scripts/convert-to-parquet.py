import os
import sys
import glob
import polars as pl
from concurrent.futures import ThreadPoolExecutor

# Column names of TPC-H Tables
tpch_headers = {
    "nation.tbl": ['n_nationkey','n_name','n_regionkey','n_comment'],
    "region.tbl": ['r_regionkey','r_name','r_comment'],
    "part.tbl": ['p_partkey','p_name','p_mfgr','p_brand','p_type','p_size','p_container','p_retailprice','p_comment'],
    "supplier.tbl": ['s_suppkey','s_name','s_address','s_nationkey','s_phone','s_acctbal','s_comment'],
    "partsupp.tbl": ['ps_partkey','ps_suppkey','ps_availqty','ps_supplycost','ps_comment'],
    "customer.tbl": ['c_custkey','c_name','c_address','c_nationkey','c_phone','c_acctbal','c_mktsegment','c_comment'],
    "orders.tbl": ['o_orderkey','o_custkey','o_orderstatus','o_totalprice','o_orderdate','o_orderpriority','o_clerk','o_shippriority','o_comment'],
    "lineitem.tbl": ['l_orderkey','l_partkey','l_suppkey','l_linenumber','l_quantity','l_extendedprice','l_discount','l_tax','l_returnflag','l_linestatus','l_shipdate','l_commitdate','l_receiptdate','l_shipinstruct','l_shipmode','l_comment'],
    "numbertable.tbl": ['ci', 'cii', 'ciii', 'ciiii', 'ciiiii', 'ciiiiii', 'ciiiiiii', 'ciiiiiiii', 'ciiiiiiiii', 'ciiiiiiiiii', 'x'],
}

# filename: %x.tbl.1 => %x.parquet.1
def convert_file_to_parquet(file_name):
    target_file_name = file_name.replace('tbl','parquet')
    if os.path.exists(target_file_name):
        return target_file_name

    # Check if the file-name has a table-name from one of the TPC-H tables.
    headers = []
    for table in tpch_headers:
        if table in file_name:
            headers = tpch_headers[table]
            break
    if len(headers) != 0:
        print(f"Found headers for {file_name}")
        df = pl.read_csv(file_name, has_header = False, sep = "|", parse_dates=True, low_memory=True, new_columns = tpch_headers[table])
    else:
        df = pl.read_csv(file_name, has_header = False, sep = "|", parse_dates=True, low_memory=True)
    df.write_parquet(target_file_name, statistics=True)
    return target_file_name


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 convert_to_parquet.py <input-dir>")
        exit(1)
    input_directory = sys.argv[1]

    # Verify input/output directories
    files = glob.glob(os.path.join(input_directory,"*.tbl*"))

    output_directory = input_directory.replace("tbl","parquet")
    if not os.path.exists(output_directory):
        os.makedirs(output_directory, exist_ok=True)

    # Convert each file to parquet
    print(f"Found {len(files)} files")
    for file_name in files:
        print(f"Converting {file_name}")
        convert_file_to_parquet(file_name)
