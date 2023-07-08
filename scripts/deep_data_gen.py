"""
Generate data for deep group-by query

Example: python ../scripts/deep_data_gen.py 10 1000000 100 5 temp_dir
 --> 10 group-by columns
 --> 1M rows per partitions, 100 partitions
 --> [0, 5] integers
"""

import numpy as np

def generate_partition(rng, num_rows, num_cols, column_cardinality):
    return rng.integers(column_cardinality, size=(num_rows, num_cols))

if __name__ == '__main__':
    import argparse
    import os
    parser = argparse.ArgumentParser(description='Generate deep query')
    parser.add_argument('num_gb_columns', type=int,
                        help='Number of group-by columns (max query depth)')
    parser.add_argument('partition_size', type=int,
                        help='Number of rows per partition (in millions)')
    parser.add_argument('num_partitions', type=int,
                        help='Number of partition')
    parser.add_argument('column_cardinality', type=int,
                        help='Number of distinct elements per group-by column')
    parser.add_argument('root_dir', type=str,
                        help='Root directory to write partitions into')
    parser.add_argument('--seed', type=int, default=4512763,
                        help='Randomization seed')
    args = parser.parse_args()

    # Fill in args
    num_gb_columns = args.num_gb_columns
    partition_size = args.partition_size
    num_partitions = args.num_partitions
    column_cardinality = args.column_cardinality
    root_dir = args.root_dir
    seed = args.seed

    # Create RNG
    rng = np.random.default_rng(seed)

    # Prepare directory
    if not os.path.exists(root_dir):
        os.makedirs(root_dir)
    print(f"Writing to {root_dir}")

    # Generate data
    for pidx in range(1, num_partitions + 1):
        partition_path = os.path.join(root_dir, f"numbertable.tbl.{pidx}")
        partition = generate_partition(rng, partition_size, num_gb_columns + 1, column_cardinality)
        np.savetxt(partition_path, partition, fmt="%d", delimiter='|')
        print(f"Wrote {partition_path}")
    column_names = ["c" + "i" * idx for idx in range(num_gb_columns)] + ["x"]
    print(f"Column names: {column_names}")
