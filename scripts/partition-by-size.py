import os
import sys

tables = {
    "nation": 25,
    "region": 5,
    "supplier": 10000,
    "customer": 150000,
    "part": 200000,
    "partsupp": 800000,
    "orders": 1500000,
    "lineitem" : 6000000,
}

def break_file_into_fixed_size(file_size, input_directory, output_directory):
    if not os.path.exists(output_directory):
        print("Making Directory", output_directory)
        os.makedirs(output_directory, exist_ok=True)
    
    for table in tables.keys():
        print(f"Partitioning {table}")
        command = [
            "split",
            f"-C {file_size}",
            "--suffix-length=3",
            "--numeric-suffixes=1",
            os.path.join(input_directory,f"{table}.tbl"),
            os.path.join(output_directory,f"{table}.tbl.")
        ]
        os.system(" ".join(command))

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 partition.py <size per file ex: 128M> <input-directory> <output-directory>")
        exit(1)
    break_file_into_fixed_size(*sys.argv[1:])
