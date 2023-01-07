import glob

DATA_BASE_DIR = '/data/DeepOLA/resources/tpc-h/data/scale=100/partition=512M/tbl'
TABLES = ["REGION", "NATION", "CUSTOMER", "PART", "SUPPLIER", "PARTSUPP", "ORDERS", "LINEITEM"]
for tbl in TABLES:
    tbl_lower = tbl.lower()
    files = sorted(glob.glob(f"{DATA_BASE_DIR}/{tbl_lower}.tbl.*"))
    for file in files:
        cmd = f"vwload -t {tbl_lower} tpchs100 {file}"
        print(cmd)
