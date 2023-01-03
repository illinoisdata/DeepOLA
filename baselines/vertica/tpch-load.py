DATA_BASE_DIR = '/home/dbadmin/data/DeepOLA/resources/tpc-h/data/scale=100/partition=512M/tbl'
TABLES = ["REGION", "NATION", "CUSTOMER", "PART", "SUPPLIER", "PARTSUPP", "ORDERS", "LINEITEM"]
for tbl in TABLES:
    tbl_lower = tbl.lower()
    cmd = f"COPY {tbl} FROM '{DATA_BASE_DIR}/{tbl_lower}.tbl.*' DELIMITER '|' NULL ''"
    print(cmd)