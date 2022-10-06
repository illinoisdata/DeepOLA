import sys

tables = ["customer", "nation", "lineitem", "orders", "part", "partsupp", "region", "supplier"]

scale = 100
partition = '512M'
num_runs = 10
schema = f"tpchs{scale}"
file_format = "parquet"
ddl_query_file = f"row-ddl-{file_format}.sql"
data_base_dir = f"/mnt/DeepOLA/resources/tpc-h/data/scale={scale}/partition={partition}/{file_format}"
hdfs_dir_path = f"/tpch-data/scale={scale}"

def create_hdfs_dir():
    command = f"hdfs dfs -mkdir -p {hdfs_dir_path}"
    return [command]

def load_hdfs_data():
    commands = []
    for table in tables:
        command = f"hdfs dfs -put {data_base_dir}/{table}.{file_format}* /tpch-data/scale={scale}"
        commands.append(command)
    return commands

def create_hive_schema():
    commands = []
    commands.append(f"hive -e 'DROP DATABASE IF EXISTS {schema} CASCADE'")
    commands.append(f"hive -e 'CREATE SCHEMA {schema}'")
    return commands

def create_hive_tables():
    query_file = open(ddl_query_file,"r").read()
    command = f'hive -e "{query_file}"'
    return [command]

def load_hive_data():
    commands = []
    for table in tables:
        command = f"hive -e 'LOAD DATA INPATH \"/tpch-data/scale={scale}/{table}.{file_format}*\" INTO TABLE {schema}.{table}'"
        commands.append(command)
    return commands

def set_trace():
    return ["set -o xtrace"]

def set_schema():
    return [f"schema={schema}"]

def run_presto_queries():
    command = f"./presto-script.sh {scale} {num_runs} > logs/scale={scale}.log 2>&1"
    return [command]

if __name__ == "__main__":
    commands = []
    commands.extend(set_trace())
    commands.extend(set_schema())
    commands.extend(create_hdfs_dir())
    commands.extend(load_hdfs_data())
    commands.extend(create_hive_schema())
    commands.extend(create_hive_tables())
    commands.extend(load_hive_data())
    commands.extend(run_presto_queries())
    print("\n".join(commands))

