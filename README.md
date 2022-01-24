# Deep On-Line Aggregation
With DeepOLA, we intend to speed-up approximate (as well as actual) query computation when the available data is divided into various chunks that can be processed online and merged to obtain the complete result.


### Project Organization

```
Query - A basic Query class which takes a set of operations and provides a method to evaluate the query on the input data.
QueryOLA - An Online aggregation Query class that takes a set of operations and provide methods to online-evaluate the query result - taking as input the previous query result and the current data.
BaseOperation - A class representing the different relational operations that can be part of the query.
```

Corresponding to each operation, we define a separate class that defines `merge` and `evaluate` functions for that relational operator. The current defined operations are:
```
WHERE(left, op, right) => op in ['<','=','>']; left should be a column and right should be a value.
AGG(key, op, column) => op in ['COUNT', 'SUM'];
```

### Setup
- Create and virtualenv using `virtualenv -p python3 venv`.
- Install the requirements.txt using `pip install -r requirements.txt`.
- Install the `deepola` package using `python3 setup.py develop`
- For data-generation, go to `tpch-dbgen` and run the `make` command.
- Once `dbgen` is build in `tpch-dbgen`, run `./data-gen.sh` from the root directory. You can configure the scale parameter in the script. 

### TPC-H Dataset
- Refer to the Github Repo: [https://github.com/dragansah/tpch-dbgen](https://github.com/dragansah/tpch-dbgen)
- We generate data with multiple chunks (using -C \<num of chunks\>)

### Running PSQL in Docker Container
- Install docker and run a postgres container. Command:  `docker run --name deepola_psql -e POSTGRES_PASSWORD=<password> -p 5432:5432 -v ~/DeepOLA:/deepola -d postgres`
- Go to the docker container and create a database. `docker exec -it <container-id> bash`; `psql -U postgres`; `create database deepola`
- From the `tpch-dbgen` folder, specify the correct directory for data (wrt container and absolute path) and run `import_postgres.sh` file.
