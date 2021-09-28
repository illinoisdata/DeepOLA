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

### TPC-H Dataset
- Refer to the Github Repo: [https://github.com/gregrahn/tpch-kit/](https://github.com/gregrahn/tpch-kit/)
- We generate data with multiple chunks (using -C \<num of chunks\>)