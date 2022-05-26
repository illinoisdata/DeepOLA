use crate::utils::*;
use std::collections::HashMap;
use polars::prelude::*;
use polars_lazy::prelude::*;

/// This node implements the following SQL query
// select
// 	l_returnflag,
// 	l_linestatus,
// 	sum(l_quantity) as sum_qty,
// 	sum(l_extendedprice) as sum_base_price,
// 	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
// 	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
// 	avg(l_quantity) as avg_qty,
// 	avg(l_extendedprice) as avg_price,
// 	avg(l_discount) as avg_disc,
// 	count(*) as count_order
// from
// 	lineitem
// where
// 	l_shipdate <= date '1998-12-01' - interval '90' day
// group by
// 	l_returnflag,
// 	l_linestatus
// order by
// 	l_returnflag,
// 	l_linestatus;
// limit -1;

pub fn query(tableinput: HashMap<String, TableInput>) {
    // Create a HashMap that stores table name and the columns in that query.
    let table_columns = HashMap::<String,Vec<&str>>::from([
        (
            "lineitem".into(),
            vec!["l_quantity","l_extendedprice","l_discount","l_tax",
            "l_returnflag", "l_linestatus","l_shipdate"]
        ),
    ]);

    let input_files = tableinput.get("lineitem").unwrap().input_files.clone();
    let schema_cols = tpch_schema("lineitem").unwrap().columns.iter().map(|x| x.name.clone()).collect::<Vec<String>>();
    for file in input_files {
        let mut df = polars::prelude::CsvReader::from_path(file).unwrap()
        .has_header(false)
        .with_delimiter(b'|')
        .finish().unwrap();
        df.set_column_names(&schema_cols).unwrap();

        let lazy_df = df.lazy()
        // Project columns
        .select(&table_columns.get("lineitem").unwrap().iter().map(|x| col(x)).collect::<Vec<Expr>>())
        // WHERE condition
        .filter(col("l_shipdate").lt_eq(lit("1998-09-01")))
        // GROUP BY
        .groupby([col("l_returnflag"),col("l_linestatus")])
        // Perform Aggregates
        .agg([
            col("l_quantity").sum().alias("sum_qty"),
            col("l_extendedprice").sum().alias("sum_base_price"),
            (col("l_extendedprice") * (lit(1) - col("l_discount"))).sum().alias("sum_disc_price"),
            (col("l_extendedprice") * (lit(1) - col("l_discount")) * (lit(1) + col("l_tax"))).sum().alias("sum_charge"),
            col("l_quantity").mean().alias("avg_qty"),
            col("l_extendedprice").mean().alias("avg_extendedprice"),
            col("l_discount").mean().alias("avg_disc"),
            col("l_linestatus").count().alias("count_order")
        ])
        // ORDER BY
        .sort_by_exprs(vec![col("l_returnflag"),col("l_linestatus")], vec![false,false]);

        println!("Result: {:?}", lazy_df.collect());
    }
}