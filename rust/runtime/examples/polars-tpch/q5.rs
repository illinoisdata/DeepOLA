use crate::utils::*;
use std::collections::HashMap;
use polars::prelude::*;
use polars_lazy::prelude::*;

// select
// 	n_name,
// 	sum(l_extendedprice * (1 - l_discount)) as revenue
// from
// 	customer,
// 	orders,
// 	lineitem,
// 	supplier,
// 	nation,
// 	region
// where
// 	c_custkey = o_custkey
// 	and l_orderkey = o_orderkey
// 	and l_suppkey = s_suppkey
// 	and c_nationkey = s_nationkey
// 	and s_nationkey = n_nationkey
// 	and n_regionkey = r_regionkey
// 	and r_name = 'ASIA'
// 	and o_orderdate >= date '1994-01-01'
// 	and o_orderdate < date '1994-01-01' + interval '1' year
// group by
// 	n_name
// order by
// 	revenue desc;

pub fn read_polars_df(tablename: &str, tableinput: &HashMap<String, TableInput>, table_columns: &HashMap<String,Vec<&str>>) -> polars_lazy::frame::LazyFrame {
    let input_files = tableinput.get(tablename).unwrap().input_files.clone();
    let schema_cols = tpch_schema(tablename).unwrap().columns.iter().map(|x| x.name.clone()).collect::<Vec<String>>();
    let mut df = polars::prelude::CsvReader::from_path(input_files[0].clone()).unwrap()
    .has_header(false)
    .with_delimiter(b'|')
    .finish().unwrap();
    df.set_column_names(&schema_cols).unwrap();
    // Project columns
    df.lazy().select(&table_columns.get(tablename).unwrap().iter().map(|x| col(x)).collect::<Vec<Expr>>())
}

pub fn query(tableinput: HashMap<String, TableInput>) {
    // Create a HashMap that stores table name and the columns in that query.
    let table_columns = HashMap::from([
        ("lineitem".into(), vec!["l_orderkey","l_suppkey","l_extendedprice","l_discount"]),
        ("orders".into(), vec!["o_orderkey","o_custkey","o_orderdate"]),
        ("customer".into(), vec!["c_custkey","c_nationkey"]),
        ("supplier".into(), vec!["s_suppkey","s_nationkey"]),
        ("nation".into(), vec!["n_nationkey","n_name","n_regionkey"]),
        ("region".into(), vec!["r_regionkey","r_name"]),
    ]);

    let region_df = read_polars_df("region", &tableinput, &table_columns).filter(col("r_name").eq(lit("ASIA")));
    let nation_df = read_polars_df("nation", &tableinput, &table_columns);
    let nation_region_df = region_df.inner_join(nation_df, col("r_regionkey"), col("n_regionkey"));

    let supplier_df = read_polars_df("supplier", &tableinput, &table_columns);
    let supplier_nation_df = nation_region_df.inner_join(supplier_df, col("n_nationkey"), col("s_nationkey"));

    let lineitem_df = read_polars_df("lineitem", &tableinput, &table_columns);
    let lineitem_supplier_df = lineitem_df.inner_join(supplier_nation_df, col("l_suppkey"), col("s_suppkey"));

    let orders_df = read_polars_df("orders", &tableinput, &table_columns).filter(col("o_orderdate").gt_eq(lit("1994-01-01"))).filter(col("o_orderdate").lt(lit("1995-01-01")));
    let lineitem_orders_df = lineitem_supplier_df.inner_join(orders_df, col("l_orderkey"), col("o_orderkey"));

    let customer_df = read_polars_df("customer", &tableinput, &table_columns);
    let orders_customer_df = lineitem_orders_df.inner_join(customer_df, col("o_custkey"), col("c_custkey"));

    let result_df = orders_customer_df
        .groupby([col("n_name")])
    //     // Perform Aggregates
        .agg([
    //         col("l_quantity").sum().alias("sum_qty"),
    //         col("l_extendedprice").sum().alias("sum_base_price"),
            (col("l_extendedprice") * (lit(1) - col("l_discount"))).sum().alias("revenue"),
    //         (col("l_extendedprice") * (lit(1) - col("l_discount")) * (lit(1) + col("l_tax"))).sum().alias("sum_charge"),
    //         col("l_quantity").mean().alias("avg_qty"),
    //         col("l_extendedprice").mean().alias("avg_extendedprice"),
    //         col("l_discount").mean().alias("avg_disc"),
    //         col("l_linestatus").count().alias("count_order")
        ])
    //     // ORDER BY
        .sort_by_exprs(vec![col("revenue")], vec![false]);

    println!("Result: {:?}", result_df.collect());
}