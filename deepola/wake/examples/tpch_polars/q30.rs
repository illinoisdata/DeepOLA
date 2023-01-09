use crate::prelude::*;

/// Query Q1 of Query-Depth Experiment
// SELECT
//     R_NAME,
//     N_NAME,
//     L_SHIPMODE,
//     O_ORDERPRIORITY,
//     O_SHIPPRIORITY,
//     L_LINESTATUS,
//     L_RETURNFLAG,
//     AVG(L_QUANTITY),
// FROM
//     LINEITEM, SUPPLIER, NATION, REGION, ORDERS
// WHERE
//     L_SUPPKEY = S_SUPPKEY AND
//     L_ORDERKEY = O_ORDERKEY AND
//     S_NATIONKEY = N_NATIONKEY AND
//     N_REGIONKEY = R_REGIONKEY AND 
//     L_SHIPDATE >= '1996-01-01' AND 
//     L_SHIPDATE < '1997-01-01'
// GROUP BY
//     R_NAME,
//     N_NAME,
//     L_SHIPMODE,
//     O_ORDERPRIORITY,
//     O_SHIPPRIORITY,
//     L_LINESTATUS,
//     L_RETURNFLAG;

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Create a HashMap that stores table name and the columns in that query.
    let table_columns = HashMap::from([
        (
            "lineitem".into(),
            vec![
                "l_suppkey",
                "l_orderkey",
                "l_quantity",
                "l_returnflag",
                "l_linestatus",
                "l_shipmode",
                "l_shipdate",
            ],
        ),
        ("orders".into(),vec!["o_orderkey", "o_orderpriority", "o_shippriority"],),
        ("supplier".into(),vec!["s_suppkey", "s_nationkey"],),
        ("nation".into(), vec!["n_nationkey", "n_regionkey", "n_name"],),
        ("region".into(), vec!["r_regionkey", "r_name"],),
    ]);

    // CSVReaderNode would be created for this table.
    let lineitem_csvreader_node = build_reader_node("lineitem".into(), &tableinput, &table_columns);
    let orders_csvreader_node = build_reader_node("orders".into(), &tableinput, &table_columns);
    let nation_csvreader_node = build_reader_node("nation".into(), &tableinput, &table_columns);
    let region_csvreader_node = build_reader_node("region".into(), &tableinput, &table_columns);
    let supplier_csvreader_node = build_reader_node("supplier".into(), &tableinput, &table_columns);

    // WHERE Node
    let where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let var_date1 = days_since_epoch(1996,1,1);
            let var_date2 = days_since_epoch(1997,1,1);
            let a = df.column("l_shipdate").unwrap();
            let mask = a.gt_eq(var_date1).unwrap() & a.lt(var_date2).unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    // Hash Join Node
    let nr_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["n_regionkey".into()])
        .right_on(vec!["r_regionkey".into()])
        .build();

    let sn_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["s_nationkey".into()])
        .right_on(vec!["n_nationkey".into()])
        .build();

    let ls_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["l_suppkey".into()])
        .right_on(vec!["s_suppkey".into()])
        .build();

    // Merge-Join Node
    let mut merger = SortedDfMerger::new();
    merger.set_left_on(vec!["l_orderkey".into()]);
    merger.set_right_on(vec!["o_orderkey".into()]);
    let lo_merge_join_node = MergerNode::<DataFrame, SortedDfMerger>::new()
        .merger(merger)
        .build();

    // GROUP BY Aggregate Node
    let mut sum_accumulator = AggAccumulator::new();
    sum_accumulator
        .set_group_key(vec![
            "r_name".to_string(), 
            "n_name".to_string(),
            "l_shipmode".to_string(),
            "o_orderpriority".to_string(),
            "o_shippriority".to_string(),
            "l_linestatus".to_string(),
            "l_returnflag".to_string(),
            ])
        .set_add_count_column(true)
        .set_aggregates(vec![
            ("l_quantity".into(), vec!["sum".into(), "count".into()]),
        ])
        .set_scaler(AggregateScaler::new_growing()
            .scale_sum("l_quantity_sum".into())
            .scale_count("l_quantity_count".into())
            .into_rc()
        );

    let groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();

    // SELECT Node
    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            // Compute AVG from SUM/COUNT.
            let columns = vec![
                Series::new("r_name", df.column("r_name").unwrap()),
                Series::new("n_name", df.column("n_name").unwrap()),
                Series::new("l_shipmode", df.column("l_shipmode").unwrap()),
                Series::new("o_orderpriority", df.column("o_orderpriority").unwrap()),
                Series::new("o_shippriority", df.column("o_shippriority").unwrap()),
                Series::new("l_linestatus", df.column("l_linestatus").unwrap()),
                Series::new("l_returnflag", df.column("l_returnflag").unwrap()),
                Series::new(
                    "l_quantity_avg",
                    (df.column("l_quantity_sum")
                        .unwrap()
                        .cast(&polars::datatypes::DataType::Float64)
                        .unwrap())
                        / (df
                            .column("l_quantity_count")
                            .unwrap()
                            .cast(&polars::datatypes::DataType::Float64)
                            .unwrap()),
                ),
            ];
            DataFrame::new(columns)
                .unwrap()
                .sort(&["l_returnflag", "l_linestatus"], vec![false, false])
                .unwrap()
        })))
        .build();

    // Connect nodes with subscription
    where_node.subscribe_to_node(&lineitem_csvreader_node, 0);
    nr_hash_join_node.subscribe_to_node(&nation_csvreader_node,0);
    nr_hash_join_node.subscribe_to_node(&region_csvreader_node,1);
    sn_hash_join_node.subscribe_to_node(&supplier_csvreader_node,0);
    sn_hash_join_node.subscribe_to_node(&nr_hash_join_node,1);
    ls_hash_join_node.subscribe_to_node(&where_node, 0);
    ls_hash_join_node.subscribe_to_node(&sn_hash_join_node,1);

    lo_merge_join_node.subscribe_to_node(&ls_hash_join_node, 0);
    lo_merge_join_node.subscribe_to_node(&orders_csvreader_node, 1);
    
    groupby_node.subscribe_to_node(&lo_merge_join_node, 0);
    select_node.subscribe_to_node(&groupby_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(select_node);
    service.add(groupby_node);
    service.add(lo_merge_join_node);
    service.add(ls_hash_join_node);
    service.add(sn_hash_join_node);
    service.add(nr_hash_join_node);
    service.add(where_node);
    service.add(nation_csvreader_node);
    service.add(region_csvreader_node);
    service.add(supplier_csvreader_node);
    service.add(orders_csvreader_node);
    service.add(lineitem_csvreader_node);
    service
}
