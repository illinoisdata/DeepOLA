use crate::prelude::*;

/// This node implements the following SQL query
// select
// 	o_year,
// 	sum(case
// 		when nation = 'BRAZIL' then volume
// 		else 0
// 	end) / sum(volume) as mkt_share
// from
// 	(
// 		select
// 			extract(year from o_orderdate) as o_year,
// 			l_extendedprice * (1 - l_discount) as volume,
// 			n2.n_name as nation
// 		from
// 			part,
// 			supplier,
// 			lineitem,
// 			orders,
// 			customer,
// 			nation n1,
// 			nation n2,
// 			region
// 		where
// 			p_partkey = l_partkey
// 			and s_suppkey = l_suppkey
// 			and l_orderkey = o_orderkey
// 			and o_custkey = c_custkey
// 			and c_nationkey = n1.n_nationkey
// 			and n1.n_regionkey = r_regionkey
// 			and r_name = 'AMERICA'
// 			and s_nationkey = n2.n_nationkey
// 			and o_orderdate between date '1995-01-01' and date '1996-12-31'
// 			and p_type = 'ECONOMY ANODIZED STEEL'
// 	) as all_nations
// group by
// 	o_year
// order by
// 	o_year;

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Table Name => Columns to Read.
    let table_columns = HashMap::from([
        (
            "nation".into(),
            vec!["n_nationkey", "n_regionkey", "n_name"],
        ),
        ("region".into(), vec!["r_regionkey", "r_name"]),
        ("customer".into(), vec!["c_custkey", "c_nationkey"]),
        ("part".into(), vec!["p_partkey", "p_type"]),
        ("supplier".into(), vec!["s_suppkey", "s_nationkey"]),
        (
            "orders".into(),
            vec!["o_orderkey", "o_custkey", "o_orderdate"],
        ),
        (
            "lineitem".into(),
            vec![
                "l_orderkey",
                "l_suppkey",
                "l_partkey",
                "l_extendedprice",
                "l_discount",
            ],
        ),
    ]);

    // CSV Reader Nodes.
    let nation_csvreader_node = build_reader_node("nation".into(), &tableinput, &table_columns);
    let region_csvreader_node = build_reader_node("region".into(), &tableinput, &table_columns);
    let customer_csvreader_node = build_reader_node("customer".into(), &tableinput, &table_columns);
    let part_csvreader_node = build_reader_node("part".into(), &tableinput, &table_columns);
    let supplier_csvreader_node = build_reader_node("supplier".into(), &tableinput, &table_columns);
    let orders_csvreader_node = build_reader_node("orders".into(), &tableinput, &table_columns);
    let lineitem_csvreader_node = build_reader_node("lineitem".into(), &tableinput, &table_columns);

    // WHERE Nodes
    let region_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let r_name = df.column("r_name").unwrap();
            let mask = r_name.equal("AMERICA").unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    let part_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let p_type = df.column("p_type").unwrap();
            let mask = p_type.equal("ECONOMY ANODIZED STEEL").unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    let orders_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let o_orderdate = df.column("o_orderdate").unwrap();
            let mask =
                o_orderdate.gt_eq("1995-01-01").unwrap() & o_orderdate.lt_eq("1996-12-31").unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    // After this rename, n_name to supp_nation
    let nr_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["n_regionkey".into()])
        .right_on(vec!["r_regionkey".into()])
        .build();

    let cn_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["c_nationkey".into()])
        .right_on(vec!["n_nationkey".into()])
        .build();

    let oc_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["o_custkey".into()])
        .right_on(vec!["c_custkey".into()])
        .build();

    let sn_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["s_nationkey".into()])
        .right_on(vec!["n_nationkey".into()])
        .build();
    let sn_expression_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let cols = vec![
                Series::new("s_suppkey", df.column("s_suppkey").unwrap()),
                Series::new("nation", df.column("n_name").unwrap()),
            ];
            DataFrame::new(cols).unwrap()
        })))
        .build();

    let ls_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["l_suppkey".into()])
        .right_on(vec!["s_suppkey".into()])
        .build();

    let lp_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["l_partkey".into()])
        .right_on(vec!["p_partkey".into()])
        .build();

    let mut merger = SortedDfMerger::new();
    merger.set_left_on(vec!["l_orderkey".into()]);
    merger.set_right_on(vec!["o_orderkey".into()]);
    let lo_merge_join_node = MergerNode::<DataFrame, SortedDfMerger>::new()
        .merger(merger)
        .build();

    // Expression Node.
    let expression_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let nation = df.column("nation").unwrap();
            let extended_price = df.column("l_extendedprice").unwrap();
            let discount = df.column("l_discount").unwrap();
            let volume = Series::new(
                "volume",
                extended_price
                    .cast(&polars::datatypes::DataType::Float64)
                    .unwrap()
                    * (discount * -1f64 + 1f64),
            );
            let mask = nation
                .equal("BRAZIL")
                .unwrap()
                .cast(&polars::datatypes::DataType::UInt32)
                .unwrap();
            let masked_volume = Series::new("masked_volume", volume.clone() * mask);
            let o_orderdate = df.column("o_orderdate").unwrap();
            let o_year = Series::new(
                "o_year",
                o_orderdate
                    .utf8()
                    .unwrap()
                    .as_date(Some("%Y-%m-%d"))
                    .unwrap()
                    .strftime("%Y")
                    .into_series(),
            );
            DataFrame::new(vec![o_year, volume, masked_volume]).unwrap()
        })))
        .build();

    // GROUP BY Node.
    let mut agg_accumulator = AggAccumulator::new();
    agg_accumulator
        .set_group_key(vec!["o_year".into()])
        .set_aggregates(vec![
            ("volume".into(), vec!["sum".into()]),
            ("masked_volume".into(), vec!["sum".into()]),
        ])
        .set_add_count_column(true);
    let groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(agg_accumulator)
        .build();
    let scaler_node = AggregateScaler::new_growing()
        .remove_count_column()  // Remove added group count column
        .scale_sum("volume_sum".into())
        .scale_sum("masked_volume_sum".into())
        .into_node();

    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            // Select row and divide to get mkt_share
            let o_year = df.column("o_year").unwrap().clone();
            let mkt_share = Series::new(
                "mkt_share",
                df.column("masked_volume_sum").unwrap() / df.column("volume_sum").unwrap(),
            );
            DataFrame::new(vec![o_year, mkt_share])
                .unwrap()
                .sort(vec!["o_year"], vec![false])
                .unwrap()
        })))
        .build();

    // Connect nodes with subscription
    region_where_node.subscribe_to_node(&region_csvreader_node, 0);
    orders_where_node.subscribe_to_node(&orders_csvreader_node, 0);
    part_where_node.subscribe_to_node(&part_csvreader_node, 0);

    nr_hash_join_node.subscribe_to_node(&nation_csvreader_node, 0);
    nr_hash_join_node.subscribe_to_node(&region_where_node, 1);

    cn_hash_join_node.subscribe_to_node(&customer_csvreader_node, 0);
    cn_hash_join_node.subscribe_to_node(&nr_hash_join_node, 1);

    oc_hash_join_node.subscribe_to_node(&orders_where_node, 0);
    oc_hash_join_node.subscribe_to_node(&cn_hash_join_node, 1);

    sn_hash_join_node.subscribe_to_node(&supplier_csvreader_node, 0);
    sn_hash_join_node.subscribe_to_node(&nation_csvreader_node, 1);

    sn_expression_node.subscribe_to_node(&sn_hash_join_node, 0);

    ls_hash_join_node.subscribe_to_node(&lineitem_csvreader_node, 0);
    ls_hash_join_node.subscribe_to_node(&sn_expression_node, 1);

    lp_hash_join_node.subscribe_to_node(&ls_hash_join_node, 0);
    lp_hash_join_node.subscribe_to_node(&part_where_node, 1);

    lo_merge_join_node.subscribe_to_node(&lp_hash_join_node, 0);
    lo_merge_join_node.subscribe_to_node(&oc_hash_join_node, 1);

    expression_node.subscribe_to_node(&lo_merge_join_node, 0);

    groupby_node.subscribe_to_node(&expression_node, 0);
    scaler_node.subscribe_to_node(&groupby_node, 0);

    select_node.subscribe_to_node(&scaler_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(nation_csvreader_node);
    service.add(lineitem_csvreader_node);
    service.add(orders_csvreader_node);
    service.add(customer_csvreader_node);
    service.add(supplier_csvreader_node);
    service.add(part_csvreader_node);
    service.add(region_csvreader_node);
    service.add(region_where_node);
    service.add(orders_where_node);
    service.add(part_where_node);
    service.add(nr_hash_join_node);
    service.add(cn_hash_join_node);
    service.add(oc_hash_join_node);
    service.add(sn_hash_join_node);
    service.add(sn_expression_node);
    service.add(ls_hash_join_node);
    service.add(lp_hash_join_node);
    service.add(lo_merge_join_node);
    service.add(expression_node);
    service.add(groupby_node);
    service.add(scaler_node);
    service.add(select_node);
    service
}
