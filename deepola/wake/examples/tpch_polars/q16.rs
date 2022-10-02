use crate::prelude::*;

/// This node implements the following SQL query
// selects_
// 	p_brand,
// 	p_type,
// 	p_size,
// 	count(distinct ps_suppkey) as supplier_cnt
// from
// 	partsupp,
// 	part
// where
// 	p_partkey = ps_partkey
// 	and p_brand <> 'Brand#45'
// 	and p_type not like 'MEDIUM POLISHED%'
// 	and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
// 	and ps_suppkey = s_suppkey (VALID)
// group by
// 	p_brand,
// 	p_type,
// 	p_size
// order by
// 	supplier_cnt desc,
// 	p_brand,
// 	p_type,
// 	p_size;

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Table Name => Columns to Read.
    let table_columns = HashMap::from([
        ("supplier".into(), vec!["s_suppkey", "s_comment"]),
        ("partsupp".into(), vec!["ps_partkey", "ps_suppkey"]),
        (
            "part".into(),
            vec!["p_partkey", "p_brand", "p_type", "p_size"],
        ),
    ]);

    // CSV Reader Nodes.
    let supplier_csvreader_node = build_reader_node("supplier".into(), &tableinput, &table_columns);
    let partsupp_csvreader_node = build_reader_node("partsupp".into(), &tableinput, &table_columns);
    let part_csvreader_node = build_reader_node("part".into(), &tableinput, &table_columns);

    // WHERE Nodes
    let part_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let p_brand = df.column("p_brand").unwrap();
            let p_type = df.column("p_type").unwrap();
            let p_size = df.column("p_size").unwrap();
            let mask1 = p_brand.equal("Brand#45").unwrap();
            let mask2: ChunkedArray<BooleanType> = p_type
                .utf8()
                .unwrap()
                .into_iter()
                .map(|opt_v| opt_v.map(|x| x.starts_with("MEDIUM POLISHED") as bool))
                .collect();
            let mask3 = p_size
                .i64()
                .unwrap()
                .into_iter()
                .map(|opt_v| opt_v.map(|x| vec![49, 14, 23, 45, 19, 3, 36, 9].contains(&x) as bool))
                .collect();
            let mask = (!mask1) & (!mask2) & mask3;
            df.filter(&mask).unwrap()
        })))
        .build();

    let supplier_where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let s_comment = df.column("s_comment").unwrap();
            lazy_static! {
                static ref MY_REGEX: Regex = Regex::new(r".*Customer.*Complaints.*").unwrap();
            }
            let mask = s_comment
                .utf8()
                .unwrap()
                .into_iter()
                .map(|opt_v| opt_v.map(|x| !MY_REGEX.is_match(x)))
                .collect();
            df.filter(&mask).unwrap()
        })))
        .build();

    // Need to set the seed dataframe in groupby_node?
    let pss_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["ps_suppkey".into()])
        .right_on(vec!["s_suppkey".into()])
        .build();

    // Need to set the seed dataframe in groupby_node?
    let psp_hash_join_node = HashJoinBuilder::new()
        .left_on(vec!["ps_partkey".into()])
        .right_on(vec!["p_partkey".into()])
        .build();

    // GROUP BY AGGREGATE Node
    let mut agg_accumulator = AggAccumulator::new();
    agg_accumulator
        .set_group_key(vec![
            "p_brand".into(),
            "p_type".into(),
            "p_size".into(),
            "ps_suppkey".into(),
        ])
        .set_aggregates(vec![("ps_partkey".into(), vec!["count".into()])]);
    let groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(agg_accumulator)
        .build();
    let cdistinct_scaler_node = MM0CountDistinct::new(
        vec!["p_brand".into(), "p_type".into(), "p_size".into()],  // groupby
        "supplier_cnt".into(),  // output_col
        "ps_partkey_count".into(),  // count_col
    ).into_node();

    // SELECT Node
    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df| {
            df.sort(
                vec!["supplier_cnt", "p_brand", "p_type", "p_size"],
                vec![true, false, false, false],
            ).unwrap()
        })))
        .build();

    // Connect nodes with subscription
    part_where_node.subscribe_to_node(&part_csvreader_node, 0);
    supplier_where_node.subscribe_to_node(&supplier_csvreader_node, 0);
    pss_hash_join_node.subscribe_to_node(&partsupp_csvreader_node, 0);
    pss_hash_join_node.subscribe_to_node(&supplier_where_node, 1);
    psp_hash_join_node.subscribe_to_node(&pss_hash_join_node, 0);
    psp_hash_join_node.subscribe_to_node(&part_where_node, 1);
    groupby_node.subscribe_to_node(&psp_hash_join_node, 0);
    cdistinct_scaler_node.subscribe_to_node(&groupby_node, 0);
    select_node.subscribe_to_node(&cdistinct_scaler_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(part_csvreader_node);
    service.add(partsupp_csvreader_node);
    service.add(supplier_csvreader_node);
    service.add(part_where_node);
    service.add(supplier_where_node);
    service.add(pss_hash_join_node);
    service.add(psp_hash_join_node);
    service.add(groupby_node);
    service.add(cdistinct_scaler_node);
    service.add(select_node);
    service
}
