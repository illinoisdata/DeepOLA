use crate::prelude::*;

/// This node implements the following SQL query
// select
// 	sum(l_extendedprice * l_discount) as revenue
// from
// 	lineitem
// where
// 	l_shipdate >= date '1994-01-01'
// 	and l_shipdate < date '1994-01-01' + interval '1' year
// 	and l_discount between .06 - 0.01 and .06 + 0.01
// 	and l_quantity < 24;

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Create a HashMap that stores table name and the columns in that query.
    let table_columns = HashMap::from([(
        "lineitem".into(),
        vec!["l_quantity", "l_extendedprice", "l_discount", "l_shipdate"],
    )]);

    // CSVReaderNode would be created for this table.
    let lineitem_csvreader_node = build_reader_node("lineitem".into(), &tableinput, &table_columns);

    // WHERE Node
    let where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let l_shipdate = df.column("l_shipdate").unwrap();
            let l_discount = df.column("l_discount").unwrap();
            let l_quantity = df.column("l_quantity").unwrap();
            let var_date_1 = days_since_epoch(1994,1,1);
            let var_date_2 = days_since_epoch(1995,1,1);
            let mask = l_shipdate.gt_eq(var_date_1).unwrap()
                & l_shipdate.lt(var_date_2).unwrap()
                & l_discount.gt_eq(0.05).unwrap()
                & l_discount.lt_eq(0.07).unwrap()
                & l_quantity.lt(24).unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    // EXPRESSION Node
    let expression_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let extended_price = df.column("l_extendedprice").unwrap();
            let discount = df.column("l_discount").unwrap();
            let columns = vec![Series::new("disc_price", extended_price * discount)];
            DataFrame::new(columns).unwrap()
        })))
        .build();

    // GROUP BY Aggregate Node
    let mut agg_accumulator = AggAccumulator::new();
    agg_accumulator
        .set_aggregates(vec![("disc_price".into(), vec!["sum".into()])])
        .set_add_count_column(true)
        .set_scaler(AggregateScaler::new_growing()
            .remove_count_column()  // Remove added group count column
            .scale_sum("disc_price_sum".into())
            .into_rc()
        );
    let groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(agg_accumulator)
        .build();

    // Connect nodes with subscription
    where_node.subscribe_to_node(&lineitem_csvreader_node, 0);
    expression_node.subscribe_to_node(&where_node, 0);
    groupby_node.subscribe_to_node(&expression_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&groupby_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(groupby_node);
    service.add(expression_node);
    service.add(where_node);
    service.add(lineitem_csvreader_node);
    service
}
