use crate::prelude::*;

/// This node implements the following SQL query
// select
//  l_returnflag,
//  l_linestatus,
//  sum(l_quantity) as sum_qty,
//  sum(l_extendedprice) as sum_base_price,
//  sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
//  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
//  avg(l_quantity) as avg_qty,
//  avg(l_extendedprice) as avg_price,
//  avg(l_discount) as avg_disc,
//  count(*) as count_order
// from
//  lineitem
// where
//  l_shipdate <= date '1998-12-01' - interval '90' day
// group by
//  l_returnflag,
//  l_linestatus
// order by
//  l_returnflag,
//  l_linestatus;
// limit -1;

pub fn query(
    tableinput: HashMap<String, TableInput>,
    output_reader: &mut NodeReader<polars::prelude::DataFrame>,
) -> ExecutionService<polars::prelude::DataFrame> {
    // Create a HashMap that stores table name and the columns in that query.
    let table_columns = HashMap::from([(
        "lineitem".into(),
        vec![
            "l_orderkey", // For COUNT(*) counting a key.
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_returnflag",
            "l_linestatus",
            "l_shipdate",
        ],
    )]);

    // CSVReaderNode would be created for this table.
    let lineitem_csvreader_node = build_reader_node_permute_files("lineitem".into(), &tableinput, &table_columns);

    // WHERE Node
    let where_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let var_date = days_since_epoch(1998,9,2);
            let a = df.column("l_shipdate").unwrap();
            let mask = a.lt_eq(var_date).unwrap();
            df.filter(&mask).unwrap()
        })))
        .build();

    // EXPRESSION Node
    let expression_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            let extended_price = df.column("l_extendedprice").unwrap();
            let discount = df.column("l_discount").unwrap();
            let tax = df.column("l_tax").unwrap();
            let columns = vec![
                Series::new(
                    "disc_price",
                    extended_price
                        .cast(&polars::datatypes::DataType::Float64)
                        .unwrap()
                        * (discount * -1f64 + 1f64),
                ),
                Series::new(
                    "charge",
                    (extended_price
                        .cast(&polars::datatypes::DataType::Float64)
                        .unwrap()
                        * (discount * -1f64 + 1f64))
                        * (tax + 1f64),
                ),
            ];
            df.hstack(&columns).unwrap()
        })))
        .build();

    // GROUP BY Aggregate Node
    let mut sum_accumulator = AggAccumulator::new();
    sum_accumulator
        .set_group_key(vec!["l_returnflag".to_string(), "l_linestatus".to_string()])
        .set_aggregates(vec![
            ("l_orderkey".into(), vec!["count".into()]),
            ("l_quantity".into(), vec!["sum".into()]),
            ("l_extendedprice".into(), vec!["sum".into()]),
            ("l_discount".into(), vec!["sum".into()]),
            ("disc_price".into(), vec!["sum".into()]),
            ("charge".into(), vec!["sum".into()]),
        ])
        .set_track_variance(true)
        .set_scaler(AggregateScaler::new_growing()
            .count_column("l_orderkey_count".into())
            .track_variance()
            .scale_count_with_variance("l_orderkey_count".into())
            .scale_sum_with_variance("l_quantity_sum".into(), "l_quantity_var".into())
            .scale_sum_with_variance("l_extendedprice_sum".into(), "l_extendedprice_var".into())
            .scale_sum_with_variance("l_discount_sum".into(), "l_discount_var".into())
            .scale_sum_with_variance("disc_price_sum".into(), "disc_price_var".into())
            .scale_sum_with_variance("charge_sum".into(), "charge_var".into())
            .track_sum_sum_covariance("l_quantity_sum".into(), "l_orderkey_count".into())
            .track_sum_sum_covariance("l_extendedprice_sum".into(), "l_orderkey_count".into())
            .track_sum_sum_covariance("l_discount_sum".into(), "l_orderkey_count".into())
            .into_rc()
        );

    let groupby_node = AccumulatorNode::<DataFrame, AggAccumulator>::new()
        .accumulator(sum_accumulator)
        .build();

    // SELECT Node
    let select_node = AppenderNode::<DataFrame, MapAppender>::new()
        .appender(MapAppender::new(Box::new(|df: &DataFrame| {
            // Compute AVG from SUM/COUNT.
            let avg_qty = Series::new(
                "avg_qty",
                (df.column("l_quantity_sum")
                    .unwrap()
                    .cast(&polars::datatypes::DataType::Float64)
                    .unwrap())
                    / (df
                        .column("l_orderkey_count")
                        .unwrap()
                        .cast(&polars::datatypes::DataType::Float64)
                        .unwrap()),
            );
            let avg_price = Series::new(
                "avg_price",
                df.column("l_extendedprice_sum").unwrap()
                    / df.column("l_orderkey_count").unwrap(),
            );
            let avg_disc = Series::new(
                "avg_disc",
                df.column("l_discount_sum").unwrap() / df.column("l_orderkey_count").unwrap(),
            );
            let columns = vec![
                Series::new("l_returnflag", df.column("l_returnflag").unwrap()),
                Series::new("l_linestatus", df.column("l_linestatus").unwrap()),
                Series::new("sum_qty", df.column("l_quantity_sum").unwrap()),
                Series::new("sum_base_price", df.column("l_extendedprice_sum").unwrap()),
                Series::new("sum_disc_price", df.column("disc_price_sum").unwrap()),
                Series::new("sum_charge", df.column("charge_sum").unwrap()),
                Series::new("count_order", df.column("l_orderkey_count").unwrap()),

                Series::new("sum_qty_var", df.column("l_quantity_sum_var").unwrap()),
                Series::new("sum_base_price_var", df.column("l_extendedprice_sum_var").unwrap()),
                Series::new("sum_disc_price_var", df.column("disc_price_sum_var").unwrap()),
                Series::new("sum_charge_var", df.column("charge_sum_var").unwrap()),
                Series::new(
                    "avg_qty_var",
                    calculate_div_var(
                        &df.column("l_quantity_sum").unwrap().cast(&polars::datatypes::DataType::Float64).unwrap(),
                        df.column("l_quantity_sum_var").unwrap(),
                        &df.column("l_orderkey_count").unwrap().cast(&polars::datatypes::DataType::Float64).unwrap(),
                        df.column("l_orderkey_count_var").unwrap(),
                        df.column("l_quantity_sum_l_orderkey_count_cov").unwrap(),
                        &avg_qty
                    )
                ),
                Series::new(
                    "avg_price_var",
                    calculate_div_var(
                        &df.column("l_extendedprice_sum").unwrap().cast(&polars::datatypes::DataType::Float64).unwrap(),
                        df.column("l_extendedprice_sum_var").unwrap(),
                        &df.column("l_orderkey_count").unwrap().cast(&polars::datatypes::DataType::Float64).unwrap(),
                        df.column("l_orderkey_count_var").unwrap(),
                        df.column("l_extendedprice_sum_l_orderkey_count_cov").unwrap(),
                        &avg_price
                    )
                ),
                Series::new(
                    "avg_disc_var",
                    calculate_div_var(
                        &df.column("l_discount_sum").unwrap().cast(&polars::datatypes::DataType::Float64).unwrap(),
                        df.column("l_discount_sum_var").unwrap(),
                        &df.column("l_orderkey_count").unwrap().cast(&polars::datatypes::DataType::Float64).unwrap(),
                        df.column("l_orderkey_count_var").unwrap(),
                        df.column("l_discount_sum_l_orderkey_count_cov").unwrap(),
                        &avg_disc
                    )
                ),
                Series::new("count_order_var", df.column("l_orderkey_count_var").unwrap()),
            ];
            DataFrame::new(columns)
                .unwrap()
                .sort(&["l_returnflag", "l_linestatus"], vec![false, false])
                .unwrap()
        })))
        .build();

    // Connect nodes with subscription
    where_node.subscribe_to_node(&lineitem_csvreader_node, 0);
    expression_node.subscribe_to_node(&where_node, 0);
    groupby_node.subscribe_to_node(&expression_node, 0);
    select_node.subscribe_to_node(&groupby_node, 0);

    // Output reader subscribe to output node.
    output_reader.subscribe_to_node(&select_node, 0);

    // Add all the nodes to the service
    let mut service = ExecutionService::<polars::prelude::DataFrame>::create();
    service.add(select_node);
    service.add(groupby_node);
    service.add(expression_node);
    service.add(where_node);
    service.add(lineitem_csvreader_node);
    service
}
