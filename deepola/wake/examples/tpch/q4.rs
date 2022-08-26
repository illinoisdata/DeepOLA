use crate::utils::*;

extern crate runtime;
use runtime::graph::*;
use runtime::data::*;
use runtime::operations::*;

use std::collections::HashMap;
use std::cmp;

// select
// 	o_orderpriority,
// 	count(*) as order_count
// from
// 	orders
// where
// 	o_orderdate >= date '1993-07-01'
// 	and o_orderdate < date '1993-07-01' + interval '3' month
// 	and exists (
// 		select
// 			*
// 		from
// 			lineitem
// 		where
// 			l_orderkey = o_orderkey
// 			and l_commitdate < l_receiptdate
// 	)
// group by
// 	o_orderpriority
// order by
// 	o_orderpriority;

pub fn query(tableinput: HashMap<String, TableInput>, output_reader: &mut NodeReader<ArrayRow>) -> ExecutionService<ArrayRow> {
    // CSV Reader node
    let lineitem_csvreader_node = build_csv_reader_node("lineitem".into(), &tableinput, &table_columns);
    let orders_csvreader_node = build_csv_reader_node("orders".into(), &tableinput, &table_columns);
    let customer_csvreader_node = build_csv_reader_node("customer".into(), &tableinput, &table_columns);

}