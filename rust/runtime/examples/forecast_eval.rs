use std::collections::HashMap;
use std::time::Instant;
use structopt::StructOpt;

use runtime::data::ArrayRow;
use runtime::data::Column;
use runtime::data::DataBlock;
use runtime::data::DataCell;
use runtime::data::DataMessage;
use runtime::data::DataType;
use runtime::data::MetaCell;
use runtime::data::Schema;
use runtime::data::DATABLOCK_CARDINALITY;
use runtime::data::DATABLOCK_TOTAL_RECORDS;
use runtime::data::DATABLOCK_TYPE;
use runtime::data::DATABLOCK_TYPE_DM;
use runtime::data::SCHEMA_META_NAME;
use runtime::forecast::cell::ForecastSelector;
use runtime::forecast::table::ForecastNode;
use runtime::graph::ExecutionNode;
use runtime::graph::ExecutionService;
use runtime::graph::NodeReader;
use runtime::operations::CSVReaderNode;


/* Parsed arguments */

#[derive(Debug, StructOpt)]
pub struct Cli {
    /// directory containing series of input csv (1.csv to N.csv)
    #[structopt(long)]
    series_dir: String,
    /// schema name [q12, q16]
    #[structopt(long)]
    schema_name: String,
    /// number of csv in the series
    #[structopt(long)]
    series_n: usize,

    /// path to final answer
    #[structopt(long)]
    final_answer_path: String,

    /// forecaster method name
    #[structopt(long)]
    forecast_by: String,
}

fn select_schema(schema_name: &str) -> Schema {
    match schema_name {
        "q12" => Schema::new(
            String::from(schema_name),
            vec![
                Column::from_key_field("l_shipmode".to_string(), DataType::Text),
                Column::from_field("high_line_count".to_string(), DataType::Integer),
                Column::from_field("low_line_count".to_string(), DataType::Integer),
            ],
        ),
        "q16" => Schema::new(
            String::from(schema_name),
            vec![
                Column::from_key_field("p_brand".to_string(), DataType::Text),
                Column::from_key_field("p_type".to_string(), DataType::Text),
                Column::from_key_field("p_size".to_string(), DataType::Integer),
                Column::from_field("supplier_cnt".to_string(), DataType::Integer),
            ],
        ),
        _ => panic!("Invalid schema name {}", schema_name)
    }
}

type Metadata = HashMap<String, MetaCell>;

fn read_csv(args: &Cli) -> (ExecutionNode<ArrayRow>, Metadata) {
    let schema = select_schema(&args.schema_name);
    let mut metadata = HashMap::from([
        (SCHEMA_META_NAME.into(), MetaCell::Schema(schema)),
        (DATABLOCK_TYPE.into(), MetaCell::from(DATABLOCK_TYPE_DM)),
        (DATABLOCK_CARDINALITY.into(), MetaCell::from(0.0)),
        (DATABLOCK_TOTAL_RECORDS.into(), MetaCell::from(1_000_000.0)),  // TODO: how to find this?
    ]);
    let csvreader = CSVReaderNode::new_with_params(
        1_000_000,  // anything larger
        ',',
        false  /* has_headers */,
        vec![]  /* filtered_cols */,
    );
    log::info!("Metadata: {:?}", metadata);
    for idx in 1 .. args.series_n + 1 {
        let series_path = format!("{}/{}.csv", args.series_dir, idx);
        log::debug!("Reading {}", series_path);
        let input_vec = vec![
            ArrayRow::from_vector(vec![DataCell::from(series_path)])
        ];
        metadata.insert(
            DATABLOCK_CARDINALITY.into(),
            MetaCell::from(idx as f64 / args.series_n as f64)  // update cardinality
        );
        let dblock = DataBlock::new(input_vec, metadata.clone());
        let dmsg = DataMessage::from(dblock);
        csvreader.write_to_self(0, dmsg);
    }
    csvreader.write_to_self(0, DataMessage::eof());
    (csvreader, metadata)
}

fn read_answer(args: &Cli) -> DataBlock<ArrayRow> {
    let answer_path = args.final_answer_path.clone();
    log::info!("Reading answer at {}", answer_path);

    // read via CSVReader
    let schema = select_schema(&args.schema_name);
    let metadata = HashMap::from([
        (SCHEMA_META_NAME.into(), MetaCell::Schema(schema)),
        (DATABLOCK_TYPE.into(), MetaCell::from(DATABLOCK_TYPE_DM)),
        (DATABLOCK_CARDINALITY.into(), MetaCell::from(1.0)),
        (DATABLOCK_TOTAL_RECORDS.into(), MetaCell::from(1_000_000.0)),  // TODO: how to find this?
    ]);
    let csvreader = CSVReaderNode::new_with_params(
        1_000_000,  // anything larger
        ',',
        false  /* has_headers */,
        vec![]  /* filtered_cols */,
    );
    let input_vec = vec![
        ArrayRow::from_vector(vec![DataCell::from(answer_path)])
    ];
    let dblock = DataBlock::new(input_vec, metadata.clone());
    let dmsg = DataMessage::from(dblock);
    csvreader.write_to_self(0, dmsg);
    csvreader.write_to_self(0, DataMessage::eof());
    let reader_node = NodeReader::new(&csvreader);

    // run and read data block
    csvreader.run();
    let read_message = reader_node.read();
    read_message.datablock().clone()
}

fn attach_forecast(
    dataset_node: &ExecutionNode<ArrayRow>,
    metadata: Metadata,
    final_time: usize,
    forecast_by: &str,
) -> ExecutionNode<ArrayRow> {
    let input_schema = metadata
        .get(SCHEMA_META_NAME)
        .unwrap()
        .to_schema();
    let forecast_node = match forecast_by {
        "default" | "" => ForecastNode::node(input_schema, final_time as f64),
        "tail" => ForecastNode::with_estimator(
            input_schema,
            final_time as f64,
            Box::new(ForecastSelector::make_tail),
        ),
        "linear_scale" => ForecastNode::with_estimator(
            input_schema,
            final_time as f64,
            Box::new(ForecastSelector::make_linear_scale),
        ),
        "least_square" => ForecastNode::with_estimator(
            input_schema,
            final_time as f64,
            Box::new(ForecastSelector::make_least_square),
        ),
        _ => panic!("Invalid forecast_by= {}", forecast_by),
    };
    forecast_node.subscribe_to_node(dataset_node, 0);
    forecast_node
}

fn extract_columns(schema: &Schema) -> (Vec<String>, Vec<String>) {
    let mut key_columns = Vec::new();
    let mut values_columns = Vec::new();
    for column in schema.columns.iter() {
        if column.key {
            key_columns.push(column.name.clone());
        } else {
            values_columns.push(column.name.clone());
        }
    }
    (key_columns, values_columns)
}

fn measure_error(pred: &DataBlock<ArrayRow>, expected: &DataBlock<ArrayRow>) -> f64 {
    // just in case the columns are shifted
    let (key_columns, values_columns) = extract_columns(expected.schema());
    let pred_key_indexes: Vec<usize> = key_columns
        .iter()
        .map(|column| pred.schema().index(column))
        .collect();
    let pred_value_indexes: Vec<usize> = values_columns
        .iter()
        .map(|column| pred.schema().index(column))
        .collect();
    let expected_key_indexes: Vec<usize> = key_columns
        .iter()
        .map(|column| expected.schema().index(column))
        .collect();
    let expected_value_indexes: Vec<usize> = values_columns
        .iter()
        .map(|column| expected.schema().index(column))
        .collect();

    // collect correct answers
    let mut expected_answer: HashMap<String, Vec<f64>> = HashMap::new();
    let mut errors: HashMap<String, Vec<f64>> = HashMap::new();
    for expected_record in expected.data().iter() {
        let expected_key = expected_record.slice_indices(&expected_key_indexes);
        let expected_value = expected_record.slice_indices(&expected_value_indexes)
            .iter()
            .map(f64::from)
            .collect::<Vec<f64>>();
        let default_errors = expected_value_indexes.iter()
            .map(|_| 1.0)  // 100% error if no prediction
            .collect::<Vec<f64>>();
        expected_answer.insert(format!("{:?}", expected_key), expected_value);
        errors.insert(format!("{:?}", expected_key), default_errors);
    }

    // measure cell-wise errors
    for pred_record in pred.data().iter() {
        let pred_key = pred_record.slice_indices(&pred_key_indexes);
        let pred_value = pred_record.slice_indices(&pred_value_indexes)
            .iter()
            .map(f64::from)
            .collect::<Vec<f64>>();
        let pred_key_str = format!("{:?}", pred_key);
        if let Some(expected_value) = expected_answer.get(&pred_key_str) {
            assert_eq!(pred_value.len(), expected_value.len());
            let row_errors = (0 .. pred_value.len()).map(|idx| {
                (expected_value[idx] - pred_value[idx]).abs() / expected_value[idx]
            }).collect();
            errors.insert(format!("{:?}", pred_key), row_errors);
        } else {
            log::error!("Predicted unexpected key group: {:?}", pred_key);
        }
    }

    // summarize errors by averaging
    let flatten_errors = errors.into_iter().flat_map(|(_k, v)| v).collect::<Vec<f64>>();
    log::debug!("Errors: {:?}", flatten_errors);
    100.0 * flatten_errors.iter().sum::<f64>() / flatten_errors.len() as f64
}

fn do_test(args: &Cli) {
    log::info!("Testing q1");
    let q1_start_time = Instant::now();

    // read expected answer
    let answer_dblock = read_answer(args);

    // setup executation nodes
    let mut service = ExecutionService::<ArrayRow>::create();
    let (dataset_node, metadata) = read_csv(args);
    let forecast_node = attach_forecast(&dataset_node, metadata, args.series_n, &args.forecast_by);
    let reader_node = NodeReader::new(&forecast_node);
    service.add(dataset_node);
    service.add(forecast_node);

    // execute!
    service.run();
    for idx in 1 .. args.series_n + 1 {
        let start_time = Instant::now();
        let read_message = reader_node.read();
        let read_data = read_message.datablock().data();
        log::trace!("Predict  {:?}", read_data);
        log::trace!("Expected {:?}", answer_dblock.data());

        let avg_perr = measure_error(read_message.datablock(), &answer_dblock);
        log::info!("Part {}: {:>9.2?}, avg_perr= {}", idx, start_time.elapsed(), avg_perr);
    }
    log::info!("Testing took {:?}", q1_start_time.elapsed());
}

fn main() {
    // execution init
    env_logger::Builder::from_default_env()
        .format_timestamp_micros()
        .init();

    // parse args
    let args = Cli::from_args();
    log::info!("{:?}", args);

    // run showcase tests
    do_test(&args);
}