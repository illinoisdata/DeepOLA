use crate::data::{DataMessage, DataBlock, ArrayRow, DataType, Column, MetaCell, DataCell};

pub fn example_city_arrow_message() -> DataMessage<ArrayRow> {
    let metadata = MetaCell::from(vec![
        Column::from_field("country".into(), DataType::Text),
        Column::from_field("state".into(), DataType::Text),
        Column::from_field("city".into(), DataType::Text),
        Column::from_field("population".into(), DataType::Integer),
        Column::from_field("area".into(), DataType::Float),
    ])
    .into_meta_map();
    let input_rows = self::example_city_arrow_rows();
    DataMessage::from(DataBlock::new(input_rows, metadata))
}

pub fn example_city_arrow_rows() -> Vec<ArrayRow> {
    vec![
        ArrayRow::from([
            "US".into(),
            "IL".into(),
            "Champaign".into(),
            100i32.into(),
            1.5f64.into(),
        ]),
        ArrayRow::from([
            "US".into(),
            "IL".into(),
            "Urbana".into(),
            200i32.into(),
            DataCell::Null(),
        ]),
        ArrayRow::from([
            "US".into(),
            "CA".into(),
            "San Francisco".into(),
            300i32.into(),
            2.5f64.into(),
        ]),
        ArrayRow::from([
            "US".into(),
            "CA".into(),
            "San Jose".into(),
            400i32.into(),
            3.5f64.into(),
        ]),
        ArrayRow::from([
            "IN".into(),
            "UP".into(),
            "Lucknow".into(),
            500i32.into(),
            4.5f64.into(),
        ]),
        ArrayRow::from([
            "IN".into(),
            "UP".into(),
            "Noida".into(),
            600i32.into(),
            DataCell::Null(),
        ]),
        ArrayRow::from([
            "IN".into(),
            "KA".into(),
            "Bangalore".into(),
            700i32.into(),
            5.5f64.into(),
        ]),
        ArrayRow::from([
            "IN".into(),
            "KA".into(),
            "Mysore".into(),
            800i32.into(),
            DataCell::Null(),
        ]),
    ]
}