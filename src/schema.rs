use arrow::datatypes::DataType;
use once_cell::sync::Lazy;
use std::sync::Arc;

pub static LIDAR_POINT_CLOUD_FIELDS: Lazy<Vec<arrow::datatypes::Field>> = Lazy::new(|| {
    vec![
        arrow::datatypes::Field::new("timestamp", DataType::UInt64, true),
        arrow::datatypes::Field::new("num_points", DataType::UInt32, true),
        arrow::datatypes::Field::new(
            "x",
            DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Float32,
                true,
            ))),
            true,
        ),
        arrow::datatypes::Field::new(
            "y",
            DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Float32,
                true,
            ))),
            true,
        ),
        arrow::datatypes::Field::new(
            "z",
            DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Float32,
                true,
            ))),
            true,
        ),
        arrow::datatypes::Field::new(
            "intensity",
            DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::UInt8,
                true,
            ))),
            true,
        ),
        arrow::datatypes::Field::new(
            "ring",
            DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::UInt8,
                true,
            ))),
            true,
        ),
    ]
});
