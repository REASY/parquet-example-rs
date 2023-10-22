use arrow::array::{ArrayRef, ListBuilder, PrimitiveBuilder, RecordBatch};
use arrow::datatypes::{Float32Type, Schema, UInt32Type, UInt64Type, UInt8Type};
use arrow::error::ArrowError;
use std::sync::Arc;

pub fn copy_to_builder_list_f32(
    builder: &mut ListBuilder<PrimitiveBuilder<Float32Type>>,
    vec: &[f32],
) {
    let x: Vec<Option<f32>> = vec.iter().map(|x| Some(*x)).collect();
    builder.append_value(x);
}

pub fn copy_to_builder_list_u8(builder: &mut ListBuilder<PrimitiveBuilder<UInt8Type>>, vec: &[u8]) {
    let x: Vec<Option<u8>> = vec.iter().map(|x| Some(*x)).collect();
    builder.append_value(x);
}

pub trait ColumnsBuilder<'a> {
    type T;

    fn get_batch(&mut self) -> Result<RecordBatch, ArrowError>;
    fn append(&mut self, msg: &'a Self::T) -> Result<(), ArrowError>;

    fn reset(&mut self) -> Result<(), ArrowError>;
}

pub struct LidarPointCloudColumnsBuilder {
    schema: Schema,
    timestamp_builder: PrimitiveBuilder<UInt64Type>,
    num_points_builder: PrimitiveBuilder<UInt32Type>,
    x_builder: ListBuilder<PrimitiveBuilder<Float32Type>>,
    y_builder: ListBuilder<PrimitiveBuilder<Float32Type>>,
    z_builder: ListBuilder<PrimitiveBuilder<Float32Type>>,
    intensity_builder: ListBuilder<PrimitiveBuilder<UInt8Type>>,
    ring_builder: ListBuilder<PrimitiveBuilder<UInt8Type>>,
}

pub struct LidarPointCloud {
    pub timestamp: u64,
    pub num_points: u32,
    pub x: Vec<f32>,
    pub y: Vec<f32>,
    pub z: Vec<f32>,
    pub intensity: Vec<u8>,
    pub ring: Vec<u8>,
}

impl<'a> ColumnsBuilder<'a> for LidarPointCloudColumnsBuilder {
    type T = LidarPointCloud;

    fn get_batch(&mut self) -> Result<RecordBatch, ArrowError> {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.timestamp_builder.finish()),
            Arc::new(self.num_points_builder.finish()),
            Arc::new(self.x_builder.finish()),
            Arc::new(self.y_builder.finish()),
            Arc::new(self.z_builder.finish()),
            Arc::new(self.intensity_builder.finish()),
            Arc::new(self.ring_builder.finish()),
        ];
        let batch = RecordBatch::try_new(Arc::new(self.schema.clone()), arrays)?;
        Ok(batch)
    }

    fn append(&mut self, msg: &LidarPointCloud) -> Result<(), ArrowError> {
        self.timestamp_builder.append_value(msg.timestamp);
        self.num_points_builder.append_value(msg.num_points);
        copy_to_builder_list_f32(&mut self.x_builder, &msg.x.as_slice());
        copy_to_builder_list_f32(&mut self.y_builder, &msg.y.as_slice());
        copy_to_builder_list_f32(&mut self.z_builder, &msg.z.as_slice());
        copy_to_builder_list_u8(&mut self.intensity_builder, &msg.intensity);
        copy_to_builder_list_u8(&mut self.ring_builder, &msg.ring);
        return Ok(());
    }

    fn reset(&mut self) -> Result<(), ArrowError> {
        self.timestamp_builder = Default::default();
        self.num_points_builder = Default::default();
        self.x_builder = Default::default();
        self.y_builder = Default::default();
        self.z_builder = Default::default();
        self.intensity_builder = Default::default();
        self.ring_builder = Default::default();
        return Ok(());
    }
}

impl LidarPointCloudColumnsBuilder {
    pub fn new(schema: Schema) -> LidarPointCloudColumnsBuilder {
        LidarPointCloudColumnsBuilder {
            schema,
            timestamp_builder: Default::default(),
            num_points_builder: Default::default(),
            x_builder: Default::default(),
            y_builder: Default::default(),
            z_builder: Default::default(),
            intensity_builder: Default::default(),
            ring_builder: Default::default(),
        }
    }
}
