#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

use rand::{Rng, SeedableRng};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use std::{env, fs};

use crate::columns_builder::{ColumnsBuilder, LidarPointCloud, LidarPointCloudColumnsBuilder};
use crate::schema::LIDAR_POINT_CLOUD_FIELDS;
use arrow::datatypes::Schema;
use clap::{Parser, ValueEnum};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use rand::prelude::StdRng;

mod columns_builder;
mod schema;

#[derive(ValueEnum, Clone, Debug)]
pub enum StatisticsMode {
    /// Compute no statistics
    None,
    /// Compute chunk-level statistics but not page-level
    Chunk,
    /// Compute page-level and chunk-level statistics
    Page,
}

#[derive(Parser, Debug, Clone)]
#[clap()]
struct AppArgs {
    /// Output path to save Parquet file(s)
    #[clap(long)]
    output_parquet_folder: String,
    /// Number of rows to generate
    #[clap(long)]
    rows: usize,
    /// Controls statistics for Parquet
    #[clap(long)]
    statistics_mode: StatisticsMode,
}

fn main() {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::builder().trim_backtraces(Some(30)).build();

    let t: Instant = Instant::now();

    if env::var_os("RUST_LOG").is_none() {
        let env = format!("parquet_example_rs=DEBUG");
        env::set_var("RUST_LOG", env);
    }
    let args = AppArgs::parse();
    println!("Received args: {:?}", args);

    {
        let output_parquet_folder_path = Path::new(args.output_parquet_folder.as_str());
        if !output_parquet_folder_path.exists() {
            fs::create_dir_all(output_parquet_folder_path).unwrap();
        }
    }

    let partition_id: usize = 0;

    let enabled_statistics = match &args.statistics_mode {
        StatisticsMode::None => EnabledStatistics::None,
        StatisticsMode::Chunk => EnabledStatistics::Chunk,
        StatisticsMode::Page => EnabledStatistics::Page,
    };

    let parquet_schema = Schema::new(LIDAR_POINT_CLOUD_FIELDS.clone());
    let arrow_schema = Arc::new(parquet_schema.clone());

    let parquet_props = WriterProperties::builder()
        .set_statistics_enabled(enabled_statistics)
        .set_compression(Compression::UNCOMPRESSED)
        // disable dictionary, it uses dict encoding that slows down due to large arrays inside struct
        .set_dictionary_enabled(false)
        // Limit MAX ROW GROUP SIZE
        .set_max_row_group_size(100)
        .build();
    let output_parquet_path = format!(
        "{}/{partition_id}_{:?}.parquet",
        args.output_parquet_folder, args.statistics_mode
    );
    let file = File::create(output_parquet_path).unwrap();

    let mut wrt = ArrowWriter::try_new(file, arrow_schema.clone(), Some(parquet_props)).unwrap();

    let mut builder = LidarPointCloudColumnsBuilder::new(parquet_schema.clone());

    let max_rows_per_batch: usize = 10;
    let mut row: usize = 0;

    let mut rng = StdRng::from_seed([
        1, 0, 0, 0, 23, 0, 0, 0, 200, 1, 0, 0, 210, 30, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0,
    ]);
    let num_points: usize = 250000;

    for i in 1..args.rows + 1 {
        let x: Vec<f32> = (0..num_points)
            .map(|_| rng.gen_range(0.0f32..200.0f32))
            .collect();
        let y = (0..num_points)
            .map(|_| rng.gen_range(0.0f32..200.0f32))
            .collect();
        let z = (0..num_points)
            .map(|_| rng.gen_range(0.0f32..200.0f32))
            .collect();

        let intensity = (0..num_points).map(|_| rng.gen_range(0u8..255u8)).collect();
        let ring = (0..num_points).map(|_| rng.gen_range(0u8..255u8)).collect();

        let lidar_point_row = LidarPointCloud {
            timestamp: i as u64,
            num_points: num_points as u32,
            x: x,
            y: y,
            z: z,
            intensity,
            ring,
        };
        builder.append(&lidar_point_row).unwrap();
        row += 1;

        if row > 0 && row % max_rows_per_batch == 0 {
            let batch = builder.get_batch().unwrap();
            wrt.write(&batch).unwrap();
            builder.reset().unwrap();
        }
        if row > 0 && row % 500 == 0 {
            let avg = row as f64 / t.elapsed().as_secs() as f64;
            println!("Processed {row} msgs with throughout {avg:.3} msg/s");
        }
    }

    // Write remaining
    {
        let batch = builder.get_batch().unwrap();
        wrt.write(&batch).unwrap();
        builder.reset().unwrap();
    }

    wrt.close().unwrap();

    let total_elapsed_secs = (t.elapsed().as_millis() as f64) / 1000.0;
    let avg = row as f64 / total_elapsed_secs;
    println!("Wrote {row} Lidar Point Cloud to parquet in {total_elapsed_secs:.3} seconds, average throughput {avg:.3} msg/s");
}
