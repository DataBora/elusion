pub use crate::PipelineScheduler;

pub use crate::{CustomDataFrame, AliasedDataFrame, CsvWriteOptions};
pub use crate::{ElusionError, ElusionResult};
// pub use crate::{PostgresSource, MySqlSource, ODBCSource};

pub use datafusion::logical_expr::col;
pub use regex::Regex;
pub use datafusion::prelude::*;
pub use datafusion::error::DataFusionError;
pub use futures::future::BoxFuture;
pub use datafusion::datasource::MemTable;
pub use std::sync::Arc;
pub use arrow::datatypes::{Field, DataType as ArrowDataType, Schema, SchemaRef};
pub use chrono::NaiveDate;
pub use arrow::array::{StringBuilder, ArrayRef,  ArrayBuilder, Float64Builder,Float32Builder, Int64Builder, Int32Builder, UInt64Builder, UInt32Builder, BooleanBuilder, Date32Builder, BinaryBuilder};

pub use arrow::record_batch::RecordBatch;
pub use ArrowDataType::*;
pub use arrow::csv::writer::WriterBuilder;

// ========= CSV 
pub use std::fs::{self, File, OpenOptions};
pub use std::io::{self, Read, Write, BufWriter};

//============ WRITERS
pub use datafusion::prelude::SessionContext;
pub use datafusion::dataframe::{DataFrame,DataFrameWriteOptions};
pub use tokio::task;

// ========= JSON   
pub use serde_json::{json, Map, Value};
pub use serde::{Deserialize, Serialize};
pub use std::collections::{HashMap, HashSet};
pub use arrow::error::Result as ArrowResult;    

pub use datafusion::arrow::datatypes::TimeUnit;

//delta
pub use std::result::Result;
pub use std::path::{Path as LocalPath, PathBuf};
pub use deltalake::operations::DeltaOps;
pub use deltalake::writer::{RecordBatchWriter, WriteMode, DeltaWriter};
pub use deltalake::{open_table, DeltaTableBuilder, DeltaTableError, ObjectStore, Path as DeltaPath};
pub use deltalake::protocol::SaveMode;
pub use deltalake::kernel::{DataType as DeltaType, Metadata, Protocol, StructType};
pub use deltalake::kernel::StructField;
pub use futures::StreamExt;
pub use deltalake::storage::object_store::local::LocalFileSystem;
// use object_store::path::Path as ObjectStorePath;


// =========== ERRROR

pub use std::fmt::{self, Debug};
pub use std::error::Error;

// ================ DATABASE

pub use arrow_odbc::odbc_api::{Environment, ConnectionOptions};
pub use arrow_odbc::OdbcReaderBuilder;
pub use lazy_static::lazy_static;

// PIVOT
pub use arrow::compute;
pub use arrow::array::StringArray;

//PLOTTING
pub use plotly::{Plot, Scatter, Bar, Histogram, BoxPlot, Pie};
pub use plotly::common::{Mode, Line, Marker, Orientation};
pub use plotly::layout::{Axis, Layout};
pub use plotly::color::Rgb;
pub use arrow::array::{Array, Float64Array,Int64Array};
pub use arrow::array::Date32Array;
pub use std::cmp::Ordering;

// STATISTICS
pub use datafusion::common::ScalarValue;

// ========== AZURE
pub use azure_storage_blobs::prelude::*;
pub use azure_storage::StorageCredentials;
pub use azure_storage::CloudLocation;
//==== pisanje
pub use azure_storage_blobs::blob::{BlockList, BlobBlockType};
pub use bytes::Bytes;
pub use datafusion::parquet::basic::Compression;
pub use datafusion::parquet::file::properties::{WriterProperties, WriterVersion};
pub use datafusion::parquet::arrow::ArrowWriter;
pub use base64::engine::general_purpose::STANDARD;
pub use base64::Engine;

// ======== Scheduler
pub use std::future::Future;
pub use tokio_cron_scheduler::{JobScheduler, Job};

// ======== From API
// pub use reqwest::Client;
// pub use urlencoding::encode;
// pub use std::fs::remove_file;
