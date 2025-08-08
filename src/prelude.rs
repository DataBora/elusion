pub use crate::features::scheduler::PipelineScheduler;

pub use crate::{CustomDataFrame, AliasedDataFrame};

pub use crate::csvwrite::csvwriteops::CsvWriteOptions;

pub use crate::custom_error::cust_error::ElusionError;
pub use crate::custom_error::cust_error::ElusionResult;
//====== postgres
pub use crate::features::postgres::PostgresConfig;
pub use crate::features::postgres::PostgresConnection;
//========= mysql
pub use crate::features::mysql::MySqlConfig;
pub use crate::features::mysql::MySqlConnection;
//====== dashboard
pub use crate::{ReportLayout, TableOptions};

pub use crate::features::calendar::DateFormat;

pub use crate::helper_funcs::extrac_row_from_df::extract_row_from_df;
pub use crate::helper_funcs::extrac_val_from_df::extract_value_from_df;

pub use regex::Regex;
pub use datafusion::prelude::*;
pub use datafusion::error::DataFusionError;
pub use futures::future::BoxFuture;
pub use datafusion::datasource::MemTable;
pub use std::sync::Arc;
pub use arrow::datatypes::{Field, DataType as ArrowDataType, Schema, SchemaRef};
pub use chrono::NaiveDate;
pub use arrow::array::{StringBuilder, ArrayRef,  ArrayBuilder, Float64Builder, Int64Builder, UInt64Builder,Array, Float64Array,Int64Array,Int32Array,TimestampNanosecondArray, Date64Array,Date32Array};

pub use arrow::record_batch::RecordBatch;
pub use ArrowDataType::*;
pub use arrow::csv::writer::WriterBuilder;

// ========= CSV 
pub use std::fs::{self, File, OpenOptions};
pub use std::io::{self, Read, Write, BufWriter};

//============ WRITERS
pub use datafusion::prelude::SessionContext;
pub use datafusion::dataframe::{DataFrame,DataFrameWriteOptions};

// ========= JSON   
pub use serde_json::{json, Map, Value};
pub use serde::{Deserialize, Serialize};
pub use std::collections::{HashMap, HashSet};
pub use arrow::error::Result as ArrowResult;    
pub use datafusion::arrow::datatypes::TimeUnit;
//---json writer
pub use arrow::array::{ListArray,TimestampMicrosecondArray,TimestampMillisecondArray,TimestampSecondArray,LargeBinaryArray,BinaryArray,LargeStringArray,Float32Array,UInt64Array,UInt32Array,BooleanArray};

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

// PIVOT
pub use arrow::compute;
pub use arrow::array::StringArray;

//PLOTTING
#[cfg(feature = "dashboard")]
pub use plotly::Plot as PlotlyPlot;
#[cfg(feature = "dashboard")]
pub use plotly::{Scatter, Bar, Histogram, BoxPlot, Pie};
#[cfg(feature = "dashboard")]
pub use plotly::common::{Mode, Line, Marker, Orientation};
#[cfg(feature = "dashboard")]
pub use plotly::layout::{Axis, Layout};
#[cfg(feature = "dashboard")]
pub use plotly::color::Rgb;
#[cfg(feature = "dashboard")]
pub use plotly::layout::update_menu::{Button,UpdateMenu,UpdateMenuDirection};
#[cfg(feature = "dashboard")]
pub use plotly::layout::{DragMode, RangeSlider};
#[cfg(feature = "dashboard")]
pub use std::cmp::Ordering;
#[cfg(not(feature = "dashboard"))]
pub struct PlotlyPlot;

#[cfg(not(feature = "dashboard"))]
pub struct Plot;
#[cfg(not(feature = "dashboard"))]
pub struct Scatter;
#[cfg(not(feature = "dashboard"))]
pub struct Bar;
#[cfg(not(feature = "dashboard"))]
pub struct Histogram;
#[cfg(not(feature = "dashboard"))]
pub struct BoxPlot;
#[cfg(not(feature = "dashboard"))]
pub struct Pie;
#[cfg(not(feature = "dashboard"))]
pub struct Mode;
#[cfg(not(feature = "dashboard"))]
pub struct Line;
#[cfg(not(feature = "dashboard"))]
pub struct Marker;
#[cfg(not(feature = "dashboard"))]
pub struct Orientation;
#[cfg(not(feature = "dashboard"))]
pub struct Axis;
#[cfg(not(feature = "dashboard"))]
pub struct Layout;
#[cfg(not(feature = "dashboard"))]
pub struct Rgb;
#[cfg(not(feature = "dashboard"))]
pub struct Button;
#[cfg(not(feature = "dashboard"))]
pub struct UpdateMenu;
#[cfg(not(feature = "dashboard"))]
pub struct UpdateMenuDirection;
#[cfg(not(feature = "dashboard"))]
pub struct DragMode;
#[cfg(not(feature = "dashboard"))]
pub struct RangeSlider;

// STATISTICS
pub use datafusion::common::ScalarValue;

// ========== AZURE
#[cfg(feature = "azure")]
pub use azure_storage_blobs::prelude::*;
#[cfg(feature = "azure")]
pub use azure_storage::StorageCredentials;
#[cfg(feature = "azure")]
pub use azure_storage::CloudLocation;
pub use futures::stream;
pub use std::io::BufReader;
pub use futures::pin_mut;
pub use csv::ReaderBuilder;
pub use csv::Trim::All;
pub use serde_json::Deserializer;
// ==== pisanje
#[cfg(feature = "azure")]
pub use azure_storage_blobs::blob::{BlockList, BlobBlockType};
pub use bytes::Bytes;
pub use datafusion::parquet::basic::Compression;
pub use datafusion::parquet::file::properties::{WriterProperties, WriterVersion};
pub use datafusion::parquet::arrow::ArrowWriter;
pub use base64::engine::general_purpose::STANDARD;
pub use base64::Engine;
pub use futures::TryStreamExt;
pub use tempfile::Builder;

// ======== Scheduler
pub use std::future::Future;
pub use tokio_cron_scheduler::{JobScheduler, Job};

// ======== From API
#[cfg(feature = "api")]
pub use reqwest::Client;
#[cfg(feature = "api")]
pub use urlencoding::encode;

pub use crate::features::api::ElusionApi;

#[cfg(not(feature = "api"))]
pub struct Client;


// ========= VIEWS and CAche
pub use std::hash::{Hash, Hasher};
pub use std::collections::hash_map::DefaultHasher;
pub use chrono::{DateTime, Utc};
pub use std::sync::Mutex;
pub use lazy_static::lazy_static;

// =========== DATE TABLE BUILDER
pub use arrow::array::Int32Builder;
pub use arrow::array::BooleanBuilder;
pub use chrono::{Datelike, Weekday, Duration, NaiveDateTime, NaiveTime};

// =========EXCEL
#[cfg(feature = "excel")]
pub use rust_xlsxwriter::{Format, Workbook, ExcelDateTime};
#[cfg(feature = "excel")]
pub use arrow::array::{Int8Array, Int16Array,UInt8Array, UInt16Array};

pub use calamine::DataType as CalamineDataType;
pub use calamine::{Reader, Xlsx, open_workbook};

//========== SHARE POINT
#[cfg(feature = "sharepoint")]
pub use reqwest;
#[cfg(feature = "sharepoint")]
pub use url;