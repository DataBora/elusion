
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
// pub use futures::Future;
// pub use datafusion::sql::TableReference;
// pub use datafusion_table_providers::postgres::PostgresTableFactory;
// pub use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
// pub use datafusion_table_providers::mysql::MySQLTableFactory;
// pub use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
// pub use datafusion_table_providers::odbc::ODBCTableFactory;
// pub use datafusion_table_providers::sql::db_connection_pool::odbcpool::ODBCPool;
// pub use datafusion_table_providers::util::secrets::to_secret_map;

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