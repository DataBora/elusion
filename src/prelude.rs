
pub use datafusion::logical_expr::{Expr, col, SortExpr};
pub use datafusion::prelude::*;
pub use datafusion::error::{DataFusionError, Result as DataFusionResult};
pub use datafusion::dataframe::{DataFrame, DataFrameWriteOptions};
pub use datafusion::datasource::MemTable;
pub use datafusion::prelude::SessionContext;

pub use datafusion::arrow::datatypes::{Field, DataType as ArrowDataType, Schema, SchemaRef};
pub use datafusion::arrow::datatypes::SchemaBuilder;
pub use datafusion::arrow::array::{
    Array, ArrayRef, StringArray,StringBuilder, Date32Array, Date64Array, Float64Array, Decimal128Array,
    Int32Array, Int64Array, ArrayBuilder, BinaryBuilder, BooleanBuilder, Float64Builder, Float32Builder,
    Int64Builder, Int32Builder, UInt64Builder, UInt32Builder, 
     Date32Builder, BinaryArray, BooleanArray, UInt64Array, UInt32Array
};
pub use datafusion::arrow::record_batch::RecordBatch;
pub use arrow::error::Result as ArrowResult;
pub use ArrowDataType::*;
pub use arrow::csv::writer::WriterBuilder;

pub use datafusion::functions_aggregate::expr_fn::{
    sum, min, max, avg, stddev, count, count_distinct, corr, first_value, grouping, var_pop,
    stddev_pop, array_agg, approx_percentile_cont, nth_value,
};

pub use futures::future::BoxFuture;
pub use tokio::task;

pub use chrono::{NaiveDate, Datelike};

pub use regex::Regex;
// JSON and Serialization Imports
pub use serde::{Deserialize, Serialize};
pub use serde_json::{Map, Value, Deserializer};

// Standard Library Imports
pub use std::collections::{HashMap, HashSet};
pub use std::sync::Arc;
pub use std::fmt;
pub use std::error::Error;
pub use std::path::Path;
pub use std::fs::{self, File, OpenOptions};
pub use std::io::{self, BufRead, BufReader, Write, Read, BufWriter};

pub use encoding_rs::WINDOWS_1252;

pub use crate::{AggregationBuilder, CustomDataFrame, SQLDataType, AliasedDataFrame, CsvWriteOptions};

pub use crate::{ElusionError, ElusionResult};

// DataFusion Imports
// pub use datafusion::prelude::*;



