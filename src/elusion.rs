// ==================== IMPORTS ==================//
pub mod prelude;
// ========== DataFrame
use datafusion::logical_expr::{col, Expr,SortExpr};
use regex::Regex;
use datafusion::prelude::*;
use datafusion::error::DataFusionError;
use futures::future::BoxFuture;
use datafusion::datasource::MemTable;
use std::sync::Arc;
use arrow::datatypes::{Field, DataType as ArrowDataType, Schema, SchemaRef};
use chrono::{NaiveDate,Datelike};
use arrow::array::{StringBuilder,StringArray, ArrayRef, Array, ArrayBuilder, Float64Builder,Float32Builder, Int64Builder, Int32Builder, UInt64Builder, UInt32Builder, BooleanBuilder, Date32Builder, BinaryBuilder, Date32Array };

use arrow::record_batch::RecordBatch;
use ArrowDataType::*;
use arrow::csv::writer::WriterBuilder;

// ========= CSV defects
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write, BufWriter};

use encoding_rs::WINDOWS_1252;

//======== AGGREGATION FUNCTIONS 
use datafusion::functions_aggregate::expr_fn::{
    sum, min, max, avg, stddev, count, count_distinct, corr, first_value, grouping,
    var_pop, stddev_pop, array_agg,approx_percentile_cont, nth_value
};
//============ WRITERS
use datafusion::prelude::SessionContext;
use datafusion::dataframe::{DataFrame,DataFrameWriteOptions};
use tokio::task;

// ========= JSON   
use serde_json::{json, Map, Value};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use arrow::error::Result as ArrowResult;    

use datafusion::arrow::datatypes::TimeUnit;

use std::result::Result;
use std::path::{Path as LocalPath, PathBuf};
use deltalake::operations::DeltaOps;
use deltalake::writer::{RecordBatchWriter, WriteMode, DeltaWriter};
use deltalake::{open_table, DeltaTableBuilder, DeltaTableError, ObjectStore, Path as DeltaPath};
use deltalake::protocol::SaveMode;
use deltalake::kernel::{DataType as DeltaType, Metadata, Protocol, StructType};
use deltalake::kernel::StructField;
use futures::StreamExt;
use deltalake::storage::object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;


// =========== ERRROR

use std::fmt::{self, Debug};
use std::error::Error;

#[derive(Debug)]
pub enum ElusionError {
    DataFusion(DataFusionError),
    Io(std::io::Error),
    Custom(String),
}

impl fmt::Display for ElusionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ElusionError::DataFusion(err) => write!(f, "DataFusion Error: {}", err),
            ElusionError::Io(err) => write!(f, "IO Error: {}", err),
            ElusionError::Custom(err) => write!(f, "{}", err),
        }
    }
}   

impl Error for ElusionError {}

impl From<DataFusionError> for ElusionError {
    fn from(err: DataFusionError) -> Self {
        ElusionError::DataFusion(err)
    }
}

impl From<std::io::Error> for ElusionError {
    fn from(err: std::io::Error) -> Self {
        ElusionError::Io(err)
    }
}

pub type ElusionResult<T> = Result<T, ElusionError>;

// =====================  AGGREGATION BUILDER =============== //

#[derive(Clone)]
pub struct AggregationBuilder {
    column: String,
    pub agg_alias: Option<String>,
    agg_type: AggregationType,
    other_column: Option<String>,
    percentile: Option<f64>,
    nth_value: Option<i64>,
}

#[derive(Clone)]
enum AggregationType {
    Sum,
    Avg,
    Min,
    Max,
    StdDev,
    Count,
    CountDistinct,
    Corr,
    Grouping,
    VarPop,
    StdDevPop,
    ArrayAgg,
    ApproxPercentile,
    FirstValue,
    NthValue,
    //nove funkcije
    // LastValue,
    // ApproxDistinct,
    // ApproxMedian,
    // BitAnd,
    // BitOr,
    // BitXor,
    // CovarSamp,
    // VarSample,
    // RegrSlope,
    // RegrIntercept,
    // RegrCount,
    // RegrR2,
    // RegrAvgX,
    // RegrAvgY,
    // RegrSXX,
    // RegrSYY,

}

impl AggregationBuilder {
    pub fn new(column: &str) -> Self {
        Self {
            column: column.to_string(),
            agg_alias: None,
            agg_type: AggregationType::Sum, // default
            other_column: None,
            percentile: None,
            nth_value: None,
        }
    }

    pub fn build_expr(&self, table_alias: &str) -> Expr {
        let qualified_column = if self.column.contains('.') {
            col(&self.column)
        } else {
            col_with_relation(table_alias, &self.column)
        };

        let base_expr = match &self.agg_type {
            AggregationType::Sum => sum(qualified_column),
            AggregationType::Avg => avg(qualified_column),
            AggregationType::Min => min(qualified_column),
            AggregationType::Max => max(qualified_column),
            AggregationType::StdDev => stddev(qualified_column),
            AggregationType::Count => count(qualified_column),
            AggregationType::CountDistinct => count_distinct(qualified_column),
            AggregationType::Corr => {
                if let Some(ref other_col) = self.other_column {
                    corr(qualified_column, col(other_col))
                } else {
                    qualified_column
                }
            },
            AggregationType::Grouping => grouping(qualified_column),
            AggregationType::VarPop => var_pop(qualified_column),
            AggregationType::StdDevPop => stddev_pop(qualified_column),
            AggregationType::ArrayAgg => array_agg(qualified_column),
            AggregationType::ApproxPercentile => {
                if let Some(p) = self.percentile {
                    approx_percentile_cont(qualified_column, lit(p), None)
                } else {
                    qualified_column
                }
            },
            AggregationType::FirstValue => first_value(qualified_column, None),
            AggregationType::NthValue => {
                if let Some(n) = self.nth_value {
                    nth_value(qualified_column, n, vec![])
                } else {
                    qualified_column
                }
            },
            // AggregationType::LastValue => last_value(qualified_column),
            // AggregationType::ApproxDistinct => approx_distinct(qualified_column),
            // AggregationType::ApproxMedian => approx_median(qualified_column),
            // AggregationType::BitAnd => bit_and(qualified_column),
            // AggregationType::BitOr => bit_or(qualified_column),
            // AggregationType::BitXor => bit_xor(qualified_column),
            // AggregationType::CovarSamp => covar_samp(qualified_column),
            // AggregationType::VarSample => var_sample(qualified_column),
            // AggregationType::RegrSlope => regr_slope(qualified_column),
            // AggregationType::RegrIntercept => regr_intercept(qualified_column),
            // AggregationType::RegrCount => regr_count(qualified_column),
            // AggregationType::RegrR2 => regr_r2(qualified_column),
            // AggregationType::RegrAvgX => regr_avgx(qualified_column),
            // AggregationType::RegrAvgY => regr_avgy(qualified_column),
            // AggregationType::RegrSXX => regr_sxx(qualified_column),
            // AggregationType::RegrSYY => regr_syy(qualified_column),
            // AggregationType::RegrSXY => regr_sxy(qualified_column),
        };

        if let Some(alias) = &self.agg_alias {
            base_expr.alias(alias.clone())
        } else {
            base_expr
        }
    }

    pub fn alias(mut self, alias: &str) -> Self {
        self.agg_alias = Some(alias.to_lowercase());
        self
    }

    pub fn sum(mut self) -> Self {
        self.agg_type = AggregationType::Sum;
        self
    }

    pub fn avg(mut self) -> Self {
        self.agg_type = AggregationType::Avg;
        self
    }

    pub fn min(mut self) -> Self {
        self.agg_type = AggregationType::Min;
        self
    }

    pub fn max(mut self) -> Self {
        self.agg_type = AggregationType::Max;
        self
    }

    pub fn stddev(mut self) -> Self {
        self.agg_type = AggregationType::StdDev;
        self
    }

    pub fn count(mut self) -> Self {
        self.agg_type = AggregationType::Count;
        self
    }

    pub fn count_distinct(mut self) -> Self {
        self.agg_type = AggregationType::CountDistinct;
        self
    }

    pub fn grouping(mut self) -> Self {
        self.agg_type = AggregationType::Grouping;
        self
    }

    pub fn var_pop(mut self) -> Self {
        self.agg_type = AggregationType::VarPop;
        self
    }

    pub fn stddev_pop(mut self) -> Self {
        self.agg_type = AggregationType::StdDevPop;
        self
    }

    pub fn array_agg(mut self) -> Self {
        self.agg_type = AggregationType::ArrayAgg;
        self
    }

    pub fn first_value(mut self) -> Self {
        self.agg_type = AggregationType::FirstValue;
        self
    }
    
    pub fn approx_percentile(mut self, percentile: f64) -> Self {
        self.agg_type = AggregationType::ApproxPercentile;
        self.percentile = Some(percentile);
        self
    }

    pub fn nth_value(mut self, n: i64) -> Self {
        self.agg_type = AggregationType::NthValue;
        self.nth_value = Some(n);
        self
    }

    pub fn corr(mut self, other_column: &str) -> Self {
        self.agg_type = AggregationType::Corr;
        self.other_column = Some(other_column.to_string());
        self
    }

    // pub fn last_value(mut self) -> Self {
    //     self.agg_type = AggregationType::LastValue;
    //     self
    // }

    // pub fn approx_distinct(mut self) -> Self {
    //     self.agg_type = AggregationType::ApproxDistinct;
    //     self
    // }

    // pub fn approx_median(mut self) -> Self {
    //     self.agg_type = AggregationType::ApproxMedian;
    //     self
    // }

    // pub fn bit_and(mut self) -> Self {
    //     self.agg_type = AggregationType::BitAnd;
    //     self
    // }

    // pub fn bit_or(mut self) -> Self {
    //     self.agg_type = AggregationType::BitOr;
    //     self
    // }

    // pub fn bit_xor(mut self) -> Self {
    //     self.agg_type = AggregationType::BitXor;
    //     self
    // }

    // pub fn covar_samp(mut self) -> Self {
    //     self.agg_type = AggregationType::CovarSamp;
    //     self
    // }

    // pub fn var_sample(mut self) -> Self {
    //     self.agg_type = AggregationType::VarSample;
    //     self
    // }

    // // Regression functions
    // pub fn regr_slope(mut self) -> Self {
    //     self.agg_type = AggregationType::RegrSlope;
    //     self
    // }

    // pub fn regr_intercept(mut self) -> Self {
    //     self.agg_type = AggregationType::RegrIntercept;
    //     self
    // }

    // pub fn regr_count(mut self) -> Self {
    //     self.agg_type = AggregationType::RegrCount;
    //     self
    // }

    // pub fn regr_r2(mut self) -> Self {
    //     self.agg_type = AggregationType::RegrR2;
    //     self
    // }

    // pub fn regr_avgx(mut self) -> Self {
    //     self.agg_type = AggregationType::RegrAvgX;
    //     self
    // }

    // pub fn regr_avgy(mut self) -> Self {
    //     self.agg_type = AggregationType::RegrAvgY;
    //     self
    // }

    // pub fn regr_sxx(mut self) -> Self {
    //     self.agg_type = AggregationType::RegrSXX;
    //     self
    // }

    // pub fn regr_syy(mut self) -> Self {
    //     self.agg_type = AggregationType::RegrSYY;
    //     self
    // }

    // pub fn regr_sxy(mut self) -> Self {
    //     self.agg_type = AggregationType::RegrSXY;
    //     self
    // }
}

// =================== CSV DETECT DEFECT ======================= //
/// Fucntion that detects defects in UTF8 for CSV files
pub fn csv_detect_defect_utf8(file_path: &str) -> Result<(), io::Error> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);

    for (line_number, line) in reader.split(b'\n').enumerate() {
        let line = line?;
        if let Err(err) = std::str::from_utf8(&line) {
            eprintln!(
                "Invalid UTF-8 detected on line {}: {:?}. Error: {:?}",
                line_number + 1,
                line,
                err
            );
            return Err(io::Error::new(io::ErrorKind::InvalidData, err));
        }
    }

    Ok(())
}

/// Function that converts invalid UTF8 for CSV files
pub fn convert_invalid_utf8(file_path: &str) -> Result<(), io::Error> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);

    let temp_file_path = format!("{}.temp", file_path);
    let mut temp_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp_file_path)?;

    for line in reader.split(b'\n') {
        let line = line?;
        match std::str::from_utf8(&line) {
            Ok(valid_utf8) => writeln!(temp_file, "{}", valid_utf8)?,
            Err(_) => {
                // Convert invalid UTF-8 to valid UTF-8 using a fallback encoding
                let (decoded, _, had_errors) = WINDOWS_1252.decode(&line);
                if had_errors {
                    eprintln!("Warning: Found invalid UTF-8 data and converted it to valid UTF-8.");
                }
                writeln!(temp_file, "{}", decoded)?;
            }
        }
    }

    // Replace original file with cleaned file
    std::fs::rename(temp_file_path, file_path)?;

    Ok(())
}

// ====================== PARSE DATES ======================== //

fn parse_date_with_formats(date_str: &str) -> Option<i32> {
    let formats = vec![
        "%Y-%m-%d", "%d.%m.%Y", "%m/%d/%Y", "%d-%b-%Y", "%a, %d %b %Y",
        "%Y/%m/%d", "%Y/%m", "%Y-%m-%dT%H:%M:%S%z", "%d%b%Y",
    ];

    // Days offset from Chrono CE to Unix epoch (1970-01-01)
    const UNIX_EPOCH_DAYS_FROM_CE: i32 = 719_163;

    for format in formats {
        if let Ok(date) = NaiveDate::parse_from_str(date_str, format) {
            let days_since_epoch = date.num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE;
            // println!(
            //     "Parsed '{}' as {:?} (Days since epoch: {}) using format '{}'",
            //     date_str, date, days_since_epoch, format
            // );
            return Some(days_since_epoch);
        }
    }
    // println!("Failed to parse date '{}'", date_str);
    None
}

/// Functions for checking quailified columns
fn col_with_relation(relation: &str, column: &str) -> Expr {
    if column.contains('.') {
        col(column) // Already qualified
    } else if !relation.is_empty() {
        col(&format!("{}.{}", relation, column)) // Add table alias
    } else {
        col(column) // Use column name as is
    }
}

/// Normalizes column naame by trimming whitespace,converting it to lowercase and replacing empty spaces with underscore.
fn normalize_column_name(name: &str) -> String {
    name.trim().to_lowercase().replace(" ", "_")
}

/// Normalizes an alias by trimming whitespace and converting it to lowercase.
fn normalize_alias(alias: &str) -> String {
    alias.trim().to_lowercase()
}

/// Normalizes a condition string by converting it to lowercase.
fn normalize_condition(condition: &str) -> String {
    condition.trim().to_lowercase()
}

// =============== PArquet LOAD options
// pub struct ParquetLoadOptions {
//     pub sort_columns: Option<Vec<SortOption>>,
//     pub partition_columns: Option<Vec<String>>
// }

// impl Default for ParquetLoadOptions {
//     fn default() -> Self {
//         ParquetLoadOptions {
//             sort_columns: None,
//             partition_columns: None,
//         }
//     }
// }
// pub struct SortOption {
//     pub column: String,
//     pub descending: bool,
//     pub nulls_first: bool,
// }


/// Create a schema dynamically from the `DataFrame` as helper for with_cte
fn cte_schema(dataframe: &DataFrame, alias: &str) -> Arc<Schema> {
   
    let fields: Vec<Field> = dataframe
        .schema()
        .fields()
        .iter()
        .map(|df_field| {
            Field::new(
                &format!("{}.{}", alias, df_field.name()), 
                df_field.data_type().clone(),             
                df_field.is_nullable(),                   
            )
        })
        .collect();

  
    Arc::new(Schema::new(fields))
}

// =================== JSON heler functions

#[derive(Deserialize, Serialize, Debug)]
struct GenericJson {
    #[serde(flatten)]
    fields: HashMap<String, Value>,
}

/// Deserializes a JSON string into the GenericJson struct.
// fn deserialize_generic_json(json_str: &str) -> serde_json::Result<GenericJson> {
//     serde_json::from_str(json_str)
// }



/// Flattens the GenericJson struct into a single-level HashMap.
fn flatten_generic_json(data: GenericJson) -> HashMap<String, Value> {
    let mut map = HashMap::new();
    // Convert HashMap to serde_json::Map
    let serde_map: Map<String, Value> = data.fields.clone().into_iter().collect();
    flatten_json_value(&Value::Object(serde_map), "", &mut map);
    map
}

/// Function to infer schema from rows
fn infer_schema_from_json(rows: &[HashMap<String, Value>]) -> SchemaRef {
    let mut fields_map: HashMap<String, ArrowDataType> = HashMap::new();
    let mut keys_set: HashSet<String> = HashSet::new();

    for row in rows {
        for (k, v) in row {
            keys_set.insert(k.clone());
            let inferred_type = infer_arrow_type(v);
            // If the key already exists, ensure the type is compatible 
            fields_map
                .entry(k.clone())
                .and_modify(|existing_type| {
                    *existing_type = promote_types(existing_type.clone(), inferred_type.clone());
                })
                .or_insert(inferred_type);
        }
    }

    let fields: Vec<Field> = keys_set.into_iter().map(|k| {
        let data_type = fields_map.get(&k).unwrap_or(&ArrowDataType::Utf8).clone();
        Field::new(&k, data_type, true)
    }).collect();

    Arc::new(Schema::new(fields))
}

fn infer_arrow_type(value: &Value) -> ArrowDataType {
    match value {
        Value::Null => ArrowDataType::Utf8, 
        Value::Bool(_) => ArrowDataType::Boolean,
        Value::Number(n) => {
            if n.is_i64() {
                ArrowDataType::Int64
            } else if n.is_u64() {
                ArrowDataType::UInt64
            } else {
                ArrowDataType::Float64
            }
        },
        Value::String(_) => ArrowDataType::Utf8,
        Value::Array(_) => ArrowDataType::Utf8, 
        Value::Object(_) => ArrowDataType::Utf8, 
    }
}

fn promote_types(a: ArrowDataType, b: ArrowDataType) -> ArrowDataType {
   
    match (a, b) {
        (Utf8, _) | (_, Utf8) => Utf8,
        (Boolean, Boolean) => Boolean,
        (Int64, Int64) => Int64,
        (UInt64, UInt64) => UInt64,
        (Float64, Float64) => Float64,
        _ => Utf8, // Default promotion to Utf8 for incompatible types
    }
}

fn build_record_batch(
    rows: &[HashMap<String, Value>],
    schema: Arc<Schema>
) -> ArrowResult<RecordBatch> {
    let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();

    for field in schema.fields() {
        let builder: Box<dyn ArrayBuilder> = match field.data_type() {
            ArrowDataType::Utf8 => Box::new(StringBuilder::new()),
            ArrowDataType::Binary => Box::new(BinaryBuilder::new()),
            ArrowDataType::Boolean => Box::new(BooleanBuilder::new()),
            ArrowDataType::Int32 => Box::new(Int32Builder::new()),
            ArrowDataType::Int64 => Box::new(Int64Builder::new()),
            ArrowDataType::UInt32 => Box::new(UInt32Builder::new()),
            ArrowDataType::UInt64 => Box::new(UInt64Builder::new()),
            ArrowDataType::Float32 => Box::new(Float32Builder::new()),
            ArrowDataType::Float64 => Box::new(Float64Builder::new()),
            ArrowDataType::Date32 => Box::new(Date32Builder::new()),
           
            _ => Box::new(StringBuilder::new()), // Default to Utf8 for unsupported types
        };
        builders.push(builder);
    }

    for row in rows {
        for (i, field) in schema.fields().iter().enumerate() {
            let key = field.name();
            let value = row.get(key);

            match field.data_type() {
                ArrowDataType::Utf8 => {
                    let builder = builders[i]
                        .as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .expect("Expected StringBuilder for Utf8 field");
                    if let Some(Value::String(s)) = value {
                        builder.append_value(s);
                    } else if let Some(Value::Number(n)) = value {
                        // Handle numbers as strings
                        builder.append_value(&n.to_string());
                    } else {
                        builder.append_null();
                    }
                },
                ArrowDataType::Binary => {
                    let builder = builders[i]
                        .as_any_mut()
                        .downcast_mut::<BinaryBuilder>()
                        .expect("Expected BinaryBuilder for Binary field");
                    if let Some(Value::String(s)) = value {
                        // Assuming the binary data is base64 encoded or similar
                        // Here, we'll convert the string to bytes directly
                        builder.append_value(s.as_bytes());
                    } else {
                        builder.append_null();
                    }
                },
                ArrowDataType::Boolean => {
                    let builder = builders[i]
                        .as_any_mut()
                        .downcast_mut::<BooleanBuilder>()
                        .expect("Expected BooleanBuilder for Boolean field");
                    if let Some(Value::Bool(b)) = value {
                        builder.append_value(*b);
                    } else {
                        builder.append_null();
                    }
                },
                ArrowDataType::Int32 => {
                    let builder = builders[i]
                        .as_any_mut()
                        .downcast_mut::<Int32Builder>()
                        .expect("Expected Int32Builder for Int32 field");
                    if let Some(Value::Number(n)) = value {
                        if let Some(i) = n.as_i64() {
                            if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                                builder.append_value(i as i32);
                            } else {
                                builder.append_null();
                            }
                        } else {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                },
                ArrowDataType::Int64 => {
                    let builder = builders[i]
                        .as_any_mut()
                        .downcast_mut::<Int64Builder>()
                        .expect("Expected Int64Builder for Int64 field");
                    if let Some(Value::Number(n)) = value {
                        if let Some(i) = n.as_i64() {
                            builder.append_value(i);
                        } else {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                },
                ArrowDataType::UInt32 => {
                    let builder = builders[i]
                        .as_any_mut()
                        .downcast_mut::<UInt32Builder>()
                        .expect("Expected UInt32Builder for UInt32 field");
                    if let Some(Value::Number(n)) = value {
                        if let Some(u) = n.as_u64() {
                            if u <= u32::MAX as u64 {
                                builder.append_value(u as u32);
                            } else {
                                builder.append_null();
                            }
                        } else {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                },
                ArrowDataType::UInt64 => {
                    let builder = builders[i]
                        .as_any_mut()
                        .downcast_mut::<UInt64Builder>()
                        .expect("Expected UInt64Builder for UInt64 field");
                    if let Some(Value::Number(n)) = value {
                        if let Some(u) = n.as_u64() {
                            builder.append_value(u);
                        } else {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                },
                
                ArrowDataType::Float32 => {
                    let builder = builders[i]
                        .as_any_mut()
                        .downcast_mut::<Float32Builder>()
                        .expect("Expected Float32Builder for Float32 field");
                    if let Some(Value::Number(n)) = value {
                        if let Some(f) = n.as_f64() {
                            builder.append_value(f as f32);
                        } else {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                },
                ArrowDataType::Float64 => {
                    let builder = builders[i]
                        .as_any_mut()
                        .downcast_mut::<Float64Builder>()
                        .expect("Expected Float64Builder for Float64 field");
                    if let Some(Value::Number(n)) = value {
                        if let Some(f) = n.as_f64() {
                            builder.append_value(f);
                        } else {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                },
                ArrowDataType::Date32 => {
                    let builder = builders[i]
                        .as_any_mut()
                        .downcast_mut::<Date32Builder>()
                        .expect("Expected Date32Builder for Date32 field");
                    if let Some(Value::String(s)) = value {
                        //  date string into days since UNIX epoch
                        // Here, we assume the date is in "YYYY-MM-DD" format
                        match NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                            Ok(date) => {
                                // UNIX epoch
                                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
                                    .expect("Failed to create epoch date");

                                let days_since_epoch = (date - epoch).num_days() as i32;
                               
                                builder.append_value(days_since_epoch);
                            }
                            Err(_) => {
                             
                                builder.append_null();
                            }
                        }
                    } else {
                      
                        builder.append_null();
                    }
                },
                _ => {
                    
                    let builder = builders[i]
                        .as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .expect("Expected StringBuilder for default field type");
                    if let Some(Value::String(s)) = value {
                        builder.append_value(s);
                    } else if let Some(v) = value {
                    
                        builder.append_value(&v.to_string());
                    } else {
                        builder.append_null();
                    }
                },
            }
        }
    }

    let mut arrays: Vec<ArrayRef> = Vec::new();
    for mut builder in builders {
        arrays.push(builder.finish());
    }

    let record_batch = RecordBatch::try_new(schema.clone(), arrays)?;

    Ok(record_batch)
}

fn read_file_to_string(file_path: &str) -> Result<String, io::Error> {
    let mut file = File::open(file_path).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("Failed to open file '{}': {}", file_path, e),
        )
    })?;
    let mut contents = String::new();
    file.read_to_string(&mut contents).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("Failed to read contents of file '{}': {}", file_path, e),
        )
    })?;
    Ok(contents)
}

async fn create_dataframe_from_json(json_str: &str, alias: &str) -> Result<DataFrame, DataFusionError> {
  
    let generic_json: GenericJson = serde_json::from_str(json_str)
    .map_err(|e| DataFusionError::Execution(format!("Failed to deserialize JSON: {}", e)))?;    

    let flattened = flatten_generic_json(generic_json);

    let rows = vec![flattened];

    let schema = infer_schema_from_json(&rows);

    let record_batch = build_record_batch(&rows, schema.clone())
    .map_err(|e| DataFusionError::Execution(format!("Failed to build RecordBatch: {}", e)))?;

    let partitions = vec![vec![record_batch]];
    let mem_table = MemTable::try_new(schema.clone(), partitions)
    .map_err(|e| DataFusionError::Execution(format!("Failed to create MemTable: {}", e)))?;

    let ctx = SessionContext::new();

    ctx.register_table(alias, Arc::new(mem_table))
    .map_err(|e| DataFusionError::Execution(format!("Failed to register Table: {}", e)))?;

    let df = ctx.table(alias).await?;

    Ok(df)
}

/// Creates a DataFusion DataFrame from multiple JSON records.
async fn create_dataframe_from_multiple_json(json_str: &str, alias: &str) -> Result<DataFrame, DataFusionError> {

    let generic_jsons: Vec<GenericJson> = serde_json::from_str(json_str)
        .map_err(|e| DataFusionError::Execution(format!("Failed to deserialize JSON: {}", e)))?;
    
    let mut rows = Vec::new();
    for generic_json in generic_jsons {
        let flattened = flatten_generic_json(generic_json);
        rows.push(flattened);
    }
    
    let schema = infer_schema_from_json(&rows);
    
    let record_batch = build_record_batch(&rows, schema.clone())
        .map_err(|e| DataFusionError::Execution(format!("Failed to build RecordBatch: {}", e)))?;
    
    let partitions = vec![vec![record_batch]];
    let mem_table = MemTable::try_new(schema.clone(), partitions)
        .map_err(|e| DataFusionError::Execution(format!("Failed to create MemTable: {}", e)))?;
    
    let ctx = SessionContext::new();
    
    ctx.register_table(alias, Arc::new(mem_table))
        .map_err(|e| DataFusionError::Execution(format!("Failed to register table '{}': {}", alias, e)))?;
    
    let df = ctx.table(alias).await
        .map_err(|e| DataFusionError::Execution(format!("Failed to retrieve DataFrame for table '{}': {}", alias, e)))?;
    
    Ok(df)
}

    /// Recursively flattens JSON values, serializing objects and arrays to strings.
    fn flatten_json_value(value: &Value, prefix: &str, out: &mut HashMap<String, Value>) {
        match value {
            Value::Object(map) => {
                for (k, v) in map {
                    let new_key = if prefix.is_empty() {
                        k.clone()
                    } else {
                        format!("{}.{}", prefix, k)
                    };
                    flatten_json_value(v, &new_key, out);
                }
            },
            Value::Array(arr) => {
                for (i, v) in arr.iter().enumerate() {
                    let new_key = if prefix.is_empty() {
                        i.to_string()
                    } else {
                        format!("{}.{}", prefix, i)
                    };
                    flatten_json_value(v, &new_key, out);
                }
            },
            // If it's a primitive (String, Number, Bool, or Null), store as is.
            other => {
                out.insert(prefix.to_owned(), other.clone());
            },
        }
    }



//======================= CSV WRITING OPTION ============================//

/// constants for validating date and time formats
// const ALLOWED_DATE_FORMATS: &[&str] = &[
//     "%Y-%m-%d",
//     "%d.%m.%Y",
//     "%m/%d/%Y",
//     "%d-%b-%Y",
//     "%a, %d %b %Y",
//     "%Y/%m/%d",
//     "%d%b%Y",
// ];

// const ALLOWED_TIME_FORMATS: &[&str] = &[
//     "%H:%M:%S",
//     "%H-%M-%S",
//     "%I:%M:%S %p",
// ];

// const ALLOWED_TIMESTAMP_FORMATS: &[&str] = &[
   
//     "%Y-%m-%d %H:%M:%S",
//     "%d.%m.%Y %H:%M:%S",
// ];

// const ALLOWED_TIMESTAMP_TZ_FORMATS: &[&str] = &[
//     "%Y-%m-%dT%H:%M:%S%z",
// ];

/// Struct to encapsulate CSV write options
#[derive(Debug, Clone)]
pub struct CsvWriteOptions {
    pub delimiter: u8,
    pub escape: u8,
    pub quote: u8,
    pub double_quote: bool,
    // pub date_format: Option<String>,
    // pub time_format: Option<String>,
    // pub timestamp_format: Option<String>,
    // pub timestamp_tz_format: Option<String>,
    pub null_value: String,
}

impl Default for CsvWriteOptions {
    fn default() -> Self {
        Self {
            delimiter: b',',
            escape: b'\\',
            quote: b'"',
            double_quote: true,
            // date_format: None,// "%Y-%m-%d".to_string(),
            // time_format: None, // "%H-%M-%S".to_string(),
            // timestamp_format: None, //"%Y-%m-%d %H:%M:%S".to_string(),
            // timestamp_tz_format: None, // "%Y-%m-%dT%H:%M:%S%z".to_string(),
            null_value: "NULL".to_string(),
        }
    }
}

impl CsvWriteOptions {
    pub fn validate(&self) -> Result<(), ElusionError> {

        // Validate delimiter
        if !self.delimiter.is_ascii() {
            return Err(ElusionError::Custom(format!(
                "Delimiter '{}' is not a valid ASCII character.",
                self.delimiter as char
            )));
        }
        
        // Validate escape character
        if !self.escape.is_ascii() {
            return Err(ElusionError::Custom(format!(
                "Escape character '{}' is not a valid ASCII character.",
                self.escape as char
            )));
        }

        // Validate quote character
        if !self.quote.is_ascii() {
            return Err(ElusionError::Custom(format!(
                "Quote character '{}' is not a valid ASCII character.",
                self.quote as char
            )));
        }
        
        // Validate null_value
        if self.null_value.trim().is_empty() {
            return Err(ElusionError::Custom("Null value representation cannot be empty.".to_string()));
        }
        
        // Ensure null_value does not contain delimiter or quote characters
        let delimiter_char = self.delimiter as char;
        let quote_char = self.quote as char;
        
        if self.null_value.contains(delimiter_char) {
            return Err(ElusionError::Custom(format!(
                "Null value '{}' cannot contain the delimiter '{}'.",
                self.null_value, delimiter_char
            )));
        }
        
        if self.null_value.contains(quote_char) {
            return Err(ElusionError::Custom(format!(
                "Null value '{}' cannot contain the quote character '{}'.",
                self.null_value, quote_char
            )));
        }
        
        Ok(())
    }
}

// =============== DELTA TABLE writing
    /// Attempt to glean the Arrow schema of a DataFusion `DataFrame` by collecting
    /// a **small sample** (up to 1 row). If there's **no data**, returns an empty schema
    /// or an error
    async fn glean_arrow_schema(df: &DataFrame) -> Result<SchemaRef, DataFusionError> {

        let limited_df = df.clone().limit(0, Some(1))?;
        
        let batches = limited_df.collect().await?;

        if let Some(first_batch) = batches.get(0) {
            Ok(first_batch.schema())
        } else {
            let empty_fields: Vec<Field> = vec![];
            let empty_schema = Schema::new(empty_fields);
            Ok(Arc::new(empty_schema))
        }
    }

        // Helper function to convert Arrow DataType to Delta DataType
        fn arrow_to_delta_type(arrow_type: &ArrowDataType) -> DeltaType {
    
            match arrow_type {
                ArrowDataType::Boolean => DeltaType::BOOLEAN,
                ArrowDataType::Int8 => DeltaType::BYTE,
                ArrowDataType::Int16 => DeltaType::SHORT,
                ArrowDataType::Int32 => DeltaType::INTEGER,
                ArrowDataType::Int64 => DeltaType::LONG,
                ArrowDataType::Float32 => DeltaType::FLOAT,
                ArrowDataType::Float64 => DeltaType::DOUBLE,
                ArrowDataType::Utf8 => DeltaType::STRING,
                ArrowDataType::Date32 => DeltaType::DATE,
                ArrowDataType::Date64 => DeltaType::DATE,
                ArrowDataType::Timestamp(TimeUnit::Second, _) => DeltaType::TIMESTAMP,
                ArrowDataType::Timestamp(TimeUnit::Millisecond, _) => DeltaType::TIMESTAMP,
                ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => DeltaType::TIMESTAMP,
                ArrowDataType::Timestamp(TimeUnit::Nanosecond, _) => DeltaType::TIMESTAMP,
                ArrowDataType::Binary => DeltaType::BINARY,
                _ => DeltaType::STRING, // Default to String for unsupported types
            }
        }
        // Helper function to transfer Delta Types to Arrow types
        // fn get_arrow_type_from_delta_schema(metadata: &Metadata, column_name: &str) -> ArrowDataType {
        //     if let Ok(schema) = metadata.schema() {
        //         for field in schema.fields() {
        //             if field.name() == column_name {
        //                 return match field.data_type() {
        //                     &DeltaType::BOOLEAN => ArrowDataType::Boolean,
        //                     &DeltaType::BYTE => ArrowDataType::Int8,
        //                     &DeltaType::SHORT => ArrowDataType::Int16,
        //                     &DeltaType::INTEGER => ArrowDataType::Int32,
        //                     &DeltaType::LONG => ArrowDataType::Int64,
        //                     &DeltaType::FLOAT => ArrowDataType::Float32,
        //                     &DeltaType::DOUBLE => ArrowDataType::Float64,
        //                     &DeltaType::STRING => ArrowDataType::Utf8,
        //                     &DeltaType::BINARY => ArrowDataType::Binary,
        //                     &DeltaType::DATE => {
        //                         ArrowDataType::Date32
        //                     },
        //                     &DeltaType::TIMESTAMP => {
        //                         ArrowDataType::Timestamp(TimeUnit::Microsecond, None)
        //                     },
        //                     _ => ArrowDataType::Utf8,
        //                 };
        //             }
        //         }
        //     }
        //     // Default to Utf8 if we can't get schema or column not found
        //     ArrowDataType::Utf8
        // }

    /// Helper struct to manage path conversions between different path types
    #[derive(Clone)]
    pub struct DeltaPathManager {
        base_path: PathBuf,
    }

    impl DeltaPathManager {
        /// Create a new DeltaPathManager from a string path
        pub fn new<P: AsRef<LocalPath>>(path: P) -> Self {
            let normalized = path
                .as_ref()
                .to_string_lossy()
                .replace('\\', "/")
                .trim_end_matches('/')
                .to_string();
            
            Self {
                base_path: PathBuf::from(normalized),
            }
        }

        /// Get the base path as a string with forward slashes
        pub fn base_path_str(&self) -> String {
            self.base_path.to_string_lossy().replace('\\', "/")
        }

        /// Get the delta log path
        pub fn delta_log_path(&self) -> DeltaPath {
            let base = self.base_path_str();
            DeltaPath::from(format!("{base}/_delta_log"))
        }

        /// Convert to ObjectStorePath
        pub fn to_object_store_path(&self) -> ObjectStorePath {
            ObjectStorePath::from(self.base_path_str())
        }

        /// Get path for table operations
        pub fn table_path(&self) -> String {
            self.base_path_str()
        }
        /// Get the drive prefix (e.g., "C:/", "D:/") from the base path
        pub fn drive_prefix(&self) -> String {
            let base_path = self.base_path_str();
            if let Some(colon_pos) = base_path.find(':') {
                base_path[..colon_pos + 2].to_string() // Include drive letter, colon, and slash
            } else {
                "/".to_string() // Fallback for non-Windows paths
            }
        }

        /// Normalize a file URI with the correct drive letter
        pub fn normalize_uri(&self, uri: &str) -> String {
            let drive_prefix = self.drive_prefix();
            
            // removing any existing drive letter prefix pattern and leading slashes
            let path = uri.trim_start_matches(|c| c != '/' && c != '\\')
                .trim_start_matches(['/', '\\']);
            
            // correct drive prefix and normalize separators
            format!("{}{}", drive_prefix, path).replace('\\', "/")
        }

        pub fn is_delta_table(&self) -> bool {
            let delta_log = self.base_path.join("_delta_log");
            let delta_log_exists = delta_log.is_dir();
            
            if delta_log_exists {
                // Additional check: is .json files in _delta_log
                if let Ok(entries) = fs::read_dir(&delta_log) {
                    for entry in entries {
                        if let Ok(entry) = entry {
                            if let Some(ext) = entry.path().extension() {
                                if ext == "json" {
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
            false
        }
    }

    /// Helper function to append a Protocol action to the Delta log
    async fn append_protocol_action(
        store: &Arc<dyn ObjectStore>,
        delta_log_path: &DeltaPath,
        protocol_action: Value,
    ) -> Result<(), DeltaTableError> {
    
        let latest_version = get_latest_version(store, delta_log_path).await?;
        let next_version = latest_version + 1;
        let protocol_file = format!("{:020}.json", next_version);

        let child_path = delta_log_path.child(&*protocol_file);
        
        let protocol_file_path = DeltaPath::from(child_path);

        let action_str = serde_json::to_string(&protocol_action)
            .map_err(|e| DeltaTableError::Generic(format!("Failed to serialize Protocol action: {e}")))?;

        store
            .put(&protocol_file_path, action_str.into_bytes().into())
            .await
            .map_err(|e| DeltaTableError::Generic(format!("Failed to write Protocol action to Delta log: {e}")))?;

        Ok(())
    }

    /// Helper function to get the latest version number in the Delta log
    async fn get_latest_version(
        store: &Arc<dyn ObjectStore>,
        delta_log_path: &DeltaPath,
    ) -> Result<i64, DeltaTableError> {
        let mut versions = Vec::new();
    
        let mut stream = store.list(Some(delta_log_path));
    
        while let Some(res) = stream.next().await {
            let metadata = res.map_err(|e| DeltaTableError::Generic(format!("Failed to list Delta log files: {e}")))?;
            // Get the location string from ObjectMeta
            let path_str = metadata.location.as_ref();

            if let Some(file_name) = path_str.split('/').last() {
                println!("Detected log file: {}", file_name); 
                if let Some(version_str) = file_name.strip_suffix(".json") {
                    if let Ok(version) = version_str.parse::<i64>() {
                        println!("Parsed version: {}", version);
                        versions.push(version);
                    }
                }
            }
        }
    
        let latest = versions.into_iter().max().unwrap_or(-1);
        println!("Latest version detected: {}", latest); 
        Ok(latest)
    }

    /// This is the lower-level writer function that actually does the work
    async fn write_to_delta_impl(
        df: &DataFrame,
        path: &str,
        partition_columns: Option<Vec<String>>,
        overwrite: bool,
        write_mode: WriteMode,
    ) -> Result<(), DeltaTableError> {
        let path_manager = DeltaPathManager::new(path);
    
        // get the Arrow schema
        let arrow_schema_ref = glean_arrow_schema(df)
            .await
            .map_err(|e| DeltaTableError::Generic(format!("Could not glean Arrow schema: {e}")))?;
    
        // Convert Arrow schema to Delta schema fields
        let delta_fields: Vec<StructField> = arrow_schema_ref
            .fields()
            .iter()
            .map(|field| {
                let nullable = field.is_nullable();
                let name = field.name().clone();
                let data_type = arrow_to_delta_type(field.data_type());
                StructField::new(name, data_type, nullable)
            })
            .collect();
    
        //  basic configuration
        let mut config: HashMap<String, Option<String>> = HashMap::new();
        config.insert("delta.minWriterVersion".to_string(), Some("7".to_string()));
        config.insert("delta.minReaderVersion".to_string(), Some("3".to_string()));
    
        if overwrite {
            // Removing the existing directory if it exists
            if let Err(e) = fs::remove_dir_all(&path_manager.base_path) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    return Err(DeltaTableError::Generic(format!(
                        "Failed to remove existing directory at '{}': {e}",
                        path
                    )));
                }
            }
    
            //directory structure
            fs::create_dir_all(&path_manager.base_path)
                .map_err(|e| DeltaTableError::Generic(format!("Failed to create directory structure: {e}")))?;
    
            //  metadata with empty HashMap
            let metadata = Metadata::try_new(
                StructType::new(delta_fields.clone()),
                partition_columns.clone().unwrap_or_default(),
                HashMap::new()
            )?;
    
            // configuration in the metadata action
            let metadata_action = json!({
                "metaData": {
                    "id": metadata.id,
                    "name": metadata.name,
                    "description": metadata.description,
                    "format": {
                        "provider": "parquet",
                        "options": {}
                    },
                    "schemaString": metadata.schema_string,
                    "partitionColumns": metadata.partition_columns,
                    "configuration": {
                        "delta.minReaderVersion": "3",
                        "delta.minWriterVersion": "7"
                    },
                    "created_time": metadata.created_time
                }
            });
    
            // store and protocol
            let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
            let delta_log_path = path_manager.delta_log_path();
            let protocol = Protocol::new(3, 7);
    
            let protocol_action = json!({
                "protocol": {
                    "minReaderVersion": protocol.min_reader_version,
                    "minWriterVersion": protocol.min_writer_version,
                    "readerFeatures": [],
                    "writerFeatures": []
                }
            });
            append_protocol_action(&store, &delta_log_path, protocol_action).await?;
            append_protocol_action(&store, &delta_log_path, metadata_action).await?;
    
            // table initialzzation
            let _ = DeltaOps::try_from_uri(&path_manager.table_path())
                .await
                .map_err(|e| DeltaTableError::Generic(format!("Failed to init DeltaOps: {e}")))?
                .create()
                .with_columns(delta_fields.clone())
                .with_partition_columns(partition_columns.clone().unwrap_or_default())
                .with_save_mode(SaveMode::Overwrite)
                .with_configuration(config.clone())
                .await?;
        } else {
            // For append mode, check if table exists
            if !DeltaTableBuilder::from_uri(&path_manager.table_path()).build().is_ok() {
                // Create directory structure
                fs::create_dir_all(&path_manager.base_path)
                    .map_err(|e| DeltaTableError::Generic(format!("Failed to create directory structure: {e}")))?;
    
                // metadata with empty HashMap
                let metadata = Metadata::try_new(
                    StructType::new(delta_fields.clone()),
                    partition_columns.clone().unwrap_or_default(),
                    HashMap::new()
                )?;
    
                // configuration in the metadata action
                let metadata_action = json!({
                    "metaData": {
                        "id": metadata.id,
                        "name": metadata.name,
                        "description": metadata.description,
                        "format": {
                            "provider": "parquet",
                            "options": {}
                        },
                        "schemaString": metadata.schema_string,
                        "partitionColumns": metadata.partition_columns,
                        "configuration": {
                            "delta.minReaderVersion": "3",
                            "delta.minWriterVersion": "7"
                        },
                        "created_time": metadata.created_time
                    }
                });
    
                //  store and protocol
                let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
                let delta_log_path = path_manager.delta_log_path();
                let protocol = Protocol::new(3, 7);
    
                let protocol_action = json!({
                    "protocol": {
                        "minReaderVersion": protocol.min_reader_version,
                        "minWriterVersion": protocol.min_writer_version,
                        "readerFeatures": [],
                        "writerFeatures": []
                    }
                });
    
                append_protocol_action(&store, &delta_log_path, protocol_action).await?;
                append_protocol_action(&store, &delta_log_path, metadata_action).await?;
    
                //  table initialization
                let _ = DeltaOps::try_from_uri(&path_manager.table_path())
                    .await
                    .map_err(|e| DeltaTableError::Generic(format!("Failed to init DeltaOps: {e}")))?
                    .create()
                    .with_columns(delta_fields.clone())
                    .with_partition_columns(partition_columns.clone().unwrap_or_default())
                    .with_save_mode(SaveMode::Append)
                    .with_configuration(config.clone())
                    .await?;
            }
        }
    
        // Load table after initialization
        let mut table = DeltaTableBuilder::from_uri(&path_manager.table_path())
            .build()
            .map_err(|e| DeltaTableError::Generic(format!("Failed to build Delta table: {e}")))?;
    
        // Ensure table is loaded
        table.load()
            .await
            .map_err(|e| DeltaTableError::Generic(format!("Failed to load table: {e}")))?;
    
        // Write data
        let batches = df
            .clone()
            .collect()
            .await
            .map_err(|e| DeltaTableError::Generic(format!("DataFusion collect error: {e}")))?;
    
        let mut writer_config = HashMap::new();
        writer_config.insert("delta.protocol.minWriterVersion".to_string(), "7".to_string());
        writer_config.insert("delta.protocol.minReaderVersion".to_string(), "3".to_string());
    
        let mut writer = RecordBatchWriter::try_new(
            &path_manager.table_path(),
            arrow_schema_ref,
            partition_columns,
            Some(writer_config)
        )?;
    
        for batch in batches {
            writer.write_with_mode(batch, write_mode).await?;
        }
    
        let version = writer
            .flush_and_commit(&mut table)
            .await
            .map_err(|e| DeltaTableError::Generic(format!("Failed to flush and commit: {e}")))?;
    
        println!("Wrote data to Delta table at version: {version}");
        Ok(())
    }


// =================== CUSTOM DATA FRAME IMPLEMENTATION ================== //

#[derive(Clone)]
pub struct CustomDataFrame {
    pub df: DataFrame,
    pub table_alias: String,

    from_table: String,
    selected_columns: Vec<String>,
    alias_map: Vec<(String, Expr)>,
    aggregations: Vec<(String, Expr)>,
    group_by_columns: Vec<String>,
    where_conditions: Vec<String>,
    having_conditions: Vec<String>,
    order_by_columns: Vec<(String, bool)>,
    limit_count: Option<usize>,
    joins: Vec<JoinClause>,
    window_functions: Vec<WindowDefinition>,
    ctes: Vec<CTEDefinition>,
    subquery_source: Option<Box<CustomDataFrame>>,
    set_operations: Vec<SetOperation>,
    query: String,
    aggregated_df: Option<DataFrame>,
}

#[derive(Clone)]
struct JoinClause {
    join_type: JoinType,
    table: String,
    alias: String,
    on_left: String,
    on_right: String,
}

#[derive(Clone)]
struct WindowDefinition {
    func: String,
    column: String,
    partition_by: Vec<String>,
    order_by: Vec<String>,
    alias: Option<String>,
}

#[derive(Clone)]
struct CTEDefinition {
    schema: Arc<Schema>,
    name: String,
    cte_df: CustomDataFrame,
}

#[derive(Clone)]
enum SetOperationType {
    Union,
    Intersect,
    Except
}

#[derive(Clone)]
struct SetOperation {
    op_type: SetOperationType,
    df: CustomDataFrame,
    all: bool,
}

pub struct AliasedDataFrame {
    pub dataframe: DataFrame,
    pub alias: String,
}

// impl AliasedDataFrame {
//     /// Displays the DataFrame using DataFusion's show method.
//     pub async fn show(&self) -> Result<(), DataFusionError> {
//         self.dataframe.clone().show().await.map_err(|e| {
//             DataFusionError::Execution(format!("Failed to display DataFrame: {}", e))
//         })
//     }
// }

impl CustomDataFrame {
    /// NEW method for loading and schema definition
    pub async fn new<'a>(
        file_path: &'a str,
        // columns: Option<Vec<(&'a str, &'a str, bool)>>,
        alias: &'a str,
        // sort_cols: Option<Vec<SortOption>>,     
        // partition_cols: Option<Vec<String>>,
    ) -> Self {
        
        // let schema = if let Some(cols) = columns {
        //     Some(Arc::new(Self::create_schema_from_str(cols)))
        // } else {
        //     None
        // };
        // Load the file into a DataFrame
        let aliased_df = Self::load(file_path, alias ) //sort_cols, partition_cols
            .await
            .expect("Failed to load file");

            CustomDataFrame {
                df: aliased_df.dataframe,
                table_alias: aliased_df.alias,
                from_table: alias.to_string(),
                selected_columns: Vec::new(),
                alias_map: Vec::new(),
                aggregations: Vec::new(),
                group_by_columns: Vec::new(),
                where_conditions: Vec::new(),
                having_conditions: Vec::new(),
                order_by_columns: Vec::new(),
                limit_count: None,
                joins: Vec::new(),
                window_functions: Vec::new(),
                ctes: Vec::new(),
                subquery_source: None,
                set_operations: Vec::new(),
                query: String::new(),
                aggregated_df: None,
            }
    }

    // ============== SQL execution on Dataframes

    /// Helper function to register a DataFrame as a table provider in the given SessionContext
    async fn register_df_as_table(
        ctx: &SessionContext,
        table_name: &str,
        df: &DataFrame,
    ) -> ElusionResult<()> {
        let batches = df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        
        let schema = df.schema();
        
        let mem_table = MemTable::try_new(schema.clone().into(), vec![batches])
            .map_err(|e| ElusionError::DataFusion(e))?;
        
        ctx.register_table(table_name, Arc::new(mem_table))
            .map_err(|e| ElusionError::DataFusion(e))?;
        
        Ok(())
    }

    /// Execute a raw SQL query involving multiple CustomDataFrame instances and return a new CustomDataFrame with the results.
    ///
    /// # Arguments
    ///
    /// * `sql` - The raw SQL query string to execute.
    /// * `alias` - The alias name for the resulting DataFrame.
    /// * `additional_dfs` - A slice of references to other CustomDataFrame instances to be registered in the context.
    ///
    /// # Returns
    ///
    /// * `ElusionResult<Self>` - A new `CustomDataFrame` containing the result of the SQL query.
    pub async fn raw_sql(
        &self,
        sql: &str,
        alias: &str,
        dfs: &[&CustomDataFrame],
    ) -> ElusionResult<Self> {
        let ctx = Arc::new(SessionContext::new());

        Self::register_df_as_table(&ctx, &self.table_alias, &self.df).await?;

        for df in dfs {
            Self::register_df_as_table(&ctx, &df.table_alias, &df.df).await?;
        }

        let df = ctx.sql(sql).await.map_err(ElusionError::DataFusion)?;
        let batches = df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        let result_mem_table = MemTable::try_new(df.schema().clone().into(), vec![batches])
            .map_err(|e| ElusionError::DataFusion(e))?;

        ctx.register_table(alias, Arc::new(result_mem_table))
            .map_err(|e| ElusionError::DataFusion(e))?;

        let result_df = ctx.table(alias).await.map_err(|e| {
            ElusionError::Custom(format!(
                "Failed to retrieve table '{}': {}",
                alias, e
            ))
        })?;

        Ok(CustomDataFrame {
            df: result_df,
            table_alias: alias.to_string(),
            from_table: alias.to_string(),
            selected_columns: Vec::new(),
            alias_map: Vec::new(),
            aggregations: Vec::new(),
            group_by_columns: Vec::new(),
            where_conditions: Vec::new(),
            having_conditions: Vec::new(),
            order_by_columns: Vec::new(),
            limit_count: None,
            joins: Vec::new(),
            window_functions: Vec::new(),
            ctes: Vec::new(),
            subquery_source: None,
            set_operations: Vec::new(),
            query: sql.to_string(),
            aggregated_df: Some(df.clone()),
        })
    }
    
    // ======================= BUILDERS ==============================//

     /// CTEs builder
     fn build_ctes(&self) -> String {
        if self.ctes.is_empty() {
            "".to_string()
        } else {
            let cte_strs: Vec<String> = self.ctes.iter().map(|cte| {
                format!("{} AS ({})", cte.name, cte.cte_df.build_query())
            }).collect();
            format!("WITH {}", cte_strs.join(", "))
        }
    }

    /// FROM clause builder
    fn build_from_clause(&self) -> String {
        if let Some(sub) = &self.subquery_source {
            format!("FROM ({}) {}", sub.build_query(), self.from_table)
        } else {
            format!("FROM {}", self.from_table)
        }
    }

    /// JOIN clause builder
    fn build_joins(&self) -> String {
        let mut join_str = String::new();
        for jc in &self.joins {
            let jt = match jc.join_type {
                JoinType::Inner => "INNER JOIN",
                JoinType::Left => "LEFT JOIN",
                JoinType::Right => "RIGHT JOIN",
                JoinType::Full => "FULL JOIN",
                JoinType::LeftSemi => "LEFT SEMI JOIN",
                JoinType::RightSemi => "RIGHT SEMI JOIN",
                JoinType::LeftAnti => "LEFT ANTI JOIN",
                JoinType::RightAnti => "RIGHT ANTI JOIN",
                JoinType::LeftMark => "LEFT MARK JOIN",
            };
            join_str.push_str(&format!(" {} {} ON {} = {}", jt, jc.alias, jc.on_left, jc.on_right));
        }
        join_str
    }


    /// Set operations builder
    fn build_set_operations(&self) -> String {
        let mut s = String::new();
        for op in &self.set_operations {
            let op_str = match op.op_type {
                SetOperationType::Union => {
                    if op.all {
                        "UNION ALL"
                    } else {
                        "UNION"
                    }
                },
                SetOperationType::Intersect => {
                    if op.all {
                        "INTERSECT ALL"
                    } else {
                        "INTERSECT"
                    }
                },
                SetOperationType::Except => {
                    if op.all {
                        "EXCEPT ALL"
                    } else {
                        "EXCEPT"
                    }
                }
            };
            s.push_str(&format!(" {} ({})", op_str, op.df.build_query()));
        }
        s
    }

    /// Select clause builder 
    fn build_select_clause(&self) -> String {
        let mut cols = if self.aggregations.is_empty() {
            if self.selected_columns.is_empty() {
                vec!["*".to_string()]
            } else {
                self.selected_columns.clone()
            }
        } else {
            let mut c = self.group_by_columns.clone();
            c.extend(self.aggregations.iter().map(|(alias, _)| alias.clone()));
            if c.is_empty() {
                vec!["*".to_string()]
            } else {
                c
            }
        };

        if !self.window_functions.is_empty() {
            // Add window functions as additional columns
            let wfuncs = self.build_window_functions();
            if !wfuncs.is_empty() {
                cols.push(wfuncs);
            }
        }

        format!("SELECT {}", cols.join(", "))
    }

    /// Build the query string based on recorded transformations
    fn build_query(&self) -> String {
        let cte_str = self.build_ctes();

        let select_clause = self.build_select_clause();
        let from_clause = self.build_from_clause();
        let join_clause = self.build_joins();

        let mut sql = if cte_str.is_empty() {
            format!("{} {}{}", select_clause, from_clause, join_clause)
        } else {
            format!("{} {} {}{}", cte_str, select_clause, from_clause, join_clause)
        };

        if !self.where_conditions.is_empty() {
            sql.push_str(&format!(" WHERE {}", self.where_conditions.join(" AND ")));
        }

        if !self.group_by_columns.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", self.group_by_columns.join(", ")));
        }

        if !self.having_conditions.is_empty() {
            sql.push_str(&format!(" HAVING {}", self.having_conditions.join(" AND ")));
        }

        if !self.order_by_columns.is_empty() {
            let order_exprs = self.order_by_columns.iter()
                .map(|(col, asc)| format!("{} {}", col, if *asc { "ASC" } else { "DESC" }))
                .collect::<Vec<_>>().join(", ");
            sql.push_str(&format!(" ORDER BY {}", order_exprs));
        }

        if let Some(count) = self.limit_count {
            sql.push_str(&format!(" LIMIT {}", count));
        }

        let sets = self.build_set_operations();
        if !sets.is_empty() {
            sql.push_str(&sets);
        }

        sql
    }

    // =============== AGGREGATION HELPER ================ //

    /// AGGREAGATION helper
    pub fn aggregation<const N: usize>(self, aggregations: [AggregationBuilder; N]) -> Self {
        self.aggregation_vec(aggregations.to_vec())
    }

    pub fn aggregation_vec(mut self, aggregations: Vec<AggregationBuilder>) -> Self {
        for builder in aggregations {
            let expr = builder.build_expr(&self.table_alias);
            let alias = builder
                .agg_alias
                .clone()
                .unwrap_or_else(|| format!("{:?}", expr))
            .to_lowercase();
            self.alias_map.push((alias.clone(), expr.clone()));
            self.aggregations.push((alias, expr));
        }
        self
    }

    // =========================== SQL FUNCTIONS ===========================//

    /// FROM SUBQUERY clause
    pub fn from_subquery(mut self, sub_df: CustomDataFrame, alias: &str) -> Self {
        self.subquery_source = Some(Box::new(sub_df));
        self.table_alias = alias.to_string();
        self.from_table = alias.to_string();
        self
    }

    /// WITH CTE claUse
    pub fn with_cte(mut self, name: &str, cte_df: CustomDataFrame) -> Self {
        // cte_schema to extract schema with alias
        let schema = cte_schema(&cte_df.df, name);
    
        self.ctes.push(CTEDefinition {
            name: name.to_string(),
            cte_df,
            schema,
        });
    
        self
    }

    /// UNION clause
    pub fn union(mut self, other: CustomDataFrame, all: bool) -> Self {
        self.set_operations.push(SetOperation {
            op_type: SetOperationType::Union,
            df: other,
            all,
        });
        self
    }

    /// INTERSECT cluase
    pub fn intersect(mut self, other: CustomDataFrame, all: bool) -> Self {
        self.set_operations.push(SetOperation {
            op_type: SetOperationType::Intersect,
            df: other,
            all,
        });
        self
    }

    /// EXCEPT clause
    pub fn except(mut self, other: CustomDataFrame, all: bool) -> Self {
        self.set_operations.push(SetOperation {
            op_type: SetOperationType::Except,
            df: other,
            all,
        });
        self
    }

    /// SELECT clause
    pub fn select<const N: usize>(self, columns: [&str; N]) -> Self {
        self.select_vec(columns.to_vec())
    }
    pub fn select_vec(mut self, columns: Vec<&str>) -> Self {

        
        let mut expressions: Vec<Expr> = Vec::new();
        let mut selected_columns: Vec<String> = Vec::new();
        // Compiling the regex once outside the loop for efficiency
        let as_keyword = Regex::new(r"(?i)\s+as\s+").unwrap(); // Case-insensitive " AS "

        for c in columns {
            // Parse column and alias (if provided)
            let parts: Vec<&str> = as_keyword.split(c).map(|s| s.trim()).collect();
            let column_name = normalize_column_name(parts[0]);
            let alias: Option<String> = parts.get(1).map(|&alias| alias.to_string()); 

            let mut expr_resolved = false;

            // if the column is an aggregation alias in `alias_map`
            if let Some((_, agg_expr)) = self.alias_map.iter().find(|(a, _)| a == &column_name) {
                let final_expr = if let Some(ref new_alias) = alias {
                    agg_expr.clone().alias(new_alias.clone())
                } else {
                    col(column_name.as_str())
                };
                expressions.push(final_expr.clone());

                // Add to selected_columns
                if let Some(ref new_alias) = alias {
                    selected_columns.push(new_alias.clone());
                } else {
                    selected_columns.push(column_name.to_string());
                }
                expr_resolved = true;
            }

            // If not an aggregation alias, checking if the column name is fully qualified
            if !expr_resolved {
                let qualified_column = if column_name.contains('.') {
                    column_name.clone()
                } else {
                    format!("{}.{}", self.table_alias, column_name)
                };

                if self.df.schema().fields().iter().any(|field| *field.name() == qualified_column) {
                    let expr = col(&qualified_column);
                    if let Some(ref new_alias) = alias {
                        expressions.push(expr.alias(new_alias.clone()));
                        selected_columns.push(new_alias.clone());
                    } else {
                        expressions.push(expr);
                        selected_columns.push(qualified_column.clone());
                    }
                    expr_resolved = true;
                }
            }

            // If still not resolved, checking if the column exists without qualification
            if !expr_resolved {
                if self.df.schema().fields().iter().any(|f| *f.name() == column_name) {
                    // Column name matches directly
                    let expr = col(&column_name);
                    if let Some(ref new_alias) = alias {
                        expressions.push(expr.alias(new_alias.clone()));
                        selected_columns.push(new_alias.clone());
                    } else {
                        expressions.push(expr);
                        selected_columns.push(column_name.clone());
                    }
                    expr_resolved = true;
                }
            }

            // If not resolved yet, checking if the column belongs to a CTE via JoinClause
            if !expr_resolved {
                for join in &self.joins {
                    if let Some(cte) = self.ctes.iter().find(|cte| cte.name == join.table) {
                        let qualified_name = format!("{}.{}", cte.name, column_name); // Fully qualify column name
                        if cte.schema.fields().iter().any(|field| *field.name() == qualified_name) {
                            let expr = col(&qualified_name);
                            if let Some(ref new_alias) = alias {
                                expressions.push(expr.alias(new_alias.clone()));
                                selected_columns.push(new_alias.clone());
                            } else {
                                expressions.push(expr);
                                selected_columns.push(qualified_name.clone());
                            }
                            expr_resolved = true;
                            break;
                        }
                    }
                }
            }

            // Check if column is a window function alias
            if !expr_resolved && self.window_functions.iter().any(|w| w.alias.as_ref().map_or(false, |a| a == &column_name)) {
                let expr = col_with_relation(&self.table_alias, &column_name);
                if let Some(ref new_alias) = alias {
                    expressions.push(expr.alias(new_alias.clone()));
                    selected_columns.push(new_alias.clone());
                } else {
                    expressions.push(expr);
                    selected_columns.push(column_name.clone());
                }
                expr_resolved = true;
            }

            // Fallback to table alias resolution for normal columns
            if !expr_resolved {
                let col_name = normalize_column_name(c);
                let expr = col_with_relation(&self.table_alias, &col_name);
                if let Some(ref new_alias) = alias {
                    expressions.push(expr.alias(new_alias.clone()));
                    selected_columns.push(new_alias.clone());
                } else {
                    expressions.push(expr);
                    selected_columns.push(col_name);
                }
                expr_resolved = true;
            }

            if !expr_resolved {
                panic!(
                    "Column '{}' not found in current table schema, alias map, or CTEs.",
                    column_name
                );
            }
    
        }

        self.selected_columns = selected_columns.clone();
        self.df = self.df.select(expressions).expect("Failed to apply SELECT.");

        // println!("SELECT Schema: {:?}", self.df.schema());

        // Update query string
        self.query = if !self.window_functions.is_empty() {
            let window_funcs = self.build_window_functions();
            format!(
                "SELECT *, {} FROM {} {}",
                self.selected_columns
                    .iter()
                    .map(|col| normalize_column_name(col))
                    .collect::<Vec<_>>()
                    .join(", "),
                window_funcs,
                self.table_alias
            )
        } else {
            format!(
                "SELECT {} FROM {}",
                self.selected_columns
                    .iter()
                    .map(|col| normalize_column_name(col))
                    .collect::<Vec<_>>()
                    .join(", "),
                self.table_alias
            )
        };

        self
    }
    
    /// GROUP BY clause
    pub fn group_by<const N: usize>(self, group_columns: [&str; N]) -> Self {
        self.group_by_vec(group_columns.to_vec())
    }
    pub fn group_by_vec(mut self, group_columns: Vec<&str>) -> Self {
        let mut resolved_group_columns = Vec::new();
        let schema = self.df.schema();

        for col in group_columns {
            //normalize columns
            let normalized_col = normalize_column_name(col);
            // Check if the column name is fully qualified
            if normalized_col.contains('.') {
                resolved_group_columns.push(col.to_string());
                continue;
            }

            // Count how many times the column name appears across all tables
            let count = schema.fields().iter().filter(|field| field.name().ends_with(&format!(".{}", col))).count();

            if count > 1 {
                panic!(
                    "Ambiguous column reference '{}'. Please qualify it with the table alias (e.g., 'table.{}').",
                    col, col
                );
            } else if count == 1 {
                // Find the fully qualified column name
                let qualified_col = schema.fields()
                    .iter()
                    .find(|field| field.name().ends_with(&format!(".{}", col)))
                    .unwrap()
                    .name()
                    .clone();
                resolved_group_columns.push(qualified_col);
            } else {
                // Column does not have a table alias, assume it's unique and present
                resolved_group_columns.push(col.to_string());
            }
        }

        let group_exprs: Vec<Expr> = resolved_group_columns
            .iter()
            .map(|col_name| {
                if col_name.contains('.') {
                    col(col_name)
                } else {
                    col_with_relation(&self.table_alias, col_name)
                }
            })
            .collect();

        let aggregate_exprs: Vec<Expr> = self
            .aggregations
            .iter()
            .map(|(_, expr)| expr.clone())
            .collect();

        self.df = self
            .df
            .aggregate(group_exprs, aggregate_exprs)
            .expect("Failed to apply GROUP BY.");

        self.aggregated_df = Some(self.df.clone());
        self.group_by_columns = resolved_group_columns; // Update group_by_columns with resolved names
        self
    }

    /// ORDER BY clause
    pub fn order_by<const N: usize>(self, columns: [&str; N], ascending: [bool; N]) -> Self {
        self.order_by_vec(columns.to_vec(), ascending.to_vec())
    }

    pub fn order_by_vec(mut self, columns: Vec<&str>, ascending: Vec<bool>) -> Self {
        assert!(
            columns.len() == ascending.len(),
            "The number of columns and sort directions must match"
        );

        let mut sort_exprs = Vec::new();
        let schema = self.df.schema();

        for (&col_name, &asc) in columns.iter().zip(ascending.iter()) {

            //normalize column names
            let lower_col_name = normalize_column_name(col_name);
            // Check if the column is an aggregation alias
            if self.aggregations.iter().any(|(alias, _)| alias == &lower_col_name) {
                // Use the aggregation alias directly
                sort_exprs.push(SortExpr {
                    expr: col(col_name),
                    asc,
                    nulls_first: true,
                });
                continue;
            }

            // Check if the column name is fully qualified
            if col_name.contains('.') {
                // Use the fully qualified column name directly
                sort_exprs.push(SortExpr {
                    expr: col(col_name),
                    asc,
                    nulls_first: true,
                });
                continue;
            }

            // Count how many times the column appears across all tables
            let count = schema.fields().iter().filter(|field| field.name().ends_with(&format!(".{}", col_name))).count();

            if count > 1 {
                panic!(
                    "Ambiguous column reference '{}'. Please qualify it with the table alias (e.g., 'table.{}').",
                    col_name, col_name
                );
            } else if count == 1 {
                // Find the fully qualified column name
                let qualified_col = schema.fields()
                    .iter()
                    .find(|field| field.name().ends_with(&format!(".{}", col_name)))
                    .unwrap()
                    .name()
                    .clone();
                sort_exprs.push(SortExpr {
                    expr: col(&qualified_col),
                    asc,
                    nulls_first: true,
                });
            } else {
                // Column does not have a table alias and is assumed to be unique
                sort_exprs.push(SortExpr {
                    expr: col(col_name),
                    asc,
                    nulls_first: true,
                });
            }
        }

        self.df = self.df.sort(sort_exprs).expect("Failed to apply ORDER BY.");

        for (c, a) in columns.into_iter().zip(ascending.into_iter()) {
            self.order_by_columns.push((c.to_string(), a));
        }

        self
    }
    
    /// LIMIT lcause
    pub fn limit(mut self, count: usize) -> Self {
        self.limit_count = Some(count);
        self.df = self.df.limit(0, Some(count)).expect("Failed to apply LIMIT.");
        self.aggregated_df = None; 
        self
    }

    
    ///FILTER clause
    /// Applies a WHERE filter with automatic lowercasing
    pub fn filter(mut self, condition: &str) -> Self {
        // Normalize the condition string to lowercase
        let normalized_condition = normalize_condition(condition);

        let expr = self.parse_condition_for_filter(&normalized_condition);
        self.df = self.df.filter(expr).expect("Can't apply Filter funciton.");
        self.where_conditions.push(normalized_condition);

        self
    }

    /// Applies a HAVING filter with automatic lowercasing
    pub fn having(mut self, condition: &str) -> Self {
        if self.aggregations.is_empty() {
           panic!("HAVING must be applied after aggregation and group_by.");
        }
        // Normalizing to lowercase
        let normalized_condition = normalize_condition(condition);

        // Parsing normalized condition
        let expr = Self::parse_condition_for_having(&normalized_condition, &self.alias_map);

        let agg_df = self.aggregated_df.as_ref().expect("Aggregated DataFrame not set after group_by()");

        let new_agg_df = agg_df
        .clone()
        .filter(expr)
        .expect("Failed to apply HAVING filter.");

        // Update the aggregated DataFrame
        self.aggregated_df = Some(new_agg_df.clone());

        // Update the main DataFrame to the filtered aggregated DataFrame
        self.df = new_agg_df;

        // Store the normalized condition
        self.having_conditions.push(normalized_condition);

        self
    }
    
    
    /// JOIN clause
    pub fn join(
        mut self,
        other: CustomDataFrame,
        condition: &str,
        join_type: &str,
    ) -> Self {
        // Map join type string to DataFusion's JoinType
        let join_type_enum = match join_type.to_uppercase().as_str() {
            "INNER" => JoinType::Inner,
            "LEFT" => JoinType::Left,
            "RIGHT" => JoinType::Right,
            "FULL" => JoinType::Full,
            "LEFT SEMI" => JoinType::LeftSemi,
            "RIGHT SEMI" => JoinType::RightSemi,
            "LEFT ANTI" => JoinType::LeftAnti,
            "RIGHT ANTI" => JoinType::RightAnti,
            "LEFT MARK" => JoinType::LeftMark,
            _ => panic!("Unsupported join type: {}", join_type),
        };

        // Parse the join condition
        let condition_parts: Vec<&str> = condition.split("==").map(|s| s.trim()).collect();
        if condition_parts.len() != 2 {
            panic!("Unsupported join type: {}", join_type);
        }

        let left_col = condition_parts[0];
        let right_col = condition_parts[1];

        // Perform the join using DataFusion's API directly
        self.df = self.df.join(
            other.df,
            join_type_enum,
            &[left_col],
            &[right_col],
            None,
        ).expect("Failed to apply JOIN.");

        // println!("Schema after simple join: {:?}", self.df.schema());
        self
    }

    /// Window functions builder
    fn build_window_functions(&self) -> String {
        if self.window_functions.is_empty() {
            "".to_string()
        } else {
            let funcs: Vec<String> = self.window_functions.iter().map(|w| {
                let qualified_col = format!("{}.{}", self.table_alias, w.column);
                let mut w_str = format!("{}({}) OVER (", w.func.to_uppercase(), qualified_col);
                
                if !w.partition_by.is_empty() {
                    let partition_cols = w.partition_by.iter()
                        .map(|c| format!("{}.{}", self.table_alias, c))
                        .collect::<Vec<_>>();
                    w_str.push_str(&format!("PARTITION BY {}", partition_cols.join(", ")));
                }
                
                if !w.order_by.is_empty() {
                    if !w.partition_by.is_empty() {
                        w_str.push_str(" ");
                    }
                    let order_cols = w.order_by.iter()
                        .map(|c| format!("{}.{}", self.table_alias, c))
                        .collect::<Vec<_>>();
                    w_str.push_str(&format!("ORDER BY {}", order_cols.join(", ")));
                }
                w_str.push(')');
                if let Some(a) = &w.alias {
                    w_str.push_str(&format!(" AS {}", a));
                }
                w_str
            }).collect();
            funcs.join(", ")
        }
    }

    /// WINDOW CLAUSE
    pub fn window<const P: usize, const O: usize>(
        self,
        func: &str,
        column: &str,
        partition_by: [&str; P],
        order_by: [&str; O],
        alias: Option<&str>,
    ) -> Self {
        self.window_vec(func, column, partition_by.to_vec(), order_by.to_vec(), alias)
    }

    pub fn window_vec(
        mut self,
        func: &str,
        column: &str,
        partition_by: Vec<&str>,
        order_by: Vec<&str>,
        alias: Option<&str>,
    ) -> Self {
        // Record window function
        self.window_functions.push(WindowDefinition {
            func: func.to_string(),
            column: column.to_string(),
            partition_by: partition_by.into_iter().map(|s| s.to_string()).collect(),
            order_by: order_by.into_iter().map(|s| s.to_string()).collect(),
            alias: alias.map(|s| s.to_string()),
        });
        self
    }

    //========= Functions on COlumns
    /// CAST function
    pub fn add_column_with_cast(mut self, column: &str, new_alias: &str, data_type: &str) -> Self {
        let expr = format!("CAST({} AS {}) AS {}", column, data_type, new_alias);
        self.selected_columns.push(expr);
        self
    }

    /// TRIM funciton
    pub fn add_column_with_trim(mut self, column: &str, new_alias: &str) -> Self {
        let expr = format!("TRIM({}) AS {}", column, new_alias);
        self.selected_columns.push(expr);
        self
    }

    /// REGEX function
    pub fn add_column_with_regex(mut self, column: &str, pattern: &str, new_alias: &str) -> Self {
        let expr = format!("REGEXP_REPLACE({}, '{}', '') AS {}", column, pattern, new_alias);
        self.selected_columns.push(expr);
        self
    }

    /// Parsing conditions for FILTER clause
    fn parse_condition_for_filter(&self, condition: &str) -> Expr {
        let re = Regex::new(r"^(.+?)\s*(=|!=|>|<|>=|<=)\s*(.+)$").unwrap();
        let condition_trimmed = condition.trim();

        if !re.is_match(condition_trimmed) {
            panic!(
                "Invalid FILTER condition format: '{}'. Expected 'column operator value'",
                condition_trimmed
            );
        }

        let caps = re.captures(condition_trimmed).expect("Invalid FILTER condition format!");
        let column = caps.get(1).unwrap().as_str().trim();
        let operator = caps.get(2).unwrap().as_str().trim();
        let value = caps.get(3).unwrap().as_str().trim().trim_matches('\'');

        let column_expr = col_with_relation(&self.table_alias, column);

        match value.parse::<f64>() {
            Ok(num_value) => match operator {
                "=" => column_expr.eq(lit(num_value)),
                "!=" => column_expr.not_eq(lit(num_value)),
                ">" => column_expr.gt(lit(num_value)),
                "<" => column_expr.lt(lit(num_value)),
                ">=" => column_expr.gt_eq(lit(num_value)),
                "<=" => column_expr.lt_eq(lit(num_value)),
                _ => panic!("Unsupported operator in FILTER condition: '{}'", operator),
            },
            Err(_) => match operator {
                "=" => column_expr.eq(lit(value)),
                "!=" => column_expr.not_eq(lit(value)),
                ">" => column_expr.gt(lit(value)),
                "<" => column_expr.lt(lit(value)),
                ">=" => column_expr.gt_eq(lit(value)),
                "<=" => column_expr.lt_eq(lit(value)),
                _ => panic!("Unsupported operator in FILTER condition: '{}'", operator),
            },
        }
    }

    /// Parsing conditions for HAVING clause
    fn parse_condition_for_having(
        condition: &str,
        alias_map: &[(String, Expr)],
    ) -> Expr {

        let re = Regex::new(r"^(.+?)\s*(=|!=|>|<|>=|<=)\s*(.+)$").unwrap();
        let caps = re.captures(condition).expect("Invalid HAVING format");
        let column = caps.get(1).unwrap().as_str().trim();
        let operator = caps.get(2).unwrap().as_str().trim();
        let value_str = caps.get(3).unwrap().as_str().trim();
    
        let column_expr = if let Some((alias, _)) = alias_map.iter().find(|(a, _)| a == column) {
            // If the column matches an aggregation alias, just use col(alias)
            col(alias.as_str())
        } else {
            // Otherwise, treat column as is
            col(column)
        };
    
        let parsed_value = match value_str.parse::<f64>() {
            Ok(num) => lit(num),
            Err(_) => lit(value_str),
        };
    
        match operator {
            "=" => column_expr.eq(parsed_value),
            "!=" => column_expr.not_eq(parsed_value),
            ">" => column_expr.gt(parsed_value),
            "<" => column_expr.lt(parsed_value),
            ">=" => column_expr.gt_eq(parsed_value),
            "<=" => column_expr.lt_eq(parsed_value),
            _ => panic!("Unsupported operator in HAVING: '{}'", operator),
        }
    }
    
   
    /// DISPLAY Query Plan
    pub fn display_query_plan(&self) {
        println!("Generated Logical Plan:");
        println!("{:?}", self.df.logical_plan());
    }
    
    /// Displays the current schema for debugging purposes.
    pub fn display_schema(&self) {
        println!("Current Schema for '{}': {:?}", self.table_alias, self.df.schema());
    }

    /// Dipslaying query genereated from chained functions
    pub fn display_query(&self) {
        let final_query = self.build_query();
        println!("Generated SQL Query: {}", final_query);
    }

    /// Display functions that display results to terminal
    pub async fn display(&self) -> Result<(), DataFusionError> {
        self.df.clone().show().await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to display DataFrame: {}", e))
        })
    }
    
    // ====================== WRITERS ==================== //
    
    /// Write the DataFrame to a Parquet file.
    ///
    /// This function wraps DataFusion's `write_parquet` method for easier usage.
    ///
    /// # Parameters
    /// - `mode`: Specifies the write mode. Accepted values are:
    ///   - `"overwrite"`: Deletes existing files at the target path before writing.
    ///   - `"append"`: Appends to the existing Parquet file if it exists.
    /// - `path`: The file path where the Parquet file will be saved.
    /// - `options`: Optional write options for customizing the output.
    ///
    /// # Example
    /// ```rust
     /// // Write to Parquet in overwrite mode
    /// custom_df.write_to_parquet("overwrite", "output.parquet", None).await?;
    ///
    /// // Write to Parquet in append mode
    /// custom_df.write_to_parquet("append", "output.parquet", None).await?;
    /// ```
    ///
    /// # Errors
    /// Returns a `DataFusionError` if the DataFrame execution or writing fails.
    pub async fn write_to_parquet(
        &self,
        mode: &str,
        path: &str,
        options: Option<DataFrameWriteOptions>,
    ) -> ElusionResult<()> {
        let write_options = options.unwrap_or_else(DataFrameWriteOptions::new);

        match mode {
            "overwrite" => {
                // file or directory removal, if it exists
                if fs::metadata(path).is_ok() {
                    fs::remove_file(path).or_else(|_| fs::remove_dir_all(path)).map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to delete existing file or directory at '{}': {}",
                            path, e
                        ))
                    })?;
                }
            }
            "append" => {
                // if the file exists
                if !fs::metadata(path).is_ok() {
                    return Err(ElusionError::Custom(format!(
                        "Append mode requires an existing file at '{}'",
                        path
                    )));
                }   
            }
            _ => {
                return Err(ElusionError::Custom(format!(
                    "Unsupported write mode: '{}'. Use 'overwrite' or 'append'.",
                    mode
                )));
            }
        }

        
        self.df.clone().write_parquet(path, write_options, None).await?;

        match mode {
            "overwrite" => println!("Data successfully overwritten to '{}'.", path),
            "append" => println!("Data successfully appended to '{}'.", path),
            _ => unreachable!(),
        }
      
        Ok(())
    }

    /// Writes the DataFrame to a CSV file in either "overwrite" or "append" mode.
    ///
    /// # Arguments
    ///
    /// * `mode` - The write mode, either "overwrite" or "append".
    /// * `path` - The file path where the CSV will be written.
    /// * `options` - Optional `DataFrameWriteOptions` for customizing the write behavior.
    ///
    /// # Returns
    ///
    /// * `ElusionResult<()>` - Ok(()) on success, or an `ElusionError` on failure.
    pub async fn write_to_csv(
        &self,
        mode: &str,
        path: &str,
        csv_options: CsvWriteOptions,
    ) -> ElusionResult<()> {
        // let write_options = csv_options.unwrap_or_else(CsvWriteOptions::default);
        csv_options.validate()?;

        let mut df = self.df.clone();
    
        // Get the schema
        let schema = df.schema();

        let mut cast_expressions = Vec::new();
        
        for field in schema.fields() {
            let cast_expr = match field.data_type() {
                // For floating point types and decimals, cast to Decimal
                ArrowDataType::Float32 | ArrowDataType::Float64 | 
                ArrowDataType::Decimal128(_, _) | ArrowDataType::Decimal256(_, _) => {
                    Some((
                        field.name().to_string(),
                        cast(col(field.name()), ArrowDataType::Decimal128(20, 4))
                    ))
                },
                // For integer types, cast to Int64
                ArrowDataType::Int8 | ArrowDataType::Int16 | ArrowDataType::Int32 | ArrowDataType::Int64 |
                ArrowDataType::UInt8 | ArrowDataType::UInt16 | ArrowDataType::UInt32 | ArrowDataType::UInt64 => {
                    Some((
                        field.name().to_string(),
                        cast(col(field.name()), ArrowDataType::Int64)
                    ))
                },
                // Leave other types as they are
                _ => None,
            };
            
            if let Some(expr) = cast_expr {
                cast_expressions.push(expr);
            }
        }
        
        // Apply all transformations at once
        for (name, cast_expr) in cast_expressions {
            df = df.with_column(&name, cast_expr)?;
        }
    
        match mode {
            "overwrite" => {
                // Remove existing file or directory if it exists
                if fs::metadata(path).is_ok() {
                    fs::remove_file(path).or_else(|_| fs::remove_dir_all(path)).map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to delete existing file or directory at '{}': {}",
                            path, e
                        ))
                    })?;
                }
            }
            "append" => {
                // Ensure the file exists for append mode
                if !fs::metadata(path).is_ok() {
                    return Err(ElusionError::Custom(format!(
                        "Append mode requires an existing file at '{}'",
                        path
                    )));
                }
            }
            _ => {
                return Err(ElusionError::Custom(format!(
                    "Unsupported write mode: '{}'. Use 'overwrite' or 'append'.",
                    mode
                )));
            }
        }

        // RecordBatches from the DataFrame
        let batches = self.df.clone().collect().await.map_err(|e| {
            ElusionError::DataFusion(DataFusionError::Execution(format!(
                "Failed to collect RecordBatches from DataFrame: {}",
                e
            )))
        })?;

        if batches.is_empty() {
            return Err(ElusionError::Custom("No data to write.".to_string()));
        }

        // clone data for the blocking task
        let batches_clone = batches.clone();
        let mode_clone = mode.to_string();
        let path_clone = path.to_string();
        let write_options_clone = csv_options.clone();

        // Spawn a blocking task to handle synchronous CSV writing
        task::spawn_blocking(move || -> Result<(), ElusionError> {
            // Open the file with appropriate options based on the mode
            let file = match mode_clone.as_str() {
                "overwrite" => OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&path_clone)
                    .map_err(|e| ElusionError::Custom(format!("Failed to open file '{}': {}", path_clone, e)))?,
                "append" => OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(&path_clone)
                    .map_err(|e| ElusionError::Custom(format!("Failed to open file '{}': {}", path_clone, e)))?,
                _ => unreachable!(), // Mode already validated
            };

            let writer = BufWriter::new(file);

            let has_headers = match mode_clone.as_str() {
                "overwrite" => true,
                "append" => false,
                _ => true, 
            };
            // initialize the CSV writer with the provided options
            let mut csv_writer = WriterBuilder::new()
                .with_header(has_headers)
                .with_delimiter(write_options_clone.delimiter)
                .with_escape(write_options_clone.escape)
                .with_quote(write_options_clone.quote)
                .with_double_quote(write_options_clone.double_quote)
                .with_null(write_options_clone.null_value)
                .build(writer);
                

                // if let Some(date_fmt) = write_options_clone.date_format {
                //     builder = builder.with_date_format(date_fmt);
                // }
                // if let Some(time_fmt) = write_options_clone.time_format {
                //     builder = builder.with_time_format(time_fmt);
                // }
                // if let Some(timestamp_fmt) = write_options_clone.timestamp_format {
                //     builder = builder.with_timestamp_format(timestamp_fmt);
                // }
                // if let Some(timestamp_tz_fmt) = write_options_clone.timestamp_tz_format {
                //     builder = builder.with_timestamp_tz_format(timestamp_tz_fmt);
                // }

               
                // Write each batch 
                for batch in batches_clone {
                    csv_writer.write(&batch).map_err(|e| {
                        ElusionError::Custom(format!("Failed to write RecordBatch to CSV: {}", e))
                    })?;
                }
            
            
                csv_writer.into_inner().flush().map_err(|e| {
                    ElusionError::Custom(format!("Failed to flush buffer: {}", e))
                })?;
                
            Ok(())
        }).await.map_err(|e| ElusionError::Custom(format!("Failed to write to CSV: {}", e)))??;

        match mode {
            "overwrite" => println!("Data successfully overwritten to '{}'.", path),
            "append" => println!("Data successfully appended to '{}'.", path),
            _ => unreachable!(),
        }

        Ok(())
    }

    // / Writes the DataFrame to a Delta Lake table in either "overwrite" or "append" mode.
    // /
    // / # Arguments
    // /
    // / * `mode` - The write mode, either "overwrite" or "append".
    // / * `path` - The directory path where the Delta table resides or will be created.
    // / * `options` - Optional `DataFrameWriteOptions` for customizing the write behavior.
    // /
    // / # Returns
    // /
    // / * `ElusionResult<()>` - Ok(()) on success, or an `ElusionError` on failure.
    /// Writes a DataFusion `DataFrame` to a Delta table at `path`, using the new `DeltaOps` API.
    /// 
    /// # Parameters
    /// - `df`: The DataFusion DataFrame to write.
    /// - `path`: URI for the Delta table (e.g., "file:///tmp/mytable" or "s3://bucket/mytable").
    /// - `partition_cols`: Optional list of columns for partitioning.
    /// - `mode`: "overwrite" or "append" (extend as needed).
    ///
    /// # Returns
    /// - `Ok(())` on success
    /// - `Err(DeltaOpsError)` if creation or writing fails.
    ///
    /// # Notes
    /// 1. "overwrite" first re-creates the table (wiping old data, depending on the implementation),
    ///    then writes the new data.
    /// 2. "append" attempts to create if the table doesn’t exist, otherwise appends rows to an existing table.
    pub async fn write_to_delta_table(
        &self,
        mode: &str,
        path: &str,
        partition_columns: Option<Vec<String>>,
    ) -> Result<(), DeltaTableError> {
        // Match on the user-supplied string to set `overwrite` and `write_mode`.
        let (overwrite, write_mode) = match mode {
            "overwrite" => {
                // Overwrite => remove existing data if it exists
                (true, WriteMode::Default)
            }
            "append" => {
                // Don’t remove existing data, just keep writing
                (false, WriteMode::Default)
            }
            "merge" => {
                // Example: you could define "merge" to auto-merge schema
                (false, WriteMode::MergeSchema)
            }
            "default" => {
                // Another alias for (false, WriteMode::Default)
                (false, WriteMode::Default)
            }
            // If you want to handle more modes or do something special, add more arms here.
            other => {
                return Err(DeltaTableError::Generic(format!(
                    "Unsupported write mode: {other}"
                )));
            }
        };

        write_to_delta_impl(
            &self.df,   // The underlying DataFusion DataFrame
            path,
            partition_columns,
            overwrite,
            write_mode,
        )
        .await
    }

    //=================== LOADERS ============================= //
    /// LOAD function for CSV file type
    ///  /// # Arguments
    ///
    /// * `file_path` - The path to the JSON file.
    /// * `alias` - The alias name for the table within DataFusion.
    ///
    /// # Returns
    ///
    pub fn load_csv<'a>(
        file_path: &'a str,
        alias: &'a str,
    ) -> BoxFuture<'a, Result<AliasedDataFrame, DataFusionError>> {
        Box::pin(async move {
            let ctx = SessionContext::new();
            let file_extension = file_path
                .split('.')
                .last()
                .unwrap_or_else(|| panic!("Unable to determine file type for path: {}", file_path))
                .to_lowercase();
    
            // Detect and fix invalid UTF-8
            if let Err(err) = csv_detect_defect_utf8(file_path) {
                eprintln!(
                    "Invalid UTF-8 data detected in file '{}': {}. Attempting in-place conversion...",
                    file_path, err
                );
                convert_invalid_utf8(file_path).expect("Failed to convert invalid UTF-8 data in-place.");
            }
    
            let df = match file_extension.as_str() {
                "csv" => {
                    let result = ctx
                        .read_csv(
                            file_path,
                            CsvReadOptions::new()
                                .has_header(true) // Detect headers
                                .schema_infer_max_records(1000), // Optional: how many records to scan for inference
                        )
                        .await;
    
                    match result {
                        Ok(df) => df,
                        Err(err) => {
                            eprintln!(
                                "Error reading CSV file '{}': {}. Ensure the file is UTF-8 encoded and free of corrupt data.",
                                file_path, err
                            );
                            return Err(err);
                        }
                    }
                }
                _ => panic!("Unsupported file type: {}", file_extension),
            };
    
            let batches = df.collect().await?;
            let mut updated_batches = Vec::new();
    
            for batch in batches {
                let mut columns = Vec::new();
                let mut updated_fields = Vec::new();
    
                for (i, field) in batch.schema().fields().iter().enumerate() {
                    let column = batch.column(i);
    
                    match column.data_type() {
                        ArrowDataType::Utf8 => {
                            let string_array = column
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .expect("Column is not a StringArray");
    
                            if field.name().to_lowercase().contains("date") {
                                // Attempt to parse dates
                                let parsed_date_values = string_array
                                    .iter()
                                    .map(|value| value.and_then(|v| parse_date_with_formats(v)))
                                    .collect::<Vec<_>>();
    
                                let date_array: ArrayRef = Arc::new(Date32Array::from(parsed_date_values));
                                columns.push(date_array);
                                updated_fields.push(Field::new(
                                    field.name(),
                                    ArrowDataType::Date32,
                                    field.is_nullable(),
                                ));
                            } else {
                                // Keep as Utf8
                                columns.push(column.clone());
                                updated_fields.push(field.as_ref().clone());
                            }
                        }
                        ArrowDataType::Int32 => {
                            columns.push(column.clone());
                            updated_fields.push(field.as_ref().clone());
                        }
                        ArrowDataType::Int64 => {
                            columns.push(column.clone());
                            updated_fields.push(field.as_ref().clone());
                        }
                        ArrowDataType::Float64 => {
                            columns.push(column.clone());
                            updated_fields.push(field.as_ref().clone());
                        }
                        ArrowDataType::Boolean => {
                            columns.push(column.clone());
                            updated_fields.push(field.as_ref().clone());
                        }
                        _ => {
                            columns.push(column.clone());
                            updated_fields.push(field.as_ref().clone());
                        }
                    }
                }
    
                let normalized_fields = updated_fields
                    .iter()
                    .map(|field| {
                        let normalized_name = normalize_column_name(field.name());
                        field.clone().with_name(&normalized_name)
                    })
                    .collect::<Vec<_>>();
                let normalized_schema = Arc::new(Schema::new(normalized_fields));
                let updated_batch = RecordBatch::try_new(normalized_schema.clone(), columns)?;
                updated_batches.push(updated_batch);
            }
    
            let normalized_alias = normalize_alias(alias);
    
            let mem_table = MemTable::try_new(
                Arc::new(Schema::new(
                    updated_batches.first().unwrap().schema().fields().to_vec(),
                )),
                vec![updated_batches],
            )?;
            ctx.register_table(&normalized_alias, Arc::new(mem_table))?;
    
            Ok(AliasedDataFrame {
                dataframe: ctx.table(&normalized_alias).await?,
                alias: alias.to_string(),
            })
        })
    }
    
    /// LOAD function for Parquet file type
    ///
    /// # Arguments
    ///
    /// * `file_path` - The path to the Parquet file.
    /// * `alias` - The alias name for the table within DataFusion.
    ///
    /// # Returns
    ///
    /// * `AliasedDataFrame` containing the DataFusion DataFrame and its alias.
    pub fn load_parquet<'a>(
        file_path: &'a str,
        alias: &'a str,
    ) -> BoxFuture<'a, Result<AliasedDataFrame, DataFusionError>> {
        Box::pin(async move {
            let ctx = SessionContext::new();
            let file_extension = file_path
                .split('.')
                .last()
                .unwrap_or_else(|| panic!("Unable to determine file type for path: {}", file_path))
                .to_lowercase();
            // Normalize alias
            let normalized_alias = normalize_alias(alias);
    
            let df = match file_extension.as_str() {
                "parquet" => {
                    let result = ctx.read_parquet(file_path, ParquetReadOptions::default()).await;
                    match result {
                        Ok(df) => {
                            println!("Successfully read Parquet file '{}'", file_path);
                            df
                        }
                        Err(err) => {
                            eprintln!(
                                "Error reading Parquet file '{}': {}. Ensure the file is UTF-8 encoded and free of corrupt data.",
                                file_path, err
                            );
                            return Err(err);
                        }
                    }
                }
                _ => {
                    eprintln!(
                        "File type '{}' is not explicitly supported. Skipping file '{}'.",
                        file_extension, file_path
                    );
                    return Err(DataFusionError::Plan(format!(
                        "Unsupported file type: {}",
                        file_extension
                    )));
                }
            };
    
           
            let batches = df.clone().collect().await?;
            let schema = df.schema().clone();
            let mem_table = MemTable::try_new(schema.into(), vec![batches])?;
    
            ctx.register_table(normalized_alias, Arc::new(mem_table))?;
    
            let aliased_df = ctx
                .table(alias)
                .await
                .map_err(|_| DataFusionError::Plan(format!("Failed to retrieve aliased table '{}'", alias)))?;
    
            Ok(AliasedDataFrame {
                dataframe: aliased_df,
                alias: alias.to_string(),
            })
        })
    }
    
    /// Loads a JSON file into a DataFusion DataFrame.
    /// # Arguments
    ///
    /// * `file_path` - The path to the JSON file.
    /// * `alias` - The alias name for the table within DataFusion.
    ///
    /// # Returns
    ///
    pub fn load_json<'a>(
        file_path: &'a str,
        alias: &'a str,
    ) -> BoxFuture<'a, Result<AliasedDataFrame, DataFusionError>> {
        Box::pin(async move {
           
            let file_contents = read_file_to_string(file_path)
                .map_err(|e| DataFusionError::Execution(format!("Failed to read file '{}': {}", file_path, e)))?;
            //println!("Raw JSON Content:\n{}", file_contents);
            let is_array = match serde_json::from_str::<Value>(&file_contents) {
                Ok(Value::Array(_)) => true,
                Ok(Value::Object(_)) => false,
                Ok(_) => false,
                Err(e) => {
                    return Err(DataFusionError::Execution(format!("Invalid JSON structure: {}", e)));
                }
            };
            
            let df = if is_array {
                create_dataframe_from_multiple_json(&file_contents, alias).await?
            } else {
              
                create_dataframe_from_json(&file_contents, alias).await?
            };
            
            Ok(AliasedDataFrame {
                dataframe: df,
                alias: alias.to_string(),
            })
        })
    }

    /// Load a Delta table at `file_path` into a DataFusion DataFrame and wrap it in `AliasedDataFrame`.
    /// 
    /// # Usage
    /// ```no_run
    /// let df = load_delta("C:\\MyDeltaTable", "my_delta").await?;
    /// ```
    pub fn load_delta<'a>(
        file_path: &'a str,
        alias: &'a str,
    ) -> BoxFuture<'a, Result<AliasedDataFrame, DataFusionError>> {
        Box::pin(async move {
            let ctx = SessionContext::new();
    
            // path manager
            let path_manager = DeltaPathManager::new(file_path);
    
            // Open Delta table using path manager
            let table = open_table(&path_manager.table_path())
                .await
                .map_err(|e| DataFusionError::Execution(format!("Failed to open Delta table: {}", e)))?;
    
            //  metadata
            // let metadata = table.metadata()
            //     .map_err(|e| DataFusionError::Execution(format!("Failed to get metadata: {}", e)))?;
            
            // let partition_cols = metadata.partition_columns.clone();

            // let partition_columns: Vec<(String, ArrowDataType)> = partition_cols
            //     .iter()
            //     .map(|col| (col.clone(), get_arrow_type_from_delta_schema(metadata, col)))
            //     .collect();
            // let partition_columns: Vec<(String, ArrowDataType)> = partition_cols
            //     .iter()
            //     .map(|col| (col.clone(), ArrowDataType::Utf8)) // Treat as Utf8 initially
            //     .collect();
            // print!("Partition columns {:?}",partition_columns);
            
            let file_paths: Vec<String> = {
                let raw_uris = table.get_file_uris()
                    .map_err(|e| DataFusionError::Execution(format!("Failed to get table files: {}", e)))?;
                
                raw_uris.map(|uri| path_manager.normalize_uri(&uri))
                    .collect()
                };
            
                
                // println!("\nProcessed paths:");
                // for (i, path) in file_paths.iter().take(10).enumerate() {
                //     println!("  {}: {}", i, path);
                // }
    
            // Debug prints
            // println!("Base path: {}", path_manager.base_path_str());
            // println!("Number of files to read: {}", file_paths.len());
            // println!("Sample file paths:");
            // for (i, path) in file_paths.iter().take(10).enumerate() {
            //     println!("  {}: {}", i, path);
            // }

            // let data_schema = metadata.schema().map_err(|e| {
            //     DataFusionError::Execution(format!("Failed to get schema from metadata: {}", e))
            // })?;
            // // Build Arrow Schema by mapping Delta types to Arrow types
            // let mut arrow_fields = Vec::new();
            // for field in data_schema.fields() {
            //     let arrow_type = get_arrow_type_from_delta_schema(&metadata, &field.name);
            //     arrow_fields.push(Field::new(&field.name, arrow_type, field.is_nullable()));
            // }
    
            // let combined_schema = Schema::new(arrow_fields);
            // print!("Combined schema {:?}", combined_schema);
    
            // ParquetReadOptions
            let parquet_options = ParquetReadOptions::new()
                // .schema(&combined_schema)
                // .table_partition_cols(partition_columns.clone())
                .parquet_pruning(false)
                .skip_metadata(false);

            // for path in &file_paths {
                // println!("Attempting to read: {}", path);
                // match std::fs::metadata(path) {
            //         Ok(meta) => println!("  File exists with size: {} bytes", meta.len()),
            //         Err(e) => println!("  File access error: {}", e),
            //     }
            // }
            // Read parquet files
            let df = ctx.read_parquet(file_paths, parquet_options).await?;

            // for col_name in &partition_cols {
            //     if df.schema().field_with_name(None, col_name).is_ok() {
            //         let target_type = get_arrow_type_from_delta_schema(&metadata, col_name);
            //         match col(col_name).cast_to(&target_type, df.schema()) {
            //             Ok(cast_expr) => {
            //                 df = df.with_column(col_name, cast_expr)?;
            //             },
            //             Err(e) => {
            //                 println!("Failed to cast column '{}': {}", col_name, e);
            //                 // Decide how to handle the error, e.g., skip casting or propagate the error
            //                 return Err(DataFusionError::Execution(format!(
            //                     "Failed to cast column '{}': {}",
            //                     col_name, e
            //                 )));
            //             }
            //         }
            //     } else {
            //         println!("Partition column '{}' not found in DataFrame.", col_name);
            //     }
            // }
            
            
            // Print schema
            // println!("Schema after reading:");
            // for field in df.schema().fields() {
            //     println!("Field '{}': {:?}", field.name(), field.data_type());
            // }
    
            // Collect data with row count verification
            // let count_before = df.clone().count().await?;
            // println!("Row count before collect: {}", count_before);
    
            let batches = df.clone().collect().await?;
            // println!("Number of batches: {}", batches.len());
            // for (i, batch) in batches.iter().enumerate() {
            //     println!("Batch {} row count: {}", i, batch.num_rows());
            // }
            let schema = df.schema().clone().into();
            // Build M  emTable
            let mem_table = MemTable::try_new(schema, vec![batches])?;
            let normalized_alias = normalize_alias(alias);
            ctx.register_table(&normalized_alias, Arc::new(mem_table))?;
            
            // Create final DataFrame
            let aliased_df = ctx.table(&normalized_alias).await?;
            
            // Verify final row count
            // let final_count = aliased_df.clone().count().await?;
            // println!("Final row count: {}", final_count);
    
            Ok(AliasedDataFrame {
                dataframe: aliased_df,
                alias: alias.to_string(),
            })
        })
    }

    /// Unified load function that determines the file type based on extension
    ///
    /// # Arguments
    ///
    /// * `file_path` - The path to the data file (CSV, JSON, Parquet).
    /// * `schema` - The Arrow schema defining the DataFrame columns. Can be `None` to infer.
    /// * `alias` - The alias name for the table within DataFusion.
    ///
    /// # Returns
    ///
    /// * `AliasedDataFrame` containing the DataFusion DataFrame and its alias.
    pub fn load<'a>(
        file_path: &'a str,
        alias: &'a str,
    ) -> BoxFuture<'a, Result<AliasedDataFrame, DataFusionError>> {
        Box::pin(async move {

            let path_manager = DeltaPathManager::new(file_path);
            if path_manager.is_delta_table() {
                return Self::load_delta(file_path, alias).await;
            }

            let ext = file_path
                .split('.')
                .last()
                .unwrap_or_default()
                .to_lowercase();

            match ext.as_str() {
                // If recognized extension, call the corresponding loader
                "csv" => Self::load_csv(file_path, alias).await,
                "json" => Self::load_json(file_path, alias).await,
                "parquet" => Self::load_parquet(file_path, alias).await,
                    "" => {
                        Err(DataFusionError::Execution(format!(
                            "Directory is not a Delta table and has no recognized extension: {file_path}"
                        )))
                    }
                    other => Err(DataFusionError::Execution(format!(
                        "Unsupported extension: {other}"
                    ))),
                }
            })
        }

    

    // pub fn load<'a>(
    //     file_path: &'a str,
    //     schema: Option<Arc<Schema>>,
    //     alias: &'a str,
    // ) -> BoxFuture<'a, Result<AliasedDataFrame, DataFusionError>> {
    //     Box::pin(async move {
    //         let ext = file_path.split('.').last().unwrap_or_default().to_lowercase();
    //         match ext.as_str() {
    //             "csv" => {
    //                 // Ensure schema is provided for CSV
    //                 if let Some(schema) = schema {
    //                     Self::load_csv(file_path, schema, alias).await
    //                 } else {
    //                     Err(DataFusionError::Plan(
    //                         "Schema must be provided for CSV files.".to_string(),
    //                     ))
    //                 }
    //             }
    //             "json" => Self::load_json(file_path, alias).await,
    //             "parquet" => Self::load_parquet(file_path, alias).await,
    //             other => Err(DataFusionError::Execution(format!(
    //                 "Unsupported extension: {}",
    //                 other
    //             ))),
    //         }
    //     })
    // }
}

