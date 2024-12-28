// ==================== IMPORTS ==================//
pub mod prelude;

// ========== DataFrame
use datafusion::logical_expr::{Expr, col, SortExpr};
use regex::Regex;
use datafusion::prelude::*;
use datafusion::error::DataFusionError; 
use futures::future::BoxFuture;
use datafusion::datasource::MemTable;
use std::sync::Arc;
use datafusion::arrow::datatypes::{Field, DataType as ArrowDataType, Schema};
use chrono::{NaiveDate,Datelike};
use arrow::array::{StringBuilder,StringArray, BinaryArray, BooleanArray, UInt64Array, UInt32Array, Date32Array,Date64Array, Float64Array, Decimal128Array, Int32Array, Int64Array, ArrayRef, Array, ArrayBuilder, Float64Builder,Float32Builder, Int64Builder, Int32Builder, UInt64Builder, UInt32Builder, BooleanBuilder, Date32Builder, BinaryBuilder,  };
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{SchemaBuilder, SchemaRef};

// ========= CSV defects
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};
use std::fs::OpenOptions;
use std::io::Write;
use encoding_rs::WINDOWS_1252;

//======== AGGREGATION FUNCTIONS 
use datafusion::functions_aggregate::expr_fn::{
    sum, min, max, avg, stddev, count, count_distinct, corr, first_value, grouping,
    var_pop, stddev_pop, array_agg,approx_percentile_cont, nth_value
};

//============ WRITERS
use std::fs;
use datafusion::prelude::SessionContext;
use datafusion::dataframe::{DataFrame,DataFrameWriteOptions};
// use datafusion::parquet::file::writer::{SerializedColumnWriter,SerializedFileWriter, SerializedRowGroupWriter, SerializedPageWriter}; 

// ========= JSON   
use serde_json::{Map, Value};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use arrow::error::Result as ArrowResult;    
// =========== ERRROR

use std::fmt;
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


// =================== DATA TYPES CONVERSIONS ==================== //

#[derive(Debug, Clone)]
pub enum SQLDataType {
    // Character Types
    Char,
    Varchar,
    Text,
    String,

    // Numeric Types
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    TinyIntUnsigned,
    SmallIntUnsigned,
    IntUnsigned,
    BigIntUnsigned,
    Float,
    Real,
    Double,
    Decimal(u8, u8), // precision, scale

    // Date/Time Types
    Date,
    Time,
    Timestamp,
    Interval,

    // Boolean Types
    Boolean,

    // Binary Types
    ByteA,

    // Unsupported Types
    Unsupported(String),
}

impl From<SQLDataType> for ArrowDataType {
    fn from(sql_type: SQLDataType) -> Self {
        match sql_type {
            // Character Types
            SQLDataType::Char | SQLDataType::Varchar | SQLDataType::Text | SQLDataType::String => ArrowDataType::Utf8,

            // Numeric Types
            SQLDataType::TinyInt => ArrowDataType::Int8,
            SQLDataType::SmallInt => ArrowDataType::Int16,
            SQLDataType::Int => ArrowDataType::Int32,
            SQLDataType::BigInt => ArrowDataType::Int64,
            SQLDataType::TinyIntUnsigned => ArrowDataType::UInt8,
            SQLDataType::SmallIntUnsigned => ArrowDataType::UInt16,
            SQLDataType::IntUnsigned => ArrowDataType::UInt32,
            SQLDataType::BigIntUnsigned => ArrowDataType::UInt64,
            SQLDataType::Float | SQLDataType::Real => ArrowDataType::Float32,
            SQLDataType::Double => ArrowDataType::Float64,
            
            
            // SQLDataType::Decimal(precision, scale) => 
            // {
            //     let precision_u8 = precision.try_into().unwrap();
            //     let scale_i8 = scale.try_into().unwrap();
            //     ArrowDataType::Decimal128(precision_u8, scale_i8)
            // }
            SQLDataType::Decimal(precision, scale) => ArrowDataType::Decimal128(precision.into(), scale.try_into().unwrap()),

            // Date/Time Types
            SQLDataType::Date => ArrowDataType::Date32,
            SQLDataType::Time => ArrowDataType::Time64(datafusion::arrow::datatypes::TimeUnit::Nanosecond),
            SQLDataType::Timestamp => ArrowDataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None),
            SQLDataType::Interval => ArrowDataType::Interval(datafusion::arrow::datatypes::IntervalUnit::MonthDayNano),

            // Boolean Types
            SQLDataType::Boolean => ArrowDataType::Boolean,

            // Binary Types
            SQLDataType::ByteA => ArrowDataType::Binary,

            // Unsupported
            SQLDataType::Unsupported(msg) => panic!("Unsupported SQL type: {}", msg),
        }
    }
}

impl SQLDataType {
    pub fn from_str(data_type: &str) -> Self {
        match data_type.to_uppercase().as_str() {
            "CHAR" => SQLDataType::Char,
            "VARCHAR" => SQLDataType::Varchar,
            "TEXT" | "STRING" => SQLDataType::Text,
            "TINYINT" => SQLDataType::TinyInt,
            "SMALLINT" => SQLDataType::SmallInt,
            "INT" | "INTEGER" => SQLDataType::Int,
            "BIGINT" => SQLDataType::BigInt,
            "FLOAT" => SQLDataType::Float,
            "DOUBLE" => SQLDataType::Double,
            "DECIMAL" => SQLDataType::Decimal(20, 4), 
            "NUMERIC" | "NUMBER" => SQLDataType::Decimal(20,4),
            "DATE" => SQLDataType::Date,
            "TIME" => SQLDataType::Time,
            "TIMESTAMP" => SQLDataType::Timestamp,
            "BOOLEAN" => SQLDataType::Boolean,
            "BYTEA" => SQLDataType::ByteA,
            _ => SQLDataType::Unsupported(data_type.to_string()),
        }
    }
}

impl From<ArrowDataType> for SQLDataType {
    fn from(arrow_type: ArrowDataType) -> Self {
        match arrow_type {
            ArrowDataType::Utf8 => SQLDataType::String,
            ArrowDataType::Int8 => SQLDataType::TinyInt,
            ArrowDataType::Int16 => SQLDataType::SmallInt,
            ArrowDataType::Int32 => SQLDataType::Int,
            ArrowDataType::Int64 => SQLDataType::BigInt,
            ArrowDataType::UInt8 => SQLDataType::TinyIntUnsigned,
            ArrowDataType::UInt16 => SQLDataType::SmallIntUnsigned,
            ArrowDataType::UInt32 => SQLDataType::IntUnsigned,
            ArrowDataType::UInt64 => SQLDataType::BigIntUnsigned,
            ArrowDataType::Float32 => SQLDataType::Float,
            ArrowDataType::Float64 => SQLDataType::Double,
            ArrowDataType::Date32 => SQLDataType::Date,
            ArrowDataType::Time64(_) => SQLDataType::Time,
            ArrowDataType::Timestamp(_, _) => SQLDataType::Timestamp,
            ArrowDataType::Boolean => SQLDataType::Boolean,
            ArrowDataType::Binary => SQLDataType::ByteA,
            _ => SQLDataType::Unsupported(format!("{:?}", arrow_type)),
        }
    }
}

// =====================  AGGREGATION BUILDER =============== //

pub struct AggregationBuilder {
    column: String,
    pub agg_alias: Option<String>,
    agg_fn: Option<Box<dyn Fn(Expr) -> Expr>>, // Store aggregation function
}

impl AggregationBuilder {
    pub fn new(column: &str) -> Self {
        Self {
            column: column.to_string(),
            agg_alias: None,
            agg_fn: None, // No aggregation function initially
        }
    }

    pub fn build_expr(&self, _table_alias: &str) -> Expr {
        // Directly reference the column without qualification
        let base_column = col(self.column.as_str());
    
        let base_expr = if let Some(agg_fn) = &self.agg_fn {
            agg_fn(base_column) 
        } else {
            base_column
        };
    
        // Apply alias if present
        if let Some(alias) = &self.agg_alias {
            base_expr.alias(alias.clone())
        } else {
            base_expr
        }
    }
    

    pub fn alias(mut self, alias: &str) -> Self {
        self.agg_alias = Some(alias.to_string());
        self
    }

    ////////// =============== FUNKCIJE =================== \\\\\\\\\\

    pub fn sum(mut self) -> Self {
        self.agg_fn = Some(Box::new(sum)); 
        self
    }

    pub fn avg(mut self) -> Self {
        self.agg_fn = Some(Box::new(avg)); 
        self
    }

    pub fn min(mut self) -> Self {
        self.agg_fn = Some(Box::new(min)); 
        self
    }

    pub fn max(mut self) -> Self {
        self.agg_fn = Some(Box::new(max)); 
        self
    }

    pub fn stddev(mut self) -> Self {
        self.agg_fn = Some(Box::new(stddev)); 
        self
    }

    pub fn count(mut self) -> Self {
        self.agg_fn = Some(Box::new(count)); 
        self
    }

    pub fn count_distinct(mut self) -> Self {
        self.agg_fn = Some(Box::new(count_distinct)); 
        self
    }

    pub fn corr(mut self, other_column: &str) -> Self {
        let other_column = other_column.to_string(); 
        self.agg_fn = Some(Box::new(move |expr| {
            corr(expr, col_with_relation("", &other_column))
        })); 
        self
    }
    


    pub fn grouping(mut self) -> Self {
        self.agg_fn = Some(Box::new(grouping)); // Store the grouping function
        self
    }

    pub fn var_pop(mut self) -> Self {
        self.agg_fn = Some(Box::new(var_pop)); // Store the population variance function
        self
    }

    pub fn stddev_pop(mut self) -> Self {
        self.agg_fn = Some(Box::new(stddev_pop)); // Store the population standard deviation function
        self
    }

    pub fn array_agg(mut self) -> Self {
        self.agg_fn = Some(Box::new(array_agg)); // Store the array aggregation function
        self
    }

    pub fn approx_percentile(mut self, percentile: f64) -> Self {
        println!("Building approx_percentile for column: {}, percentile: {}", self.column, percentile); // Example log
        self.agg_fn = Some(Box::new(move |expr| {
            approx_percentile_cont(expr, Expr::Literal(percentile.into()), None)
        }));
        self
    }
    

    pub fn first_value(mut self) -> Self {
        self.agg_fn = Some(Box::new(|expr| first_value(expr, None))); // First value function
        self
    }

    pub fn nth_value(mut self, n: i64) -> Self {
        self.agg_fn = Some(Box::new(move |expr| nth_value(expr, n, vec![]))); 
        self
    }
    

    
}


impl From<&str> for AggregationBuilder {
    fn from(column: &str) -> Self {
        AggregationBuilder::new(column)
    }
}

impl From<AggregationBuilder> for Expr {
    fn from(builder: AggregationBuilder) -> Self {
        builder.build_expr("default_alias") // Replace "default_alias" if context requires
    }
}

// =================== CSV DETECT DEFECT ======================= //

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

pub fn parse_date_with_formats(date_str: &str) -> Option<i32> {
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

// =============== QUERY LOADER =================== //


// ===================== SCHEMA VALIDATION ===================== //

fn col_with_relation(relation: &str, column: &str) -> Expr {
    if column.contains('.') {
        col(column) // Already qualified
    } else {
        col(&format!("{}.{}", relation, column)) // Add table alias
    }
}

// fn col_with_relation(relation: &str, column: &str) -> Expr {
//     if column.contains('.') {
//         col(column) // Already qualified
//     } else if !relation.is_empty() {
//         col(&format!("{}.{}", relation, column)) // Add table alias
//     } else {
//         col(column) // Use column name as is
//     }
// }

fn validate_schema(schema: &Schema, df: &DataFrame) {
    let df_fields = df.schema().fields();

    for field in schema.fields() {
        if !df_fields.iter().any(|f| f.name() == field.name()) {
            panic!(
                "Column '{}' not found in the loaded CSV file. Available columns: {:?}",
                field.name(),
                df_fields.iter().map(|f| f.name()).collect::<Vec<_>>()
            );
        }
    }
}

fn normalize_column_name(name: &str) -> String {
    name.trim().to_lowercase().replace(" ", "_")
}

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

/// Flattens the GenericJson struct into a single-level HashMap.
fn flatten_generic_json(data: GenericJson) -> HashMap<String, Value> {
    let mut map = HashMap::new();
    // Convert HashMap to serde_json::Map
    let serde_map: Map<String, Value> = data.fields.clone().into_iter().collect();
    flatten_json_value(&Value::Object(serde_map), "", &mut map);
    map
}

// Function to infer schema from rows
fn infer_schema_from_json(rows: &[HashMap<String, Value>]) -> SchemaRef {
    let mut fields_map: HashMap<String, ArrowDataType> = HashMap::new();
    let mut keys_set: HashSet<String> = HashSet::new();

    for row in rows {
        for (k, v) in row {
            keys_set.insert(k.clone());
            let inferred_type = infer_arrow_type(v);
            // If the key already exists, ensure the type is compatible (e.g., promote to Utf8 if types vary)
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
        Value::Null => ArrowDataType::Utf8, // Default to Utf8 for NULLs
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
        Value::Array(_) => ArrowDataType::Utf8, // Simplify: store JSON arrays as strings
        Value::Object(_) => ArrowDataType::Utf8, // Simplify: store JSON objects as strings
    }
}

fn promote_types(a: ArrowDataType, b: ArrowDataType) -> ArrowDataType {
    use ArrowDataType::*;
    match (a, b) {
        (Utf8, _) | (_, Utf8) => Utf8,
        (Boolean, Boolean) => Boolean,
        (Int64, Int64) => Int64,
        (UInt64, UInt64) => UInt64,
        (Float64, Float64) => Float64,
        // Add more type promotions as needed
        _ => Utf8, // Default promotion to Utf8 for incompatible types
    }
}

fn build_record_batch(
    rows: &[HashMap<String, Value>],
    schema: Arc<Schema>
) -> ArrowResult<RecordBatch> {
    let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();

    // Initialize builders based on schema
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
            // Add more types as needed
            _ => Box::new(StringBuilder::new()), // Default to Utf8 for unsupported types
        };
        builders.push(builder);
    }

    // Populate builders with row data
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
                        // Parse the date string into days since UNIX epoch
                        // Here, we assume the date is in "YYYY-MM-DD" format
                        match chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                            Ok(date) => {
                                let days_since_epoch = date.num_days_from_ce() - chrono::NaiveDate::from_ymd(1970, 1, 1).num_days_from_ce();
                                builder.append_value(days_since_epoch as i32);
                            },
                            Err(_) => {
                                builder.append_null();
                            }
                        }
                    } else {
                        builder.append_null();
                    }
                },
                _ => {
                    // Default to appending as string
                    let builder = builders[i]
                        .as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .expect("Expected StringBuilder for default field type");
                    if let Some(Value::String(s)) = value {
                        builder.append_value(s);
                    } else if let Some(v) = value {
                        // Serialize other types to string
                        builder.append_value(&v.to_string());
                    } else {
                        builder.append_null();
                    }
                },
            }
        }
    }

    // 9. Create the RecordBatch
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
    // Deserialize JSON into GenericJson struct
    let generic_json: GenericJson = serde_json::from_str(json_str)
    .map_err(|e| DataFusionError::Execution(format!("Failed to deserialize JSON: {}", e)))?;    

    // Flatten the data
    let flattened = flatten_generic_json(generic_json);

    // Collect all rows (for simplicity, assuming single record)
    let rows = vec![flattened];

    // Infer schema
    let schema = infer_schema_from_json(&rows);

    // Build RecordBatch
    let record_batch = build_record_batch(&rows, schema.clone())
    .map_err(|e| DataFusionError::Execution(format!("Failed to build RecordBatch: {}", e)))?;

    // Create MemTable
    let partitions = vec![vec![record_batch]];
    let mem_table = MemTable::try_new(schema.clone(), partitions)
    .map_err(|e| DataFusionError::Execution(format!("Failed to create MemTable: {}", e)))?;

    // Create a new SessionContext
    let ctx = SessionContext::new();

    // Register the table
    ctx.register_table(alias, Arc::new(mem_table))
    .map_err(|e| DataFusionError::Execution(format!("Failed to register Table: {}", e)))?;

    // Retrieve the DataFrame
    let df = ctx.table(alias).await?;

    Ok(df)
}

/// Creates a DataFusion DataFrame from multiple JSON records.
async fn create_dataframe_from_multiple_json(json_str: &str, alias: &str) -> Result<DataFrame, DataFusionError> {
    // 1. Deserialize JSON into Vec<GenericJson> struct
    let generic_jsons: Vec<GenericJson> = serde_json::from_str(json_str)
        .map_err(|e| DataFusionError::Execution(format!("Failed to deserialize JSON: {}", e)))?;
    
    // 2. Flatten the data
    let mut rows = Vec::new();
    for generic_json in generic_jsons {
        let flattened = flatten_generic_json(generic_json);
        rows.push(flattened);
    }
    
    // 3. Infer schema
    let schema = infer_schema_from_json(&rows);
    
    // 4. Build RecordBatch
    let record_batch = build_record_batch(&rows, schema.clone())
        .map_err(|e| DataFusionError::Execution(format!("Failed to build RecordBatch: {}", e)))?;
    
    // 5. Create MemTable
    let partitions = vec![vec![record_batch]];
    let mem_table = MemTable::try_new(schema.clone(), partitions)
        .map_err(|e| DataFusionError::Execution(format!("Failed to create MemTable: {}", e)))?;
    
    // 6. Create a new SessionContext
    let ctx = SessionContext::new();
    
    // 7. Register the table
    ctx.register_table(alias, Arc::new(mem_table))
        .map_err(|e| DataFusionError::Execution(format!("Failed to register table '{}': {}", alias, e)))?;
    
    // 8. Retrieve the DataFrame
    let df = ctx.table(alias).await
        .map_err(|e| DataFusionError::Execution(format!("Failed to retrieve DataFrame for table '{}': {}", alias, e)))?;
    
    Ok(df)
}


// async fn create_dataframe_from_large_json(file_path: &str, alias: &str) -> Result<DataFrame, DataFusionError> {
//     let file = File::open(file_path).map_err(|e| {
//         DataFusionError::Execution(format!("Failed to open file '{}': {}", file_path, e))
//     })?;
//     let reader = BufReader::new(file);
//     let stream = Deserializer::from_reader(reader).into_iter::<EntertainmentData>();

//     let mut rows = Vec::new();
//     for item in stream {
//         match item {
//             Ok(data) => rows.push(flatten_entertainment_data(data)),
//             Err(e) => {
//                 // Handle or log the error
//                 eprintln!("Error deserializing JSON record: {}", e);
//                 continue; // Skip the faulty record
//             }
//         }
//     }

//     // Proceed with schema inference and RecordBatch building
//     let schema = infer_schema_from_json(&rows);
//     let record_batch = build_record_batch(&rows, schema.clone())?;
//     let partitions = vec![vec![record_batch]];
//     let mem_table = MemTable::try_new(schema.clone(), partitions)?;
//     let ctx = SessionContext::new();
//     ctx.register_table(alias, Arc::new(mem_table))?;
//     let df = ctx.table(alias).await?;
//     Ok(df)
// }

// Function to create DataFrame from multiple JSON records
// async fn create_dataframe_from_multiple_json(json_str: &str, alias: &str) -> Result<DataFrame, DataFusionError> {
//     // Deserialize JSON into Vec<EntertainmentData> enum
//     let entertainment_datas: Vec<EntertainmentData> = serde_json::from_str(json_str);

//     // Flatten the data
//     let mut rows = Vec::new();
//     for data in entertainment_datas {
//         let flattened = flatten_entertainment_data(data);
//         rows.push(flattened);
//     }

//     // Infer schema
//     let schema = infer_schema_from_rows(&rows);

//     // Build RecordBatch
//     let record_batch = build_record_batch(&rows, schema.clone())?;

//     // Create MemTable
//     let partitions = vec![vec![record_batch]];
//     let mem_table = MemTable::try_new(schema.clone(), partitions)?;

//     // Create a new SessionContext
//     let ctx = SessionContext::new();

//     // Register the table
//     ctx.register_table(alias, Arc::new(mem_table))?;

//     // Retrieve the DataFrame
//     let df = ctx.table(alias).await?;

//     Ok(df)
// }



// ============================= CUSTOM DATA FRAME ==============================//

// ================ STRUCTS ==================//

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

// /// Enum to specify file type
// enum FileType<'a> {
//     Csv(Vec<(&'a str, &'a str, bool)>),
//     Json,
// }

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

// =================== CUSTOM DATA FRAME IMPLEMENTATION ================== //

impl CustomDataFrame {

    
    /// NEW method for loading and schema definition
    pub async fn new<'a>(
        file_path: &'a str,
        columns: Vec<(&'a str, &'a str, bool)>,
        alias: &'a str,
    ) -> Self {
        let schema = Arc::new(Self::create_schema_from_str(columns));

        // Load the file into a DataFrame
        let aliased_df = Self::load(file_path, schema.clone(), alias)
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

        // for cte in &self.ctes {
        //     Self::register_df_as_table(&ctx, &cte.name, &cte.cte_df.df).await?;
        // }

        // for df in additional_dfs {
        //     Self::register_df_as_table(&ctx, &df.table_alias, &df.df).await?;
            
        //     for cte in &df.ctes {
        //         Self::register_df_as_table(&ctx, &cte.name, &cte.cte_df.df).await?;
        //     }
        // }

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

    /// Utility function to create schema from user-defined column info
    fn create_schema_from_str(columns: Vec<(&str, &str, bool)>) -> Schema {
        let fields = columns
            .into_iter()
            .map(|(name, sql_type_str, nullable)| {
                let sql_type = SQLDataType::from_str(sql_type_str);
                // If the type is DATE, map it to Utf8 initially
                let arrow_type = if matches!(sql_type, SQLDataType::Date) {
                    ArrowDataType::Utf8
                } else {
                    sql_type.into()
                };
                Field::new(&normalize_column_name(name), arrow_type, nullable)
            })
            .collect::<Vec<_>>();

        Schema::new(fields)
    }
    

    /// LOAD function for CSV file type
    pub fn load_csv<'a>(
        file_path: &'a str,
        schema: Arc<Schema>,
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

            // Read and validate the CSV
            let df = match file_extension.as_str() {
                "csv" => {
                    let result = ctx
                        .read_csv(
                            file_path,
                            CsvReadOptions::new()
                                .schema(&schema)
                                .has_header(true)
                                .file_extension(".csv"),
                        )
                        .await;

                    match result {
                        Ok(df) => {
                            validate_schema(&schema, &df);
                            df
                        }
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
            let mut schema_builder = SchemaBuilder::new();
            let mut new_batches = Vec::new();
            let mut updated_fields = Vec::new();

            // Process each batch for date conversion
            for batch in &batches {
                let mut columns = Vec::new();

                for (i, field) in schema.fields().iter().enumerate() {
                    if field.data_type() == &ArrowDataType::Utf8 && field.name().contains("date") {
                        let column = batch.column(i);
                        let string_array = column
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .expect("Column is not a StringArray");
                        
                        let date_values = string_array
                            .iter()
                            .map(|value| value.and_then(|v| parse_date_with_formats(v)))
                            .collect::<Vec<_>>();

                        let date_array: ArrayRef = Arc::new(Date32Array::from(date_values));
                        columns.push(date_array);
                        updated_fields.push(Field::new(field.name(), ArrowDataType::Date32, field.is_nullable()));
                    } else {
                        // Retain other columns
                        columns.push(batch.column(i).clone());
                        updated_fields.push(field.as_ref().clone());
                    }
                }

                let temp_schema = Arc::new(Schema::new(updated_fields.clone()));
                let new_batch = RecordBatch::try_new(temp_schema.clone(), columns)?;
                new_batches.push(new_batch);
                updated_fields.clear();
            }

            for field in schema.fields() {
                if field.data_type() == &ArrowDataType::Utf8 && field.name().contains("date") {
                    schema_builder.push(Field::new(field.name(), ArrowDataType::Date32, field.is_nullable()));
                } else {
                    schema_builder.push(field.as_ref().clone());
                }
            }
            let final_schema = Arc::new(schema_builder.finish());

            let partitions: Vec<Vec<RecordBatch>> = new_batches.into_iter().map(|batch| vec![batch]).collect();
            let mem_table = MemTable::try_new(final_schema.clone(), partitions)?;

            ctx.register_table(alias, Arc::new(mem_table))?;

            // println!("Registering table with alias: {}", alias);
            // println!("Loading file: {}", file_path);
            // println!("Loaded schema: {:?}", final_schema);

            let aliased_df = ctx.table(alias).await.expect("Failed to retrieve aliased table");
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
    /// * `schema` - The Arrow schema defining the DataFrame columns.
    /// * `alias` - The alias name for the table within DataFusion.
    ///
    /// # Returns
    ///
    /// An `AliasedDataFrame` containing the DataFrame and its alias.
    // pub fn load_json<'a>(
    //     file_path: &'a str,
    //     schema: Arc<Schema>,
    //     alias: &'a str,
    // ) -> BoxFuture<'a, Result<AliasedDataFrame, DataFusionError>> {
    //     Box::pin(async move {
    //         // 1. Create a DataFusion context.
    //         let ctx = SessionContext::new();

    //         // 2. Read the JSON file into a string.
    //         let file_contents = read_file_to_string(file_path)
    //             .map_err(|e| DataFusionError::Execution(format!("Failed to read file: {}", e)))?;

    //         // 3. Parse the JSON string into a serde_json::Value.
    //         let root_val: Value = serde_json::from_str(&file_contents)
    //             .map_err(|e| DataFusionError::Execution(format!("Invalid JSON: {}", e)))?;

    //         // 4. Transform single top-level map of movie titles to array of objects.
    //         let transformed = transform_single_map_to_array(root_val);

    //         // 5. Perform dynamic JSON parsing: unwind arrays, merge objects.
    //         let parsed_docs = dynamic_json_parsing(transformed);

    //         // 6. Flatten the JSON Values into row-based HashMaps with normalized keys.
    //         let row_maps = flatten_to_row_maps(&parsed_docs);

    //         // 7. Build an all-string RecordBatch according to the schema.
    //         let string_record_batch = build_string_record_batch_from_rows(
    //             &row_maps,     // The row maps with normalized keys.
    //             schema.clone() // The user schema specifying column order & names.
    //         ).map_err(|e| DataFusionError::Execution(format!("Build batch error: {}", e)))?;

    //         // 8. Register MemTable with the string RecordBatch.
    //         let partitions: Vec<Vec<RecordBatch>> = vec![vec![string_record_batch]];
    //         let mem_table = MemTable::try_new(schema.clone(), partitions)
    //             .map_err(|e| DataFusionError::Execution(format!("MemTable error: {}", e)))?;

    //         ctx.register_table(alias, Arc::new(mem_table))
    //             .map_err(|e| DataFusionError::Execution(format!("Register table error: {}", e)))?;

    //         // 9. Retrieve the DataFrame from the context.
    //         let df = ctx.table(alias).await.map_err(|e| {
    //             DataFusionError::Execution(format!("Failed to retrieve table '{}': {}", alias, e))
    //         })?;

    //         // 10. Return the aliased DataFrame.
    //         Ok(AliasedDataFrame {
    //             dataframe: df,
    //             alias: alias.to_string(),
    //         })
    //     })
    // }

    ///  load)json()  that handles both CSV and JSON in one pass,
    /// with **only one** final MemTable registration to avoid duplicates.
    /// Load JSON, transform single big object of movie titles into array of movie objects,
/// flatten, and optionally apply typed parsing (like CSV).
    pub fn load_json<'a>(
        file_path: &'a str,
        alias: &'a str,
    ) -> BoxFuture<'a, Result<AliasedDataFrame, DataFusionError>> {
        Box::pin(async move {
            // 1. Read the file contents.
            let file_contents = read_file_to_string(file_path)
                .map_err(|e| DataFusionError::Execution(format!("Failed to read file '{}': {}", file_path, e)))?;

            // Debug: Print the raw JSON content
            println!("Raw JSON Content:\n{}", file_contents);

            // 2. Check if the JSON is an array or a single object
            let is_array = match serde_json::from_str::<Value>(&file_contents) {
                Ok(Value::Array(_)) => true,
                Ok(Value::Object(_)) => false,
                Ok(_) => false,
                Err(e) => {
                    return Err(DataFusionError::Execution(format!("Invalid JSON structure: {}", e)));
                }
            };

            // 3. Depending on the JSON structure, parse accordingly
            let df = if is_array {
                // JSON is an array of objects
                create_dataframe_from_multiple_json(&file_contents, alias).await?
            } else {
                // JSON is a single object
                create_dataframe_from_json(&file_contents, alias).await?
            };

            
            Ok(AliasedDataFrame {
                dataframe: df,
                alias: alias.to_string(),
            })
        })
    }

    
    /// unified load() funciton
    pub fn load<'a>(
        file_path: &'a str,
        schema: Arc<Schema>,
        alias: &'a str // so we can pass transforms
    ) -> BoxFuture<'a, Result<AliasedDataFrame, DataFusionError>> {
        Box::pin(async move {
            let ext = file_path.split('.').last().unwrap_or_default().to_lowercase();
            match ext.as_str() {
                "csv" => Self::load_csv(file_path, schema, alias).await,
                "json" => Self::load_json(file_path, alias).await,
                other => Err(DataFusionError::Execution(format!("Unsupported extension: {}", other))),
            }
        })
    }


    /// DISPLAY Query Plan
    pub fn display_query_plan(&self) {
        println!("Generated Logical Plan:");
        println!("{:?}", self.df.logical_plan());
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

    /// Window functions builder
    fn build_window_functions(&self) -> String {
        if self.window_functions.is_empty() {
            "".to_string()
        } else {
            let funcs: Vec<String> = self.window_functions.iter().map(|w| {
                let mut w_str = format!("{}({}) OVER (", w.func.to_uppercase(), w.column);
                if !w.partition_by.is_empty() {
                    w_str.push_str(&format!("PARTITION BY {}", w.partition_by.join(", ")));
                }
                if !w.order_by.is_empty() {
                    if !w.partition_by.is_empty() {
                        w_str.push_str(" ");
                    }
                    w_str.push_str(&format!("ORDER BY {}", w.order_by.join(", ")));
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
    pub fn aggregation(mut self, aggregations: Vec<AggregationBuilder>) -> Self {
        for builder in aggregations {
            let expr = builder.build_expr(&self.table_alias);
            let alias = builder
                .agg_alias
                .clone()
                .unwrap_or_else(|| format!("{:?}", expr));
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
        // Use the updated `cte_schema` to extract schema with alias
        let schema = cte_schema(&cte_df.df, name);
    
        // Add the CTE to the list of definitions with its schema
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
    pub fn select(mut self, columns: Vec<&str>) -> Self {
        let mut expressions: Vec<Expr> = Vec::new();
        let mut selected_columns: Vec<String> = Vec::new();
    
        for c in columns {
            // Parse column and alias (if provided)
            let as_keyword = Regex::new(r"(?i)\s+as\s+").unwrap(); // Case-insensitive " AS "
            let parts: Vec<&str> = as_keyword.split(c).map(|s| s.trim()).collect();
            let column_name = normalize_column_name(parts[0]); 
            let alias: Option<String> = parts.get(1).map(|&alias| normalize_column_name(alias)); 
    
            let mut expr_resolved = false;
             //  if the column belongs to the current schema
            if !expr_resolved {
                let qualified_column = if column_name.contains('.') {
                    column_name.clone()
                } else {
                    format!("{}.{}", self.table_alias, column_name)
                };

                if self.df.schema().fields().iter().any(|field| *field.name() == qualified_column) {
                    let expr = col(&qualified_column);
                    if let Some(ref alias) = alias {
                        expressions.push(expr.alias(alias));
                        selected_columns.push(alias.clone());
                    } else {
                        expressions.push(expr);
                        selected_columns.push(qualified_column.clone());
                    }
                    expr_resolved = true;
                }
            }
    
            // if column is an aggregation alias in `alias_map`
            if let Some((agg_alias, _)) = self.alias_map.iter().find(|(a, _)| a == &column_name) {
                let expr = col(agg_alias.as_str());
                if let Some(ref alias) = alias {
                    expressions.push(expr.alias(alias));
                    selected_columns.push(alias.clone());
                } else {
                    expressions.push(expr);
                    selected_columns.push(agg_alias.clone());
                }
                expr_resolved = true;
            }
    
            // if column exists in the current schema (including fully qualified names)
            if !expr_resolved {
                if self.df.schema().fields().iter().any(|f| *f.name() == column_name) {
                    // Column name matches directly
                    let expr = col(&column_name);
                    if let Some(ref alias) = alias {
                        expressions.push(expr.alias(alias));
                        selected_columns.push(alias.clone());
                    } else {
                        expressions.push(expr);
                        selected_columns.push(column_name.clone());
                    }
                    expr_resolved = true;
                }
            }
    
      
            // if column belongs to a CTE via JoinClause
            if !expr_resolved {
                for join in &self.joins {
                    if let Some(cte) = self.ctes.iter().find(|cte| cte.name == join.table) {
                        let qualified_name = format!("{}.{}", cte.name, column_name); // Fully qualify column name
                        if cte.schema.fields().iter().any(|field| *field.name() == qualified_name) {
                            let expr = col(&qualified_name);
                            if let Some(ref alias) = alias {
                                expressions.push(expr.alias(alias));
                                selected_columns.push(alias.clone());
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
    
            // Fallback to table alias resolution for normal columns
            if !expr_resolved {
                let col_name = normalize_column_name(c);
                let expr = col_with_relation(&self.table_alias, &col_name);
                if let Some(ref alias) = alias {
                    expressions.push(expr.alias(alias));
                    selected_columns.push(alias.clone());
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
    
        // Update query string
        self.query = format!(
            "SELECT {} FROM {}",
            self.selected_columns
                .iter()
                .map(|col| normalize_column_name(col))
                .collect::<Vec<_>>()
                .join(", "),
            self.table_alias
        );
    
        self
    }
    
    /// GROUP BY clause
    pub fn group_by(mut self, group_columns: Vec<&str>) -> Self {
        let group_exprs: Vec<Expr> = group_columns
            .iter()
            .map(|&col_name| col(col_name))
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
        self
    }
    
    /// ORDER BY clause
    pub fn order_by(mut self, columns: Vec<&str>, ascending: Vec<bool>) -> Self {
        assert!(
            columns.len() == ascending.len(),
            "The number of columns and sort directions must match"
        );
    
        let mut sort_exprs = Vec::new();
    
        for (&col_name, &asc) in columns.iter().zip(ascending.iter()) {
            let is_agg_alias = self.aggregations.iter().any(|(alias, _)| alias == col_name);
            let is_group_col = self.group_by_columns.iter().any(|gc| gc == col_name);
            
            let expr = if is_agg_alias || is_group_col {
                col(col_name)
            } else {
                col(col_name)
            };
    
            sort_exprs.push(SortExpr {
                expr,
                asc,
                nulls_first: true,
            });
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
    pub fn filter(mut self, condition: &str) -> Self {
        let expr = self.parse_condition_for_filter(condition);
        self.df = self.df.filter(expr).expect("Failed to apply WHERE filter");
        self.where_conditions.push(condition.to_string());
        self
    }

    pub fn having(mut self, condition: &str) -> Self {
        if self.aggregations.is_empty() {
            panic!("HAVING must be applied after aggregation and group_by.");
        }
    
        let expr = Self::parse_condition_for_having(condition, &self.alias_map);
    
        let agg_df = self.aggregated_df.as_ref().expect("Aggregated DataFrame not set after group_by()");
        let new_agg_df = agg_df
            .clone()
            .filter(expr)
            .expect("Failed to apply HAVING filter.");
    
        self.aggregated_df = Some(new_agg_df);
        self.having_conditions.push(condition.to_string());
        self
    }
    
    /// JOIN clause
    pub fn join(
        mut self,
         other: CustomDataFrame,
        condition: &str,
        join_type: &str,
    ) -> Self {
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
            panic!("Invalid join condition format. Use: 'table.column == table.column'");
        }
    
        let left_col = condition_parts[0];
        let right_col = condition_parts[1];
    
        let left_table = left_col.split('.').next().unwrap();
        let right_table = right_col.split('.').next().unwrap();
    
        let left_column = left_col.split('.').last().unwrap();
        let right_column = right_col.split('.').last().unwrap();
    
        let left_df = if let Some(cte) = self.ctes.iter().find(|cte| cte.name == left_table) {
            cte.cte_df.df.clone()
        } else {
            self.df.clone()
        };
    
        let right_df = if let Some(cte) = self.ctes.iter().find(|cte| cte.name == right_table) {
            cte.cte_df.df.clone()
        } else {
            other.df.clone()
        };
    
        self.df = left_df
            .join(
                right_df,
                join_type_enum,
                &[left_column],
                &[right_column],
                None,
            )
            .expect("Failed to apply JOIN.");
    
        self.joins.push(JoinClause {
            join_type: join_type_enum,
            table: if let Some(cte) = self.ctes.iter().find(|cte| cte.name == right_table) {
                cte.name.clone() 
            } else {
                other.table_alias.clone() 
            },
            alias: other.table_alias.clone(),
            on_left: left_col.to_string(),
            on_right: right_col.to_string(),
        });
    
        self
    }
    

    /// WINDOW CLAUSE
    pub fn window(
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
    // pub fn display(&self) -> BoxFuture<'_, ElusionResult<()>> {
    //     let df = self.aggregated_df.as_ref().unwrap_or(&self.df);
    
    //     Box::pin(async move {
    //         let batches = df.clone().collect().await?;
    //         let schema = df.schema();
    
    //         let column_names = schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>();
    
    //         let header_row = column_names
    //             .iter()
    //             .map(|name| format!("{:<20}", name))
    //             .collect::<Vec<String>>()
    //             .join(" | ");
    //         println!("{}", header_row);
    
    //         let separator_row = column_names
    //             .iter()
    //             .map(|_| format!("{}", "-".repeat(20)))
    //             .collect::<Vec<String>>()
    //             .join(" | ");
    //         println!("{}", separator_row);
    
    //         let mut row_count = 0;
    //         'outer: for batch in &batches {
    //             for row in 0..batch.num_rows() {
    //                 if row_count >= 100 {
    //                     break 'outer;
    //                 }
    
    //                 let mut row_data = Vec::new();
    //                 for (col_idx, column) in batch.columns().iter().enumerate() {
    //                     let field = schema.field(col_idx);
    //                     let value = if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
    //                         array.value(row).to_string()
    //                     } 
    //                      // Boolean
    //                      else if let Some(array) = column.as_any().downcast_ref::<arrow::array::BooleanArray>() {
    //                         array.value(row).to_string()
    //                     }
                        
    //                     // Integers
    //                     else if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
    //                         array.value(row).to_string()
    //                     } else if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
    //                         array.value(row).to_string()
    //                     } else if let Some(array) = column.as_any().downcast_ref::<arrow::array::UInt32Array>() {
    //                         array.value(row).to_string()
    //                     } else if let Some(array) = column.as_any().downcast_ref::<arrow::array::UInt64Array>() {
    //                         array.value(row).to_string()
    //                     }
                        
    //                     // Floats
    //                     else if let Some(array) = column.as_any().downcast_ref::<Float64Array>() {
    //                         format!("{:.4}", array.value(row))
    //                     } 
    //                     else if let Some(array) = column.as_any().downcast_ref::<arrow::array::Float32Array>() {
    //                         format!("{:.4}", array.value(row))
    //                     }
    //                     else if let Some(array) = column.as_any().downcast_ref::<Decimal128Array>() {
    //                         if let ArrowDataType::Decimal128(precision, scale) = field.data_type() {
    //                             let raw_value = array.value(row);
    //                             let negative = raw_value < 0;
    //                             let abs_value = raw_value.abs();
    //                             let mut digits_str = abs_value.to_string();
    //                             let digits_len = digits_str.len();
    //                             let scale_usize = *scale as usize;
    //                             let precision_usize = *precision as usize;
                        
    //                             // if the number of digits is less than the scale, pad with leading zeros
    //                             // scale=4, digits="12" => needed_zeros=2 => "0.0012"
    //                             if scale_usize > 0 {
    //                                 if digits_len > scale_usize {
    //                                     let point_pos = digits_len - scale_usize;
    //                                     digits_str.insert(point_pos, '.');
    //                                 } else {
    //                                     let needed_zeros = scale_usize - digits_len;
    //                                     let zero_padding = "0".repeat(needed_zeros);
    //                                     digits_str = format!("0.{}{}", zero_padding, digits_str);
    //                                 }
    //                             }
                        
    //                             if negative {
    //                                 digits_str.insert(0, '-');
    //                             }
                        
    //                             let total_digits = digits_str.chars().filter(|c| c.is_ascii_digit()).count();
                        
    //                             // if total digits exceed precision, truncate from the right.
    //                             // precision=5, number="123456.78" => too many digits, truncate extra from right.
    //                             if total_digits > precision_usize {
                                    
    //                                 let excess = total_digits - precision_usize;
    //                                 let mut digit_count = 0;
    //                                 let mut chars: Vec<char> = digits_str.chars().collect();
    //                                 while digit_count < excess {
    //                                     // Remove characters from the end that are digits, skipping decimal and sign
    //                                     if let Some(ch) = chars.pop() {
    //                                         if ch.is_ascii_digit() {
    //                                             digit_count += 1;
    //                                         } else {
    //                                             chars.push(ch);
    //                                             break;
    //                                         }
    //                                     } else {
    //                                         break;
    //                                     }
    //                                 }
                        
    //                                 // if we end with a trailing '.', remove it
    //                                 if chars.last() == Some(&'.') {
    //                                     chars.pop();
    //                                 }
                        
    //                                 digits_str = chars.into_iter().collect();
    //                             }
                        
    //                             digits_str
    //                         } else {
    //                             array.value(row).to_string()
    //                         }
    //                     }
                        
                        
                        
    //                     //DATE 32   
    //                     else if let Some(array) = column.as_any().downcast_ref::<Date32Array>() {
    //                         let days_since_epoch = array.value(row);
    //                         NaiveDate::from_num_days_from_ce_opt(1970 * 365 + days_since_epoch)
    //                             .map(|d| d.to_string())
    //                             .unwrap_or_else(|| "Invalid date".to_string())
    //                     }  
    //                     // Date64 (milliseconds since epoch)
    //                     else if let Some(array) = column.as_any().downcast_ref::<Date64Array>() {
    //                         let millis_since_epoch = array.value(row);
    //                         let days_since_epoch = millis_since_epoch / (1000 * 60 * 60 * 24);
    //                         NaiveDate::from_num_days_from_ce_opt(1970 * 365 + days_since_epoch as i32)
    //                             .map(|d| d.to_string())
    //                             .unwrap_or_else(|| "Invalid date".to_string())
    //                     }
                        
                        
    //                   // Timestamps
    //                 //   else if let Some(array) = column.as_any().downcast_ref::<arrow::array::TimestampNanosecondArray>() {
    //                 //     // Timestamp is in nanoseconds
    //                 //     let nanos = array.value(row);
    //                 //     chrono::NaiveDateTime::from_timestamp_nanos(nanos)
    //                 //         .map(|d| d.to_string())
    //                 //         .unwrap_or_else(|| "Invalid timestamp".to_string())
    //                 // } else if let Some(array) = column.as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>() {
    //                 //     // Timestamp is in microseconds
    //                 //     let micros = array.value(row);
    //                 //     chrono::NaiveDateTime::from_timestamp_micros(micros)
    //                 //         .map(|d| d.to_string())
    //                 //         .unwrap_or_else(|| "Invalid timestamp".to_string())
    //                 // } else if let Some(array) = column.as_any().downcast_ref::<arrow::array::TimestampMillisecondArray>() {
    //                 //     // Timestamp is in milliseconds
    //                 //     let millis = array.value(row);
    //                 //     chrono::NaiveDateTime::from_timestamp_millis(millis)
    //                 //         .map(|d| d.to_string())
    //                 //         .unwrap_or_else(|| "Invalid timestamp".to_string())
    //                 // } else if let Some(array) = column.as_any().downcast_ref::<arrow::array::TimestampSecondArray>() {
    //                 //     // Timestamp is in seconds
    //                 //     let sec = array.value(row);
    //                 //     chrono::NaiveDateTime::from_timestamp_opt(sec, 0)
    //                 //         .map(|d| d.to_string())
    //                 //         .unwrap_or_else(|| "Invalid timestamp".to_string())
    //                 // }
                    


    //                     // Binary data
    //                     else if let Some(array) = column.as_any().downcast_ref::<arrow::array::BinaryArray>() {
    //                         let bytes = array.value(row);
    //                         format!("0x{}", hex::encode(bytes))
    //                     } else if let Some(array) = column.as_any().downcast_ref::<arrow::array::LargeBinaryArray>() {
    //                         let bytes = array.value(row);
    //                         format!("0x{}", hex::encode(bytes))
    //                     }
    //                     // Large string
    //                     else if let Some(array) = column.as_any().downcast_ref::<arrow::array::LargeStringArray>() {
    //                         array.value(row).to_string()
    //                     }    

    //                     else {
    //                         "Unsupported Type".to_string()
    //                     };
    
    //                     row_data.push(value);
    //                 }
    
    //                 let formatted_row = row_data
    //                     .iter()
    //                     .map(|v| format!("{:<20}", v))
    //                     .collect::<Vec<String>>()
    //                     .join(" | ");
    //                 println!("{}", formatted_row);
    
    //                 row_count += 1;
    //             }
    //         }
    
    //         if row_count == 0 {
    //             println!("No data to display.");
    //         } else if row_count < 100 {
    //             println!("\nDisplayed limit() number of rows.");
    //         } else {
    //             println!("\nDisplayed the first 100 rows.");
    //         }
    
    //         Ok(())
    //     })
    // }

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

}