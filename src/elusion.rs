pub mod prelude;

use datafusion::logical_expr::col;
use regex::Regex;
use datafusion::prelude::*;
use datafusion::error::DataFusionError;
use futures::future::BoxFuture;
use datafusion::datasource::MemTable;
use std::sync::Arc;
use arrow::datatypes::{Field, DataType as ArrowDataType, Schema, SchemaRef};
use chrono::NaiveDate;
use arrow::array::{StringBuilder, ArrayRef,  ArrayBuilder, Float64Builder,Float32Builder, Int64Builder, Int32Builder, UInt64Builder, UInt32Builder, BooleanBuilder, Date32Builder, BinaryBuilder};

use arrow::record_batch::RecordBatch;
use ArrowDataType::*;
use arrow::csv::writer::WriterBuilder;

// ========= CSV defects
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write, BufWriter};


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
// use object_store::path::Path as ObjectStorePath;


// =========== ERRROR

use std::fmt::{self, Debug};
use std::error::Error;

// Define your custom error type
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

#[derive(Clone, Debug)]
pub struct Join {
    dataframe: CustomDataFrame,
    condition: String,
    join_type: String,
}
// Define the CustomDataFrame struct
#[derive(Clone, Debug)]
pub struct CustomDataFrame {
    df: DataFrame,
    table_alias: String,
    from_table: String,
    selected_columns: Vec<String>,
    pub alias_map: Vec<(String, String)>,
    aggregations: Vec<String>,
    group_by_columns: Vec<String>,
    where_conditions: Vec<String>,
    having_conditions: Vec<String>,
    order_by_columns: Vec<(String, bool)>, 
    limit_count: Option<u64>,
    joins: Vec<Join>,
    window_functions: Vec<String>,
    ctes: Vec<String>,
    pub subquery_source: Option<String>,
    set_operations: Vec<String>,
    pub query: String,
    pub aggregated_df: Option<DataFrame>,
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
/// Creates a DataFusion DataFrame from JSON records.
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
// ================== NORMALIZERS
//===WRITERS
/// Normalizes column naame by trimming whitespace,converting it to lowercase and replacing empty spaces with underscore.
// fn normalize_column_name_write(name: &str) -> String {
//     name.trim().to_lowercase().replace(" ", "_")
// }

/// Normalizes an alias by trimming whitespace and converting it to lowercase.
fn normalize_alias_write(alias: &str) -> String {
    alias.trim().to_lowercase()
}

/// Normalizes a condition string by converting it to lowercase.
// fn normalize_condition_write(condition: &str) -> String {
//     condition.trim().to_lowercase()
// }

//==== DATAFRAME NORAMLIZERS
/// Normalizes column name by trimming whitespace and properly quoting table aliases and column names.
fn normalize_column_name(name: &str) -> String {
    if let Some(pos) = name.find('.') {
        let table = &name[..pos];
        let column = &name[pos + 1..];
        format!("\"{}\".\"{}\"", table.trim().replace(" ", "_"), column.trim().replace(" ", "_"))
    } else {
        format!("\"{}\"", name.trim().replace(" ", "_"))
    }
}
/// Normalizes an alias by trimming whitespace and converting it to lowercase.
fn normalize_alias(alias: &str) -> String {
    // alias.trim().to_lowercase()
    format!("\"{}\"", alias.trim().to_lowercase())
}

/// Normalizes a condition string by properly quoting table aliases and column names.
fn normalize_condition(condition: &str) -> String {
    // let re = Regex::new(r"(\b\w+)\.(\w+\b)").unwrap();
    let re = Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b").unwrap();
    
    re.replace_all(condition.trim(), "\"$1\".\"$2\"").to_string()
}

/// Normalizes an expression by properly quoting table aliases and column names.
/// Example:
/// - "SUM(s.OrderQuantity) AS total_quantity" becomes "SUM(\"s\".\"OrderQuantity\") AS total_quantity"
/// Normalizes an expression by properly quoting table aliases and column names.
fn normalize_expression(expression: &str) -> String {
    // Split the expression on AS to separate the main expression and the alias
    let parts: Vec<&str> = expression.splitn(2, " AS ").collect();
    if parts.len() == 2 {
        let expr_part = parts[0].trim();
        let alias_part = parts[1].trim();

        // First check if it's an aggregation function
        if is_aggregate_expression(expr_part) {
            // Handle aggregation function
            let normalized_inner = normalize_expression(expr_part);
            return format!("{} AS \"{}\"", normalized_inner, alias_part.replace(" ", "_").to_lowercase());
        }

        // Handle scalar functions with possible nesting and parameters
        let re = Regex::new(r"(?P<func>[A-Za-z_][A-Za-z0-9_]*)\s*\((?P<args>[^)]+)\)(?:\s*,\s*(?P<param>[0-9]+)\s*)?").unwrap();
        
        let normalized_expr = re.replace_all(expr_part, |caps: &regex::Captures| {
            let func = &caps["func"];
            let args = &caps["args"];
            
            // Normalize inner arguments recursively
            let normalized_args = normalize_expression(args);
            
            if let Some(param) = caps.name("param") {
                format!("{}({}, {})", func, normalized_args, param.as_str())
            } else {
                format!("{}({})", func, normalized_args)
            }
        }).to_string();

        // Quote the alias
        let normalized_alias = format!("\"{}\"", alias_part.replace(" ", "_").to_lowercase());

        format!("{} AS {}", normalized_expr, normalized_alias)
    } else {
        // Handle column references
        let col_re = Regex::new(r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<column>[A-Za-z_][A-Za-z0-9_]*)").unwrap();
        if col_re.is_match(expression) {
            col_re.replace_all(expression.trim(), "\"${alias}\".\"${column}\"").to_string()
        } else {
            // If it's not a column reference, return as is
            expression.to_string()
        }
    }
}

/// Helper function to determine if a string is an expression.
fn is_expression(s: &str) -> bool {
    // Check for presence of arithmetic operators or function-like patterns
    let operators = ['+', '-', '*', '/', '%' ,'(', ')', ',', '.'];
    let has_operator = s.chars().any(|c| operators.contains(&c));
    let has_function = Regex::new(r"\b[A-Za-z_][A-Za-z0-9_]*\s*\(").unwrap().is_match(s);
    has_operator || has_function
}

/// Returns true if the string contains only alphanumeric characters and underscores.
fn is_simple_column(s: &str) -> bool {
    let re = Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$").unwrap();
    re.is_match(s)
}

/// Helper function to determine if an expression is an aggregate.
fn is_aggregate_expression(expr: &str) -> bool {
    let aggregate_functions = [
    //agg funcs
    "SUM", "AVG", "MAX", "MIN", "MEAN", "MEDIAN","COUNT", "LAST_VALUE", "FIRST_VALUE",  
    "GROUPING", "STRING_AGG", "ARRAY_AGG","VAR", "VAR_POP", "VAR_POPULATION", "VAR_SAMP", "VAR_SAMPLE",  
    "BIT_AND", "BIT_OR", "BIT_XOR", "BOOL_AND", "BOOL_OR",
    
    "ABS", "FLOOR", "CEIL", "SQRT", "ISNAN", "ISZERO",  "PI", "POW", "POWER", "RADIANS", "RANDOM", "ROUND",  
   "FACTORIAL", "ACOS", "ACOSH", "ASIN", "ASINH",  "COS", "COSH", "COT", "DEGREES", "EXP","SIN", "SINH", "TAN", "TANH", "TRUNC", "CBRT", "ATAN", "ATAN2", "ATANH", "GCD", "LCM", "LN",  "LOG", "LOG10", "LOG2", "NANVL", "SIGNUM"
   ];
   //scalar funcs
   aggregate_functions.iter().any(|&func| expr.to_uppercase().starts_with(func))
 
}


/// window functions normalization
fn normalize_window_function(expression: &str) -> String {
    // Split into parts: function part and OVER part
    let parts: Vec<&str> = expression.split(" OVER ").collect();
    if parts.len() != 2 {
        return expression.to_string();
    }

    // Normalize the function part (before OVER)
    let function_part = parts[0].trim();
    let normalized_function = if function_part.contains("(") {
        // If it's an aggregation function, normalize the column reference inside it
        let re = Regex::new(r"(\w+)\(([\w.]+)\)").unwrap();
        re.replace(function_part, |caps: &regex::Captures| {
            let func_name = &caps[1];
            let column_ref = normalize_column_name(&caps[2]);
            format!("{}({})", func_name, column_ref)
        }).to_string()
    } else {
        // If it's a window function without arguments (like ROW_NUMBER)
        function_part.to_string()
    };

    // Normalize the OVER clause
    let over_part = parts[1].trim();
    // let re_cols = Regex::new(r"(\b\w+)\.(\w+\b)").unwrap();
    let re_cols = Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b").unwrap();
    let normalized_over = re_cols.replace_all(over_part, "\"$1\".\"$2\"").to_string();

    format!("{} OVER {}", normalized_function, normalized_over)
}
// ================= DELTA
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

/// Helper struct to manage path conversions between different path types
#[derive(Clone)]
struct DeltaPathManager {
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
    // pub fn to_object_store_path(&self) -> ObjectStorePath {
    //     ObjectStorePath::from(self.base_path_str())
    // }

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


// Auxiliary struct to hold aliased DataFrame
pub struct AliasedDataFrame {
    dataframe: DataFrame,
    alias: String,
}

impl CustomDataFrame {

     /// Helper function to register a DataFrame as a table provider in the given SessionContext
     async fn register_df_as_table(
        ctx: &SessionContext,
        table_name: &str,
        df: &DataFrame,
    ) -> ElusionResult<()> {
        let batches = df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        let schema = df.schema();

        let mem_table = MemTable::try_new(schema.clone().into(), vec![batches])
            .map_err(|e| ElusionError::DataFusion(DataFusionError::Execution(e.to_string())))?;

        ctx.register_table(table_name, Arc::new(mem_table))
            .map_err(|e| ElusionError::DataFusion(DataFusionError::Execution(e.to_string())))?;

        Ok(())
    }

    /// Execute a raw SQL query involving multiple CustomDataFrame instances and return a new CustomDataFrame with the results.
    ///
    /// # Arguments
    ///
    /// * sql - The raw SQL query string to execute.
    /// * alias - The alias name for the resulting DataFrame.
    /// * additional_dfs - A slice of references to other CustomDataFrame instances to be registered in the context.
    ///
    /// # Returns
    ///
    /// * `ElusionResult<Self>` - A new CustomDataFrame containing the result of the SQL query.
    // async fn raw_sql(
    //     &self,
    //     sql: &str,
    //     alias: &str,
    //     dfs: &[&CustomDataFrame],
    // ) -> ElusionResult<Self> {
    //     let ctx = Arc::new(SessionContext::new());

    //     Self::register_df_as_table(&ctx, &self.table_alias, &self.df).await?;

    //     for df in dfs {
    //         Self::register_df_as_table(&ctx, &df.table_alias, &df.df).await?;
    //     }

    //     let df = ctx.sql(sql).await.map_err(ElusionError::DataFusion)?;
    //     let batches = df.clone().collect().await.map_err(ElusionError::DataFusion)?;
    //     let result_mem_table = MemTable::try_new(df.schema().clone().into(), vec![batches])
    //         .map_err(|e| ElusionError::DataFusion(DataFusionError::Execution(e.to_string())))?;

    //     ctx.register_table(alias, Arc::new(result_mem_table))
    //         .map_err(|e| ElusionError::DataFusion(DataFusionError::Execution(e.to_string())))?;

    //     let result_df = ctx.table(alias).await.map_err(|e| {
    //         ElusionError::Custom(format!(
    //             "Failed to retrieve table '{}': {}",
    //             alias, e
    //         ))
    //     })?;

    //     Ok(CustomDataFrame {
    //         df: result_df,
    //         table_alias: alias.to_string(),
    //         from_table: alias.to_string(),
    //         selected_columns: Vec::new(),
    //         alias_map: Vec::new(),
    //         aggregations: Vec::new(),
    //         group_by_columns: Vec::new(),
    //         where_conditions: Vec::new(),
    //         having_conditions: Vec::new(),
    //         order_by_columns: Vec::new(),
    //         limit_count: None,
    //         joins: Vec::new(),
    //         window_functions: Vec::new(),
    //         ctes: Vec::new(),
    //         subquery_source: None,
    //         set_operations: Vec::new(),
    //         query: sql.to_string(),
    //         aggregated_df: Some(df.clone()),
    //     })
    // }

    /// NEW method for loading and schema definition
    pub async fn new<'a>(
        file_path: &'a str,
        alias: &'a str,
    ) -> ElusionResult<Self> {
        let aliased_df = Self::load(file_path, alias).await?;

        Ok(CustomDataFrame {
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
        })
    }
   
    // ==================== API Methods ====================

      /// Add JOIN clauses 
      pub fn join(mut self, other: CustomDataFrame, condition: &str, join_type: &str) -> Self {
        self.joins.push(Join {
            dataframe: other,
            condition: normalize_condition(condition),
            join_type: join_type.to_string(),
        });
        self
    }

    /// Add multiple JOIN clauses using const generics.
    /// Accepts an array of tuples: [ (CustomDataFrame, &str, &str); N ]
    ///
    /// # Arguments
    ///
    /// * `joins` - An array of tuples containing (CustomDataFrame, condition, join_type)
    ///
    /// # Returns
    ///
    /// * `Self` - Returns the modified CustomDataFrame for method chaining
    pub fn join_many<const N: usize>(self, joins: [(CustomDataFrame, &str, &str); N]) -> Self {
        let join_inputs = joins.into_iter().map(|(df, cond, jt)| Join {
            dataframe: df,
            condition: normalize_condition(cond),
            join_type: jt.to_string(),
        }).collect::<Vec<_>>();
        self.join_many_vec(join_inputs)
    }

    /// Add multiple JOIN clauses using a `Vec<Join>`
    pub fn join_many_vec(mut self, joins: Vec<Join>) -> Self {
        self.joins.extend(joins);
        self
    }

    /// GROUP BY clause using const generics
    pub fn group_by<const N: usize>(self, group_columns: [&str; N]) -> Self {
        self.group_by_vec(group_columns.to_vec())
    }
    pub fn group_by_vec(mut self, columns: Vec<&str>) -> Self {
        self.group_by_columns = columns
            .into_iter()
            .map(|s| {
                if is_simple_column(s) {
                    normalize_column_name(s)
                } else if s.contains(" AS ") {
                    // Handle expressions with aliases
                    let expr_part = s.split(" AS ")
                        .next()
                        .unwrap_or(s);
                    normalize_expression(expr_part)
                } else {
                    // Handle expressions without aliases
                    normalize_expression(s)
                }
            })
            .collect();
        self
    }

    /// GROUP_BY_ALL function that usifies all SELECT() olumns and reduces need for writing all columns
    pub fn group_by_all(mut self) -> Self {
        let mut all_group_by = Vec::new();
    
        // Process all selected columns
        for col in &self.selected_columns {
            if is_simple_column(&col) {
                // Simple column case
                all_group_by.push(col.clone());
            } else if is_expression(&col) {
                // Expression case - extract the part before AS
                if col.contains(" AS ") {
                    if let Some(expr_part) = col.split(" AS ").next() {
                        let expr = expr_part.trim().to_string();
                        if !all_group_by.contains(&expr) {
                            all_group_by.push(expr);
                        }
                    }
                } else {
                    // Expression without AS
                    if !all_group_by.contains(col) {
                        all_group_by.push(col.clone());
                    }
                }
            } else {
                // Table.Column case
                all_group_by.push(col.clone());
            }
        }
    
        self.group_by_columns = all_group_by;
        self
    }

    /// Add multiple WHERE conditions using const generics.
    /// Allows passing arrays like ["condition1", "condition2", ...]
    pub fn filter_many<const N: usize>(self, conditions: [&str; N]) -> Self {
        self.filter_vec(conditions.to_vec())
    }

    /// Add multiple WHERE conditions using a Vec<&str>
    pub fn filter_vec(mut self, conditions: Vec<&str>) -> Self {
        self.where_conditions.extend(conditions.into_iter().map(|c| normalize_condition(c)));
        self
    }

    /// Add a single WHERE condition
    pub fn filter(mut self, condition: &str) -> Self {
        self.where_conditions.push(normalize_condition(condition));
        self
    }

    /// Add multiple HAVING conditions using const generics.
    /// Allows passing arrays like ["condition1", "condition2", ...]
    pub fn having_many<const N: usize>(self, conditions: [&str; N]) -> Self {
        self.having_conditions_vec(conditions.to_vec())
    }

    /// Add multiple HAVING conditions using a Vec<&str>
    pub fn having_conditions_vec(mut self, conditions: Vec<&str>) -> Self {
        self.having_conditions.extend(conditions.into_iter().map(|c| normalize_condition(c)));
        self
    }

    /// Add a single HAVING condition
    pub fn having(mut self, condition: &str) -> Self {
        self.having_conditions.push(normalize_condition(condition));
        self
    }

    /// Add ORDER BY clauses using const generics.
    /// Allows passing arrays like ["column1", "column2"], [true, false]
    pub fn order_by<const N: usize>(self, columns: [&str; N], ascending: [bool; N]) -> Self {
        let normalized_columns: Vec<String> = columns.iter()
            .map(|c| normalize_column_name(c))
            .collect();
        self.order_by_vec(normalized_columns, ascending.to_vec())
    }

    /// Add ORDER BY clauses using vectors
    pub fn order_by_vec(mut self, columns: Vec<String>, ascending: Vec<bool>) -> Self {
        // Ensure that columns and ascending have the same length
        assert!(
            columns.len() == ascending.len(),
            "Columns and ascending flags must have the same length"
        );

        // Zip the columns and ascending flags into a Vec of tuples
        self.order_by_columns = columns.into_iter()
            .zip(ascending.into_iter())
            .collect();
        self
    }

    /// Add multiple ORDER BY clauses using const generics.
    /// Allows passing arrays of tuples: [ ("column1", true), ("column2", false); N ]
    pub fn order_by_many<const N: usize>(self, orders: [( &str, bool ); N]) -> Self {
        let orderings = orders.into_iter()
            .map(|(col, asc)| (normalize_column_name(col), asc))
            .collect::<Vec<_>>();
        self.order_by_many_vec(orderings)
    }

    /// Add multiple ORDER BY clauses using a Vec<(String, bool)>
    pub fn order_by_many_vec(mut self, orders: Vec<(String, bool)>) -> Self {
        self.order_by_columns = orders;
        self
    }

    /// Add LIMIT clause
    pub fn limit(mut self, count: u64) -> Self {
        self.limit_count = Some(count);
        self
    }

    // And in the CustomDataFrame implementation:
    pub fn window(mut self, window_expr: &str) -> Self {
        let normalized = normalize_window_function(window_expr);
        self.window_functions.push(normalized);
        self
    }

    /// Add CTEs using const generics.
    /// Allows passing arrays like ["cte1", "cte2", ...]
    pub fn with_ctes<const N: usize>(self, ctes: [&str; N]) -> Self {
        self.with_ctes_vec(ctes.to_vec())
    }

    /// Add CTEs using a Vec<&str>
    pub fn with_ctes_vec(mut self, ctes: Vec<&str>) -> Self {
        self.ctes.extend(ctes.into_iter().map(|c| c.to_string()));
        self
    }

    /// Add a single CTE
    pub fn with_cte_single(mut self, cte: &str) -> Self {
        self.ctes.push(cte.to_string());
        self
    }

    /// Add SET operations (UNION, INTERSECT, etc.)
    pub fn set_operation(mut self, set_op: &str) -> Self {
        self.set_operations.push(set_op.to_string());
        self
    }

/// Apply multiple string functions to create new columns in the SELECT clause.
    ///
    /// # Arguments
    ///
    /// * `expressions` - A fixed-size array of string expressions.
    ///
    /// # Example
    ///
    /// ```rust
    /// .apply_functions([
    ///     "LEFT(p.ProductName, 10) AS short_product_name",
    ///     "RIGHT(p.ProductName, 5) AS end_product_name",
    ///     "CONCAT(TRIM(s.first_name), ' ', TRIM(s.last_name)) AS full_name"
    /// ])
    /// ```
    pub fn string_functions<const N: usize>(mut self, expressions: [&str; N]) -> Self {
        for expr in expressions.iter() {
            // Add to SELECT clause
            self.selected_columns.push(normalize_expression(expr));
    
            // If GROUP BY is used, extract the expression part (before AS)
            // and add it to GROUP BY columns
            if !self.group_by_columns.is_empty() {
                let expr_part = expr.split(" AS ")
                    .next()
                    .unwrap_or(expr);
                self.group_by_columns.push(normalize_expression(expr_part));
            }
        }
        self
    }

    /// Add aggregations to the SELECT clause using const generics.
    /// Ensures that only valid aggregate expressions are included.
    pub fn agg<const N: usize>(self, aggregations: [&str; N]) -> Self {
        self.agg_vec(
            aggregations.iter()
                .filter(|&expr| is_aggregate_expression(expr))
                .map(|s| normalize_expression(s))
                .collect()
        )
    }

    /// Add aggregations to the SELECT clause using a `Vec<String>`
    /// Ensures that only valid aggregate expressions are included.
    pub fn agg_vec(mut self, aggregations: Vec<String>) -> Self {
        let valid_aggs = aggregations.into_iter()
            .filter(|expr| is_aggregate_expression(expr))
            .collect::<Vec<_>>();

        self.aggregations.extend(valid_aggs);
        self
    }


     /// SELECT clause using const generics
     pub fn select<const N: usize>(self, columns: [&str; N]) -> Self {
        self.select_vec(columns.to_vec())
    }

    /// Add selected columns to the SELECT clause using a Vec<&str>
    pub fn select_vec(mut self, columns: Vec<&str>) -> Self {
        if !self.group_by_columns.is_empty() {
            let mut valid_selects = Vec::new();

            for col in columns {
                if is_expression(col) {
                    if is_aggregate_expression(col) {
                        valid_selects.push(normalize_expression(col));
                    } else {
                        // Expression is not an aggregate; include it in GROUP BY
                        self.group_by_columns.push(col.to_string());
                        valid_selects.push(normalize_expression(col));
                    }
                } else {
                    // Simple column
                    let normalized_col = normalize_column_name(col);
                    if self.group_by_columns.contains(&normalized_col) {
                        valid_selects.push(normalized_col);
                    } else {
                        // Automatically add to GROUP BY
                        self.group_by_columns.push(normalized_col.clone());
                        valid_selects.push(normalized_col);
                    }
                }
            }
            self.selected_columns = valid_selects;
        } else {
            // Existing behavior when GROUP BY is not used
            // Extract aggregate aliases to exclude them from selected_columns
            let aggregate_aliases: Vec<String> = self
                .aggregations
                .iter()
                .filter_map(|agg| {
                    agg.split(" AS ")
                        .nth(1)
                        .map(|alias| normalize_alias(alias))
                })
                .collect();

            self.selected_columns = columns
                .into_iter()
                .filter(|col| !aggregate_aliases.contains(&normalize_alias(col)))
                .map(|s| {
                    if is_expression(s) {
                        normalize_expression(s)
                    } else {
                        normalize_column_name(s)
                    }
                })
                .collect();
        }

        self
    }

    /// Construct the SQL query based on the current state, including joins
    fn construct_sql(&self) -> String {
        let mut query = String::new();
     
        // WITH clause for CTEs
        if !self.ctes.is_empty() {
            query.push_str("WITH ");
            query.push_str(&self.ctes.join(", "));
            query.push_str(" ");
        }
     
        // SELECT clause
        query.push_str("SELECT ");
        let mut select_parts = Vec::new();
     
        if !self.group_by_columns.is_empty() {
            // Add aggregations first
            select_parts.extend(self.aggregations.clone());
            
            // Add GROUP BY columns and selected columns
            for col in &self.selected_columns {
                if !select_parts.contains(col) {
                    select_parts.push(col.clone());
                }
            }
        } else {
            // No GROUP BY - add all parts
            select_parts.extend(self.aggregations.clone());
            select_parts.extend(self.selected_columns.clone());
        }
     
        // Add window functions last
        select_parts.extend(self.window_functions.clone());
     
        if select_parts.is_empty() {
            query.push_str("*");
        } else {
            query.push_str(&select_parts.join(", "));
        }
     
        // FROM clause
        query.push_str(&format!(
            " FROM \"{}\" AS {}",
            self.from_table.trim(),
            self.table_alias
        ));
     
        // Joins
        for join in &self.joins {
            query.push_str(&format!(
                " {} JOIN \"{}\" AS {} ON {}",
                join.join_type,
                join.dataframe.from_table,
                join.dataframe.table_alias,
                join.condition
            ));
        }
     
        // WHERE clause
        if !self.where_conditions.is_empty() {
            query.push_str(" WHERE ");
            query.push_str(&self.where_conditions.join(" AND "));
        }
     
        // GROUP BY clause
        if !self.group_by_columns.is_empty() {
            query.push_str(" GROUP BY ");
            query.push_str(&self.group_by_columns.join(", "));
        }
     
        // HAVING clause
        if !self.having_conditions.is_empty() {
            query.push_str(" HAVING ");
            query.push_str(&self.having_conditions.join(" AND "));
        }
     
        // ORDER BY clause
        if !self.order_by_columns.is_empty() {
            query.push_str(" ORDER BY ");
            let orderings: Vec<String> = self.order_by_columns.iter()
                .map(|(col, asc)| format!("{} {}", col, if *asc { "ASC" } else { "DESC" }))
                .collect();
            query.push_str(&orderings.join(", "));
        }
     
        // LIMIT clause
        if let Some(limit) = self.limit_count {
            query.push_str(&format!(" LIMIT {}", limit));
        }
     
        query
     }

    /// Execute the constructed SQL and return a new CustomDataFrame
    pub async fn elusion(&self, alias: &str) -> ElusionResult<Self> {
        let ctx = Arc::new(SessionContext::new());

        // Register the base table
        Self::register_df_as_table(&ctx, &self.table_alias, &self.df).await?;

        // Register all joined tables
        for join in &self.joins {
            Self::register_df_as_table(&ctx, &join.dataframe.table_alias, &join.dataframe.df).await?;
        }

        // Construct and log the SQL query
        let sql = self.construct_sql();
        // println!("Executing SQL: {}", sql); // Log the SQL being executed

        // Execute the SQL query
        let df = ctx.sql(&sql).await.map_err(|e| {
            ElusionError::Custom(format!(
                "Failed to execute SQL '{}': {}",
                sql, e
            ))
        })?;

        // Collect the results into batches
        let batches = df.clone().collect().await.map_err(ElusionError::DataFusion)?;

        // Create a MemTable from the result batches
        let result_mem_table = MemTable::try_new(df.schema().clone().into(), vec![batches])
            .map_err(|e| ElusionError::DataFusion(DataFusionError::Execution(e.to_string())))?;

        // Register the result as a new table with the provided alias
        ctx.register_table(alias, Arc::new(result_mem_table))
            .map_err(|e| ElusionError::DataFusion(DataFusionError::Execution(e.to_string())))?;

        // Retrieve the newly registered table
        let result_df = ctx.table(alias).await.map_err(|e| {
            ElusionError::Custom(format!(
                "Failed to retrieve table '{}': {}",
                alias, e
            ))
        })?;
        // Return a new CustomDataFrame with the new alias
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
            query: sql,
            aggregated_df: Some(df.clone()),
        })
    }

    /// Display functions that display results to terminal
    pub async fn display(&self) -> Result<(), DataFusionError> {
        self.df.clone().show().await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to display DataFrame: {}", e))
        })
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
    /// Displays the SQL query generated from the chained functions
    pub fn display_query(&self) {
        let final_query = self.construct_sql();
        println!("Generated SQL Query: {}", final_query);
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
/// 
/// Load a DataFrame from a file and assign an alias
pub async fn load_csv(file_path: &str, alias: &str) -> Result<AliasedDataFrame, DataFusionError> {
    let ctx = SessionContext::new();
    let file_extension = file_path
        .split('.')
        .last()
        .unwrap_or_else(|| panic!("Unable to determine file type for path: {}", file_path))
        .to_lowercase();

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

    Ok(AliasedDataFrame {
        dataframe: df,
        alias: alias.to_string(),
    })
}
    // async fn load_csv<'a>(file_path: &'a str, alias: &'a str) -> ElusionResult<AliasedDataFrame> {
//     let ctx = SessionContext::new();
//     let df = ctx.read_csv(file_path, CsvReadOptions::new()).await
//         .map_err(ElusionError::DataFusion)?;
//     Ok(AliasedDataFrame {
//         dataframe: df,
//         alias: alias.to_string(),
//     })
// }

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
        let normalized_alias = normalize_alias_write(alias);

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

        
        let file_paths: Vec<String> = {
            let raw_uris = table.get_file_uris()
                .map_err(|e| DataFusionError::Execution(format!("Failed to get table files: {}", e)))?;
            
            raw_uris.map(|uri| path_manager.normalize_uri(&uri))
                .collect()
            };
        
        // ParquetReadOptions
        let parquet_options = ParquetReadOptions::new()
            // .schema(&combined_schema)
            // .table_partition_cols(partition_columns.clone())
            .parquet_pruning(false)
            .skip_metadata(false);

        let df = ctx.read_parquet(file_paths, parquet_options).await?;


        let batches = df.clone().collect().await?;
        // println!("Number of batches: {}", batches.len());
        // for (i, batch) in batches.iter().enumerate() {
        //     println!("Batch {} row count: {}", i, batch.num_rows());
        // }
        let schema = df.schema().clone().into();
        // Build M  emTable
        let mem_table = MemTable::try_new(schema, vec![batches])?;
        let normalized_alias = normalize_alias_write(alias);
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
pub async fn load(
    file_path: &str,
    alias: &str,
) -> Result<AliasedDataFrame, DataFusionError> {
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
        "csv" => Self::load_csv(file_path, alias).await,
        "json" => Self::load_json(file_path, alias).await,
        "parquet" => Self::load_parquet(file_path, alias).await,
        "" => Err(DataFusionError::Execution(format!(
            "Directory is not a Delta table and has no recognized extension: {file_path}"
        ))),
        other => Err(DataFusionError::Execution(format!(
            "Unsupported extension: {other}"
        ))),
    }
}
  
   

   
}


// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[tokio::test]
//     async fn test_multiple_joins() -> ElusionResult<()> {
//         let df_sales = CustomDataFrame::new("sales.csv", "s").await?;
//         let df_customers = CustomDataFrame::new("customers.csv", "c").await?;
//         let df_products = CustomDataFrame::new("products.csv", "p").await?;

//         let three_joins = df_sales
//             .join(df_customers.clone(), "s.CustomerKey = c.CustomerKey", "INNER")
//             .join(df_products.clone(), "s.ProductKey = p.ProductKey", "INNER")
//             .agg(vec![
//                 "SUM(s.OrderQuantity) AS total_quantity".to_string(),
//             ])
//             .group_by(vec![
//                 "c.CustomerKey",
//                 "c.FirstName",
//                 "c.LastName",
//                 "p.ProductName",
//             ])
//             .having("SUM(s.OrderQuantity) > 10")
//             .select(vec![
//                 "c.CustomerKey",
//                 "c.FirstName",
//                 "c.LastName",
//                 "p.ProductName",
//                 "total_quantity",
//             ])
//             .order_by(vec!["total_quantity"], vec![false])
//             .limit(10);

//         // Execute the query
//         let executed_three_joins = three_joins.execute("test_result_three_joins").await?;

//         // Verify the schema contains the expected columns
//         let schema = executed_three_joins.df.schema();
//         assert!(schema.field_with_name("total_quantity").is_ok());
//         assert!(schema.field_with_name("CustomerKey").is_ok());
//         assert!(schema.field_with_name("FirstName").is_ok());
//         assert!(schema.field_with_name("LastName").is_ok());
//         assert!(schema.field_with_name("ProductName").is_ok());

//         Ok(())
//     }
// }