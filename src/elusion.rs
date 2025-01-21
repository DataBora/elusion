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

// ============= DATABASE
// use futures::Future;
// use datafusion::sql::TableReference;
// use datafusion_table_providers::postgres::PostgresTableFactory;
// use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
// use datafusion_table_providers::mysql::MySQLTableFactory;
// use datafusion_table_providers::sql::db_connection_pool::mysqlpool::MySQLConnectionPool;
// use datafusion_table_providers::odbc::ODBCTableFactory;
// use datafusion_table_providers::sql::db_connection_pool::odbcpool::ODBCPool;
// use datafusion_table_providers::util::secrets::to_secret_map;
// use datafusion_federation;
// use mongodb::{Client, options::ClientOptions, bson::Document};
// use futures::stream::TryStreamExt;

// PIVOT
use arrow::compute;
use arrow::array::StringArray;

// PLOTTING
use plotly::{Plot, Scatter, Bar, Histogram, BoxPlot, Pie};
use plotly::common::{Mode, Line, Marker, Orientation};
use plotly::layout::{Axis, Layout};
use plotly::color::Rgb;
use arrow::array::{Array, Float64Array,Int64Array};
use arrow::array::Date32Array;
use std::cmp::Ordering;


// custom error type
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

// Database connectors
/// A trait for data sources that can register a table in a SessionContext
// pub trait DataSource {
//     fn register(&self, ctx: &SessionContext, alias: &str) -> impl Future<Output = ElusionResult<()>> + Send;
// }

// pub struct MongoDbSource {
//     connection_string: String,
//     database: String,
//     collection: String,
// }
// impl MongoDbSource {
//     async fn fetch_documents(&self) -> Result<Vec<Document>, ElusionError> {
//         let client_options = ClientOptions::parse(&self.connection_string)
//             .await
//             .map_err(|e| ElusionError::Custom(format!("MongoDB connection error: {}", e)))?;

//         let client = Client::with_options(client_options)
//             .map_err(|e| ElusionError::Custom(format!("MongoDB client error: {}", e)))?;

//         let collection = client
//             .database(&self.database)
//             .collection::<Document>(&self.collection);

//             let filter = Document::new();
//             let cursor = collection.find(filter)
//                 .await
//                 .map_err(|e| ElusionError::Custom(format!("MongoDB find error: {}", e)))?;

//         cursor.try_collect().await.map_err(|e| ElusionError::Custom(format!("MongoDB cursor error: {}", e)))
//     }
// }


// impl DataSource for MongoDbSource {
//     async fn register(&self, _ctx: &SessionContext, alias: &str) -> Result<(), ElusionError> {
//         println!("Connecting to MongoDB...");

//         // Fetch documents from MongoDB
//         let documents = self.fetch_documents().await?;

//         println!("Fetched {} documents from MongoDB.", documents.len());

//         // Convert documents to JSON string
//         let json_string = serde_json::to_string(&documents)
//             .map_err(|e| ElusionError::Custom(format!("JSON serialization error: {}", e)))?;

//         // Create DataFrame based on the number of documents
//         if documents.len() > 1 {
//             println!("Creating DataFrame from multiple documents...");
//             create_dataframe_from_multiple_json(&json_string, alias).await
//                 .map_err(|e| ElusionError::Custom(format!("DataFrame creation error: {}", e)))?;
//         } else {
//             println!("Creating DataFrame from single document...");
//             create_dataframe_from_json(&json_string, alias).await
//                 .map_err(|e| ElusionError::Custom(format!("DataFrame creation error: {}", e)))?;
//         };

//         println!("Table '{}' registered successfully.", alias);
//         Ok(())
//     }
// }

// /// A struct capturing connection info + table name for Postgres
// pub struct PostgresSource {
//     host: String,
//     user: String,
//     password: String,
//     database: String,
//     port: String,
//     table_name: String,
// }

// impl PostgresSource {
//     pub fn new(
//         host: &str,
//         user: &str,
//         password: &str,
//         database: &str,
//         port: &str,
//         table_name: &str,
//     ) -> Self {
//         Self {
//             host: host.to_owned(),
//             user: user.to_owned(),
//             password: password.to_owned(),
//             database: database.to_owned(),
//             port: port.to_owned(),
//             table_name: table_name.to_owned(),
//         }
//     }
// }

// impl DataSource for PostgresSource {
//     async fn register(&self, ctx: &SessionContext, alias: &str) -> ElusionResult<()> {
      
//         let params = to_secret_map(HashMap::from([
//             ("host".to_owned(), self.host.clone()),
//             ("user".to_owned(), self.user.clone()),
//             ("pass".to_owned(), self.password.clone()),
//             ("db".to_owned(), self.database.clone()),
//             ("port".to_owned(), self.port.clone()),
//             ("sslmode".to_owned(), "disable".to_owned()), // or "enable" if needed
//         ]));

//         let pool = Arc::new(
//             PostgresConnectionPool::new(params)
//                 .await
//                 .map_err(|e| ElusionError::Custom(e.to_string()))?
//         );

//         let factory = PostgresTableFactory::new(pool);

//         let table_provider = factory
//             .table_provider(TableReference::partial("public", &*self.table_name))
//             .await
//             .map_err(|e| ElusionError::Custom(e.to_string()))?;

//         ctx.register_table(alias, table_provider)
//             .map_err(ElusionError::DataFusion)?;

//         Ok(())
//     }
// }

// /// MySQL Source
// pub struct MySqlSource {
//     connection_string: String,
//     table_name: String,
//     ssl_mode: String,
// }

// impl MySqlSource {
//     pub fn new(connection_string: &str, table_name: &str, ssl_mode: &str) -> Self {
//         Self {
//             connection_string: connection_string.to_owned(),
//             table_name: table_name.to_owned(),
//             ssl_mode: ssl_mode.to_owned(),
//         }
//     }

// }

// impl DataSource for MySqlSource {
//     fn register(&self, ctx: &SessionContext, alias: &str) -> impl Future<Output = ElusionResult<()>> + Send {
//         async move {
//             println!("Starting MySQL connection process...");
            
//             let params = to_secret_map(HashMap::from([
//                 ("connection_string".to_owned(), self.connection_string.clone()),
//                 ("sslmode".to_owned(), self.ssl_mode.clone()),
//                 // performance parameters
//                 ("pool_min_idle".to_owned(), "5".to_owned()),  
//                 ("pool_max_size".to_owned(), "20".to_owned()), 
//                 ("statement_cache_capacity".to_owned(), "100".to_owned()),
//                 ("prefer_socket".to_owned(), "true".to_owned()),
//             ]));

//             println!("Creating MySQL connection pool...");
//             let pool = match tokio::time::timeout(
//                 std::time::Duration::from_secs(10), 
//                 MySQLConnectionPool::new(params)
//             ).await {
//                 Ok(pool_result) => {
//                     match pool_result {
//                         Ok(pool) => {
//                             println!("MySQL pool created successfully");
//                             pool
//                         },
//                         Err(e) => {
//                             println!("Failed to create MySQL pool: {}", e);
//                             return Err(ElusionError::Custom(format!("MySQL pool creation error: {}", e)));
//                         }
//                     }
//                 },
//                 Err(_) => {
//                     println!("MySQL pool creation timed out");
//                     return Err(ElusionError::Custom("MySQL connection pool creation timed out".to_string()));
//                 }
//             };

//             println!("Creating MySQL table factory...");
//             let pool = Arc::new(pool);
//             let factory = MySQLTableFactory::new(pool);

//             println!("Creating table provider for table: {}", self.table_name);
//             let table_provider = factory
//                 .table_provider(TableReference::bare(&*self.table_name))
//                 .await
//                 .map_err(|e| {
//                     println!("Failed to create table provider: {}", e);
//                     ElusionError::Custom(format!("Failed to create table provider: {}", e))
//                 })?;

//             println!("Registering table with alias: {}", alias);
//             ctx.register_table(alias, table_provider)
//                 .map_err(|e| {
//                     println!("Failed to register table: {}", e);
//                     ElusionError::DataFusion(e)
//                 })?;

//             println!("MySQL table registration completed successfully");
//             Ok(())
//         }
//     }
// }

// /// ODBC Source
// pub struct ODBCSource {
//     connection_string: String,
//     table_name: String,
// }

// impl ODBCSource {
//     pub fn new(connection_string: &str, table_name: &str) -> Self {
//         Self {
//             connection_string: connection_string.to_owned(),
//             table_name: table_name.to_owned(),
//         }
//     }
// }

// impl DataSource for ODBCSource {
//     fn register(&self, ctx: &SessionContext, alias: &str) -> impl Future<Output = ElusionResult<()>> + Send {
//         async move {
//             let params = to_secret_map(HashMap::from([(
//                 "connection_string".to_owned(),
//                 self.connection_string.clone(),
//             )]));

//             let pool = Arc::new(
//                 ODBCPool::new(params)
//                     .map_err(|e| ElusionError::Custom(e.to_string()))?
//             );

//             let factory = ODBCTableFactory::new(pool);

//             let table_provider = factory
//                 .table_provider(TableReference::bare(&*self.table_name), None)
//                 .await
//                 .map_err(|e| ElusionError::Custom(e.to_string()))?;

//             ctx.register_table(alias, table_provider)
//                 .map_err(ElusionError::DataFusion)?;

//             Ok(())
//         }
//     }
// }

// JOin struct
#[derive(Clone, Debug)]
pub struct Join {
    dataframe: CustomDataFrame,
    condition: String,
    join_type: String,
}
// CustomDataFrame struct
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
    union_tables: Option<Vec<(String, DataFrame, String)>>, 
    original_expressions: Vec<String>,
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
async fn create_dataframe_from_json(
    json_str: &str, 
    alias: &str
) -> Result<DataFrame, DataFusionError> {
  
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

//==== DATAFRAME NORMALIZERS
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
    let parts: Vec<&str> = expression.splitn(2, " AS ").collect();
    
    if parts.len() == 2 {
        let expr_part = parts[0].trim();
        let alias_part = parts[1].trim();
        
        // Normalize the expression part
        let normalized_expr = if is_aggregate_expression(expr_part) {
            normalize_aggregate_expression(expr_part)
        } else {
            normalize_simple_expression(expr_part)
        };

        format!("{} AS \"{}\"", 
            normalized_expr,
            alias_part.replace(" ", "_").to_lowercase()
        )
    } else {
        // No alias part
        if is_aggregate_expression(expression) {
            normalize_aggregate_expression(expression)
        } else {
            normalize_simple_expression(expression)
        }
    }
}

fn normalize_aggregate_expression(expr: &str) -> String {
    // Handle nested expressions in aggregate functions
    let re = Regex::new(r"^([A-Za-z_][A-Za-z0-9_]*)\s*\((.*)\)$").unwrap();
    if let Some(caps) = re.captures(expr.trim()) {
        let func_name = &caps[1];
        let args = &caps[2];
        
        // Normalize arguments recursively
        let normalized_args = normalize_simple_expression(args);
        format!("{}({})", func_name, normalized_args)
    } else {
        expr.to_string()
    }
}

fn normalize_simple_expression(expr: &str) -> String {
    // Handle column references and operators
    let col_re = Regex::new(r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<column>[A-Za-z_][A-Za-z0-9_]*)").unwrap();
    
    let mut normalized = expr.to_string();
    // Normalize column references
    normalized = col_re.replace_all(&normalized, "\"$1\".\"$2\"").to_string();
    
    // Handle arithmetic expressions - keep operators and parentheses as is
    normalized
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
    //scalar funcs unnecessary here but willl use it else where
    "ABS", "FLOOR", "CEIL", "SQRT", "ISNAN", "ISZERO",  "PI", "POW", "POWER", "RADIANS", "RANDOM", "ROUND",  
   "FACTORIAL", "ACOS", "ACOSH", "ASIN", "ASINH",  "COS", "COSH", "COT", "DEGREES", "EXP","SIN", "SINH", "TAN", "TANH", "TRUNC", "CBRT", "ATAN", "ATAN2", "ATANH", "GCD", "LCM", "LN",  "LOG", "LOG10", "LOG2", "NANVL", "SIGNUM"
   ];
   
   aggregate_functions.iter().any(|&func| expr.to_uppercase().starts_with(func))
 
}


/// window functions normalization
fn normalize_window_function(expression: &str) -> String {
    // 1) Split into "<function_part> OVER <over_part>"
    let parts: Vec<&str> = expression.splitn(2, " OVER ").collect();
    if parts.len() != 2 {
        // No "OVER"? Just return as-is.
        return expression.to_string();
    }

    let function_part = parts[0].trim();
    let over_part = parts[1].trim();

    //  regex to capture:
    //    - The function name (one or more word chars)
    //    - The entire parenthesized argument list (anything until the final closing parenthesis).
    //      ^(\w+)\(     : start, capture function name, then an open parenthesis
    //      (.*)         : capture everything, including commas, until the last close parenthesis
    //      \)$          : a close parenthesis at the end of string
    let func_regex = Regex::new(r"^(\w+)\((.*)\)$").unwrap();

    // If there's no parenthesis at all skip argument processing
    let (normalized_function, maybe_args) = if let Some(caps) = func_regex.captures(function_part) {
        let func_name = &caps[1];
        let arg_list_str = &caps[2]; //  "s.OrderQuantity, 1, 0"

        // split by commas, trim, and normalize each argument if it's a column reference
        let raw_args: Vec<&str> = arg_list_str.split(',').map(|s| s.trim()).collect();
        
        //  transform each argument if it looks like "s.Column"
        let normalized_args: Vec<String> = raw_args
            .iter()
            .map(|arg| normalize_function_arg(arg))
            .collect();

        (func_name.to_string(), Some(normalized_args))
    } else {
        // no parentheses matched → maybe "ROW_NUMBER()" or "ROW_NUMBER" or "DENSE_RANK()"
        // just return the entire function_part as-is (minus trailing "()" if any).
        (function_part.to_string(), None)
    };

    // rebuild the function call
    let rebuilt_function = if let Some(args) = maybe_args {
        // Join normalized arguments with commas
        format!("{}({})", normalized_function, args.join(", "))
    } else {
        // "ROW_NUMBER()" or "ROW_NUMBER"
        normalized_function
    };

    //normalize the OVER(...) clause - convert s.Column to "s"."Column"
    let re_cols = Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b").unwrap();
    let normalized_over = re_cols.replace_all(over_part, "\"$1\".\"$2\"").to_string();

    format!("{} OVER {}", rebuilt_function, normalized_over)
}

/// Helper: Normalize one argument if it looks like a table.column reference.
fn normalize_function_arg(arg: &str) -> String {
    // regex matches `tableAlias.columnName`
    let re_table_col = Regex::new(r"^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)$").unwrap();

    if let Some(caps) = re_table_col.captures(arg) {
        let table = &caps[1];
        let col = &caps[2];
        format!("\"{}\".\"{}\"", table, col)
    } else {
        // if it's just a numeric or some other literal, leave it as is
        arg.to_string()
    }
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
    
    // pub async fn from_source<T: DataSource>(source: T, alias: &str) -> ElusionResult<Self> {
    //     println!("Creating session from state...");
    //     let ctx = SessionContext::new();
        
    //     println!("Registering table source...");
    //     // Register the table in the context
    //     source.register(&ctx, alias).await?;

    //     println!("Getting table from context...");
    //     // Get the registered table as a DataFrame
    //     let df = ctx.table(alias)
    //         .await
    //         .map_err(|e| ElusionError::Custom(e.to_string()))?;

    //         println!("Table successfully registered and retrieved");
    //     Ok(CustomDataFrame {
    //         df,
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
    //         query: String::new(),
    //         aggregated_df: None,
    //         union_tables: None
    //     })
    // }

    // pub async fn from_source_with_federation<T: DataSource>(source: T, alias: &str) -> ElusionResult<Self> {
    //     println!("Creating session with federation state...");
    //     // Create a session with federation optimizer enabled
    //     let state = datafusion_federation::default_session_state();
    //     let ctx = SessionContext::new_with_state(state);

    //     println!("Registering table source...");
    //     // Register the table in the context
    //     source.register(&ctx, alias).await?;
        
    //     println!("Getting table from context...");
    //     // Get the registered table as a DataFrame
    //     let df = ctx.table(alias)
    //         .await
    //         .map_err(|e| ElusionError::Custom(e.to_string()))?;

    //     println!("Table successfully registered and retrieved");
    //     Ok(CustomDataFrame {
    //         df,
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
    //         query: String::new(),
    //         aggregated_df: None,
    //         union_tables: None
    //     })
    // }

   

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
            union_tables: None,
            original_expressions: Vec::new(),
        })
    }
   
    // ==================== API Methods ====================

    /// Add JOIN clauses 
    pub fn join<const N: usize>(
        mut self,
        other: CustomDataFrame,
        conditions: [&str; N],  // Keeping single array of conditions with "="
        join_type: &str
    ) -> Self {
        let condition = conditions.iter()
            .map(|&cond| normalize_condition(cond))  // Keeping the "=" in condition
            .collect::<Vec<_>>()
            .join(" AND ");
    
        self.joins.push(Join {
            dataframe: other,
            condition,
            join_type: join_type.to_string(),
        });
        self
    }

    /// Add multiple JOIN clauses using const generics.
    /// Accepts Array of (DataFrame, conditions, join_type)
    pub fn join_many<const N: usize, const M: usize>(
        self,
        joins: [(CustomDataFrame, [&str; M], &str); N] 
    ) -> Self {
        let join_inputs = joins.into_iter()
            .map(|(df, conds, jt)| {
                let condition = conds.iter()
                    .map(|&cond| normalize_condition(cond))
                    .collect::<Vec<_>>()
                    .join(" AND ");
    
                Join {
                    dataframe: df,
                    condition,
                    join_type: jt.to_string(),
                }
            })
            .collect::<Vec<_>>();
        self.join_many_vec(join_inputs)
    }
    
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

    /// Adding aggregations to the SELECT clause using const generics.
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

     
     /// Perform a UNION with another DataFrame
     pub fn union(mut self, other: CustomDataFrame) -> Self {
        // Check if number of columns match
        if self.selected_columns.len() != other.selected_columns.len() {
            panic!(
                "Number of columns must match for UNION. First DataFrame has {} columns, second has {} columns",
                self.selected_columns.len(),
                other.selected_columns.len()
            );
        }

        // Check if column names match
        for (self_col, other_col) in self.selected_columns.iter().zip(other.selected_columns.iter()) {
            if normalize_column_name(self_col) != normalize_column_name(other_col) {
                panic!(
                    "Column names must match for UNION: {} vs {}",
                    self_col, other_col
                );
            }
        }

         // Storing joined tables and their relationships for registration
        let joined_tables: Vec<(String, DataFrame, String)> = self.joins.iter()
        .chain(other.joins.iter())
        .map(|join| (
            join.dataframe.table_alias.clone(), 
            join.dataframe.df.clone(),
            join.dataframe.from_table.clone()
        ))
        .collect();

        // Removing existing alias from the constructed SQL
        let self_query = self.construct_sql()
            .trim_end_matches(&format!(" AS {}", self.table_alias))
            .to_string();
        let other_query = other.construct_sql()
            .trim_end_matches(&format!(" AS {}", other.table_alias))
            .to_string();

        // Wraping each SELECT in its own parentheses
        let wrapped_self_query = format!("({})", self_query);
        let wrapped_other_query = format!("({})", other_query);

        // Construct the UNION subquery
        let union_query = format!(
            "{} UNION {}",
            wrapped_self_query,
            wrapped_other_query
        );

        // Wraping the entire UNION in parentheses
        let union_subquery = format!("({})", union_query);

        // Updating the current DataFrame
        self.from_table = union_subquery;

        // Storing tables and joins state
        self.union_tables = Some(joined_tables);
        self.joins = self.joins;  // Keeping original joins

                
        // reset clauses
        self.selected_columns = Vec::new();
        self.aggregations = Vec::new();
        self.group_by_columns = Vec::new();
        self.where_conditions = Vec::new();
        self.having_conditions = Vec::new();
        self.order_by_columns = Vec::new();
        self.limit_count = None;
        self.joins = Vec::new();
        self.window_functions = Vec::new();
        self.ctes = Vec::new();
        self.set_operations = Vec::new();
        self.subquery_source = Some(union_query.clone());

        self
    }

    /// Perform a UNION ALL between two CustomDataFrames.
    /// Unlike `UNION`, `UNION ALL` does not remove duplicates.
    pub fn union_all(mut self, other: CustomDataFrame) -> Self {
        // Ensuring both DataFrames have the same number of columns
        if self.selected_columns.len() != other.selected_columns.len() {
            panic!(
                "Number of columns must match for UNION ALL. First DataFrame has {} columns, second has {} columns",
                self.selected_columns.len(),
                other.selected_columns.len()
            );
        }

        // Ensuring both DataFrames have the same *column names*
        for (self_col, other_col) in self.selected_columns.iter().zip(other.selected_columns.iter()) {
            if normalize_column_name(self_col) != normalize_column_name(other_col) {
                panic!(
                    "Column names must match for UNION ALL: {} vs {}",
                    self_col, other_col
                );
            }
        }

        // Merging the joined tables from both DataFrames
        let joined_tables: Vec<(String, DataFrame, String)> = self.joins.iter()
            .chain(other.joins.iter())
            .map(|join| (
                join.dataframe.table_alias.clone(), 
                join.dataframe.df.clone(),
                join.dataframe.from_table.clone()
            ))
            .collect();

        // Construct the sub-queries without the final alias
        let self_query = self.construct_sql()
            .trim_end_matches(&format!(" AS {}", self.table_alias))
            .to_string();
        let other_query = other.construct_sql()
            .trim_end_matches(&format!(" AS {}", other.table_alias))
            .to_string();

        // Wrap each SELECT query in parentheses
        let wrapped_self_query = format!("({})", self_query);
        let wrapped_other_query = format!("({})", other_query);

        // Replace `UNION` with `UNION ALL`
        let union_all_query = format!(
            "{} UNION ALL {}",
            wrapped_self_query,
            wrapped_other_query
        );

        // Wrap the entire UNION ALL statement in parentheses
        let union_subquery = format!("({})", union_all_query);

        // Update self to point to this new sub-query
        self.from_table = union_subquery;

        // Store the merged join state for registration in `elusion()`
        self.union_tables = Some(joined_tables);

        // field reset
        self.selected_columns.clear();
        self.aggregations.clear();
        self.group_by_columns.clear();
        self.where_conditions.clear();
        self.having_conditions.clear();
        self.order_by_columns.clear();
        self.limit_count = None;
        self.joins.clear();
        self.window_functions.clear();
        self.ctes.clear();
        self.set_operations.clear();
        self.subquery_source = Some(union_all_query.clone());

        self
    }


    /// Perform an INTERSECT between two CustomDataFrames.
    /// This returns rows common to *both* dataframes (removes duplicates).
    pub fn intersect(mut self, other: CustomDataFrame) -> Self {
        // Ensure both DataFrames have the same number of columns
        if self.selected_columns.len() != other.selected_columns.len() {
            panic!(
                "Number of columns must match for INTERSECT. \
                First DataFrame has {} columns, second has {} columns",
                self.selected_columns.len(),
                other.selected_columns.len()
            );
        }

        // Ensure both DataFrames have the same *column names*
        for (self_col, other_col) in self.selected_columns.iter().zip(other.selected_columns.iter()) {
            if normalize_column_name(self_col) != normalize_column_name(other_col) {
                panic!(
                    "Column names must match for INTERSECT: {} vs {}",
                    self_col, other_col
                );
            }
        }

        // Merge the joined tables from both DataFrames
        let joined_tables: Vec<(String, DataFrame, String)> = self.joins.iter()
            .chain(other.joins.iter())
            .map(|join| (
                join.dataframe.table_alias.clone(),
                join.dataframe.df.clone(),
                join.dataframe.from_table.clone()
            ))
            .collect();

        // Construct sub-queries without the final alias
        let self_query = self.construct_sql()
            .trim_end_matches(&format!(" AS {}", self.table_alias))
            .to_string();
        let other_query = other.construct_sql()
            .trim_end_matches(&format!(" AS {}", other.table_alias))
            .to_string();

        // Wrap each SELECT query in parentheses
        let wrapped_self_query = format!("({})", self_query);
        let wrapped_other_query = format!("({})", other_query);

        // Use INTERSECT instead of UNION
        let intersect_query = format!(
            "{} INTERSECT {}",
            wrapped_self_query,
            wrapped_other_query
        );

        // Wrap the entire INTERSECT statement in parentheses
        let intersect_subquery = format!("({})", intersect_query);

        // Update self to point to this new sub-query
        self.from_table = intersect_subquery;
        self.union_tables = Some(joined_tables);

        // field reset, because they are now part of the sub-query
        self.selected_columns.clear();
        self.aggregations.clear();
        self.group_by_columns.clear();
        self.where_conditions.clear();
        self.having_conditions.clear();
        self.order_by_columns.clear();
        self.limit_count = None;
        self.joins.clear();
        self.window_functions.clear();
        self.ctes.clear();
        self.set_operations.clear();
        self.subquery_source = Some(intersect_query.clone());

        self
    }

    /// Perform an EXCEPT between two CustomDataFrames.
    /// This returns rows in the *left* DataFrame that are *not* in the right DataFrame.
    pub fn except(mut self, other: CustomDataFrame) -> Self {
        // Ensure both DataFrames have the same number of columns
        if self.selected_columns.len() != other.selected_columns.len() {
            panic!(
                "Number of columns must match for EXCEPT. \
                First DataFrame has {} columns, second has {} columns",
                self.selected_columns.len(),
                other.selected_columns.len()
            );
        }

        // Ensure both DataFrames have the same *column names*
        for (self_col, other_col) in self.selected_columns.iter().zip(other.selected_columns.iter()) {
            if normalize_column_name(self_col) != normalize_column_name(other_col) {
                panic!(
                    "Column names must match for EXCEPT: {} vs {}",
                    self_col, other_col
                );
            }
        }

        // Merge the joined tables from both DataFrames
        let joined_tables: Vec<(String, DataFrame, String)> = self.joins.iter()
            .chain(other.joins.iter())
            .map(|join| (
                join.dataframe.table_alias.clone(),
                join.dataframe.df.clone(),
                join.dataframe.from_table.clone()
            ))
            .collect();

        // Construct sub-queries without the final alias
        let self_query = self.construct_sql()
            .trim_end_matches(&format!(" AS {}", self.table_alias))
            .to_string();
        let other_query = other.construct_sql()
            .trim_end_matches(&format!(" AS {}", other.table_alias))
            .to_string();

        // Wrap each SELECT query in parentheses
        let wrapped_self_query = format!("({})", self_query);
        let wrapped_other_query = format!("({})", other_query);

        // Use EXCEPT
        let except_query = format!(
            "{} EXCEPT {}",
            wrapped_self_query,
            wrapped_other_query
        );

        // Wrap the entire EXCEPT statement in parentheses
        let except_subquery = format!("({})", except_query);

        // Update self to point to this new sub-query
        self.from_table = except_subquery;
        self.union_tables = Some(joined_tables);

        // field reset
        self.selected_columns.clear();
        self.aggregations.clear();
        self.group_by_columns.clear();
        self.where_conditions.clear();
        self.having_conditions.clear();
        self.order_by_columns.clear();
        self.limit_count = None;
        self.joins.clear();
        self.window_functions.clear();
        self.ctes.clear();
        self.set_operations.clear();
        self.subquery_source = Some(except_query.clone());

        self
    }

    /// Pivot the DataFrame
    pub async fn pivot<const N: usize>(
        mut self,
        row_keys: [&str; N],
        pivot_column: &str,
        value_column: &str,
        aggregate_func: &str,
    ) -> ElusionResult<Self> {
        let ctx = Arc::new(SessionContext::new());
        
        // current DataFrame
        Self::register_df_as_table(&ctx, &self.table_alias, &self.df).await?;

        let schema = self.df.schema();
        // println!("Current DataFrame schema fields: {:?}", 
        //     schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());

        // Find columns in current schema
        let exact_pivot_column = schema.fields().iter()
            .find(|f| f.name().to_uppercase() == pivot_column.to_uppercase())
            .ok_or_else(|| {
                let available = schema.fields().iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>();
                ElusionError::Custom(format!(
                    "Column {} not found in current data. Available columns: {:?}", 
                    pivot_column, available
                ))
            })?
            .name();
            
        let exact_value_column = schema.fields().iter()
            .find(|f| f.name().to_uppercase() == value_column.to_uppercase())
            .ok_or_else(|| {
                let available = schema.fields().iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>();
                ElusionError::Custom(format!(
                    "Column {} not found in current data. Available columns: {:?}", 
                    value_column, available
                ))
            })?
            .name();

        // let distinct_query = format!(
        //     "SELECT DISTINCT \"{}\" \
        //      FROM \"{}\" AS {} \
        //      WHERE \"{}\" IS NOT NULL \
        //      AND \"{}\" IS NOT NULL \
        //      GROUP BY \"{}\" \
        //      HAVING {}(\"{}\") > 0", 
        //     exact_pivot_column,
        //     self.from_table,
        //     self.table_alias,
        //     exact_pivot_column,
        //     exact_value_column,
        //     exact_pivot_column,
        //     aggregate_func,
        //     exact_value_column
        // );
        let distinct_query = format!(
            "SELECT DISTINCT \"{}\" \
             FROM \"{}\" AS {} \
             WHERE \"{}\" IS NOT NULL \
             AND \"{}\" IS NOT NULL \
             ORDER BY \"{}\"",
            exact_pivot_column,
            self.from_table,
            self.table_alias,
            exact_pivot_column,
            exact_value_column,
            exact_pivot_column
        );

        let distinct_df = ctx.sql(&distinct_query).await
            .map_err(|e| ElusionError::Custom(format!("Failed to execute distinct query: {}", e)))?;
        
        let distinct_batches = distinct_df.collect().await
            .map_err(|e| ElusionError::Custom(format!("Failed to collect distinct values: {}", e)))?;

        // Extract distinct values into a Vec<String>
        let distinct_values: Vec<String> = distinct_batches
            .iter()
            .flat_map(|batch| {
                let array = batch.column(0);
                match array.data_type() {
                    ArrowDataType::Utf8 => {
                        let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                        (0..batch.num_rows())
                            .map(|i| string_array.value(i).to_string())
                            .collect::<Vec<_>>()
                    },
                    _ => {
                        // For non-string types, convert to string representation
                        let string_array = compute::cast(array, &ArrowDataType::Utf8)
                            .unwrap();
                        let string_array = string_array.as_any().downcast_ref::<StringArray>().unwrap();
                        (0..batch.num_rows())
                            .map(|i| string_array.value(i).to_string())
                            .collect::<Vec<_>>()
                    }
                }
            })
            .collect();

        // Create pivot columns for each distinct value
        let pivot_cols: Vec<String> = distinct_values
            .iter()
            .map(|val| {
                // Generate pivoted column expression
                let value_expr = if schema.field_with_name(None, &exact_pivot_column)
                    .map(|f| matches!(f.data_type(), ArrowDataType::Int32 | ArrowDataType::Int64 | ArrowDataType::Float32 | ArrowDataType::Float64))
                    .unwrap_or(false) {
                    // Numeric comparison without quotes
                    format!(
                        "COALESCE({}(CASE WHEN \"{}\" = '{}' THEN \"{}\" END), 0)",
                        aggregate_func,
                        exact_pivot_column,
                        val,
                        exact_value_column
                    )
                } else {
                    // String comparison with quotes
                    format!(
                        "COALESCE({}(CASE WHEN \"{}\" = '{}' THEN \"{}\" END), 0)",
                        aggregate_func,
                        exact_pivot_column,
                        val.replace("'", "''"),  // Escape single quotes
                        exact_value_column
                    )
                };

                // Format the full column expression with alias
                format!(
                    "{} AS \"{}_{}\"",
                    value_expr,
                    exact_pivot_column,
                    val.replace("\"", "\"\"")  // Escape double quotes in alias
                )
            })
            .collect();

            let row_keys_str = row_keys.iter()
            .map(|&key| {
                let exact_key = schema.fields().iter()
                    .find(|f| f.name().to_uppercase() == key.to_uppercase())
                    .map_or(key.to_string(), |f| f.name().to_string());
                format!("\"{}\"", exact_key)
            })
            .collect::<Vec<_>>()
            .join(", ");
        
        // Create the final pivot query
        let pivot_subquery = format!(
            "(SELECT {}, {} FROM \"{}\" AS {} GROUP BY {})",
            row_keys_str,
            pivot_cols.join(", "),
            self.from_table,
            self.table_alias,
            row_keys_str
        );

        // Update the DataFrame state
        self.from_table = pivot_subquery;
        self.selected_columns.clear();
        self.group_by_columns.clear();
        
        // Add row keys to selected columns
        self.selected_columns.extend(row_keys.iter().map(|&s| s.to_string()));
        
        // Add pivot columns to selected columns
        for val in distinct_values {
            self.selected_columns.push(
                format!("{}_{}",
                    normalize_column_name(pivot_column),
                    normalize_column_name(&val)
                )
            );
        }

        

        Ok(self)
    }

    /// Unpivot the DataFrame (melt operation)
    pub async fn unpivot<const N: usize, const M: usize>(
        mut self,
        id_columns: [&str; N],
        value_columns: [&str; M],
        name_column: &str,
        value_column: &str,
    ) -> ElusionResult<Self> {
        let ctx = Arc::new(SessionContext::new());
        
        // Register the current DataFrame
        Self::register_df_as_table(&ctx, &self.table_alias, &self.df).await?;
    
        let schema = self.df.schema();
        // println!("Current DataFrame schema fields: {:?}", 
        //     schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());
    
        // Find exact id columns from schema
        let exact_id_columns: Vec<String> = id_columns.iter()
            .map(|&id| {
                schema.fields().iter()
                    .find(|f| f.name().to_uppercase() == id.to_uppercase())
                    .map(|f| f.name().to_string())
                    .ok_or_else(|| {
                        let available = schema.fields().iter()
                            .map(|f| f.name())
                            .collect::<Vec<_>>();
                        ElusionError::Custom(format!(
                            "ID column '{}' not found in current data. Available columns: {:?}",
                            id, available
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
    
        // Find exact value columns from schema
        let exact_value_columns: Vec<String> = value_columns.iter()
            .map(|&val| {
                schema.fields().iter()
                    .find(|f| f.name().to_uppercase() == val.to_uppercase())
                    .map(|f| f.name().to_string())
                    .ok_or_else(|| {
                        let available = schema.fields().iter()
                            .map(|f| f.name())
                            .collect::<Vec<_>>();
                        ElusionError::Custom(format!(
                            "Value column '{}' not found in current data. Available columns: {:?}",
                            val, available
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
    
        // Create individual SELECT statements for each value column
        let selects: Vec<String> = exact_value_columns.iter().map(|val_col| {
            let id_cols_str = exact_id_columns.iter()
                .map(|id| format!("\"{}\"", id))
                .collect::<Vec<_>>()
                .join(", ");
    
            // // Use the final part of the value column for the label
            // let label = if let Some(pos) = val_col.rfind('_') {
            //     &val_col[pos + 1..]
            // } else {
            //     val_col
            // };
            
            format!(
                "SELECT {}, '{}' AS \"{}\", \"{}\" AS \"{}\" FROM \"{}\" AS {}",
                id_cols_str,
                val_col,
                // label,
                name_column,
                val_col,
                value_column,
                self.from_table,
                self.table_alias
            )
        }).collect();
    
        // Combine all SELECT statements with UNION ALL
        let unpivot_subquery = format!(
            "({})",
            selects.join(" UNION ALL ")
        );
    
        // Update the DataFrame state
        self.from_table = unpivot_subquery;
        self.selected_columns.clear();
        
        // Add identifier columns with proper quoting
        self.selected_columns.extend(
            exact_id_columns.iter()
                .map(|id| format!("\"{}\"", id))
        );
        self.selected_columns.push(format!("\"{}\"", name_column));
        self.selected_columns.push(format!("\"{}\"", value_column));
    
        Ok(self)
    }
    


     /// SELECT clause using const generics
     pub fn select<const N: usize>(self, columns: [&str; N]) -> Self {
        self.select_vec(columns.to_vec())
    }

    /// Add selected columns to the SELECT clause using a Vec<&str>
    pub fn select_vec(mut self, columns: Vec<&str>) -> Self {

         // Store original expressions with AS clauses
        self.original_expressions = columns
        .iter()
        .filter(|&col| col.contains(" AS "))
        .map(|&s| s.to_string())
        .collect();

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

        // Determine if it's a subquery with no selected columns
        let is_subquery = self.from_table.starts_with('(') && self.from_table.ends_with(')');
        let no_selected_columns = self.selected_columns.is_empty() && self.aggregations.is_empty() && self.window_functions.is_empty();

        if is_subquery && no_selected_columns {
            // Return only the subquery without wrapping it in SELECT * FROM
            query.push_str(&format!("{}", self.from_table));
        } else {
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
            query.push_str(" FROM ");
            if is_subquery {
                // It's a subquery; do not assign alias here
                query.push_str(&format!("{}", self.from_table));
            } else {
                // Regular table; quote as usual
                query.push_str(&format!(
                    "\"{}\" AS {}",
                    self.from_table.trim(),
                    self.table_alias
                ));
            }

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
        }

        query
    }

    /// Execute the constructed SQL and return a new CustomDataFrame
    pub async fn elusion(&self, alias: &str) -> ElusionResult<Self> {
        let ctx = Arc::new(SessionContext::new());

        // Always register the base table first
        Self::register_df_as_table(&ctx, &self.table_alias, &self.df).await?;

        // For non-UNION queries, also register joined tables
        if self.union_tables.is_none() {
            for join in &self.joins {
                Self::register_df_as_table(&ctx, &join.dataframe.table_alias, &join.dataframe.df).await?;
            }
        }

        // For UNION queries with joins
        if let Some(tables) = &self.union_tables {
            for (table_alias, df, _) in tables {
                // 1) If table_alias is already registered, skip
                if ctx.table(table_alias).await.is_ok() {
                    // Already registered, so just skip
                    continue;
                }
                // 2) Otherwise, register the table
                Self::register_df_as_table(&ctx, table_alias, df).await?;
            }
        }

        let sql = if self.from_table.starts_with('(') && self.from_table.ends_with(')') {
            format!("SELECT * FROM {} AS {}", self.from_table, alias)
        } else {
            self.construct_sql()
        };

        // println!("Constructed SQL:\n{}", sql);
        
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
            union_tables: None,
            original_expressions: self.original_expressions.clone(), 
        })
    }

    /// Display functions that display results to terminal
    pub async fn display(&self) -> Result<(), DataFusionError> {
        self.df.clone().show().await.map_err(|e| {
            DataFusionError::Execution(format!("Failed to display DataFrame: {}", e))
        })
    }
    /// DISPLAY Query Plan
    // pub fn display_query_plan(&self) {
    //     println!("Generated Logical Plan:");
    //     println!("{:?}", self.df.logical_plan());
    // }
    
    /// Displays the current schema for debugging purposes.
    // pub fn display_schema(&self) {
    //     println!("Current Schema for '{}': {:?}", self.table_alias, self.df.schema());
    // }

    /// Dipslaying query genereated from chained functions
    /// Displays the SQL query generated from the chained functions
    // pub fn display_query(&self) {
    //     let final_query = self.construct_sql();
    //     println!("Generated SQL Query: {}", final_query);
    // }

// ====================== WRITERS ==================== //

/// Write the DataFrame to a Parquet file
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

/// Writes a DataFusion `DataFrame` to a Delta table at `path`
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
            //  "merge" to auto-merge schema
            (false, WriteMode::MergeSchema)
        }
        "default" => {
            // Another alias for (false, WriteMode::Default)
            (false, WriteMode::Default)
        }
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

/// Loads a JSON file into a DataFusion DataFrame
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

/// Load a Delta table at `file_path` into a DataFusion DataFrame and wrap it in `AliasedDataFrame`
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


// -------------------- PLOTING -------------------------- //
    ///Create line plot
    pub async fn plot_line(
        &self, 
        x_col: &str, 
        y_col: &str,
        show_markers: bool,
        title: Option<&str>
    ) -> ElusionResult<Plot> {
        let batches = self.df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        let batch = &batches[0];
        
        let x_idx = batch.schema().index_of(x_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", x_col, e)))?;
        let y_idx = batch.schema().index_of(y_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", y_col, e)))?;

        // Get arrays and convert to vectors
        let x_values: Vec<f64> = convert_to_f64_vec(batch.column(x_idx))?;
        let y_values: Vec<f64> = convert_to_f64_vec(batch.column(y_idx))?;

        // Sort values chronologically
        let (sorted_x, sorted_y) = sort_by_date(&x_values, &y_values);

        // Create trace with appropriate mode
        let trace = if show_markers {
            Scatter::new(sorted_x, sorted_y)
                .mode(Mode::LinesMarkers)
                .name(&format!("{} vs {}", y_col, x_col))
                .line(Line::new()
                    .color(Rgb::new(55, 128, 191))
                    .width(2.0))
                .marker(Marker::new()
                    .color(Rgb::new(55, 128, 191))
                    .size(8))
        } else {
            Scatter::new(sorted_x, sorted_y)
                .mode(Mode::Lines)
                .name(&format!("{} vs {}", y_col, x_col))
                .line(Line::new()
                    .color(Rgb::new(55, 128, 191))
                    .width(2.0))
        };
            
        let mut plot = Plot::new();
        plot.add_trace(trace);
        
        // Check if x column is a date type and set axis accordingly
        let x_axis = if matches!(batch.column(x_idx).data_type(), ArrowDataType::Date32) {
            Axis::new()
                .title(x_col.to_string())
                .grid_color(Rgb::new(229, 229, 229))
                .show_grid(true)
                .type_(plotly::layout::AxisType::Date)
        } else {
            Axis::new()
                .title(x_col.to_string())
                .grid_color(Rgb::new(229, 229, 229))
                .show_grid(true)
        };

        let layout = Layout::new()
            .title(title.unwrap_or(&format!("{} vs {}", y_col, x_col))) 
            .x_axis(x_axis)
            .y_axis(Axis::new()
                .title(y_col.to_string())     
                .grid_color(Rgb::new(229, 229, 229))
                .show_grid(true));

        plot.set_layout(layout);
        Ok(plot)
    }

    /// Create time series Plot
    pub async fn plot_time_series(
        &self,
        date_col: &str,
        value_col: &str,
        show_markers: bool,
        title: Option<&str>,
    ) -> ElusionResult<Plot> {
        let batches = self.df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        let batch = &batches[0];
        
        let x_idx = batch.schema().index_of(date_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", date_col, e)))?;
        let y_idx = batch.schema().index_of(value_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", value_col, e)))?;

        // Check if x column is a date type
        if !matches!(batch.column(x_idx).data_type(), ArrowDataType::Date32) {
            return Err(ElusionError::Custom(
                format!("Column {} must be a Date32 type for time series plot", date_col)
            ));
        }

        let x_values = convert_to_f64_vec(batch.column(x_idx))?;
        let y_values = convert_to_f64_vec(batch.column(y_idx))?;

        // Sort values chronologically
        let (sorted_x, sorted_y) = sort_by_date(&x_values, &y_values);

        let trace = if show_markers {
            Scatter::new(sorted_x, sorted_y)
                .mode(Mode::LinesMarkers)
                .name(value_col)
                .line(Line::new()
                    .color(Rgb::new(55, 128, 191))
                    .width(2.0))
                .marker(Marker::new()
                    .color(Rgb::new(55, 128, 191))
                    .size(8))
        } else {
            Scatter::new(sorted_x, sorted_y)
                .mode(Mode::Lines)
                .name(value_col)
                .line(Line::new()
                    .color(Rgb::new(55, 128, 191))
                    .width(2.0))
        };

        let mut plot = Plot::new();
        plot.add_trace(trace);

        let layout = Layout::new()
            .title(title.unwrap_or(&format!("{} over Time", value_col)))
            .x_axis(Axis::new()
                .title(date_col.to_string())
                .grid_color(Rgb::new(229, 229, 229))
                .show_grid(true)
                .type_(plotly::layout::AxisType::Date))
            .y_axis(Axis::new()
                .title(value_col.to_string())
                .grid_color(Rgb::new(229, 229, 229))
                .show_grid(true));

        plot.set_layout(layout);
        Ok(plot)
    }

    /// Create a scatter plot from two columns
    pub async fn plot_scatter(
        &self,
        x_col: &str,
        y_col: &str,
        marker_size: Option<usize>,
    ) -> ElusionResult<Plot> {
        let batches = self.df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        let batch = &batches[0];
        
        let x_idx = batch.schema().index_of(x_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", x_col, e)))?;
        let y_idx = batch.schema().index_of(y_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", y_col, e)))?;

        let x_values: Vec<f64> = convert_to_f64_vec(batch.column(x_idx))?;
        let y_values: Vec<f64> = convert_to_f64_vec(batch.column(y_idx))?;

        let trace = Scatter::new(x_values, y_values)
            .mode(Mode::Markers)
            .name(&format!("{} vs {}", y_col, x_col))
            .marker(Marker::new()
                .color(Rgb::new(55, 128, 191))
                .size(marker_size.unwrap_or(8)));

        let mut plot = Plot::new();
        plot.add_trace(trace);
        
        let layout = Layout::new()
            .title(format!("Scatter Plot: {} vs {}", y_col, x_col))
            .x_axis(Axis::new().title(x_col.to_string()))
            .y_axis(Axis::new().title(y_col.to_string()));

        plot.set_layout(layout);
        Ok(plot)
    }

    /// Create a bar chart from two columns
    pub async fn plot_bar(
        &self,
        x_col: &str,
        y_col: &str,
        orientation: Option<&str>, 
        title: Option<&str>,
    ) -> ElusionResult<Plot> {
        let batches = self.df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        let batch = &batches[0];

        let x_idx = batch.schema().index_of(x_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", x_col, e)))?;
        let y_idx = batch.schema().index_of(y_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", y_col, e)))?;

        let (x_values, y_values) = if batch.column(x_idx).data_type() == &ArrowDataType::Utf8 {
            (convert_to_string_vec(batch.column(x_idx))?, convert_to_f64_vec(batch.column(y_idx))?)
        } else {
            (convert_to_string_vec(batch.column(y_idx))?, convert_to_f64_vec(batch.column(x_idx))?)
        };

        let trace = match orientation.unwrap_or("v") {
            "h" => {
                Bar::new(x_values.clone(), y_values.clone())
                    .orientation(Orientation::Horizontal)
                    .name(&format!("{} by {}", y_col, x_col))
            },
            _ => {
                Bar::new(x_values, y_values)
                    .orientation(Orientation::Vertical)
                    .name(&format!("{} by {}", y_col, x_col))
            }
        };

        let mut plot = Plot::new();
        plot.add_trace(trace);

        let layout = Layout::new()
            .title(title.unwrap_or(&format!("Bar Chart: {} by {}", y_col, x_col)))
            .x_axis(Axis::new().title(x_col.to_string()))
            .y_axis(Axis::new().title(y_col.to_string()));

        plot.set_layout(layout);
        Ok(plot)
    }

    /// Create a histogram from a single column
    pub async fn plot_histogram(
        &self,
        col: &str,
        bins: Option<usize>,
        title: Option<&str>
    ) -> ElusionResult<Plot> {
        let batches = self.df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        let batch = &batches[0];

        let idx = batch.schema().index_of(col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", col, e)))?;

        let values = convert_to_f64_vec(batch.column(idx))?;

        let trace = Histogram::new(values)
            .name(col)
            .n_bins_x(bins.unwrap_or(30));

        let mut plot = Plot::new();
        plot.add_trace(trace);

        let layout = Layout::new()
            .title(title.unwrap_or(&format!("Histogram of {}", col)))
            .x_axis(Axis::new().title(col.to_string()))
            .y_axis(Axis::new().title("Count".to_string()));

        plot.set_layout(layout);
        Ok(plot)
    }

    /// Create a box plot from a column
    pub async fn plot_box(
        &self,
        value_col: &str,
        group_by_col: Option<&str>,
        title: Option<&str>,
    ) -> ElusionResult<Plot> {
        let batches = self.df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        let batch = &batches[0];
        
        // Get value column index
        let value_idx = batch.schema().index_of(value_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", value_col, e)))?;

        // Convert values column
        let values = convert_to_f64_vec(batch.column(value_idx))?;

        let trace = if let Some(group_col) = group_by_col {
            // Get group column index
            let group_idx = batch.schema().index_of(group_col)
                .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", group_col, e)))?;

            // Convert group column to strings
            let groups = convert_to_f64_vec(batch.column(group_idx))?;

            BoxPlot::new(values)
                .x(groups) // Groups on x-axis
                .name(value_col)
        } else {
            BoxPlot::new(values)
                .name(value_col)
        };

        let mut plot = Plot::new();
        plot.add_trace(trace);

        let layout = Layout::new()
            .title(title.unwrap_or(&format!("Distribution of {}", value_col)))
            .y_axis(Axis::new()
                .title(value_col.to_string())
                .grid_color(Rgb::new(229, 229, 229))
                .show_grid(true))
            .x_axis(Axis::new()
                .title(group_by_col.unwrap_or("").to_string())
                .grid_color(Rgb::new(229, 229, 229))
                .show_grid(true));

        plot.set_layout(layout);
        Ok(plot)
    }

     /// Create a pie chart from two columns: labels and values
     pub async fn plot_pie(
        &self,
        label_col: &str,
        value_col: &str,
        title: Option<&str>,
    ) -> ElusionResult<Plot> {
        let batches = self.df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        let batch = &batches[0];

        // Get column indices
        let label_idx = batch.schema().index_of(label_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", label_col, e)))?;
        let value_idx = batch.schema().index_of(value_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", value_col, e)))?;

        // Convert columns to appropriate types
        let labels = convert_to_string_vec(batch.column(label_idx))?;
        let values = convert_to_f64_vec(batch.column(value_idx))?;

        // Create the pie chart trace
        let trace = Pie::new(values)
            .labels(labels)
            .name(value_col)
            .hole(0.0);

        let mut plot = Plot::new();
        plot.add_trace(trace);

        // Create layout
        let layout = Layout::new()
            .title(title.unwrap_or(&format!("Distribution of {}", value_col)))
            .show_legend(true);

        plot.set_layout(layout);
        Ok(plot)
    }

    /// Create a donut chart (pie chart with a hole)
    pub async fn plot_donut(
        &self,
        label_col: &str,
        value_col: &str,
        title: Option<&str>,
        hole_size: Option<f64>, // Value between 0 and 1
    ) -> ElusionResult<Plot> {
        let batches = self.df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        let batch = &batches[0];

        let label_idx = batch.schema().index_of(label_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", label_col, e)))?;
        let value_idx = batch.schema().index_of(value_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", value_col, e)))?;

        let labels = convert_to_string_vec(batch.column(label_idx))?;
        let values = convert_to_f64_vec(batch.column(value_idx))?;

        // Ensure hole size is between 0 and 1
        let hole_size = hole_size.unwrap_or(0.5).max(0.0).min(1.0);

        let trace = Pie::new(values)
            .labels(labels)
            .name(value_col)
            .hole(hole_size); 

        let mut plot = Plot::new();
        plot.add_trace(trace);

        let layout = Layout::new()
            .title(title.unwrap_or(&format!("Distribution of {}", value_col)))
            .show_legend(true);

        plot.set_layout(layout);
        Ok(plot)
    }
   
    /// Save plot to HTML file
    pub async fn save_plot(plot: &Plot, filename: &str, path: Option<&str>) -> ElusionResult<()> {
        let file_path = if let Some(dir_path) = path {
            let path = std::path::Path::new(dir_path);
            // Create directories if they don't exist
            if !path.exists() {
                std::fs::create_dir_all(path)
                    .map_err(|e| ElusionError::Custom(
                        format!("Failed to create directory '{}': {}", dir_path, e)
                    ))?;
            }
            path.join(filename)
        } else {
            std::path::Path::new(filename).to_path_buf()
        };

        // Convert the path to string for write_html
        let file_path_str = file_path.to_str()
            .ok_or_else(|| ElusionError::Custom("Invalid path".to_string()))?;

        // Write the plot directly since plotly's types aren't thread-safe
        plot.write_html(file_path_str);

        Ok(())
    }
    /// Create a report containing multiple plots in a single HTML file
    pub async fn create_report(
        plots: &[(&Plot, &str)],
        report_title: &str,
        filename: &str,
        path: Option<&str>,
    ) -> ElusionResult<()> {
        // Handle file path
        let file_path = if let Some(dir_path) = path {
            let path = std::path::Path::new(dir_path);
            if !path.exists() {
                std::fs::create_dir_all(path)
                    .map_err(|e| ElusionError::Custom(
                        format!("Failed to create directory '{}': {}", dir_path, e)
                    ))?;
            }
            path.join(filename)
        } else {
            std::path::Path::new(filename).to_path_buf()
        };

        let file_path_str = file_path.to_str()
            .ok_or_else(|| ElusionError::Custom("Invalid path".to_string()))?;

        // Create HTML content
        let mut html_content = format!(
            r#"<!DOCTYPE html>
                <html>
                <head>
                    <title>{}</title>
                    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
                    <style>
                        body {{
                            font-family: Arial, sans-serif;
                            margin: 0;
                            padding: 20px;
                            background-color: #f5f5f5;
                        }}
                        .container {{
                            max-width: 1200px;
                            margin: 0 auto;
                            background-color: white;
                            padding: 20px;
                            border-radius: 8px;
                            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                        }}
                        h1 {{
                            color: #333;
                            text-align: center;
                            margin-bottom: 30px;
                        }}
                        .plot-container {{
                            margin-bottom: 30px;
                            padding: 15px;
                            background-color: white;
                            border-radius: 4px;
                            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
                        }}
                        .plot-title {{
                            font-size: 18px;
                            font-weight: bold;
                            margin-bottom: 10px;
                            color: #444;
                        }}
                        .grid {{
                            display: grid;
                            grid-template-columns: 1fr;
                            gap: 20px;
                        }}
                    </style>
                </head>
                <body>
                    <div class="container">
                        <h1>{}</h1>
                        <div class="grid">
                "#,
            report_title, report_title
        );

        // Add each plot
        for (index, (plot, title)) in plots.iter().enumerate() {
            let div_id = format!("plot_{}", index);
            let data_json = serde_json::to_string(plot.data())
                .map_err(|e| ElusionError::Custom(format!("Failed to serialize plot data: {}", e)))?;
            let layout_json = serde_json::to_string(plot.layout())
                .map_err(|e| ElusionError::Custom(format!("Failed to serialize plot layout: {}", e)))?;
                
            html_content.push_str(&format!(
                r#"            
                <div class="plot-container">
                    <div class="plot-title">{}</div>
                    <div id="{}"></div>
                    <script>
                        var data = {};
                        var layout = {};
                        Plotly.newPlot("{}", data, layout);
                    </script>
                </div>
                "#,
                title,
                div_id,
                data_json,
                layout_json,
                div_id
            ));
        }

        // Close HTML
        html_content.push_str(
        r#"        
                    </div>
                    </div>
                    </body>
                </html>"#
        );

        // Write to file
        let mut file = File::create(file_path_str)
            .map_err(|e| ElusionError::Custom(format!("Failed to create file: {}", e)))?;
        
        file.write_all(html_content.as_bytes())
            .map_err(|e| ElusionError::Custom(format!("Failed to write to file: {}", e)))?;

        Ok(())
    }

   
}


fn convert_to_f64_vec(array: &dyn Array) -> ElusionResult<Vec<f64>> {
    match array.data_type() {
        ArrowDataType::Float64 => {
            let float_array = array.as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| ElusionError::Custom("Failed to downcast to Float64Array".to_string()))?;
            Ok(float_array.values().to_vec())
        },
        ArrowDataType::Int64 => {
            let int_array = array.as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| ElusionError::Custom("Failed to downcast to Int64Array".to_string()))?;
            Ok(int_array.values().iter().map(|&x| x as f64).collect())
        },
        ArrowDataType::Date32 => {
            let date_array = array.as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| ElusionError::Custom("Failed to downcast to Date32Array".to_string()))?;
            Ok(convert_date32_to_timestamps(date_array))
        },
        ArrowDataType::Utf8 => {
            let string_array = array.as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| ElusionError::Custom("Failed to downcast to StringArray".to_string()))?;
            let mut values = Vec::with_capacity(array.len());
            for i in 0..array.len() {
                let value = string_array.value(i).parse::<f64>().unwrap_or(0.0);
                values.push(value);
            }
            Ok(values)
        },
        other_type => {
            Err(ElusionError::Custom(format!("Unsupported data type for plotting: {:?}", other_type)))
        }
    }
}

fn convert_to_string_vec(array: &dyn Array) -> ElusionResult<Vec<String>> {
    match array.data_type() {
        ArrowDataType::Utf8 => {
            let string_array = array.as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| ElusionError::Custom("Failed to downcast to StringArray".to_string()))?;
            
            let mut values = Vec::with_capacity(array.len());
            for i in 0..array.len() {
                values.push(string_array.value(i).to_string());
            }
            Ok(values)
        },
        other_type => {
            Err(ElusionError::Custom(format!("Expected string type but got: {:?}", other_type)))
        }
    }
}

fn convert_date32_to_timestamps(array: &Date32Array) -> Vec<f64> {
    array.values()
        .iter()
        .map(|&days| {
            // Convert days since epoch to timestamp
            let date = NaiveDate::from_num_days_from_ce_opt(days + 719163)
                .unwrap_or(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
            let datetime = date.and_hms_opt(0, 0, 0).unwrap();
            datetime.and_utc().timestamp() as f64 * 1000.0 // Convert to milliseconds for plotly
        })
        .collect()
}

// Helper function to sort date-value pairs
fn sort_by_date(x_values: &[f64], y_values: &[f64]) -> (Vec<f64>, Vec<f64>) {
    let mut pairs: Vec<(f64, f64)> = x_values.iter()
        .cloned()
        .zip(y_values.iter().cloned())
        .collect();
    
    // Sort by date (x values)
    pairs.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
    
    // Unzip back into separate vectors
    pairs.into_iter().unzip()
}