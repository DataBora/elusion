use crate::prelude::*;
#[cfg(feature = "postgres")]
use tokio_postgres::{Client as PostgresClient,NoTls, Error as PgError, Row as PostgresRow};
#[cfg(feature = "postgres")]
use tokio_postgres::types::{Type, ToSql};
#[cfg(feature = "postgres")]
use tokio::sync::Mutex as PostgresMutex;
#[cfg(feature = "postgres")]
use rust_decimal::Decimal;
#[cfg(feature = "postgres")]
use rust_decimal::prelude::ToPrimitive;

/// PostgreSQL connection configuration options
#[cfg(feature = "postgres")]
#[derive(Debug, Clone)]
pub struct PostgresConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub pool_size: Option<usize>,
}

#[cfg(feature = "postgres")]
impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5433,
            user: "postgres".to_string(),
            password: String::new(),
            database: "postgres".to_string(),
            pool_size: Some(5),
        }
    }
}
#[cfg(feature = "postgres")]
impl PostgresConfig {
    /// Create a new PostgresConfig with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a connection string from the configuration
    pub fn connection_string(&self) -> String {
        let mut params = Vec::new();
        
        params.push(format!("host={}", self.host));
        params.push(format!("port={}", self.port));
        params.push(format!("user={}", self.user));
        params.push(format!("dbname={}", self.database));
        
        if !self.password.is_empty() {
            params.push(format!("password={}", self.password));
        }
        
        // Add sslmode=prefer for better compatibility
        params.push("sslmode=prefer".to_string());
        
        params.join(" ")
    }
}

#[cfg(not(feature = "postgres"))]
#[derive(Debug, Clone)]
pub struct PostgresConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub pool_size: Option<usize>,
}

#[cfg(not(feature = "postgres"))]
impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: String::new(),
            database: "postgres".to_string(),
            pool_size: Some(5),
        }
    }
}

#[cfg(not(feature = "postgres"))]
impl PostgresConfig {
    /// Create a new PostgresConfig with default values (stub)
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a connection string from the configuration (stub)
    pub fn connection_string(&self) -> String {
        String::new()
    }
}

/// PostgreSQL connection manager that supports connection pooling
#[cfg(feature = "postgres")]
pub struct PostgresConnection {
    config: PostgresConfig,
    client_pool: Arc<PostgresMutex<Vec<PostgresClient>>>,
}

#[cfg(not(feature = "postgres"))]
pub struct PostgresConnection {
}

#[cfg(feature = "postgres")]
impl PostgresConnection {
    /// Create a new PostgreSQL connection manager
    pub async fn new(config: PostgresConfig) -> Result<Self, PgError> {
        let pool_size = config.pool_size.unwrap_or(5);
        let mut clients = Vec::with_capacity(pool_size);

        // Create initial connection
        let conn_str = config.connection_string();
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
        
        // Spawn the connection task
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });
        
        clients.push(client);
        
        // Create pool of connections
        for _ in 1..pool_size {
            let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
            
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("Connection error: {}", e);
                }
            });
            
            clients.push(client);
        }

        Ok(Self {
            config,
            client_pool: Arc::new(PostgresMutex::new(clients)),
        })
    }

    /// Get a client from the pool
    async fn get_client(&self) -> Result<PostgresClient, PgError> {
        let mut pool = self.client_pool.lock().await;
        
        if let Some(client) = pool.pop() {
            Ok(client)
        } else {
            // If pool is empty, create a new connection
            let conn_str = self.config.connection_string();
            let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
            
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("Connection error: {}", e);
                }
            });
            
            Ok(client)
        }
    }

    /// Return a client to the pool
    async fn return_client(&self, client: PostgresClient) {
        let mut pool = self.client_pool.lock().await;
        
        if pool.len() < self.config.pool_size.unwrap_or(5) {
            pool.push(client);
        }
        // If pool is at capacity, client will be dropped
    }

    /// Execute a query that returns rows
    pub async fn query(&self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<PostgresRow>, PgError> {
        let client = self.get_client().await?;
        
        let result = client.query(query, params).await;
        
        self.return_client(client).await;
        
        result
    }

    /// Check if the connection is valid
    pub async fn ping(&self) -> Result<(), PgError> {
        let client = self.get_client().await?;
        
        let result = client.execute("SELECT 1", &[]).await;
        
        self.return_client(client).await;
        
        result.map(|_| ())
    }

}

#[cfg(not(feature = "postgres"))]
impl PostgresConnection {
    /// Create a new PostgreSQL connection manager (stub)
    pub async fn new(_config: PostgresConfig) -> Result<Self, ElusionError> {
        Err(ElusionError::Custom("*** Warning ***: Postgres feature not enabled. Add feature under [dependencies]".to_string()))
    }

    /// Execute a query that returns rows (stub)
    pub async fn query(&self, _query: &str, _params: &[&str]) -> Result<Vec<()>, ElusionError> {
        Err(ElusionError::Custom("*** Warning ***: Postgres feature not enabled. Add feature under [dependencies]".to_string()))
    }

    /// Check if the connection is valid (stub)
    pub async fn ping(&self) -> Result<(), ElusionError> {
        Err(ElusionError::Custom("*** Warning ***: Postgres feature not enabled. Add feature under [dependencies]".to_string()))
    }
}

#[cfg(feature = "postgres")]
impl From<tokio_postgres::error::Error> for ElusionError {
    fn from(err: tokio_postgres::error::Error) -> Self {
        ElusionError::Custom(format!("PostgreSQL error: {}", err))
    }
}

  /// Create a DataFrame from a PostgreSQL query
    #[cfg(feature = "postgres")]
    pub async fn from_postgres_impl(
        conn: &PostgresConnection,
        query: &str,
        alias: &str
    ) -> ElusionResult<CustomDataFrame> {
        let rows = conn.query(query, &[])
            .await
            .map_err(|e| ElusionError::Custom(format!("PostgreSQL query error: {}", e)))?;

        if rows.is_empty() {
            return Err(ElusionError::Custom("Query returned no rows".to_string()));
        }

        // Extract column info from the first row
        let first_row = &rows[0];
        let columns = first_row.columns();
//         for (i, column) in first_row.columns().iter().enumerate() {
//     println!("Column {}: '{}' -> PG type: {:?}", i, column.name(), column.type_());
// }
        // Create schema from column metadata
        let mut fields = Vec::with_capacity(columns.len());
        for column in columns {
            let column_name = column.name();
            let pg_type = column.type_();
            
            // Map PostgreSQL types to Arrow types
            let arrow_type = match *pg_type {
                Type::BOOL => ArrowDataType::Boolean,
                Type::INT2 | Type::INT4 => ArrowDataType::Int64,
                Type::INT8 => ArrowDataType::Int64,
                Type::FLOAT4 => ArrowDataType::Float32,
                Type::FLOAT8 => ArrowDataType::Float64,
                Type::TEXT | Type::VARCHAR | Type::CHAR | Type::NAME | Type::UNKNOWN => ArrowDataType::Utf8,
                Type::NUMERIC => ArrowDataType::Float64, 
                Type::TIMESTAMP | Type::TIMESTAMPTZ => ArrowDataType::Utf8, //ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                Type::DATE => ArrowDataType::Date32,
                Type::TIME | Type::TIMETZ => ArrowDataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
                Type::UUID | Type::JSON | Type::JSONB => ArrowDataType::Utf8, 
                
                _ => ArrowDataType::Utf8, // Fallback for unsupported types
            };
            
            fields.push(Field::new(column_name, arrow_type, true));
        }
        
        let schema = Arc::new(Schema::new(fields));
        
        // Build arrays for each column
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
        
        for col_idx in 0..columns.len() {
            // let column = &columns[col_idx];
            let field = schema.field(col_idx);
            
            match field.data_type() {
                ArrowDataType::Boolean => {
                    let mut builder = BooleanBuilder::new();
                    for row in &rows {
                        match row.try_get::<_, Option<bool>>(col_idx) {
                            Ok(Some(value)) => builder.append_value(value),
                            Ok(None) => builder.append_null(),
                            Err(_) => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                ArrowDataType::Int32 => {
                    let mut builder = Int64Builder::new();
                    for row in &rows {
                        match row.try_get::<_, Option<i32>>(col_idx) {
                            Ok(Some(value)) => builder.append_value(value as i64),
                            Ok(None) => builder.append_null(),
                            Err(_) => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                ArrowDataType::Int64 => {
                    let mut builder = Int64Builder::new();
                    for row in &rows {
                        // Try multiple approaches to get the integer value
                        if let Ok(Some(value)) = row.try_get::<_, Option<i64>>(col_idx) {
                            builder.append_value(value);
                        } else if let Ok(Some(value)) = row.try_get::<_, Option<i32>>(col_idx) {
                            builder.append_value(value as i64);
                        } else if let Ok(Some(value)) = row.try_get::<_, Option<i16>>(col_idx) {
                            builder.append_value(value as i64);
                        } else {
                            builder.append_null();
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                ArrowDataType::Float64 => {
                    let mut builder = Float64Builder::new();
                    for row in &rows {
                        // First try to get as Decimal 
                        if let Ok(Some(decimal)) = row.try_get::<_, Option<Decimal>>(col_idx) {
                            if let Some(float_val) = decimal.to_f64() {
                                builder.append_value(float_val);
                            } else {
                                builder.append_null();
                            }
                        } else if let Ok(Some(value)) = row.try_get::<_, Option<f64>>(col_idx) {
                            builder.append_value(value);
                        } else if let Ok(Some(value)) = row.try_get::<_, Option<f32>>(col_idx) {
                            builder.append_value(value as f64);
                        } else if let Ok(Some(value)) = row.try_get::<_, Option<i64>>(col_idx) {
                            builder.append_value(value as f64);
                        } else if let Ok(Some(value)) = row.try_get::<_, Option<i32>>(col_idx) {
                            builder.append_value(value as f64);
                        } else if let Ok(Some(value_str)) = row.try_get::<_, Option<String>>(col_idx) {
                            if let Ok(num) = value_str.parse::<f64>() {
                                builder.append_value(num);
                            } else {
                                builder.append_null();
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                ArrowDataType::Float32 => {
                    let mut builder = Float64Builder::new();
                    for row in &rows {
                        match row.try_get::<_, Option<f64>>(col_idx) {
                            Ok(Some(value)) => builder.append_value(value),
                            Ok(None) => builder.append_null(),
                            Err(_) => match row.try_get::<_, Option<f32>>(col_idx) {
                                Ok(Some(value)) => builder.append_value(value as f64),
                                Ok(None) => builder.append_null(),
                                Err(_) => builder.append_null(),
                            },
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                ArrowDataType::Utf8 => {
                    let mut builder = StringBuilder::new();
                    for row in &rows {
                        match row.try_get::<_, Option<String>>(col_idx) {
                            Ok(Some(value)) => builder.append_value(value),
                            Ok(None) => builder.append_null(),
                            Err(_) => {
                                // Try as &str if String fails
                                if let Ok(Some(value)) = row.try_get::<_, Option<&str>>(col_idx) {
                                    builder.append_value(value);
                                } else if let Ok(Some(dt)) = row.try_get::<_, Option<chrono::NaiveDateTime>>(col_idx) {
                                    // Now this should work with the chrono feature enabled
                                    builder.append_value(dt.format("%Y-%m-%d %H:%M:%S").to_string());
                                } else if let Ok(Some(dt)) = row.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(col_idx) {
                                    builder.append_value(dt.format("%Y-%m-%d %H:%M:%S UTC").to_string());
                                } else if let Ok(Some(ts)) = row.try_get::<_, Option<std::time::SystemTime>>(col_idx) {
                                    // Try SystemTime as fallback
                                    if let Ok(duration) = ts.duration_since(std::time::UNIX_EPOCH) {
                                        let secs = duration.as_secs();
                                        let datetime = chrono::DateTime::from_timestamp(secs as i64, 0)
                                            .unwrap_or_default();
                                        builder.append_value(datetime.format("%Y-%m-%d %H:%M:%S").to_string());
                                    } else {
                                        builder.append_null();
                                    }
                                } else if let Ok(Some(value)) = row.try_get::<_, Option<f64>>(col_idx) {
                                    builder.append_value(value.to_string());
                                } else if let Ok(Some(value)) = row.try_get::<_, Option<i64>>(col_idx) {
                                    builder.append_value(value.to_string());
                                } else if let Ok(Some(value)) = row.try_get::<_, Option<bool>>(col_idx) {
                                    builder.append_value(value.to_string());
                                } else {
                                    // Debug: let's see what type this actually is
                                    println!("Debug: Could not convert column {} ({})", col_idx, columns[col_idx].name());
                                    builder.append_null();
                                }
                            },
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                // Handle other types as strings for now
                _ => {
                    let mut builder = StringBuilder::new();
                    for row in &rows {
                        // Get the value as text representation
                        match row.try_get::<_, Option<String>>(col_idx) {
                            Ok(Some(value)) => builder.append_value(value),
                            Ok(None) => builder.append_null(),
                            Err(_) => {
                                // Try common types that implement FromSql
                                if let Ok(value) = row.try_get::<_, Option<f64>>(col_idx) {
                                    if let Some(num) = value {
                                        builder.append_value(num.to_string());
                                    } else {
                                        builder.append_null();
                                    }
                                } else if let Ok(value) = row.try_get::<_, Option<i64>>(col_idx) {
                                    if let Some(num) = value {
                                        builder.append_value(num.to_string());
                                    } else {
                                        builder.append_null();
                                    }
                                } else if let Ok(value) = row.try_get::<_, Option<bool>>(col_idx) {
                                    if let Some(b) = value {
                                        builder.append_value(b.to_string());
                                    } else {
                                        builder.append_null();
                                    }
                                } else {
                                    // Last resort: use a placeholder
                                    builder.append_value(format!("?column?_{}", col_idx));
                                }
                            },
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
            }
        }
        
        // Create a record batch
        let batch = RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| ElusionError::Custom(format!("Failed to create record batch: {}", e)))?;
        
        // Create a DataFusion DataFrame
        let ctx = SessionContext::new();
        let mem_table = MemTable::try_new(schema, vec![vec![batch]])
            .map_err(|e| ElusionError::Custom(format!("Failed to create memory table: {}", e)))?;
        
        ctx.register_table(alias, Arc::new(mem_table))
            .map_err(|e| ElusionError::Custom(format!("Failed to register table: {}", e)))?;
        
        let df = ctx.table(alias).await
            .map_err(|e| ElusionError::Custom(format!("Failed to create DataFrame: {}", e)))?;
        
        // Return CustomDataFrame
        Ok(CustomDataFrame {
            df,
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
            query: String::new(),
            aggregated_df: None,
            union_tables: None,
            original_expressions: Vec::new(),
            needs_normalization: false,
            raw_selected_columns: Vec::new(),
            raw_group_by_columns: Vec::new(),
            raw_where_conditions: Vec::new(),
            raw_having_conditions: Vec::new(),
            raw_join_conditions: Vec::new(),
            raw_aggregations: Vec::new(),
            uses_group_by_all: false
        })
    }