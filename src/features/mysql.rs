use crate::prelude::*;
#[cfg(feature = "mysql")]
use mysql_async::{Pool as MySqlPool, OptsBuilder as MySqlOptsBuilder, Conn as MySqlConn, Row as MySqlRow, Value as MySqlValue, Error as MySqlError};
#[cfg(feature = "mysql")]
use mysql_async::prelude::*;

/// MySQL connection configuration options
#[cfg(feature = "mysql")]
#[derive(Debug, Clone)]
pub struct MySqlConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub pool_size: Option<usize>,
}

#[cfg(feature = "mysql")]
impl Default for MySqlConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 3306,
            user: "root".to_string(),
            password: String::new(),
            database: "mysql".to_string(),
            pool_size: Some(5),
        }
    }
}

#[cfg(feature = "mysql")]
impl MySqlConfig {
    /// Create a new MySqlConfig with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a connection string from the configuration
    pub fn connection_string(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            self.user,
            self.password,
            self.host,
            self.port,
            self.database
        )
    }
}

#[cfg(not(feature = "mysql"))]
#[derive(Debug, Clone)]
pub struct MySqlConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub pool_size: Option<usize>,
}

#[cfg(not(feature = "mysql"))]
impl Default for MySqlConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 3306,
            user: "root".to_string(),
            password: String::new(),
            database: "mysql".to_string(),
            pool_size: Some(5),
        }
    }
}

#[cfg(not(feature = "mysql"))]
impl MySqlConfig {
    /// Create a new MySqlConfig with default values (stub)
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a connection string from the configuration (stub)
    pub fn connection_string(&self) -> String {
        String::new()
    }
}

/// MySQL connection manager that supports connection pooling
#[cfg(feature = "mysql")]
pub struct MySqlConnection {
    pool: MySqlPool,
}

#[cfg(not(feature = "mysql"))]
pub struct MySqlConnection {
}

#[cfg(feature = "mysql")]
impl MySqlConnection {
    /// Create a new MySQL connection manager
    pub async fn new(config: MySqlConfig) -> Result<Self, MySqlError> {
        let opts = MySqlOptsBuilder::default()
            .ip_or_hostname(config.host)
            .tcp_port(config.port)
            .user(Some(config.user))
            .pass(Some(config.password))
            .db_name(Some(config.database));

        let pool = MySqlPool::new(opts);
        
        Ok(Self { pool })
    }

    /// Get a client from the pool
    async fn get_conn(&self) -> Result<MySqlConn, MySqlError> {
        self.pool.get_conn().await
    }

    /// Execute a query that returns rows
    pub async fn query(&self, query: &str) -> Result<Vec<MySqlRow>, MySqlError> {
        let mut conn = self.get_conn().await?;
        let result = conn.query(query).await?;
        Ok(result)
    }

    /// Check if the connection is valid
    pub async fn ping(&self) -> Result<(), MySqlError> {
        let mut conn = self.get_conn().await?;
        
        conn.ping().await
    }

    /// Disconnect and close the pool
    pub async fn disconnect(self) -> Result<(), MySqlError> {
        self.pool.disconnect().await
    }
}

#[cfg(not(feature = "mysql"))]
impl MySqlConnection {
    /// Create a new MySQL connection manager (stub)
    pub async fn new(_config: MySqlConfig) -> Result<Self, ElusionError> {
        Err(ElusionError::Custom("*** Warning ***: MySQL feature not enabled. Add feature = [\"mysql\"] under [dependencies]".to_string()))
    }

    /// Execute a query that returns rows (stub)
    pub async fn query<P>(&self, _query: &str, _params: P) -> Result<Vec<()>, ElusionError>
    where
        P: Send,
    {
        Err(ElusionError::Custom("*** Warning ***: MySQL feature not enabled. Add feature = [\"mysql\"] under [dependencies]".to_string()))
    }

    /// Check if the connection is valid (stub)
    pub async fn ping(&self) -> Result<(), ElusionError> {
        Err(ElusionError::Custom("*** Warning ***: MySQL feature not enabled. Add feature = [\"mysql\"] under [dependencies]".to_string()))
    }
}

#[cfg(feature = "mysql")]
impl From<mysql_async::Error> for ElusionError {
    fn from(err: mysql_async::Error) -> Self {
        ElusionError::Custom(format!("MySQL error: {}", err))
    }
}

 #[cfg(feature = "mysql")]
    pub async fn from_mysql_impl(
        conn: &MySqlConnection,
        query: &str,
        alias: &str
    ) -> ElusionResult<CustomDataFrame> {

        let rows = conn.query(query)
                .await
                .map_err(|e| ElusionError::Custom(format!("MySQL query error: {}", e)))?;

            if rows.is_empty() {
                return Err(ElusionError::Custom("Query returned no rows".to_string()));
            }


        // Get column names from the first row
        let first_row = &rows[0];
        let column_names: Vec<String> = first_row.columns_ref()
            .iter()
            .map(|col| col.name_str().to_string())
            .collect();
        
        // Create schema from column metadata
        let mut fields = Vec::with_capacity(column_names.len());
        for column_name in &column_names {
            // Get the column value from the first row to determine its type
            let value_opt = first_row.get_opt::<MySqlValue, _>(column_name.as_str());
            
            // Map MySQL types to Arrow types
            let arrow_type = match value_opt {
                Some(Ok(MySqlValue::NULL)) => ArrowDataType::Utf8, // Default to string for NULL values
                Some(Ok(MySqlValue::Bytes(_))) => ArrowDataType::Utf8,
                Some(Ok(MySqlValue::Int(_))) => ArrowDataType::Int64,
                Some(Ok(MySqlValue::UInt(_))) => ArrowDataType::UInt64,
                Some(Ok(MySqlValue::Float(_))) => ArrowDataType::Float32,
                Some(Ok(MySqlValue::Double(_))) => ArrowDataType::Float64,
                // MySQL Date format: YYYY-MM-DD
                Some(Ok(MySqlValue::Date(_, _, _, _, _, _, _))) => ArrowDataType::Date32,
                // MySQL Time format: HH:MM:SS
                Some(Ok(MySqlValue::Time(_, _, _, _, _, _))) => ArrowDataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
                _ => ArrowDataType::Utf8, // Default to string for any other type
            };
            
            fields.push(Field::new(column_name, arrow_type, true));
        }
        
        let schema = Arc::new(Schema::new(fields));
        
        // Build arrays for each column
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(column_names.len());
        
        for (col_idx, col_name) in column_names.iter().enumerate() {
            let field = schema.field(col_idx);
            
            match field.data_type() {
                ArrowDataType::Boolean => {
                    let mut builder = BooleanBuilder::new();
                    for row in &rows {
                        let value_opt = row.get_opt::<bool, _>(col_name.as_str());
                        match value_opt {
                            Some(Ok(value)) => builder.append_value(value),
                            _ => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                ArrowDataType::Int64 => {
                    let mut builder = Int64Builder::new();
                    for row in &rows {
                        let value_opt = row.get_opt::<i64, _>(col_name.as_str());
                        match value_opt {
                            Some(Ok(value)) => builder.append_value(value),
                            _ => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                ArrowDataType::UInt64 => {
                    let mut builder = UInt64Builder::new();
                    for row in &rows {
                        let value_opt = row.get_opt::<u64, _>(col_name.as_str());
                        match value_opt {
                            Some(Ok(value)) => builder.append_value(value),
                            _ => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                ArrowDataType::Float32 => {
                    let mut builder = Float64Builder::new();
                    for row in &rows {
                        let value_opt = row.get_opt::<f32, _>(col_name.as_str());
                        match value_opt {
                            Some(Ok(value)) => builder.append_value(value as f64),
                            _ => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                ArrowDataType::Float64 => {
                    let mut builder = Float64Builder::new();
                    for row in &rows {
                        let value_opt = row.get_opt::<f64, _>(col_name.as_str());
                        match value_opt {
                            Some(Ok(value)) => builder.append_value(value),
                            _ => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                ArrowDataType::Date32 => {
                    let mut builder = StringBuilder::new();
                    for row in &rows {
                        let value_opt = row.get_opt::<MySqlValue, _>(col_name.as_str());
                        match value_opt {
                            Some(Ok(MySqlValue::Date(year, month, day, _, _, _, _))) => {
                                builder.append_value(format!("{:04}-{:02}-{:02}", year, month, day));
                            },
                            _ => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                ArrowDataType::Timestamp(_, _) => {
                    let mut builder = StringBuilder::new();
                    for row in &rows {
                        let value_opt = row.get_opt::<MySqlValue, _>(col_name.as_str());
                        match value_opt {
                            Some(Ok(MySqlValue::Date(year, month, day, hour, minute, second, micros))) => {
                                builder.append_value(format!(
                                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
                                    year, month, day, hour, minute, second, micros
                                ));
                            },
                            _ => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                ArrowDataType::Time64(_) => {
                    let mut builder = StringBuilder::new();
                    for row in &rows {
                        let value_opt = row.get_opt::<MySqlValue, _>(col_name.as_str());
                        match value_opt {
                            Some(Ok(MySqlValue::Time(neg, hours, minutes, seconds, _, micros))) => {
                                let sign = if neg { "-" } else { "" };
                                builder.append_value(format!(
                                    "{}{}:{:02}:{:02}.{:06}", 
                                    sign, hours, minutes, seconds, micros
                                ));
                            },
                            _ => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                _ => {
                    // Default to string for all other types
                    let mut builder = StringBuilder::new();
                    for row in &rows {
                        let value_opt = row.get_opt::<String, _>(col_name.as_str());
                        match value_opt {
                            Some(Ok(value)) => builder.append_value(value),
                            Some(Err(_)) => {
                                // Try to get the value as a byte array
                                let bytes_opt = row.get_opt::<Vec<u8>, _>(col_name.as_str());
                                match bytes_opt {
                                    Some(Ok(bytes)) => match String::from_utf8(bytes) {
                                        Ok(s) => builder.append_value(s),
                                        Err(_) => builder.append_value("[binary data]"),
                                    },
                                    _ => builder.append_null(),
                                }
                            },
                            None => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
            }
        }
        
        let batch = RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| ElusionError::Custom(format!("Failed to create record batch: {}", e)))?;
        
        let ctx = SessionContext::new();
        let mem_table = MemTable::try_new(schema, vec![vec![batch]])
            .map_err(|e| ElusionError::Custom(format!("Failed to create memory table: {}", e)))?;
        
        ctx.register_table(alias, Arc::new(mem_table))
            .map_err(|e| ElusionError::Custom(format!("Failed to register table: {}", e)))?;
        
        let df = ctx.table(alias).await
            .map_err(|e| ElusionError::Custom(format!("Failed to create DataFrame: {}", e)))?;
        
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

