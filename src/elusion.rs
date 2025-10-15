pub mod prelude;
mod features;
mod helper_funcs;
mod custom_error;
mod normalizers;
mod csvwrite;
mod sqlbuilder;

// =========== DF
use regex::Regex;
use datafusion::prelude::*;
use futures::future::BoxFuture;
use datafusion::datasource::MemTable;
use std::sync::Arc;
use arrow::datatypes::{DataType as ArrowDataType};
use arrow::array::{ArrayRef, Array, Float64Array,Int64Array};
 
use arrow::record_batch::RecordBatch;
use arrow::csv::writer::WriterBuilder;

// ========= CSV
use std::fs::{self, File, OpenOptions};
use std::io::{Write, BufWriter};

//============ WRITERS
use datafusion::prelude::SessionContext;
use datafusion::dataframe::{DataFrame,DataFrameWriteOptions};

// ========= JSON   
use serde_json::Value;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet}; 

// ========== DELTA
use deltalake::DeltaTableError;
use std::result::Result;
use std::path::Path as LocalPath;
use deltalake::writer::WriteMode;
use deltalake::open_table; 

//============== Features
use crate::features::delta::write_to_delta_impl;
use crate::features::delta::DeltaPathManager;
use crate::features::calendar::DateFormat;

// =========== ERRROR
use std::fmt::Debug;
use crate::custom_error::cust_error::ElusionError;
use crate::custom_error::cust_error::ElusionResult;
use crate::custom_error::cust_error::extract_column_from_duplicate_error;
use crate::custom_error::cust_error::extract_missing_column;
use crate::custom_error::cust_error::extract_column_from_projection_error;
use crate::custom_error::cust_error::extract_table_from_join_error;
use crate::custom_error::cust_error::extract_function_from_error;
use crate::custom_error::cust_error::extract_column_from_agg_error;
use crate::custom_error::cust_error::detect_function_usage_in_error;
use crate::custom_error::cust_error::generate_enhanced_groupby_suggestion;
use crate::custom_error::cust_error::extract_window_function_name;
use crate::custom_error::cust_error::extract_window_function_columns;

// ======== PIVOT
use arrow::compute;
use arrow::array::StringArray;

// ======= Dash
pub use features::dashboard::{ReportLayout, TableOptions};
use crate::prelude::PlotlyPlot;

// ======== STATS
use datafusion::common::ScalarValue;
use std::io::BufReader;
use serde_json::Deserializer;

//========== VIEWS
use chrono::{DateTime, Utc};
use crate::features::cashandview::MATERIALIZED_VIEW_MANAGER;
use crate::features::cashandview::QUERY_CACHE;
use crate::features::cashandview::QueryCache;

// =========== DATE TABLE BUILDER
use chrono::Weekday;

//=========POSTGRESS
use crate::features::postgres::PostgresConnection;

//================ MYSQL
use crate::features::mysql::MySqlConnection;

//============ helper funcs
use crate::helper_funcs::registertable::register_df_as_table;
use crate::helper_funcs::array_val_to_json::array_value_to_json;
use crate::helper_funcs::build_rec_batch::build_record_batch;
use crate::helper_funcs::infer_schema_json::infer_schema_from_json;

//============== normalizers
use crate::normalizers::normalize::is_simple_column;
use crate::normalizers::normalize::normalize_alias;
use crate::normalizers::normalize::normalize_column_name;
use crate::normalizers::normalize::lowercase_column_names;
use crate::normalizers::normalize::normalize_condition;
use crate::normalizers::normalize::normalize_expression;
use crate::normalizers::normalize::normalize_condition_filter;
use crate::normalizers::normalize::is_expression;
use crate::normalizers::normalize::is_aggregate_expression;
use crate::normalizers::normalize::normalize_alias_write;
use crate::normalizers::normalize::normalize_window_function;
use crate::normalizers::normalize::normalize_simple_expression;
use crate::normalizers::normalize::resolve_alias_to_original;
use crate::normalizers::normalize::is_groupable_column;
use crate::normalizers::normalize::extract_base_column_name;

//======= csv 
use crate::features::csv::load_csv_with_type_handling;
use crate::csvwrite::csvwriteops::CsvWriteOptions;

// ======= optimizers
use crate::sqlbuilder::sqlbuild::SqlBuilder;

// ====== STREAM
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use crate::features::csv::load_csv_smart;
use arrow::util::pretty::pretty_format_batches;

//cache redis
use crate::features::redis::RedisCacheConnection;
use crate::features::redis::RedisCacheStats;
use crate::features::redis::clear_redis_cache_impl;
use crate::features::redis::create_redis_cache_connection;
use crate::features::redis::create_redis_cache_connection_with_config;
use crate::features::redis::elusion_with_redis_cache_impl;
use crate::features::redis::get_redis_cache_stats_impl;
use crate::features::redis::invalidate_redis_cache_impl;

// ==== xml
use crate::features::xml::XmlProcessingMode;
use crate::features::xml::load_xml_with_mode;

// ===== struct to manage ODBC DB connections
#[derive(Debug, PartialEq, Clone)]
pub enum DatabaseType {
    MySQL,
    PostgreSQL,
    MongoDB,
    SQLServer,
    Unknown
}

#[derive(Clone, Debug)]
pub struct Join {
    dataframe: CustomDataFrame,
    condition: String,
    join_type: String,
}

#[derive(Debug)]
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

    needs_normalization: bool,
    raw_selected_columns: Vec<String>,    
    raw_group_by_columns: Vec<String>,    
    raw_where_conditions: Vec<String>,    
    raw_having_conditions: Vec<String>,  
    raw_join_conditions: Vec<String>,     
    raw_aggregations: Vec<String>,

    uses_group_by_all: bool
}

impl Clone for CustomDataFrame {
    fn clone(&self) -> Self {
        Self {
            df: self.df.clone(),
            table_alias: self.table_alias.clone(),
            from_table: self.from_table.clone(),
            query: self.query.clone(),
            selected_columns: self.selected_columns.clone(),
            alias_map: self.alias_map.clone(),
            aggregations: self.aggregations.clone(),
            group_by_columns: self.group_by_columns.clone(),
            where_conditions: self.where_conditions.clone(),
            having_conditions: self.having_conditions.clone(),
            order_by_columns: self.order_by_columns.clone(),
            joins: self.joins.clone(),
            window_functions: self.window_functions.clone(),
            ctes: self.ctes.clone(),
            set_operations: self.set_operations.clone(),
            original_expressions: self.original_expressions.clone(),
            limit_count: self.limit_count,
            subquery_source: self.subquery_source.clone(),
            aggregated_df: self.aggregated_df.clone(),
            union_tables: self.union_tables.clone(),
            needs_normalization: self.needs_normalization,
            raw_selected_columns: self.raw_selected_columns.clone(),   
            raw_group_by_columns: self.raw_group_by_columns.clone(),  
            raw_where_conditions: self.raw_where_conditions.clone(),  
            raw_having_conditions: self.raw_having_conditions.clone(),
            raw_join_conditions: self.raw_join_conditions.clone(),
            raw_aggregations: self.raw_aggregations.clone(),

            uses_group_by_all: self.uses_group_by_all
        }
    }
}

// =================== JSON heler functions

#[derive(Deserialize, Serialize, Debug)]
struct GenericJson {
    #[serde(flatten)]
    fields: HashMap<String, Value>,
}

// ================= Statistics
#[derive(Debug, Default)]
pub struct ColumnStats {
    pub columns: Vec<ColumnStatistics>,
}

#[derive(Debug)]
pub struct ColumnStatistics {
    pub name: String,
    pub total_count: i64,
    pub non_null_count: i64,
    pub mean: Option<f64>,
    pub min_value: ScalarValue,
    pub max_value: ScalarValue,
    pub std_dev: Option<f64>,
}


#[derive(Debug)]
pub struct NullAnalysis {
    pub counts: Vec<NullCount>,
}

#[derive(Debug)]
pub struct NullCount {
    pub column_name: String,
    pub total_rows: i64,
    pub null_count: i64,
    pub null_percentage: f64,
}
//error
#[derive(Debug)]
pub struct ColumnErrorContext {
    context: String,
    location: String,
    suggestion: String,
}

// Auxiliary struct to hold aliased DataFrame
pub struct AliasedDataFrame {
    dataframe: DataFrame,
    alias: String,
}

impl CustomDataFrame {

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
   
    /// Creates an empty DataFrame with a minimal schema and a single row
    /// This can be used as a base for date tables or other data generation
    pub async fn empty() -> ElusionResult<Self> {
        // Create a new session context
        let ctx = SessionContext::new();

        let sql = "SELECT 1 as dummy";
        
        // Execute the SQL to create the single-row DataFrame
        let df = ctx.sql(sql).await
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Single Row Creation".to_string(),
                reason: format!("Failed to create single-row DataFrame: {}", e),
                suggestion: "💡 Verify SQL execution capabilities in context.".to_string()
            })?;
        
        // Return a new CustomDataFrame with the single-row DataFrame
        Ok(CustomDataFrame {
            df,
            table_alias: "dummy_table".to_string(),
            from_table: "dummy_table".to_string(),
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

    // ========= CALENDAR
    /// Create a simple date range table with standard date components
    pub async fn create_date_range_table(
        start_date: &str,
        end_date: &str,
        alias: &str
    ) -> ElusionResult<Self> {
        crate::features::calendar::create_date_range_table_impl(start_date, end_date, alias).await
    }

    /// Create a date range table with multiple date formats and period ranges
    pub async fn create_formatted_date_range_table(
        start_date: &str,
        end_date: &str,
        alias: &str,
        format_name: String,
        format: DateFormat,
        include_period_ranges: bool,
        week_start_day: Weekday
    ) -> ElusionResult<Self> {
        crate::features::calendar::create_formatted_date_range_table_impl(
            start_date, 
            end_date, 
            alias, 
            format_name, 
            format, 
            include_period_ranges, 
            week_start_day
        ).await
    }
    
      /// Create a materialized view from the current DataFrame state
      pub async fn create_view(
          &self,
          view_name: &str,
          ttl_seconds: Option<u64>,
      ) -> ElusionResult<()> {
          // Get the SQL that would be executed for this DataFrame
          let sql = self.construct_sql();
          
          // Create a new SessionContext for this operation
          let ctx = SessionContext::new();
          
          // Register necessary tables
          register_df_as_table(&ctx, &self.table_alias, &self.df).await?;
          
          for join in &self.joins {
              register_df_as_table(&ctx, &join.dataframe.table_alias, &join.dataframe.df).await?;
          }
          
          // Create the materialized view
          let mut manager = MATERIALIZED_VIEW_MANAGER.lock().unwrap();
          manager.create_view(&ctx, view_name, &sql, ttl_seconds).await
      }
      
      /// Get a DataFrame from a materialized view
      pub async fn from_view(view_name: &str) -> ElusionResult<Self> {
          let ctx = SessionContext::new();
          let manager = MATERIALIZED_VIEW_MANAGER.lock().unwrap();
          
          let df = manager.get_view_as_dataframe(&ctx, view_name).await?;
          
          Ok(CustomDataFrame {
            df,
            table_alias: view_name.to_string(),
            from_table: view_name.to_string(),
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
      
      /// Refresh a materialized view
      pub async fn refresh_view(view_name: &str) -> ElusionResult<()> {
          let ctx = SessionContext::new();
          let mut manager = MATERIALIZED_VIEW_MANAGER.lock().unwrap();
          manager.refresh_view(&ctx, view_name).await
      }
      
      /// Drop a materialized view
      pub async fn drop_view(view_name: &str) -> ElusionResult<()> {
          let mut manager = MATERIALIZED_VIEW_MANAGER.lock().unwrap();
          manager.drop_view(view_name)
      }
      
      /// List all materialized views
      pub async fn list_views() -> Vec<(String, DateTime<Utc>, Option<u64>)> {
        let manager = MATERIALIZED_VIEW_MANAGER.lock().unwrap();
        let views = manager.list_views();
        
        if views.is_empty() {
            println!("There are no materialized views created.");
        }
        
        views
    }
      
      /// Execute query with caching
      pub async fn elusion_with_cache(&self, alias: &str) -> ElusionResult<Self> {
          let sql = self.construct_sql();
          
          // Try to get from cache first
          let mut cache = QUERY_CACHE.lock().unwrap();
          if let Some(cached_result) = cache.get_cached_result(&sql) {
              println!("✅ Using cached result for query");
              
              // Create a DataFrame from the cached result
              let ctx = SessionContext::new();
              let schema = cached_result[0].schema();
              
              let mem_table = MemTable::try_new(schema.clone(), vec![cached_result])
                  .map_err(|e| ElusionError::Custom(format!("Failed to create memory table from cache: {}", e)))?;
              
              ctx.register_table(alias, Arc::new(mem_table))
                  .map_err(|e| ElusionError::Custom(format!("Failed to register table from cache: {}", e)))?;
              
              let df = ctx.table(alias).await
                  .map_err(|e| ElusionError::Custom(format!("Failed to create DataFrame from cache: {}", e)))?;
              
              return Ok(CustomDataFrame {
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
                query: sql,
                aggregated_df: None,
                union_tables: None,
                original_expressions: self.original_expressions.clone(),
                needs_normalization: false,
                raw_selected_columns: Vec::new(),
                raw_group_by_columns: Vec::new(),
                raw_where_conditions: Vec::new(),
                raw_having_conditions: Vec::new(),
                raw_join_conditions: Vec::new(),
                raw_aggregations: Vec::new(),

                uses_group_by_all: false
              });
          }
          
          // Not in cache, execute the query
          let result = self.elusion(alias).await?;
          
          // Cache the result
          let batches = result.df.clone().collect().await
              .map_err(|e| ElusionError::Custom(format!("Failed to collect batches: {}", e)))?;
          
          cache.cache_query(&sql, batches);
          
          Ok(result)
      }
      
      /// Invalidate cache for specific tables
      pub fn invalidate_cache(table_names: &[String]) {
          let mut cache = QUERY_CACHE.lock().unwrap();
          cache.invalidate(table_names);
      }
      
      /// Clear the entire query cache
      pub fn clear_cache() {
        let mut cache = QUERY_CACHE.lock().unwrap();
        let size_before = cache.cached_queries.len();
        cache.clear();
        println!("Cache cleared: {} queries removed from cache.", size_before);
    }
      
      /// Modify cache settings
      pub fn configure_cache(max_size: usize, ttl_seconds: Option<u64>) {
          *QUERY_CACHE.lock().unwrap() = QueryCache::new(max_size, ttl_seconds);
      }


    // ========== REDIS CACHE
    /// Cashing query result to Redis
    pub async fn elusion_with_redis_cache(
        &self, 
        redis_conn: &RedisCacheConnection,
        alias: &str,
        ttl_seconds: Option<u64>
    ) -> ElusionResult<Self> {
        elusion_with_redis_cache_impl(self, redis_conn, alias, ttl_seconds).await
    }

    /// Clear Redis cache for specific patterns
    pub async fn clear_redis_cache(
        redis_conn: &RedisCacheConnection,
        pattern: Option<&str>
    ) -> ElusionResult<()> {
        clear_redis_cache_impl(redis_conn, pattern).await
    }


    /// Get Redis cache statistics
    pub async fn redis_cache_stats(
        redis_conn: &RedisCacheConnection
    ) -> ElusionResult<RedisCacheStats> {
        get_redis_cache_stats_impl(redis_conn).await
    }

    /// Invalidate Redis cache by pattern
    pub async fn invalidate_redis_cache(
        redis_conn: &RedisCacheConnection,
        table_names: &[&str]
    ) -> ElusionResult<()> {
        invalidate_redis_cache_impl(redis_conn, table_names).await
    }

    /// Convinient funciton to create Redis cache connection
    pub async fn create_redis_cache_connection() -> ElusionResult<RedisCacheConnection> {
        create_redis_cache_connection().await
    }

    /// Convinient funciton to create Redis cache connection with configurations
    pub async fn create_redis_cache_connection_with_config(
        host: &str,
        port: u16,
        password: Option<&str>,
        database: Option<u8>,
    ) -> ElusionResult<RedisCacheConnection> {
        create_redis_cache_connection_with_config(host, port, password, database).await
    }


    //=========== SHARE POINT
    #[cfg(feature = "sharepoint")]
    pub async fn load_from_sharepoint(
        site_url: &str,
        file_path: &str,
        alias: &str,
    ) -> ElusionResult<Self> {
        crate::features::sharepoint::load_from_sharepoint_impl(
            site_url, file_path, alias
        ).await
    }

    #[cfg(not(feature = "sharepoint"))]
    pub async fn load_from_sharepoint(
        _site_url: &str,
        _file_path: &str,
        _alias: &str,
    ) -> ElusionResult<Self> {
        Err(ElusionError::InvalidOperation {
            operation: "SharePoint File Loading".to_string(),
            reason: "SharePoint feature not enabled".to_string(),
            suggestion: "💡 Add 'sharepoint' to your features: features = [\"sharepoint\"]".to_string(),
        })
    }

    #[cfg(feature = "sharepoint")]
    pub async fn load_folder_from_sharepoint(
        site_url: &str,
        folder_path: &str,
        file_extensions: Option<Vec<&str>>,
        result_alias: &str,
    ) -> ElusionResult<Self> {
        crate::features::sharepoint::load_folder_from_sharepoint_impl(
            site_url, folder_path, file_extensions, result_alias
        ).await
    }

    #[cfg(not(feature = "sharepoint"))]
    pub async fn load_folder_from_sharepoint(
        _site_url: &str,
        _folder_path: &str,
        _file_extensions: Option<Vec<&str>>,
        _result_alias: &str,
    ) -> ElusionResult<Self> {
        Err(ElusionError::InvalidOperation {
            operation: "SharePoint Folder Loading".to_string(),
            reason: "SharePoint feature not enabled".to_string(),
            suggestion: "💡 Add 'sharepoint' to your features: features = [\"sharepoint\"]".to_string(),
        })
    }

    #[cfg(feature = "sharepoint")]
    pub async fn load_folder_from_sharepoint_with_filename_column(
        site_url: &str,
        folder_path: &str,
        file_extensions: Option<Vec<&str>>,
        result_alias: &str,
    ) -> ElusionResult<Self> {
        crate::features::sharepoint::load_folder_from_sharepoint_with_filename_column_impl(
            site_url, folder_path, file_extensions, result_alias
        ).await
    }

    #[cfg(not(feature = "sharepoint"))]
    pub async fn load_folder_from_sharepoint_with_filename_column(
        _site_url: &str,
        _folder_path: &str,
        _file_extensions: Option<Vec<&str>>,
        _result_alias: &str,
    ) -> ElusionResult<Self> {
        Err(ElusionError::InvalidOperation {
            operation: "SharePoint Folder Loading with Filename".to_string(),
            reason: "SharePoint feature not enabled".to_string(),
            suggestion: "💡 Add 'sharepoint' to your features: features = [\"sharepoint\"]".to_string(),
        })
    }

    // service principal
    #[cfg(feature = "sharepoint")]
    pub async fn load_from_sharepoint_with_service_principal(
        tenant_id: &str,
        client_id: &str,
        client_secret: &str,
        site_url: &str,
        file_path: &str,
        alias: &str,
    ) -> ElusionResult<Self> {
        crate::features::sharepoint::load_from_sharepoint_with_service_principal_impl(
           tenant_id, client_id, client_secret, site_url, file_path, alias
        ).await
    }

    #[cfg(not(feature = "sharepoint"))]
    pub async fn load_from_sharepoint_with_service_principal(
        _tenant_id: &str,
        _client_id: &str,
        _client_secret: &str,
         _site_url: &str,
        _file_path: &str,
        _alias: &str,
    ) -> ElusionResult<Self> {
        Err(ElusionError::InvalidOperation {
            operation: "SharePoint File Loading".to_string(),
            reason: "SharePoint feature not enabled".to_string(),
            suggestion: "💡 Add 'sharepoint' to your features: features = [\"sharepoint\"]".to_string(),
        })
    }

    #[cfg(feature = "sharepoint")]
    pub async fn load_folder_from_sharepoint_with_service_principal(
        tenant_id: &str,
        client_id: &str,
        client_secret: &str,
        site_url: &str,
        folder_path: &str,
        file_extensions: Option<Vec<&str>>,
        result_alias: &str,
    ) -> ElusionResult<Self> {
        crate::features::sharepoint::load_folder_from_sharepoint_with_service_principal_impl(
            tenant_id, client_id, client_secret, site_url, folder_path, file_extensions, result_alias
        ).await
    }

    #[cfg(not(feature = "sharepoint"))]
    pub async fn load_folder_from_sharepoint_with_service_principal(
        _tenant_id: &str,
        _client_id: &str,
        _client_secret: &str,
        _site_url: &str,
        _folder_path: &str,
        _file_extensions: Option<Vec<&str>>,
        _result_alias: &str,
    ) -> ElusionResult<Self> {
        Err(ElusionError::InvalidOperation {
            operation: "SharePoint Folder Loading".to_string(),
            reason: "SharePoint feature not enabled".to_string(),
            suggestion: "💡 Add 'sharepoint' to your features: features = [\"sharepoint\"]".to_string(),
        })
    }

    #[cfg(feature = "sharepoint")]
    pub async fn load_folder_from_sharepoint_with_filename_column_with_service_principal(
        tenant_id: &str,
        client_id: &str,
        client_secret: &str,
        site_url: &str,
        folder_path: &str,
        file_extensions: Option<Vec<&str>>,
        result_alias: &str,
    ) -> ElusionResult<Self> {
        crate::features::sharepoint::load_folder_from_sharepoint_with_filename_column_with_service_principal_impl(
            tenant_id, client_id, client_secret, site_url, folder_path, file_extensions, result_alias
        ).await
    }

    #[cfg(not(feature = "sharepoint"))]
    pub async fn load_folder_from_sharepoint_with_filename_column_with_service_principal(
        _tenant_id: &str,
        _client_id: &str,
        _client_secret: &str,
         _site_url: &str,
        _folder_path: &str,
        _file_extensions: Option<Vec<&str>>,
        _result_alias: &str,
    ) -> ElusionResult<Self> {
        Err(ElusionError::InvalidOperation {
            operation: "SharePoint Folder Loading".to_string(),
            reason: "SharePoint feature not enabled".to_string(),
            suggestion: "💡 Add 'sharepoint' to your features: features = [\"sharepoint\"]".to_string(),
        })
    }
    
    // ====== POSTGRESS

    #[cfg(feature = "postgres")]
    pub async fn from_postgres(
        conn: &PostgresConnection,
        query: &str,
        alias: &str
    ) -> ElusionResult<Self> {
        crate::features::postgres::from_postgres_impl(conn, query, alias).await
    }

    #[cfg(not(feature = "postgres"))]
    pub async fn from_postgres(
        _conn: &PostgresConnection,
        _query: &str,
        _alias: &str
    ) -> ElusionResult<Self> {
        Err(ElusionError::Custom("*** Warning ***: Postgres feature not enabled. Add feature under [dependencies]".to_string()))
    }

    // ========== MYSQL ============
   #[cfg(feature = "mysql")]
    pub async fn from_mysql(
        conn: &MySqlConnection,
        query: &str,
        alias: &str
    ) -> ElusionResult<Self> {
        crate::features::mysql::from_mysql_impl(conn, query, alias).await
    }

    #[cfg(not(feature = "mysql"))]
    pub async fn from_mysql(
        _conn: &MySqlConnection,
        _query: &str,
        _alias: &str
    ) -> ElusionResult<Self> {
        Err(ElusionError::Custom("*** Warning ***: MySQL feature not enabled. Add feature = [\"mysql\"] under [dependencies]".to_string()))
    }

    // ==================== DATAFRAME Methods ====================

    /// Add JOIN clauses 
    pub fn join<const N: usize>(
        mut self,
        other: CustomDataFrame,
        conditions: [&str; N],
        join_type: &str
    ) -> Self {
        // raw conditions
        self.raw_join_conditions.extend(conditions.iter().map(|&s| s.to_string()));
        self.needs_normalization = true;

        let condition = conditions.iter()
            .map(|&cond| normalize_condition(cond))
            .collect::<Vec<_>>()
            .join(" AND ");

        self.joins.push(Join {
            dataframe: other,
            condition,
            join_type: join_type.to_string(),
        });

        // Add complexity warning (NEW OPTIMIZATION)
        if self.should_warn_complexity() {
            println!("⚠️  Complex query detected - consider calling .elusion() to materialize intermediate results for better performance");
        }

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
        // raw for potential optimization
        self.raw_group_by_columns.extend(columns.iter().map(|&s| s.to_string()));
        self.needs_normalization = true;

        self.group_by_columns = columns
            .into_iter()
            .map(|s| {
                // try to resolve alias to original column from raw_selected_columns
                let resolved_column = resolve_alias_to_original(s, &self.raw_selected_columns);
                
                if is_simple_column(&resolved_column) {
                    normalize_column_name(&resolved_column)
                } else if resolved_column.to_uppercase().contains(" AS ") {
                    // Handle expressions with aliases - extract part before AS
                    let as_pattern = regex::Regex::new(r"(?i)\s+AS\s+").unwrap();
                    if let Some(as_match) = as_pattern.find(&resolved_column) {
                        let expr_part = resolved_column[..as_match.start()].trim();
                        normalize_expression(expr_part, &self.table_alias)
                    } else {
                        normalize_expression(&resolved_column, &self.table_alias)
                    }
                } else {
                    // Handle expressions without aliases
                    normalize_expression(&resolved_column, &self.table_alias)
                }
            })
            .collect();
        self
    }

    /// GROUP_BY_ALL function that usifies all SELECT() olumns and reduces need for writing all columns
    pub fn group_by_all(mut self) -> Self {
        self.uses_group_by_all = true;
        
        let mut all_group_by = Vec::new();
        
      //  println!("raw_selected_columns: {:?}", self.raw_selected_columns);

        for col_expr in &self.raw_selected_columns {
          //  println!("Processing column: '{}'", col_expr);
            
            // Extract base expression (before AS alias)
            let base_expression = if col_expr.to_uppercase().contains(" AS ") {
                let as_pattern = regex::Regex::new(r"(?i)\s+AS\s+").unwrap();
                if let Some(as_match) = as_pattern.find(col_expr) {
                    col_expr[..as_match.start()].trim()
                } else {
                    col_expr.as_str()
                }
            } else {
                col_expr.as_str()
            };

            // if base_expression.to_uppercase().contains(" OVER ") {
            //  //   println!("  -> SKIPPED: window function");
            //     continue;
            // }
            
            if is_groupable_column(base_expression) {
                let normalized = if is_simple_column(base_expression) {
                    normalize_column_name(base_expression)
                } else {
                    normalize_expression(base_expression, &self.table_alias)
                };
                
                if !all_group_by.contains(&normalized) {
                    all_group_by.push(normalized.clone());
                   // println!("  -> ADDED simple column to group_by: '{}'", normalized);
                }
            } else {
               // println!("  -> SKIPPED: not groupable (function/computed/aggregate)");
            }
        }

      //  println!("Final group_by_columns: {:?}", all_group_by);

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
        // raw for potential batch optimization
        self.raw_where_conditions.extend(conditions.iter().map(|&s| s.to_string()));
        self.needs_normalization = true;

        self.where_conditions.extend(
            conditions.into_iter().map(|c| normalize_condition_filter(c))
        );
        self
    }
    /// Add a single WHERE condition
    pub fn filter(mut self, condition: &str) -> Self {
        // Store raw
        self.raw_where_conditions.push(condition.to_string());
        self.needs_normalization = true;

        self.where_conditions.push(normalize_condition_filter(condition));
        self
    }
    
    /// Add multiple HAVING conditions using const generics.
    /// Allows passing arrays like ["condition1", "condition2", ...]
    pub fn having_many<const N: usize>(self, conditions: [&str; N]) -> Self {
        self.having_conditions_vec(conditions.to_vec())
    }

    /// Add multiple HAVING conditions using a Vec<&str>
    pub fn having_conditions_vec(mut self, conditions: Vec<&str>) -> Self {
        // Store raw
        self.raw_having_conditions.extend(conditions.iter().map(|&s| s.to_string()));
        self.needs_normalization = true;

        self.having_conditions.extend(
            conditions.into_iter().map(|c| normalize_condition(c))
        );
        self
    }

    /// Add a single HAVING condition
    pub fn having(mut self, condition: &str) -> Self {
        // Store raw
        self.raw_having_conditions.push(condition.to_string());
        self.needs_normalization = true;

        self.having_conditions.push(normalize_condition(condition));
        self
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

    /// Add multiple ORDER BY clauses using a Vec<(String, bool)>
    pub fn order_by_many_vec(mut self, orders: Vec<(String, bool)>) -> Self {
        self.order_by_columns = orders;
        self
    }
    /// Order by columns using string directions: "ASC" or "DESC"
    pub fn order_by<const N: usize>(self, columns: [&str; N], directions: [&str; N]) -> Self {
        let ascending: Vec<bool> = directions.iter()
            .map(|&dir| match dir.to_uppercase().as_str() {
                "ASC" | "ASCENDING" => true,
                "DESC" | "DESCENDING" => false,
                _ => panic!("Invalid sort direction: '{}'. Use 'ASC' or 'DESC'", dir),
            })
            .collect();
        
        let normalized_columns: Vec<String> = columns.iter()
            .map(|c| normalize_column_name(c))
            .collect();
        
        self.order_by_vec(normalized_columns, ascending)
    }

    /// Order by with tuples using string directions
    pub fn order_by_many<const N: usize>(self, orders: [(&str, &str); N]) -> Self {
        let orderings = orders.into_iter()
            .map(|(col, dir)| {
                let ascending = match dir.to_uppercase().as_str() {
                    "ASC" | "ASCENDING" => true,
                    "DESC" | "DESCENDING" => false,
                    _ => panic!("Invalid sort direction: '{}'. Use 'ASC' or 'DESC'", dir),
                };
                (normalize_column_name(col), ascending)
            })
            .collect::<Vec<_>>();
        self.order_by_many_vec(orderings)
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

        self.needs_normalization = true;

        for expr in expressions.iter() {
            // Add to SELECT clause
            self.selected_columns.push(normalize_expression(expr, &self.table_alias));

            if !self.group_by_columns.is_empty() {
                let expr_part = expr.split(" AS ")
                    .next()
                    .unwrap_or(expr);
                self.group_by_columns.push(normalize_expression(expr_part, &self.table_alias));
            }
        }
        self
    }

    /// Add datetime functions to the SELECT clause
    pub fn datetime_functions<const N: usize>(mut self, expressions: [&str; N]) -> Self {
        self.needs_normalization = true;

        for expr in expressions.iter() {
            self.selected_columns.push(normalize_expression(expr, &self.table_alias));

            if !self.group_by_columns.is_empty() {
                let expr_part = expr.split(" AS ")
                    .next()
                    .unwrap_or(expr);
                self.group_by_columns.push(normalize_expression(expr_part, &self.table_alias));
            }
        }
        self
    }

    /// Adding aggregations to the SELECT clause using const generics.
    /// Ensures that only valid aggregate expressions are included.
    pub fn agg<const N: usize>(self, aggregations: [&str; N]) -> Self {
        self.clone().agg_vec(
            aggregations.iter()
                .filter(|&expr| is_aggregate_expression(expr))
                .map(|s| normalize_expression(s, &self.table_alias))
                .collect()
        )
    }

    /// Add aggregations to the SELECT clause using a `Vec<String>`
    /// Ensures that only valid aggregate expressions are included.
    pub fn agg_vec(mut self, aggregations: Vec<String>) -> Self {
        // raw for optimization
        self.raw_aggregations.extend(aggregations.iter().cloned());
        self.needs_normalization = true;

        let valid_aggs = aggregations.into_iter()
            .filter(|expr| is_aggregate_expression(expr))
            .collect::<Vec<_>>();

        self.aggregations.extend(valid_aggs);
        self
    }

     /// Performs a APPEND with another DataFrame
     pub async fn append(self, other: CustomDataFrame) -> ElusionResult<Self> {

        // if self.df.schema() != other.df.schema() {
        //     return Err(ElusionError::SetOperationError {
        //         operation: "APPEND".to_string(),
        //         reason: "Schema mismatch between dataframes".to_string(),
        //         suggestion: "Ensure both dataframes have identical column names and types".to_string(),
        //     });
        // }

        let ctx = Arc::new(SessionContext::new());
        
        let mut batches_self = self.df.clone().collect().await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Collecting batches from first dataframe".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if the dataframe is valid and not empty".to_string(),
        })?;

        let batches_other = other.df.clone().collect().await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Collecting batches from second dataframe".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if the dataframe is valid and not empty".to_string(),
        })?;

        batches_self.extend(batches_other);
    
        let mem_table = MemTable::try_new(self.df.schema().clone().into(), vec![batches_self])
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Creating memory table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Verify data consistency, number of columns or memory availability".to_string(),
        })?;
    
        let alias = "append_result";

        ctx.register_table(alias, Arc::new(mem_table))
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Registering table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if table name is unique in context".to_string(),
        })?;
    
        let df = ctx.table(alias).await
            .map_err(|e| ElusionError::Custom(format!("Failed to create union DataFrame: {}", e)))?;
    
        Ok(CustomDataFrame {
            df,
            table_alias: alias.to_string(),
            from_table: alias.to_string(),
            selected_columns: self.selected_columns.clone(),
            alias_map: self.alias_map.clone(),
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
            original_expressions: self.original_expressions.clone(),
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
    /// Performs APPEND on multiple dataframes
    pub async fn append_many<const N: usize>(self, others: [CustomDataFrame; N]) -> ElusionResult<Self> {

        if N == 0 {
            return Err(ElusionError::SetOperationError {
                operation: "APPEND MANY".to_string(),
                reason: "No dataframes provided for append operation".to_string(),
                suggestion: "💡 Provide at least one dataframe to append".to_string(),
            });
        }

        // for (i, other) in others.iter().enumerate() {
        //     if self.df.schema() != other.df.schema() {
        //         return Err(ElusionError::SetOperationError {
        //             operation: "APPEND MANY".to_string(),
        //             reason: format!("Schema mismatch with dataframe at index {}", i),
        //             suggestion: "💡 Ensure all dataframes have identical column names and types".to_string(),
        //         });
        //     }
        // }

        let ctx = Arc::new(SessionContext::new());
        
        let mut all_batches = self.df.clone().collect().await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Collecting base dataframe".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if the dataframe is valid and not empty".to_string(),
        })?;
        
        for (i, other) in others.iter().enumerate() {
            let other_batches = other.df.clone().collect().await
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: format!("Collecting dataframe at index {}", i),
                    reason: e.to_string(),
                    suggestion: "💡 Check if the dataframe is valid and not empty".to_string(),
                })?;
            all_batches.extend(other_batches);
        }
    
        let mem_table = MemTable::try_new(self.df.schema().clone().into(), vec![all_batches])
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Creating memory table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Verify data consistency and memory availability".to_string(),
        })?;
    
        let alias = "union_many_result";

        ctx.register_table(alias, Arc::new(mem_table))
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Registering result table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if table name is unique in context".to_string(),
        })?;
    
        let df = ctx.table(alias).await
        .map_err(|e| ElusionError::SetOperationError {
            operation: "APPEND MANY".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Verify final table creation".to_string(),
        })?;
    
        Ok(CustomDataFrame {
            df,
            table_alias: alias.to_string(),
            from_table: alias.to_string(),
            selected_columns: self.selected_columns.clone(),
            alias_map: self.alias_map.clone(),
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
            original_expressions: self.original_expressions.clone(),
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
    /// Performs UNION on two dataframes
    pub async fn union(self, other: CustomDataFrame) -> ElusionResult<Self> {

        // if self.df.schema() != other.df.schema() {
        //     return Err(ElusionError::SetOperationError {
        //         operation: "UNION".to_string(),
        //         reason: "Schema mismatch between dataframes".to_string(),
        //         suggestion: "💡 Ensure both dataframes have identical column names and types".to_string(),
        //     });
        // }

        let ctx = Arc::new(SessionContext::new());
        
        register_df_as_table(&ctx, &self.table_alias, &self.df).await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Registering first table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if table name is unique and data is valid".to_string(),
        })?;

        register_df_as_table(&ctx, &other.table_alias, &other.df).await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Registering second table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if table name is unique and data is valid".to_string(),
        })?;
    
        
        let sql = format!(
            "SELECT DISTINCT * FROM {} UNION SELECT DISTINCT * FROM {}",
            normalize_alias(&self.table_alias),
            normalize_alias(&other.table_alias)
        );

        let df = ctx.sql(&sql).await
        .map_err(|e| ElusionError::SetOperationError {
            operation: "UNION".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Verify SQL syntax and schema compatibility".to_string(),
        })?;
    
        Ok(CustomDataFrame {
            df,
            table_alias: "union_result".to_string(),
            from_table: "union_result".to_string(),
            selected_columns: self.selected_columns.clone(),
            alias_map: self.alias_map.clone(),
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
            original_expressions: self.original_expressions.clone(),
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
    /// Performs UNION on multiple dataframes
    pub async fn union_many<const N: usize>(self, others: [CustomDataFrame; N]) -> ElusionResult<Self> {

        if N == 0 {
            return Err(ElusionError::SetOperationError {
                operation: "UNION MANY".to_string(),
                reason: "No dataframes provided for union operation".to_string(),
                suggestion: "💡 Provide at least one dataframe to union with".to_string(),
            });
        }

        // for (i, other) in others.iter().enumerate() {
        //     if self.df.schema() != other.df.schema() {
        //         return Err(ElusionError::SetOperationError {
        //             operation: "UNION MANY".to_string(),
        //             reason: format!("Schema mismatch with dataframe at index {}", i),
        //             suggestion: "💡 Ensure all dataframes have identical column names and types".to_string(),
        //         });
        //     }
        // }

        let ctx = Arc::new(SessionContext::new());
        
        register_df_as_table(&ctx, &self.table_alias, &self.df).await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Registering base table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if table name is unique and data is valid".to_string(),
        })?;
        
        for (i, other) in others.iter().enumerate() {
            let alias = format!("union_source_{}", i);
            register_df_as_table(&ctx, &alias, &other.df).await
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: format!("Registering table {}", i),
                    reason: e.to_string(),
                    suggestion: "💡 Check if table name is unique and data is valid".to_string(),
                })?;
        }
        
        let mut sql = format!("SELECT DISTINCT * FROM {}", normalize_alias(&self.table_alias));
        for i in 0..N {
            sql.push_str(&format!(" UNION SELECT DISTINCT * FROM {}", 
                normalize_alias(&format!("union_source_{}", i))));
        }
    
        let df = ctx.sql(&sql).await
        .map_err(|e| ElusionError::SetOperationError {
            operation: "UNION MANY".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Verify SQL syntax and schema compatibility".to_string(),
        })?;
    
        Ok(CustomDataFrame {
            df,
            table_alias: "union_many_result".to_string(),
            from_table: "union_many_result".to_string(),
            selected_columns: self.selected_columns.clone(),
            alias_map: self.alias_map.clone(),
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
            original_expressions: self.original_expressions.clone(),
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

    /// Performs UNION_ALL  on two dataframes
    pub async fn union_all(self, other: CustomDataFrame) -> ElusionResult<Self> {

        // if self.df.schema() != other.df.schema() {
        //     return Err(ElusionError::SetOperationError {
        //         operation: "UNION ALL".to_string(),
        //         reason: "Schema mismatch between dataframes".to_string(),
        //         suggestion: "💡 Ensure both dataframes have identical column names and types".to_string(),
        //     });
        // }

        let ctx = Arc::new(SessionContext::new());
        
        register_df_as_table(&ctx, &self.table_alias, &self.df).await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Registering first table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if table name is unique and data is valid".to_string(),
        })?;

        register_df_as_table(&ctx, &other.table_alias, &other.df).await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Registering second table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if table name is unique and data is valid".to_string(),
        })?;
        
        let sql = format!(
            "SELECT * FROM {} UNION ALL SELECT * FROM {}",
            normalize_alias(&self.table_alias),
            normalize_alias(&other.table_alias)
        );
    
        let df = ctx.sql(&sql).await
        .map_err(|e| ElusionError::SetOperationError {
            operation: "UNION ALL".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Verify SQL syntax and schema compatibility".to_string(),
        })?;
    
        Ok(CustomDataFrame {
            df,
            table_alias: "union_all_result".to_string(),
            from_table: "union_all_result".to_string(),
            selected_columns: self.selected_columns.clone(),
            alias_map: self.alias_map.clone(),
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
            original_expressions: self.original_expressions.clone(),
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
    /// Performs UNIONA_ALL on multiple dataframes
    pub async fn union_all_many<const N: usize>(self, others: [CustomDataFrame; N]) -> ElusionResult<Self> {

        if N == 0 {
            return Err(ElusionError::SetOperationError {
                operation: "UNION ALL MANY".to_string(),
                reason: "No dataframes provided for union operation".to_string(),
                suggestion: "💡 Provide at least one dataframe to union with".to_string(),
            });
        }

        // for (i, other) in others.iter().enumerate() {
        //     if self.df.schema() != other.df.schema() {
        //         return Err(ElusionError::SetOperationError {
        //             operation: "UNION ALL MANY".to_string(),
        //             reason: format!("Schema mismatch with dataframe at index {}", i),
        //             suggestion: "💡 Ensure all dataframes have identical column names and types".to_string(),
        //         });
        //     }
        // }

        let ctx = Arc::new(SessionContext::new());
        
        register_df_as_table(&ctx, &self.table_alias, &self.df).await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Registering base table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if table name is unique and data is valid".to_string(),
        })?;
        
        for (i, other) in others.iter().enumerate() {
            let alias = format!("union_all_source_{}", i);
            register_df_as_table(&ctx, &alias, &other.df).await
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: format!("Registering table {}", i),
                    reason: e.to_string(),
                    suggestion: "💡 Check if table name is unique and data is valid".to_string(),
                })?;
        }
        
        let mut sql = format!("SELECT * FROM {}", normalize_alias(&self.table_alias));
        for i in 0..N {
            sql.push_str(&format!(" UNION ALL SELECT * FROM {}", 
                normalize_alias(&format!("union_all_source_{}", i))));
        }
    
        let df = ctx.sql(&sql).await
        .map_err(|e| ElusionError::SetOperationError {
            operation: "UNION ALL MANY".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Verify SQL syntax and schema compatibility".to_string(),
        })?;
    
        Ok(CustomDataFrame {
            df,
            table_alias: "union_all_many_result".to_string(),
            from_table: "union_all_many_result".to_string(),
            selected_columns: self.selected_columns.clone(),
            alias_map: self.alias_map.clone(),
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
            original_expressions: self.original_expressions.clone(),
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
    /// Performs EXCEPT on two dataframes
    pub async fn except(self, other: CustomDataFrame) -> ElusionResult<Self> {

        // if self.df.schema() != other.df.schema() {
        //     return Err(ElusionError::SetOperationError {
        //         operation: "EXCEPT".to_string(),
        //         reason: "Schema mismatch between dataframes".to_string(),
        //         suggestion: "💡 Ensure both dataframes have identical column names and types".to_string(),
        //     });
        // }

        let ctx = Arc::new(SessionContext::new());
        
         register_df_as_table(&ctx, &self.table_alias, &self.df).await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Registering first table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if table name is unique and data is valid".to_string(),
        })?;

        register_df_as_table(&ctx, &other.table_alias, &other.df).await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Registering second table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if table name is unique and data is valid".to_string(),
        })?;
        
        let sql = format!(
            "SELECT * FROM {} EXCEPT SELECT * FROM {}",
            normalize_alias(&self.table_alias),
            normalize_alias(&other.table_alias)
        );
    
        let df = ctx.sql(&sql).await
        .map_err(|e| ElusionError::SetOperationError {
            operation: "EXCEPT".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Verify SQL syntax and schema compatibility".to_string(),
        })?;
    
        Ok(CustomDataFrame {
            df,
            table_alias: "except_result".to_string(),
            from_table: "except_result".to_string(),
            selected_columns: self.selected_columns.clone(),
            alias_map: self.alias_map.clone(),
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
            original_expressions: self.original_expressions.clone(),
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
    
    /// Performs INTERSECT on two dataframes
    pub async fn intersect(self, other: CustomDataFrame) -> ElusionResult<Self> {

        // if self.df.schema() != other.df.schema() {
        //     return Err(ElusionError::SetOperationError {
        //         operation: "INTERSECT".to_string(),
        //         reason: "Schema mismatch between dataframes".to_string(),
        //         suggestion: "💡 Ensure both dataframes have identical column names and types".to_string(),
        //     });
        // }

        let ctx = Arc::new(SessionContext::new());
        
        register_df_as_table(&ctx, &self.table_alias, &self.df).await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Registering first table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if table name is unique and data is valid".to_string(),
        })?;

        register_df_as_table(&ctx, &other.table_alias, &other.df).await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Registering second table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if table name is unique and data is valid".to_string(),
        })?;
        
        let sql = format!(
            "SELECT * FROM {} INTERSECT SELECT * FROM {}",
            normalize_alias(&self.table_alias),
            normalize_alias(&other.table_alias)
        );
    
        let df = ctx.sql(&sql).await
        .map_err(|e| ElusionError::SetOperationError {
            operation: "INTERSECT".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Verify SQL syntax and schema compatibility".to_string(),
        })?;
    
        Ok(CustomDataFrame {
            df,
            table_alias: "intersect_result".to_string(),
            from_table: "intersect_result".to_string(),
            selected_columns: self.selected_columns.clone(),
            alias_map: self.alias_map.clone(),
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
            original_expressions: self.original_expressions.clone(),
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
        register_df_as_table(&ctx, &self.table_alias, &self.df).await?;

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
        register_df_as_table(&ctx, &self.table_alias, &self.df).await?;
    
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
    
            //  final part of the value column for the label
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
                name_column.to_lowercase(),
                val_col,
                value_column.to_lowercase(),
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

    /// Fill down null values (deferred execution - follows select/filter pattern)
    pub fn fill_down<const N: usize>(
        mut self, 
        columns: [&str; N]
    ) -> Self {
        let normalized_columns: Vec<String> = columns
            .iter()
            .map(|col| {
                col.trim().replace(" ", "_").to_lowercase()
            })
            .collect();
        
     //   println!("Debug: Adding fill_down for columns: {:?} -> normalized: {:?}", columns, normalized_columns);
        
        let operation = format!("FILL_DOWN:{}", normalized_columns.join(","));
        self.set_operations.push(operation);
        self
    }

    /// Alternative implementation using set_operations if you prefer that pattern
    pub fn fill_down_with_set_ops<const N: usize>(
        mut self, 
        columns: [&str; N]
    ) -> Self {
        let operation = format!("FILL_DOWN:{}", columns.join(","));
        self.set_operations.push(operation);
        self
    }

    /// Fill down null values (immediate execution - follows append/union pattern)  
    pub async fn fill_down_now<const N: usize>(
        self,
        columns: [&str; N], 
        alias: &str
    ) -> ElusionResult<CustomDataFrame> {
        self.fill_down_vec_now(columns.to_vec(), alias).await
    }

    pub async fn fill_down_vec_now(
        self,
        columns: Vec<&str>,
        alias: &str  
    ) -> ElusionResult<CustomDataFrame> {
        let ctx = SessionContext::new();
        
        // Register current DataFrame
        register_df_as_table(&ctx, &self.table_alias, &self.df).await?;
        
        // Get all data first, then process it manually since DataFusion window functions don't work well
        let all_data_sql = format!("SELECT * FROM {}", normalize_alias(&self.table_alias));
        let temp_df = ctx.sql(&all_data_sql).await?;
        let batches = temp_df.clone().collect().await?;
        
        if batches.is_empty() {
            return Ok(CustomDataFrame {
                df: temp_df,
                table_alias: alias.to_string(),
                from_table: alias.to_string(),
                selected_columns: Vec::new(),
                alias_map: self.alias_map,
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
                query: all_data_sql,
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
            });
        }
        
        // Process each batch to implement fill down manually
        let schema = batches[0].schema();
        let mut processed_batches = Vec::new();
        
        // Find column indices for fill down columns
        let fill_column_indices: Vec<usize> = columns
            .iter()
            .filter_map(|col_name| {
                schema.fields().iter().position(|field| field.name() == col_name)
            })
            .collect();
        
      //  println!("Debug: Fill column indices: {:?} for columns: {:?}", fill_column_indices, columns);
        
        for batch in batches {
            let mut new_columns = Vec::new();
            let mut fill_values: Vec<Option<String>> = vec![None; fill_column_indices.len()];
            
            // Process each column
            for (col_idx, _field) in schema.fields().iter().enumerate() {
                let array = batch.column(col_idx);
                
                if let Some(fill_idx) = fill_column_indices.iter().position(|&idx| idx == col_idx) {
                    // This is a fill-down column - process it
                    let string_array = array.as_any().downcast_ref::<arrow::array::StringArray>()
                        .ok_or_else(|| ElusionError::Custom("Expected string array".to_string()))?;
                    
                    let mut new_values = Vec::new();
                    for i in 0..string_array.len() {
                        // Fix the type error - string_array.value(i) returns &str, not Option<&str>
                        if string_array.is_null(i) {
                            new_values.push(fill_values[fill_idx].clone());
                        } else {
                            let value = string_array.value(i);
                            if !value.trim().is_empty() {
                                fill_values[fill_idx] = Some(value.to_string());
                                new_values.push(Some(value.to_string()));
                            } else {
                                new_values.push(fill_values[fill_idx].clone());
                            }
                        }
                    }
                    
                    let new_array = arrow::array::StringArray::from(new_values);
                    new_columns.push(Arc::new(new_array) as ArrayRef);
                } else {
                    // Regular column - keep as is
                    new_columns.push(array.clone());
                }
            }
            
            // Fix the error conversion issue
            let new_batch = RecordBatch::try_new(schema.clone(), new_columns)
                .map_err(|e| ElusionError::Custom(format!("Failed to create record batch: {}", e)))?;
            processed_batches.push(new_batch);
        }
        
        // Create new DataFrame from processed batches
        let result_mem_table = MemTable::try_new(schema.clone().into(), vec![processed_batches])
            .map_err(|e| ElusionError::Custom(format!("Failed to create mem table: {}", e)))?;
        ctx.register_table(alias, Arc::new(result_mem_table))
            .map_err(|e| ElusionError::Custom(format!("Failed to register table: {}", e)))?;
        let result_df = ctx.table(alias).await
            .map_err(|e| ElusionError::Custom(format!("Failed to get table: {}", e)))?;
        
        Ok(CustomDataFrame {
            df: result_df,
            table_alias: alias.to_string(),
            from_table: alias.to_string(),
            selected_columns: Vec::new(),
            alias_map: self.alias_map,
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
            query: format!("-- Manual fill down processing for columns: {:?}", columns),
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

    /// Handle various set operations 
    fn handle_set_operation(&self, operation: &str, base_sql: String) -> String {
        if let Some(columns_and_value) = operation.strip_prefix("FILL_NULL:") {
            self.handle_fill_null_operation(columns_and_value, base_sql)
        } else if let Some(columns_str) = operation.strip_prefix("DROP_NULL:") {
            self.handle_drop_null_operation(columns_str, base_sql)
        } else if let Some(columns_str) = operation.strip_prefix("FILL_DOWN:") {
            self.handle_fill_down_operation(columns_str, base_sql)
        } else if let Some(skip_count) = operation.strip_prefix("SKIP_ROWS:") {
            self.handle_skip_rows_operation(skip_count, base_sql)  
        } else if operation.starts_with("UNION") {
            base_sql
        } else {
            base_sql
        }
    }

    /// Handle the FILL_DOWN operation by wrapping the base SQL in a CTE
    fn handle_fill_down_operation(&self, columns_str: &str, base_sql: String) -> String {
        let columns: Vec<&str> = columns_str.split(',').collect();
      //  println!("Debug: handle_fill_down_operation called with columns: {:?}", columns);
        
            let selected_cols = if self.selected_columns.is_empty() {
        self.df.schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>()
    } else {
        self.selected_columns
            .iter()
            .map(|col| {
                // Need to find the LAST occurrence of " as " to get the alias
                // because there might be " as " inside CAST functions
                
                let col_lower = col.to_lowercase();
                
                // Find all " as " positions
                let mut as_positions = Vec::new();
                let mut search_from = 0;
                while let Some(pos) = col_lower[search_from..].find(" as ") {
                    as_positions.push(search_from + pos);
                    search_from = search_from + pos + 4;
                }
                
                // Find which " as " is the actual alias separator
                // by checking parentheses depth
                for &as_pos in as_positions.iter().rev() {
                    let before_as = &col[..as_pos];
                    
                    // Count parentheses depth
                    let mut paren_depth = 0;
                    for ch in before_as.chars() {
                        match ch {
                            '(' => paren_depth += 1,
                            ')' => paren_depth -= 1,
                            _ => {}
                        }
                    }
                    
                    // If parentheses are balanced, this is the alias separator
                    if paren_depth == 0 {
                        let alias = &col[as_pos + 4..]; // Skip " as "
                        return alias.trim().trim_matches('"').to_string();
                    }
                }
                
                // No valid " as " found, extract column name normally
                col.trim_matches('"')
                    .split('.')
                    .last()
                    .unwrap_or(col)
                    .trim_matches('"')
                    .to_string()
            })
            .collect()
    };
        
       // println!("Debug: selected_cols after processing: {:?}", selected_cols);
       // println!("Debug: columns to fill: {:?}", columns);
        
        // Handle both NULL and string "null" values by converting them to actual NULLs first
        let fill_expressions: Vec<String> = selected_cols
            .iter()
            .map(|col_name| {
                if columns.contains(&col_name.as_str()) {
                    //println!("Debug: Processing fill_down for column: {}", col_name);
                    // First convert "null" strings and empty strings to actual NULLs,
                    // then use LAST_VALUE with IGNORE NULLS
                    format!(
                        r#"LAST_VALUE(
                            CASE 
                                WHEN "{0}" IS NULL OR TRIM("{0}") = '' OR TRIM("{0}") = 'null' THEN NULL
                                ELSE "{0}"
                            END
                        ) IGNORE NULLS OVER (
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) AS "{0}""#,
                        col_name
                    )
                } else {
                  //  println!("Debug: NOT processing fill_down for column: {} (not in fill list)", col_name);
                    format!(r#""{0}""#, col_name)
                }
            })
            .collect();
        
        let result_sql = format!(
            r#"WITH fill_down_base AS (
                {}
            )
            SELECT {} 
            FROM fill_down_base"#,
            base_sql,
            fill_expressions.join(", ")
        );
        
     //  println!("Debug: Final SQL: {}", result_sql);
        result_sql
    }

    /// Skip the first n rows of the DataFrame 
    pub fn skip_rows(mut self, n: u64) -> Self {
        if n == 0 {
            // No-op if trying to skip 0 rows
            return self;
        }

        let operation = format!("SKIP_ROWS:{}", n);
        self.set_operations.push(operation);
        self
    }

    /// Handle SKIP_ROWS operation 
    fn handle_skip_rows_operation(&self, skip_count_str: &str, base_sql: String) -> String {
        let skip_count = match skip_count_str.parse::<u64>() {
            Ok(n) => n,
            Err(_) => return base_sql, // Invalid number, return unchanged
        };

        if skip_count == 0 {
            return base_sql; // No-op for skip 0
        }

        format!(
            r#"WITH skip_rows_base AS (
                {}
            ), 
            skip_rows_numbered AS (
                SELECT *, 
                       ROW_NUMBER() OVER () as rn
                FROM skip_rows_base
            )
            SELECT * EXCEPT (rn)
            FROM skip_rows_numbered 
            WHERE rn > {}"#,
            base_sql,
            skip_count
        )
    }

    /// Fill null values in specified columns with a given value 
    pub fn fill_null<const N: usize>(mut self, columns: [&str; N], fill_value: &str) -> Self {
        if N == 0 {
            // Skip invalid operations - will be caught during elusion()
            return self;
        }

        // Add fill null operation to set_operations
        let columns_str = columns.join(",");
        let operation = format!("FILL_NULL:{}:{}", columns_str, fill_value);
        self.set_operations.push(operation);
        self
    }

    /// Drop rows that contain null values in specified columns (chainable - similar to Polars drop_nulls())
    pub fn drop_null<const N: usize>(mut self, columns: [&str; N]) -> Self {
        if N == 0 {
            // Skip invalid operations - will be caught during elusion()
            return self;
        }

        let columns_str = columns.join(",");
        let operation = format!("DROP_NULL:{}", columns_str);
        self.set_operations.push(operation);
        self
    }

    fn handle_fill_null_operation(&self, columns_and_value: &str, base_sql: String) -> String {
        let parts: Vec<&str> = columns_and_value.split(':').collect();
        if parts.len() != 2 {
            return base_sql;
        }
        
        let columns_str = parts[0];
        let fill_value = parts[1];
        let fill_columns: Vec<&str> = columns_str.split(',').collect();
        
        // Build the select expressions for the fill_null operation
        let select_expressions: Vec<String> = if self.selected_columns.is_empty() {
            self.df.schema()
                .fields()
                .iter()
                .map(|f| {
                    let col_name = f.name();
                    if fill_columns.iter().any(|&c| c.eq_ignore_ascii_case(col_name)) {
                        format!(
                            r#"CASE
                                WHEN "{0}" IS NULL OR
                                    TRIM("{0}") = '' OR
                                    UPPER(TRIM("{0}")) = 'NULL' OR
                                    UPPER(TRIM("{0}")) = 'NA' OR
                                    UPPER(TRIM("{0}")) = 'N/A' OR
                                    UPPER(TRIM("{0}")) = 'NONE' OR
                                    TRIM("{0}") = '-' OR
                                    TRIM("{0}") = '?' OR
                                    TRIM("{0}") = 'NaN' OR
                                    UPPER(TRIM("{0}")) = 'NAN'
                                THEN '{1}'
                                ELSE "{0}"
                            END AS "{0}""#,
                            col_name, fill_value
                        )
                    } else {
                        format!("\"{}\"", col_name)
                    }
                })
                .collect()
        } else {
            let column_names_and_aliases = self.selected_columns
                .iter()
                .map(|expr| {
                    // Find the alias if it exists, otherwise use the column name
                    if let Some(as_pos) = Self::find_alias_position(expr) {
                        let alias = expr[as_pos + 4..].trim().trim_matches('"').to_string();
                        (alias.clone(), Some(alias))
                    } else {
                        // Extract the column name
                        let col_name = expr.trim_matches('"')
                            .split('.')
                            .last()
                            .unwrap_or(expr)
                            .to_string();
                        (col_name, None)
                    }
                })
                .collect::<Vec<_>>();
            
            column_names_and_aliases
                .iter()
                .map(|(col_name, alias_opt)| {
                    let target_name = alias_opt.as_ref().unwrap_or(col_name);
                    
                    // Check if this column should be filled
                    if fill_columns.iter().any(|&c| c.eq_ignore_ascii_case(target_name)) {
                        format!(
                            r#"CASE
                                WHEN "{0}" IS NULL OR
                                    TRIM("{0}") = '' OR
                                    UPPER(TRIM("{0}")) = 'NULL' OR
                                    UPPER(TRIM("{0}")) = 'NA' OR
                                    UPPER(TRIM("{0}")) = 'N/A' OR
                                    UPPER(TRIM("{0}")) = 'NONE' OR
                                    TRIM("{0}") = '-' OR
                                    TRIM("{0}") = '?' OR
                                    TRIM("{0}") = 'NaN' OR
                                    UPPER(TRIM("{0}")) = 'NAN'
                                THEN '{1}'
                                ELSE "{0}"
                            END AS "{0}""#,
                            target_name, fill_value
                        )
                    } else {
                        // Just reference the column by its name/alias from the base CTE
                        format!("\"{}\"", target_name)
                    }
                })
                .collect()
        };
        
        format!(
            r#"WITH fill_null_base AS (
                {}
            )
            SELECT {} 
            FROM fill_null_base"#,
            base_sql,
            select_expressions.join(", ")
        )
    }

    // Helper function to find alias position considering parentheses
    fn find_alias_position(expr: &str) -> Option<usize> {
        let expr_lower = expr.to_lowercase();
        let mut as_positions = Vec::new();
        let mut search_from = 0;
        
        while let Some(pos) = expr_lower[search_from..].find(" as ") {
            as_positions.push(search_from + pos);
            search_from = search_from + pos + 4;
        }
        
        for &as_pos in as_positions.iter().rev() {
            let before_as = &expr[..as_pos];
            let mut paren_depth = 0;
            
            for ch in before_as.chars() {
                match ch {
                    '(' => paren_depth += 1,
                    ')' => paren_depth -= 1,
                    _ => {}
                }
            }
            
            if paren_depth == 0 {
                return Some(as_pos);
            }
        }
        
        None
    }

    /// Handle DROP_NULL operation with enhanced null detection
    fn handle_drop_null_operation(&self, columns_str: &str, base_sql: String) -> String {
        let columns: Vec<&str> = columns_str.split(',').collect();
        
        let where_conditions: Vec<String> = columns
            .iter()
            .map(|&col| {
                let normalized_col = col.trim().replace(" ", "_").to_lowercase();
                let quoted_col = format!("\"{}\"", normalized_col);
                format!(
                    r#"({0} IS NOT NULL AND 
                       TRIM({0}) != '' AND 
                       UPPER(TRIM({0})) != 'NULL' AND
                       UPPER(TRIM({0})) != 'NA' AND
                       UPPER(TRIM({0})) != 'N/A' AND
                       UPPER(TRIM({0})) != 'NONE' AND
                       TRIM({0}) != '-' AND
                       TRIM({0}) != '?' AND
                       TRIM({0}) != 'NaN' AND
                       UPPER(TRIM({0})) != 'NAN')"#,
                    quoted_col
                )
            })
            .collect();
        
        format!(
            r#"WITH drop_null_base AS (
                {}
            )
            SELECT * 
            FROM drop_null_base 
            WHERE {}"#,
            base_sql,
            where_conditions.join(" AND ")
        )
    }

    /// Head funciton that returns the first n rows of the DataFrame
    pub async fn head(&self, n: u64) -> ElusionResult<Self> {
        let limit = n;
        
        if limit == 0 {
            return Err(ElusionError::LimitError {
                message: "Head limit cannot be zero".to_string(),
                value: 0,
                suggestion: "💡 Use a positive number for head() limit".to_string(),
            });
        }

        let ctx = SessionContext::new();
        
        register_df_as_table(&ctx, &self.table_alias, &self.df).await?;
        
        let sql = format!(
            "SELECT * FROM {} LIMIT {}",
            normalize_alias(&self.table_alias),
            limit
        );
        
        let head_df = ctx.sql(&sql).await
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Head Operation".to_string(),
                reason: format!("Failed to execute head query: {}", e),
                suggestion: "💡 Check if DataFrame contains valid data".to_string(),
            })?;

        let result_alias = format!("{}_head", self.table_alias);
        
        let batches = head_df.clone().collect().await
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Head Data Collection".to_string(),
                reason: format!("Failed to collect head results: {}", e),
                suggestion: "💡 Verify DataFrame contains data".to_string(),
            })?;

        let mem_table = MemTable::try_new(head_df.schema().clone().into(), vec![batches])
            .map_err(|e| ElusionError::SchemaError {
                message: format!("Failed to create head result table: {}", e),
                schema: Some(head_df.schema().to_string()),
                suggestion: "💡 Check schema compatibility".to_string(),
            })?;

        ctx.register_table(&result_alias, Arc::new(mem_table))
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Head Result Registration".to_string(),
                reason: format!("Failed to register head result: {}", e),
                suggestion: "💡 Try using a different result alias".to_string(),
            })?;

        let result_df = ctx.table(&result_alias).await
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Head Result Creation".to_string(),
                reason: format!("Failed to create head result DataFrame: {}", e),
                suggestion: "💡 Verify table registration succeeded".to_string(),
            })?;

        Ok(CustomDataFrame {
            df: result_df,
            table_alias: result_alias.clone(),
            from_table: result_alias.clone(),
            selected_columns: self.selected_columns.clone(),
            alias_map: self.alias_map.clone(),
            aggregations: Vec::new(),
            group_by_columns: Vec::new(),
            where_conditions: Vec::new(),
            having_conditions: Vec::new(),
            order_by_columns: Vec::new(),
            limit_count: Some(limit),
            joins: Vec::new(),
            window_functions: Vec::new(),
            ctes: Vec::new(),
            subquery_source: None,
            set_operations: Vec::new(),
            query: sql,
            aggregated_df: Some(head_df),
            union_tables: None,
            original_expressions: self.original_expressions.clone(),
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

    /// TAIL funcitons showing the last n rows of the DataFrame
    pub async fn tail(&self, n: u64) -> ElusionResult<Self> {
        let limit = n;
        
        if limit == 0 {
            return Err(ElusionError::LimitError {
                message: "Tail limit cannot be zero".to_string(),
                value: 0,
                suggestion: "💡 Use a positive number for tail() limit".to_string(),
            });
        }

        let ctx = SessionContext::new();
        
        register_df_as_table(&ctx, &self.table_alias, &self.df).await?;
        
        let count_sql = format!(
            "SELECT COUNT(*) as total_count FROM {}",
            normalize_alias(&self.table_alias)
        );
        
        let count_df = ctx.sql(&count_sql).await
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Tail Count Operation".to_string(),
                reason: format!("Failed to count rows for tail: {}", e),
                suggestion: "💡 Check if DataFrame contains valid data".to_string(),
            })?;

        let count_batches = count_df.collect().await
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Tail Count Collection".to_string(),
                reason: format!("Failed to collect count results: {}", e),
                suggestion: "💡 Verify DataFrame contains data".to_string(),
            })?;

        if count_batches.is_empty() {
            return Err(ElusionError::InvalidOperation {
                operation: "Tail Operation".to_string(),
                reason: "No data found in DataFrame".to_string(),
                suggestion: "💡 Ensure DataFrame contains data before using tail()".to_string(),
            });
        }

        let total_count = count_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| ElusionError::InvalidOperation {
                operation: "Tail Count Extraction".to_string(),
                reason: "Failed to extract row count".to_string(),
                suggestion: "💡 This is an internal error, please report it".to_string(),
            })?
            .value(0);

        if total_count == 0 {
            return Err(ElusionError::InvalidOperation {
                operation: "Tail Operation".to_string(),
                reason: "DataFrame is empty".to_string(),
                suggestion: "💡 Ensure DataFrame contains data before using tail()".to_string(),
            });
        }

        let offset = if total_count <= limit as i64 {
            0 // If total rows <= requested rows, start from beginning
        } else {
            total_count - limit as i64
        };

        let sql = format!(
            "SELECT * FROM {} LIMIT {} OFFSET {}",
            normalize_alias(&self.table_alias),
            limit,
            offset
        );
        
        let tail_df = ctx.sql(&sql).await
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Tail Operation".to_string(),
                reason: format!("Failed to execute tail query: {}", e),
                suggestion: "💡 Check if DataFrame contains valid data".to_string(),
            })?;

        let result_alias = format!("{}_tail", self.table_alias);
        
        let batches = tail_df.clone().collect().await
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Tail Data Collection".to_string(),
                reason: format!("Failed to collect tail results: {}", e),
                suggestion: "💡 Verify DataFrame contains data".to_string(),
            })?;

        let mem_table = MemTable::try_new(tail_df.schema().clone().into(), vec![batches])
            .map_err(|e| ElusionError::SchemaError {
                message: format!("Failed to create tail result table: {}", e),
                schema: Some(tail_df.schema().to_string()),
                suggestion: "💡 Check schema compatibility".to_string(),
            })?;

        ctx.register_table(&result_alias, Arc::new(mem_table))
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Tail Result Registration".to_string(),
                reason: format!("Failed to register tail result: {}", e),
                suggestion: "💡 Try using a different result alias".to_string(),
            })?;

        let result_df = ctx.table(&result_alias).await
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Tail Result Creation".to_string(),
                reason: format!("Failed to create tail result DataFrame: {}", e),
                suggestion: "💡 Verify table registration succeeded".to_string(),
            })?;

        Ok(CustomDataFrame {
            df: result_df,
            table_alias: result_alias.clone(),
            from_table: result_alias.clone(),
            selected_columns: self.selected_columns.clone(),
            alias_map: self.alias_map.clone(),
            aggregations: Vec::new(),
            group_by_columns: Vec::new(),
            where_conditions: Vec::new(),
            having_conditions: Vec::new(),
            order_by_columns: Vec::new(),
            limit_count: Some(limit),
            joins: Vec::new(),
            window_functions: Vec::new(),
            ctes: Vec::new(),
            subquery_source: None,
            set_operations: Vec::new(),
            query: sql,
            aggregated_df: Some(tail_df),
            union_tables: None,
            original_expressions: self.original_expressions.clone(),
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

    /// Display the first n rows  head() + display()
    pub async fn show_head(&self, n: u64) -> ElusionResult<()> {
        let head_df = self.head(n).await?;
        head_df.display().await
    }

    /// Display the last n rows tail() + display()
    pub async fn show_tail(&self, n: u64) -> ElusionResult<()> {
        let tail_df = self.tail(n).await?;
        tail_df.display().await
    }

    /// PEEK function that shows quick overview of the DataFrame showing both head and tail
    pub async fn peek(&self, n: u64) -> ElusionResult<()> {
        let limit = n;
        
        println!("📊 DataFrame Overview:");
        println!("🔝 First {} rows:", limit);
        self.show_head(limit).await?;
        
        println!("\n🔽 Last {} rows:", limit);
        self.show_tail(limit).await?;
        
        Ok(())
    }

     /// SELECT clause using const generics
    pub fn select<const N: usize>(self, columns: [&str; N]) -> Self {
        self.select_vec(columns.to_vec())
    }

    pub fn select_vec(mut self, columns: Vec<&str>) -> Self {
        let has_star_expansion = columns.iter().any(|&col| col == "*" || col.ends_with(".*") || col.ends_with(". *"));
        
        let final_columns = if has_star_expansion {
            // For star expansion: expand first, then remove duplicates
            let expanded = self.expand_star_columns(columns);
            self.remove_duplicate_columns(expanded)
        } else {
            // For explicit columns: keep exactly what user specified (no duplicate removal)
            columns.into_iter().map(|s| s.to_string()).collect()
        };

        let column_refs: Vec<&str> = final_columns.iter().map(|s| s.as_str()).collect();
        
        // Store original expressions with AS clauses 
        self.original_expressions = column_refs
            .iter()
            .filter(|&col| col.contains(" AS "))
            .map(|&s| s.to_string())
            .collect();

        // Store raw columns for lazy processing
        self.raw_selected_columns.extend(column_refs.iter().map(|&s| s.to_string()));
        self.needs_normalization = true;

        let mut all_columns = self.selected_columns.clone();
        
        if !self.group_by_columns.is_empty() {
            // For GROUP BY case
            for col in column_refs {
                if is_expression(col) {
                    if is_aggregate_expression(col) {
                        // Aggregation goes to SELECT only
                        all_columns.push(normalize_expression(col, &self.table_alias));
                    } else {
                        // Non-aggregate expression
                        let expr_without_alias = if col.contains(" AS ") {
                            col.split(" AS ").next().unwrap_or(col).trim()
                        } else {
                            col
                        };
                        
                        // Add to GROUP BY without alias
                        let group_by_expr = normalize_simple_expression(expr_without_alias, &self.table_alias);
                        if !self.group_by_columns.contains(&group_by_expr) {
                            self.group_by_columns.push(group_by_expr);
                        }
                        
                        // Add to SELECT with alias
                        all_columns.push(normalize_expression(col, &self.table_alias));
                    }
                } else {
                    // Simple column
                    let normalized_col = normalize_column_name(col);
                    if !self.group_by_columns.contains(&normalized_col) {
                        self.group_by_columns.push(normalized_col.clone());
                    }
                    all_columns.push(normalized_col);
                }
            }
        } else {
            // No GROUP BY case
            let aggregate_aliases: Vec<String> = self
                .aggregations
                .iter()
                .filter_map(|agg| {
                    agg.split(" AS ")
                        .nth(1)
                        .map(|alias| normalize_alias(alias))
                })
                .collect();

            all_columns.extend(
                column_refs
                    .into_iter()
                    .filter(|col| !aggregate_aliases.contains(&normalize_alias(col)))
                    .map(|s| {
                        if is_expression(s) {
                            normalize_expression(s, &self.table_alias)
                        } else {
                            normalize_column_name(s)
                        }
                    })
            );
        }

        // Remove duplicates while preserving order
        let mut seen = HashSet::new();
        self.selected_columns = all_columns
            .into_iter()
            .filter(|x| seen.insert(x.clone()))
            .collect();

        self
    }

    /// Remove duplicate columns based on base column name, keeping first occurrence
    fn remove_duplicate_columns(&self, columns: Vec<String>) -> Vec<String> {
        let mut seen_base_names = HashSet::new();
        let mut result = Vec::new();
    
       // println!("Input columns: {:?}", columns);
        
        for col in columns {
            let base_name = extract_base_column_name(&col);
          //  println!("Processing '{}' -> base_name: '{}'", col, base_name);
            
            if !seen_base_names.contains(&base_name) {
                seen_base_names.insert(base_name.clone());
                result.push(col.clone());
              //  println!("  -> KEEPING: '{}'", col);
            } else {
              //  println!("  -> SKIPPING duplicate: '{}'", col);
            }
        }
        
       // println!("Final deduplicated columns: {:?}", result);
        
        result
    }

    /// Simple star expansion using available schemas
    fn expand_star_columns(&self, columns: Vec<&str>) -> Vec<String> {
        let mut result = Vec::new();
        
        for col in columns {
            match col {
                "*" => {
                    // Add all columns from main table
                    let schema = self.df.schema();
                    for field in schema.fields() {
                        result.push(format!("{}.{}", self.table_alias, field.name()));
                    }
                    
                    // Add all columns from joined tables
                    for join in &self.joins {
                        let join_schema = join.dataframe.df.schema();
                        for field in join_schema.fields() {
                            result.push(format!("{}.{}", join.dataframe.table_alias, field.name()));
                        }
                    }
                }
                table_star if table_star.ends_with(".*") => {
                    let table_alias = &table_star[..table_star.len() - 2];
                    
                    // Check main table
                    if table_alias == self.table_alias {
                        let schema = self.df.schema();
                        for field in schema.fields() {
                            result.push(format!("{}.{}", table_alias, field.name()));
                        }
                    } else {
                        // Check joined tables
                        for join in &self.joins {
                            if join.dataframe.table_alias == table_alias {
                                let schema = join.dataframe.df.schema();
                                for field in schema.fields() {
                                    result.push(format!("{}.{}", table_alias, field.name()));
                                }
                                break;
                            }
                        }
                    }
                }
                regular => {
                    result.push(regular.to_string());
                }
            }
        }
        
        result
    }

    /// Extract JSON properties from a column containing JSON strings
    pub fn json<'a, const N: usize>(mut self, columns: [&'a str; N]) -> Self {
        let mut json_expressions = Vec::new();
        
        for expr in columns.iter() {
            // Parse the expression: "column.'$jsonPath' AS alias"
            let re = Regex::new(r"(?i)\s+AS\s+").unwrap();
            let parts: Vec<&str> = re.split(expr).collect();

            if parts.len() != 2 {
                continue; // skip invalid expressions, will be checked at .elusion() 
            }
            
            let path_part = parts[0].trim();
            let alias = parts[1].trim().to_lowercase();
            

            if !path_part.contains(".'$") {
                continue; // Skip invalid expressions
            }
            
            let col_path_parts: Vec<&str> = path_part.split(".'$").collect();
            let column_name = col_path_parts[0].trim();
            let json_path = col_path_parts[1].trim_end_matches('\'');
            
            // Normalize the column name with proper table prefix
            let normalized_column = if column_name.contains('.') {
                // Already has table prefix - normalize it
                normalize_column_name(column_name)
            } else {
                // Add the main table alias
                format!("\"{}\".\"{}\"", 
                    self.table_alias.to_lowercase(), 
                    column_name.to_lowercase())
            };
            
            let search_pattern = format!("\"{}\":", json_path);
            
            let sql_expr = format!(
                "CASE 
                    WHEN POSITION('{}' IN {}) > 0 THEN
                        TRIM(BOTH '\"' FROM 
                            SUBSTRING(
                                {}, 
                                POSITION('{}' IN {}) + {}, 
                                CASE
                                    WHEN POSITION(',\"' IN SUBSTRING({}, POSITION('{}' IN {}) + {})) > 0 THEN
                                        POSITION(',\"' IN SUBSTRING({}, POSITION('{}' IN {}) + {})) - 1
                                    WHEN POSITION('}}' IN SUBSTRING({}, POSITION('{}' IN {}) + {})) > 0 THEN
                                        POSITION('}}' IN SUBSTRING({}, POSITION('{}' IN {}) + {})) - 1
                                    ELSE 300 -- arbitrary large value
                                END
                            )
                        )
                    ELSE NULL
                END as \"{}\"",
                search_pattern, normalized_column,
                normalized_column, 
                search_pattern, normalized_column, search_pattern.len(),
                normalized_column, search_pattern, normalized_column, search_pattern.len(),
                normalized_column, search_pattern, normalized_column, search_pattern.len(),
                normalized_column, search_pattern, normalized_column, search_pattern.len(),
                normalized_column, search_pattern, normalized_column, search_pattern.len(),
                alias
            );
            
            json_expressions.push(sql_expr);
        }

        self.selected_columns.extend(json_expressions);
        
        self
    }

    /// Extract values from JSON array objects using regexp_like and string functions
    pub fn json_array<'a, const N: usize>(mut self, columns: [&'a str; N]) -> Self {
        let mut json_expressions = Vec::new();
        
        for expr in columns.iter() {
            // Parse the expression: "column.'$ValueField:IdField=IdValue' AS alias"
            let re = Regex::new(r"(?i)\s+AS\s+").unwrap();
            let parts: Vec<&str> = re.split(expr).collect();

            if parts.len() != 2 {
                continue; 
            }
            
            let path_part = parts[0].trim();
            let alias = parts[1].trim().to_lowercase();
            
            if !path_part.contains(".'$") {
                continue; 
            }
            
            let col_path_parts: Vec<&str> = path_part.split(".'$").collect();
            let column_name = col_path_parts[0].trim();
            let filter_expr = col_path_parts[1].trim_end_matches('\'');
            
            // Normalize the column name with proper table prefix
            let normalized_column = if column_name.contains('.') {
                // Already has table prefix - normalize it
                normalize_column_name(column_name)
            } else {
                // sdding main table alias
                format!("\"{}\".\"{}\"", 
                    self.table_alias.to_lowercase(), 
                    column_name.to_lowercase())
            };
        
            let filter_parts: Vec<&str> = filter_expr.split(':').collect();
            
            let sql_expr: String;
            
            if filter_parts.len() == 2 {
                // Format: "column.'$ValueField:IdField=IdValue' AS alias"
                let value_field = filter_parts[0].trim();
                let condition = filter_parts[1].trim();
                
                let condition_parts: Vec<&str> = condition.split('=').collect();
                if condition_parts.len() != 2 {
                    continue; 
                }
                
                let id_field = condition_parts[0].trim();
                let id_value = condition_parts[1].trim();
    
                sql_expr = format!(
                    "CASE 
                        WHEN regexp_like({}, '\\{{\"{}\":\"{}\",[^\\}}]*\"{}\":(\"[^\"]*\"|[0-9.]+|true|false)', 'i') THEN
                            CASE
                                WHEN regexp_like(
                                    regexp_match(
                                        {},
                                        '\\{{\"{}\":\"{}\",[^\\}}]*\"{}\":(\"[^\"]*\")',
                                        'i'
                                    )[1],
                                    '\"[^\"]*\"'
                                ) THEN
                                    -- Handle string values by removing quotes
                                    regexp_replace(
                                        regexp_match(
                                            {},
                                            '\\{{\"{}\":\"{}\",[^\\}}]*\"{}\":\"([^\"]*)\"',
                                            'i'
                                        )[1],
                                        '\"',
                                        ''
                                    )
                                ELSE
                                    -- Handle numeric and boolean values
                                    regexp_match(
                                        {},
                                        '\\{{\"{}\":\"{}\",[^\\}}]*\"{}\":([0-9.]+|true|false)',
                                        'i'
                                    )[1]
                            END
                        ELSE NULL
                    END as \"{}\"",
                    normalized_column, id_field, id_value, value_field,
                    normalized_column, id_field, id_value, value_field,
                    normalized_column, id_field, id_value, value_field,
                    normalized_column, id_field, id_value, value_field,
                    alias
                );
            } else {
                continue; 
            }
            
            json_expressions.push(sql_expr);
        }

        self.selected_columns.extend(json_expressions);
        
        self
    }

    fn should_warn_complexity(&self) -> bool {
        self.joins.len() > 3 || 
        self.selected_columns.len() > 15 ||
        self.where_conditions.len() > 8
    }

    /// Enhanced GROUP BY normalization - strips aliases and ensures proper expression format
    fn normalize_group_by_columns(&self) -> Vec<String> {
        self.group_by_columns.iter().map(|col| {
            // Strip any aliases that might have leaked in
            if col.contains(" as \"") {
                if let Some(as_pos) = col.find(" as \"") {
                    col[..as_pos].trim().to_string()
                } else {
                    col.clone()
                }
            } else {
                col.clone()
            }
        }).collect()
    }

    /// Remove duplicate rows across all columns, keeping the first occurrence
    pub async fn drop_duplicates(
        &self,
        alias: &str,
    ) -> ElusionResult<Self> {
        let ctx = SessionContext::new();
        
        register_df_as_table(&ctx, &self.table_alias, &self.df).await?;
        
        let columns_to_check: Vec<String> = self.df.schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        
        let sql = Self::build_drop_duplicates_sql_first(&self.table_alias, &columns_to_check);
        
        let result_df = ctx.sql(&sql).await
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "drop_duplicates execution".to_string(),
                reason: format!("Failed to execute drop_duplicates: {}", e),
                suggestion: "💡 Check if the DataFrame contains valid data".to_string(),
            })?;
        
        let batches = result_df.clone().collect().await
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "drop_duplicates collection".to_string(),
                reason: format!("Failed to collect results: {}", e),
                suggestion: "💡 Check memory availability".to_string(),
            })?;
        
        let mem_table = MemTable::try_new(
            result_df.schema().clone().into(),
            vec![batches]
        ).map_err(|e| ElusionError::SchemaError {
            message: format!("Failed to create result table: {}", e),
            schema: Some(result_df.schema().to_string()),
            suggestion: "💡 Check schema compatibility".to_string(),
        })?;
        
        ctx.register_table(alias, Arc::new(mem_table))
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Result Registration".to_string(),
                reason: format!("Failed to register result table: {}", e),
                suggestion: "💡 Try using a different alias name".to_string(),
            })?;
        
        let final_df = ctx.table(alias).await
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Result Retrieval".to_string(),
                reason: format!("Failed to retrieve final result: {}", e),
                suggestion: "💡 This might be an internal issue - try a different alias".to_string(),
            })?;
        
        Ok(CustomDataFrame {
            df: final_df,
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
            aggregated_df: Some(result_df),
            union_tables: None,
            original_expressions: Vec::new(),
            needs_normalization: false,
            raw_selected_columns: Vec::new(),
            raw_group_by_columns: Vec::new(),
            raw_where_conditions: Vec::new(),
            raw_having_conditions: Vec::new(),
            raw_join_conditions: Vec::new(),
            raw_aggregations: Vec::new(),
            uses_group_by_all: false,
        })
    }

    /// Remove duplicate rows based on specified columns, keeping the first occurrence
    pub async fn drop_duplicates_by_column(
        &self,
        columns: &[&str],
        alias: &str,
    ) -> ElusionResult<Self> {
        // Validate that columns are not empty
        if columns.is_empty() {
            return Err(ElusionError::InvalidOperation {
                operation: "drop_duplicates_by_column".to_string(),
                reason: "No columns specified for duplicate detection".to_string(),
                suggestion: "💡 Provide at least one column name, or use drop_duplicates() to check all columns".to_string(),
            });
        }

        let ctx = SessionContext::new();
        
        register_df_as_table(&ctx, &self.table_alias, &self.df).await?;
        
        // Validate that all specified columns exist in the DataFrame
        let schema_fields: Vec<String> = self.df.schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        
        let columns_to_check: Vec<String> = columns.iter().map(|s| s.to_string()).collect();
        
        for col in &columns_to_check {
            if !schema_fields.contains(col) {
                return Err(ElusionError::InvalidOperation {
                    operation: "drop_duplicates_by_column".to_string(),
                    reason: format!("Column '{}' does not exist in DataFrame", col),
                    suggestion: format!("💡 Available columns: {}", schema_fields.join(", ")),
                });
            }
        }
        
        let sql = Self::build_drop_duplicates_sql_first(&self.table_alias, &columns_to_check);
        
        let result_df = ctx.sql(&sql).await
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "drop_duplicates_by_column execution".to_string(),
                reason: format!("Failed to execute drop_duplicates_by_column: {}", e),
                suggestion: "💡 Check if the DataFrame contains valid data".to_string(),
            })?;
        
        let batches = result_df.clone().collect().await
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "drop_duplicates_by_column collection".to_string(),
                reason: format!("Failed to collect results: {}", e),
                suggestion: "💡 Check memory availability".to_string(),
            })?;
        
        let mem_table = MemTable::try_new(
            result_df.schema().clone().into(),
            vec![batches]
        ).map_err(|e| ElusionError::SchemaError {
            message: format!("Failed to create result table: {}", e),
            schema: Some(result_df.schema().to_string()),
            suggestion: "💡 Check schema compatibility".to_string(),
        })?;
        
        ctx.register_table(alias, Arc::new(mem_table))
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Result Registration".to_string(),
                reason: format!("Failed to register result table: {}", e),
                suggestion: "💡 Try using a different alias name".to_string(),
            })?;
        
        let final_df = ctx.table(alias).await
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Result Retrieval".to_string(),
                reason: format!("Failed to retrieve final result: {}", e),
                suggestion: "💡 This might be an internal issue - try a different alias".to_string(),
            })?;
        
        Ok(CustomDataFrame {
            df: final_df,
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
            aggregated_df: Some(result_df),
            union_tables: None,
            original_expressions: Vec::new(),
            needs_normalization: false,
            raw_selected_columns: Vec::new(),
            raw_group_by_columns: Vec::new(),
            raw_where_conditions: Vec::new(),
            raw_having_conditions: Vec::new(),
            raw_join_conditions: Vec::new(),
            raw_aggregations: Vec::new(),
            uses_group_by_all: false,
        })
    }
    
 
    //helper for drop duplicates
    fn build_drop_duplicates_sql_first(table_alias: &str, columns: &[String]) -> String {
        let partition_cols = columns
            .iter()
            .map(|c| format!("\"{}\"", c))
            .collect::<Vec<_>>()
            .join(", ");
        
        format!(
            r#"WITH ranked_rows AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY {} ORDER BY 1) as rn
                FROM {}
            )
            SELECT * EXCEPT (rn)
            FROM ranked_rows
            WHERE rn = 1"#,
            partition_cols,
            normalize_alias(table_alias)
        )
    }


    /// Construct the SQL query based on the current state, including joins
    /// Optimized SQL construction with SqlBuilder and reduced allocations
    fn construct_sql(&self) -> String {

        // println!("DEBUG selected_columns: {:?}", self.selected_columns);
        // println!("DEBUG aggregations: {:?}", self.aggregations);
        // Estimate capacity based on data size to reduce reallocations
        let estimated_capacity = self.estimate_sql_size();
        let mut builder = SqlBuilder::with_capacity(estimated_capacity);
        
        // Handle CTEs
        builder.with_ctes(&self.ctes);

        // Check for subquery pattern
        let is_subquery = self.from_table.starts_with('(') && self.from_table.ends_with(')');
        let no_selected_columns = self.selected_columns.is_empty() 
            && self.aggregations.is_empty() 
            && self.window_functions.is_empty();

        if is_subquery && no_selected_columns {
            // Return subquery directly
            return format!("{}{}", 
                if self.ctes.is_empty() { "" } else { &builder.buffer },
                self.from_table
            );
        }

        // Build SELECT parts
        let select_parts = self.build_select_parts();

        let normalized_group_by = if !self.group_by_columns.is_empty() {
            self.normalize_group_by_columns()
        } else {
            Vec::new()
        };
        
        // Build the main query
        builder.select(&select_parts)
               .from_table(&self.from_table, Some(&self.table_alias))
               .joins(&self.joins)
               .where_clause(&self.where_conditions)
               .group_by(&normalized_group_by)
               .having(&self.having_conditions)
               .order_by(&self.order_by_columns)
               .limit(self.limit_count);

        // Apply set operations
        let mut final_query = builder.build();
        for operation in &self.set_operations {
            final_query = self.handle_set_operation(operation, final_query);
        }

        final_query
    }

    /// Estimate SQL size to pre-allocate buffer capacity
    fn estimate_sql_size(&self) -> usize {
        let base_size = 200; 
        let select_size = self.selected_columns.iter().map(|s| s.len()).sum::<usize>() 
                         + self.aggregations.iter().map(|s| s.len()).sum::<usize>()
                         + self.window_functions.iter().map(|s| s.len()).sum::<usize>();
        let joins_size = self.joins.iter().map(|j| j.condition.len() + 50).sum::<usize>();
        let where_size = self.where_conditions.iter().map(|s| s.len()).sum::<usize>();
        
        base_size + select_size + joins_size + where_size + self.from_table.len()
    }

    /// Build SELECT parts with reduced allocations
    fn build_select_parts(&self) -> Vec<String> {
        let mut select_parts = Vec::new();

        // Always add aggregations first
        select_parts.extend_from_slice(&self.aggregations);
        
        if !self.group_by_columns.is_empty() {
            // For GROUP BY queries, just use selected_columns as-is
            // group_by_all() already ensured they match
            select_parts.extend_from_slice(&self.selected_columns);
        } else {
            // No GROUP BY - add all parts normally
            select_parts.extend_from_slice(&self.selected_columns);
        }

        // window functions
        select_parts.extend_from_slice(&self.window_functions);
        
        // Remove exact duplicates
        let mut seen = HashSet::new();
        select_parts.retain(|x| seen.insert(x.clone()));
        
        select_parts
    }
    
    /// Execute the constructed SQL and return a new CustomDataFrame
    pub async fn elusion(&self, alias: &str) -> ElusionResult<Self> {
        // fail fast
        if alias.trim().is_empty() {
            return Err(ElusionError::InvalidOperation {
                operation: "Elusion".to_string(),
                reason: "Alias cannot be empty".to_string(),
                suggestion: "💡 Provide a valid table alias".to_string()
            });
        }

        let ctx = Arc::new(SessionContext::new());

        // Pre-validate group_by_all() issues before SQL construction
        if self.has_group_by_all() {
            if let Err(validation_error) = self.validate_group_by_all_compatibility() {
                return Err(validation_error);
            }
        }

        // spawn_blocking for CPU-intensive SQL construction
        let sql = if self.is_complex_query() {
            let self_clone = self.clone(); // Only clone if needed for complex queries

            tokio::task::spawn_blocking(move || self_clone.construct_sql())
                .await
                .map_err(|e| ElusionError::Custom(format!("SQL construction task failed: {}", e)))?
        } else {
            self.construct_sql()
        };

        // Batch register tables for better performance
        self.register_all_tables(&ctx).await?;

        // Handle subquery special case efficiently
        let final_sql = if self.from_table.starts_with('(') && self.from_table.ends_with(')') {
            format!("SELECT * FROM {} AS {}", self.from_table, alias)
        } else {
            sql
        };
       // println!("{:?}", final_sql);

        // Execute the SQL query with context-aware error handling
        let df = ctx.sql(&final_sql).await
            .map_err(|e| {

        let error_msg = e.to_string();

       // println!("Full DataFusion error: {}", error_msg);

        if error_msg.contains("not found") || error_msg.contains("No field named") || 
           (error_msg.contains("could not be resolved") && !error_msg.contains("OVER")) {
            
            let missing_col = Self::extract_missing_column_comprehensive(&error_msg)
                .unwrap_or_else(|| {
                   // println!("Failed to extract column name, using 'unknown'");
                    "unknown".to_string()
                });
            
           // println!("Extracted column: {}", missing_col);
            
            let available_columns = self.get_available_columns();
            let error_context = self.determine_missing_column_context(&missing_col, &error_msg);
            
            return ElusionError::MissingColumnWithContext {
                column: missing_col,
                available_columns,
                context: error_context.context,
                location: error_context.location,
                suggestion: error_context.suggestion,
            };
        }

        // window function errors
        else if error_msg.contains("could not be resolved") && 
            (error_msg.to_uppercase().contains("OVER") || 
                error_msg.to_uppercase().contains("PARTITION BY") || 
                error_msg.to_uppercase().contains("ROW_NUMBER")) {
                
                let missing_cols = extract_window_function_columns(&error_msg);
                let missing_col = missing_cols.first().unwrap_or(&"unknown".to_string()).clone();
                
                // Check if this is a group_by_all() + window function issue
                if self.has_group_by_all() {
                    let function_context = Some(format!("Window function references column '{}' not in SELECT", missing_col));
                    return self.create_group_by_all_error(&missing_col, function_context, &error_msg);
                }
                
                // Regular window function error (not group_by_all)
                ElusionError::WindowFunctionError {
                    message: format!("Window function references columns not in SELECT"),
                    function: extract_window_function_name(&error_msg).unwrap_or("WINDOW_FUNCTION".to_string()),
                    details: format!("Missing columns: {}", missing_cols.join(", ")),
                    suggestion: format!(
                        "💡 Window function error - missing columns from SELECT. 🔧 Solution: Add missing columns to .select(): {} ✅ Example fix: .select ([\"your_existing_cols\", \"{}\"])",
                        missing_cols.iter().map(|col| format!("\"{}\"", col)).collect::<Vec<_>>().join(", "),
                        missing_cols.join("\", \"")
                    ),
                }
            }
            // could not be resolved errors
            else if error_msg.contains("could not be resolved from available columns") {
                let missing_col = extract_missing_column(&error_msg).unwrap_or("unknown".to_string());
                let function_context = detect_function_usage_in_error(&error_msg, &missing_col);
                
                // Check if this is a group_by_all() related issue
                if self.has_group_by_all() {
                    return self.create_group_by_all_error(&missing_col, function_context, &error_msg);
                }
                
                // Original GROUP BY error handling for manual group_by
                ElusionError::GroupByError {
                    message: "Column in SELECT clause missing from GROUP BY".to_string(),
                    invalid_columns: vec![missing_col.clone()],
                    function_context: function_context.clone(),
                    suggestion: generate_enhanced_groupby_suggestion(&missing_col, function_context.as_deref()),
                }
            }
            // andle duplicate column cases
            else if error_msg.contains("duplicate qualified field name") || 
                    error_msg.contains("Schema contains duplicate qualified field name") {
                ElusionError::DuplicateColumn {
                    column: extract_column_from_duplicate_error(&error_msg)
                        .unwrap_or("unknown".to_string()),
                    locations: vec!["result schema".to_string()],
                }
            }
            // projection duplicate names SELECT clause issue
            else if error_msg.contains("projections require unique expression names") ||
                    error_msg.contains("have the same name") {
                ElusionError::DuplicateColumn {
                    column: extract_column_from_projection_error(&error_msg)
                        .unwrap_or("unknown".to_string()),
                    locations: vec!["SELECT clause".to_string()],
                }
            }
            // JOIN errors
            else if error_msg.contains("join") || error_msg.contains("JOIN") {
                ElusionError::JoinError {
                    message: error_msg.clone(),
                    left_table: self.table_alias.clone(),
                    right_table: extract_table_from_join_error(&error_msg)
                        .unwrap_or("unknown".to_string()),
                    suggestion: "💡 Check JOIN conditions and ensure table aliases match those used in .join_many([...])".to_string(),
                }
            }
            // Aggregation errors
            else if error_msg.contains("aggregate") || 
                    error_msg.contains("SUM") || error_msg.contains("AVG") || 
                    error_msg.contains("COUNT") {
                ElusionError::AggregationError {
                    message: error_msg.clone(),
                    function: extract_function_from_error(&error_msg)
                        .unwrap_or("unknown".to_string()),
                    column: extract_column_from_agg_error(&error_msg)
                        .unwrap_or("unknown".to_string()),
                    suggestion: "💡 Check aggregation syntax in .agg([...]) and ensure columns exist in your tables".to_string(),
                }
            }
            // HAVING  errors  
            else if error_msg.contains("having") || error_msg.contains("HAVING") {
                ElusionError::InvalidOperation {
                    operation: "HAVING clause evaluation".to_string(),
                    reason: error_msg.clone(),
                    suggestion: "💡 HAVING conditions must reference aggregated columns or their aliases from .agg([...])".to_string(),
                }
            }
            // Column not found error
            else if error_msg.contains("not found") || error_msg.contains("No field named") {
                ElusionError::MissingColumn {
                    column: extract_missing_column(&error_msg)
                        .unwrap_or("unknown".to_string()),
                    available_columns: self.get_available_columns(),
                }
            }
            // Generic 
            else {
                ElusionError::InvalidOperation {
                    operation: "SQL Execution".to_string(),
                    reason: format!("Failed to execute SQL: {}", e),
                    suggestion: self.get_contextual_suggestion(&error_msg),
                }
            }
        })?;

        let (batches, schema) = if self.is_large_result_expected() {
            let df_clone = df.clone();
            tokio::task::spawn_blocking(move || {
                futures::executor::block_on(async {
                    let batches = df_clone.clone().collect().await?;
                    let schema = df_clone.schema().clone();
                    Ok::<_, datafusion::error::DataFusionError>((batches, schema))
                })
            })
            .await
            .map_err(|e| ElusionError::Custom(format!("Data collection task failed: {}", e)))?
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Data Collection".to_string(),
                reason: format!("Failed to collect results: {}", e),
                suggestion: "💡 Query executed successfully but failed to collect results. Try reducing result size with .limit() or check for memory issues".to_string()
            })?
        } else {
            let batches = df.clone().collect().await
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: "Data Collection".to_string(),
                    reason: format!("Failed to collect results: {}", e),
                    suggestion: "💡 Query executed but data collection failed. Check for data type issues or memory constraints".to_string()
                })?;
            (batches, df.schema().clone())
        };

        let result_mem_table = MemTable::try_new(schema.clone().into(), vec![batches])
            .map_err(|e| {
                let error_msg = e.to_string();
                if error_msg.contains("duplicate") || error_msg.contains("Schema") {
                    ElusionError::SchemaError {
                        message: format!("Schema registration failed: {}", e),
                        schema: Some(schema.to_string()),
                        suggestion: "💡 Result schema has conflicting column names. Use unique aliases in .select([...]) or avoid .elusion() for this query".to_string()
                    }
                } else {
                    ElusionError::SchemaError {
                        message: format!("Failed to create result table: {}", e),
                        schema: Some(schema.to_string()),
                        suggestion: "💡 Verify result schema compatibility and data types".to_string()
                    }
                }
            })?;

        ctx.register_table(alias, Arc::new(result_mem_table))
            .map_err(|e| {
                let error_msg = e.to_string();
                if error_msg.contains("already exists") || error_msg.contains("duplicate") {
                    ElusionError::InvalidOperation {
                        operation: "Result Registration".to_string(),
                        reason: format!("Table alias '{}' already exists", alias),
                        suggestion: format!("💡 Choose a different alias name or use a unique identifier like '{}_v2'", alias)
                    }
                } else {
                    ElusionError::InvalidOperation {
                        operation: "Result Registration".to_string(),
                        reason: format!("Failed to register result table: {}", e),
                        suggestion: "💡 Try using a different alias name or check for naming conflicts".to_string()
                    }
                }
            })?;

        let result_df = ctx.table(alias).await
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Result Retrieval".to_string(),
                reason: format!("Failed to retrieve final result: {}", e),
                suggestion: "💡 Table was registered but retrieval failed. This might be an internal issue - try a different alias".to_string()
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
            query: final_sql,
            aggregated_df: Some(df),
            union_tables: None,
            original_expressions: self.original_expressions.clone(),
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

    //============= error heleprs for elusion()

    fn has_group_by_all(&self) -> bool {
        self.uses_group_by_all 
    }

    fn validate_group_by_all_compatibility(&self) -> ElusionResult<()> {
        let mut missing_columns = Vec::new();
        let mut window_function_deps = Vec::new();
        
        // Check window functions for column dependencies
        for window_func in &self.window_functions {
            let dependencies = self.extract_column_dependencies(window_func);
            for dep in dependencies {
                if !self.is_column_in_select(&dep) {
                    missing_columns.push(dep.clone());
                    window_function_deps.push((window_func.clone(), dep));
                }
            }
        }
        
        // Check aggregation dependencies
        for agg in &self.aggregations {
            let dependencies = self.extract_column_dependencies(agg);
            for dep in dependencies {
                if !self.is_column_in_select(&dep) && !missing_columns.contains(&dep) {
                    missing_columns.push(dep);
                }
            }
        }
        
        if !missing_columns.is_empty() {
            return Err(ElusionError::GroupByAllCompatibilityError {
                missing_columns: missing_columns.clone(),
                window_function_dependencies: window_function_deps,
                suggestion: self.generate_group_by_all_fix_suggestion(&missing_columns),
            });
        }
        
        Ok(())
    }

    fn create_group_by_all_error(
        &self, 
        missing_col: &str, 
        function_context: Option<String>,
        original_error: &str
    ) -> ElusionError {
        let error_upper = original_error.to_uppercase();
        let is_window_function = error_upper.contains("OVER") || 
                            error_upper.contains("PARTITION BY") ||
                            error_upper.contains("ORDER BY") ||
                            error_upper.contains("ROW_NUMBER") ||
                            error_upper.contains("RANK");

        let current_select_columns = self.get_current_select_columns_formatted();
        let current_group_by_columns = self.get_current_group_by_columns_formatted();

        if is_window_function {
            ElusionError::GroupByAllWindowError {
                missing_column: missing_col.to_string(),
                window_function_context: function_context.unwrap_or_else(|| 
                    format!("Window function needs column '{}'", missing_col)
                ),
                suggestion: format!(
                    "🪟 group_by_all() + Window Function Issue. Your window function needs column '{}' but it's not in .select([...]). 🔧 Quick Fix - Add '{}' to your .select(): .select([{}, \"{}\" ]) 🔧 Alternative - Use manual .group_by(): .group_by([{}])",
                    missing_col, 
                    missing_col, 
                    current_select_columns,
                    missing_col,
                    current_group_by_columns
                ),
            }
        } else {
            ElusionError::GroupByAllDependencyError {
                missing_column: missing_col.to_string(),
                dependency_context: function_context.unwrap_or_else(|| 
                    format!("Column '{}' is referenced but not in SELECT clause", missing_col)
                ),
                suggestion: format!(
                    "🔧 group_by_all() Issue: Missing column '{}' referenced in query. The problem: Your query references '{}' but it's not in your .select([...]) clause. Since group_by_all() groups by ALL selected columns, it needs '{}' to be selected first. 💡 Solutions: [1] Add '{}' to your .select([...]) clause: .select([{},\"{}\" ]). [2] Use manual .group_by([...]) instead of .group_by_all(): .group_by([{}]). Only group by the columns you actually want to group by. [3] Remove the dependency on '{}' from your query",
                    missing_col, 
                    missing_col, 
                    missing_col, 
                    missing_col, 
                    current_select_columns,
                    missing_col,
                    current_group_by_columns,
                    missing_col
                ),
            }
        }
    }

    fn extract_column_dependencies(&self, expression: &str) -> Vec<String> {
        let mut dependencies = Vec::new();
        
        // Extract from PARTITION BY and ORDER BY in window functions
        if let Some(caps) = regex::Regex::new(r"PARTITION BY\s+([a-zA-Z_][a-zA-Z0-9_]*)")
            .unwrap().captures(expression) {
            if let Some(col) = caps.get(1) {
                dependencies.push(col.as_str().to_string());
            }
        }
        
        if let Some(caps) = regex::Regex::new(r"ORDER BY\s+([a-zA-Z_][a-zA-Z0-9_]*)")
            .unwrap().captures(expression) {
            if let Some(col) = caps.get(1) {
                dependencies.push(col.as_str().to_string());
            }
        }
        
        // Extract from function arguments
        if let Some(caps) = regex::Regex::new(r"\(([^)]+)\)").unwrap().captures(expression) {
            if let Some(args) = caps.get(1) {
                for arg in args.as_str().split(',') {
                    let clean_arg = arg.trim();
                    if regex::Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").unwrap().is_match(clean_arg) {
                        dependencies.push(clean_arg.to_string());
                    }
                }
            }
        }
        
        dependencies
    }

    pub fn extract_missing_column(error: &str) -> Option<String> {
        println!("🔍 DEBUG - Extracting column from: {}", error);
        
        if let Some(cap) = regex::Regex::new(r"No field named ([a-zA-Z_][a-zA-Z0-9_]*)\.")
            .unwrap().captures(error) {
            let col = cap.get(1)?.as_str().to_string();
            println!("🔍 DEBUG - Found via 'No field named': {}", col);
            return Some(col);
        }
        
        if let Some(cap) = regex::Regex::new(r"Expression ([a-zA-Z_][a-zA-Z0-9_]*) could not be resolved")
            .unwrap().captures(error) {
            let col = cap.get(1)?.as_str().to_string();
            println!("🔍 DEBUG - Found via 'Expression could not be resolved': {}", col);
            return Some(col);
        }
        
        if let Some(cap) = regex::Regex::new(r"Expression [a-zA-Z_][a-zA-Z0-9_]*\.([a-zA-Z_][a-zA-Z0-9_]*) could not be resolved")
            .unwrap().captures(error) {
            let col = cap.get(1)?.as_str().to_string();
            println!("🔍 DEBUG - Found via 'Expression table.column': {}", col);
            return Some(col);
        }
        
        if let Some(cap) = regex::Regex::new(r"(?:PARTITION BY|ORDER BY)\s+([a-zA-Z_][a-zA-Z0-9_]*)")
            .unwrap().captures(error) {
            let col = cap.get(1)?.as_str().to_string();
            println!("🔍 DEBUG - Found via window function: {}", col);
            return Some(col);
        }
        
        println!("🔍 DEBUG - No column name extracted");
        None
    }

    fn extract_missing_column_comprehensive(error: &str) -> Option<String> {
       // println!("Comprehensive extraction from: {}", error);
        
        // DataFusion Schema Error
        if error.contains("Schema error:") && error.contains("No field named") {
            // Match: "No field named COLUMN_NAME." (note the period at the end)
            if let Some(cap) = regex::Regex::new(r"No field named ([a-zA-Z_][a-zA-Z0-9_]*)\.")
                .unwrap().captures(error) {
                let col = cap.get(1)?.as_str().to_string();
              //  println!("🔍 Schema error column: {}", col);
                return Some(col);
            }
        }
        
        // DataFusion Planning Error:
        if error.contains("Expression") && error.contains("could not be resolved") {
            if let Some(start) = error.find("Expression ") {
                let remaining = &error[start + 11..];
                if let Some(end) = remaining.find(" could not be resolved") {
                    let expr = remaining[..end].trim();
                    
                    // Handle table.column format - extract just column
                    if let Some(dot_pos) = expr.rfind('.') {
                        let col = &expr[dot_pos + 1..];
                     //   println!("Planning error table.column: {}", col);
                        return Some(col.to_string());
                    } else {
                      //  println!("Planning error column: {}", expr);
                        return Some(expr.to_string());
                    }
                }
            }
        }
        
        // Fallback to original function
        extract_missing_column(error)
    }

    fn is_column_in_select(&self, column: &str) -> bool {
        self.raw_selected_columns.iter().any(|sel| {
            // Handle aliases and table prefixes
            sel.to_lowercase().contains(&column.to_lowercase()) ||
            sel.split(" as ").next().unwrap_or(sel)
                .split('.').last().unwrap_or(sel)
                .to_lowercase() == column.to_lowercase()
        })
    }

    fn is_column_used_in_window_functions(&self, missing_col: &str) -> bool {
        self.window_functions.iter().any(|window_func| {
            window_func.to_lowercase().contains(&missing_col.to_lowercase())
        })
    }
    
    fn is_column_used_in_aggregations(&self, missing_col: &str) -> bool {
        self.aggregations.iter().any(|agg| {
            agg.to_lowercase().contains(&missing_col.to_lowercase())
        })
    }
    
    fn is_column_used_in_where(&self, missing_col: &str) -> bool {
        self.where_conditions.iter().any(|condition| {
            condition.to_lowercase().contains(&missing_col.to_lowercase())
        })
    }
    
    fn is_column_likely_in_select(&self, missing_col: &str) -> bool {
        // Check direct match first
        if self.raw_selected_columns.iter().any(|col| {
            col.to_lowercase().contains(&missing_col.to_lowercase())
        }) {
            return true;
        }
        
        // Check if this looks like a malformed alias (contains underscore patterns)
        if missing_col.contains("_") {
            let parts: Vec<&str> = missing_col.split('_').collect();
            if parts.len() >= 2 {
                let first_part = parts[0];
                let last_part = parts[parts.len() - 1];
                
                // Check if any selected column contains these parts
                for selected_col in &self.raw_selected_columns {
                    if selected_col.to_lowercase().contains(&first_part.to_lowercase()) ||
                       selected_col.to_lowercase().contains(&last_part.to_lowercase()) {
                        return true;
                    }
                }
            }
        }
        
        false
    }

    fn determine_missing_column_context(&self, missing_col: &str, _error_msg: &str) -> ColumnErrorContext {

        if self.is_column_used_in_window_functions(missing_col) {
            return ColumnErrorContext {
                context: format!("Column '{}' is referenced in a window function", missing_col),
                location: "window() function".to_string(),
                suggestion: format!(
                    "💡 Check your .window() function. Either add '{}' to .select([...]) or fix the column name in your window function", 
                    missing_col
                ),
            };
        }
        
        if self.is_column_used_in_aggregations(missing_col) {
            return ColumnErrorContext {
                context: format!("Column '{}' is referenced in an aggregation", missing_col),
                location: "agg() function".to_string(),
                suggestion: format!(
                    "💡 Check your .agg([...]) function. Either add '{}' to .select([...]) or fix the column name in your aggregation", 
                    missing_col
                ),
            };
        }
        
        if self.is_column_used_in_where(missing_col) {
            return ColumnErrorContext {
                context: format!("Column '{}' is referenced in WHERE conditions", missing_col),
                location: "filter() or filter_many() function".to_string(),
                suggestion: format!(
                    "💡 Check your .filter() or .filter_many() conditions. Column '{}' doesn't exist in the table", 
                    missing_col
                ),
            };
        }
        
        if self.is_column_likely_in_select(missing_col) {
            return ColumnErrorContext {
                context: format!("Column '{}' is wrongly referenced, or is NOT referenced at all in SELECT([...]) clause", missing_col),
                location: "select([...]) function".to_string(),
                suggestion: format!(
                    "💡 [1] Use .df_schema() to see available columns. [2] Check your .select([...]), .string_functions([...]) and .datetime_functions([...]). [3] Column '{}' doesn't exist in the table OR is missing in SELECT function if you are using GROUP_BY_ALL(). [4] Maybe try to use GROUP_BY([...]) function and specify all select([...]) columns, and aliased columns from string_functions([...]) and .datetime_functions([...]).", 
                    missing_col
                ),
            };
        }
        
        ColumnErrorContext {
            context: format!("Column '{}' is referenced somewhere in your query", missing_col),
            location: "unknown location".to_string(),
            suggestion: format!(
                "Column '{}' doesn't exist. 💡 Check all your functions: .select(), .filter(), .window(), .agg(), .order_by()", 
                missing_col
            ),
        }
    }

    fn generate_group_by_all_fix_suggestion(&self, missing_columns: &[String]) -> String {
        let columns_list = missing_columns.iter()
            .map(|col| format!("\"{}\"", col))
            .collect::<Vec<_>>()
            .join(", ");
            
        format!(
            "🔧 group_by_all() requires all referenced columns in SELECT. Missing columns: {}. 💡 Quick Fixes: [1] Add missing columns to .select(): .select([\"your_existing_columns\", {}]) [2] Use manual .group_by() instead: .group_by([{}]) // only the columns you actually want to group by. [3] Restructure your query to avoid hidden dependencies",
            missing_columns.join(", "),
            columns_list,
            self.get_likely_group_by_columns().join(", ")
        )
    }

    fn get_likely_group_by_columns(&self) -> Vec<String> {
        self.raw_selected_columns
            .iter()
            .filter(|col| {
                // Exclude aggregated columns and window functions
                let col_upper = col.to_uppercase();
                !col_upper.contains("SUM(") &&
                !col_upper.contains("COUNT(") &&
                !col_upper.contains("AVG(") &&
                !col_upper.contains("MIN(") &&
                !col_upper.contains("MAX(") &&
                !col_upper.contains("ROW_NUMBER(") &&
                !col_upper.contains("RANK(") &&
                !col_upper.contains("DENSE_RANK(")
            })
            .map(|col| {
                // Extract column name, handling aliases and table prefixes
                if let Some(alias_pos) = col.to_lowercase().find(" as ") {
                    format!("\"{}\"", col[alias_pos + 4..].trim())
                } else if let Some(dot_pos) = col.rfind('.') {
                    format!("\"{}\"", &col[dot_pos + 1..])
                } else {
                    format!("\"{}\"", col.trim())
                }
            })
            .collect()
    }

    fn get_current_select_columns_formatted(&self) -> String {
        if self.raw_selected_columns.is_empty() {
            "    // No columns selected yet".to_string()
        } else {
            self.raw_selected_columns
                .iter()
                .map(|col| format!("\"{}\"", col))
                .collect::<Vec<_>>()
                .join(",\n")
        }
    }

    fn get_current_group_by_columns_formatted(&self) -> String {
        if self.raw_selected_columns.is_empty() {
            "// No columns available".to_string()
        } else {
            // Extract non-aggregate columns for GROUP BY suggestion
            let non_agg_columns: Vec<String> = self.raw_selected_columns
                .iter()
                .filter(|col| {
                    let col_upper = col.to_uppercase();
                    !col_upper.contains("SUM(") &&
                    !col_upper.contains("COUNT(") &&
                    !col_upper.contains("AVG(") &&
                    !col_upper.contains("MIN(") &&
                    !col_upper.contains("MAX(") &&
                    !col_upper.contains("ROW_NUMBER(") &&
                    !col_upper.contains("RANK(") &&
                    !col_upper.contains("DENSE_RANK(")
                })
                .map(|col| {
                    // Extract alias or column name for GROUP BY
                    if let Some(alias_pos) = col.to_lowercase().find(" as ") {
                        format!("\"{}\"", col[alias_pos + 4..].trim())
                    } else {
                        // Handle table.column format
                        if let Some(dot_pos) = col.rfind('.') {
                            format!("\"{}\"", &col[dot_pos + 1..])
                        } else {
                            format!("\"{}\"", col.trim())
                        }
                    }
                })
                .collect();

            if non_agg_columns.is_empty() {
                "// All columns are aggregated - manual GROUP BY may not be needed".to_string()
            } else {
                non_agg_columns.join(", ")
            }
        }
    }

    fn get_table_aliases_for_suggestion(&self) -> Vec<String> {
        let mut aliases = vec![self.table_alias.clone()];
        
        // Add aliases from joins
        for join in &self.joins {
            aliases.push(join.dataframe.table_alias.clone());
        }
        
        aliases
    }
    fn get_contextual_suggestion(&self, error_msg: &str) -> String {
        let has_joins = !self.joins.is_empty();
        let has_aggs = !self.aggregations.is_empty();
        let has_group_by = !self.group_by_columns.is_empty();
        let has_star = self.raw_selected_columns.iter().any(|col| col.contains("*"));
        let has_window_functions = !self.window_functions.is_empty();
        
        if has_joins && error_msg.contains("duplicate") {
            let table_aliases = self.get_table_aliases_for_suggestion();
            let example_column = self.get_example_column_for_alias_suggestion();
            
            if table_aliases.len() >= 2 && !example_column.is_empty() {
                format!(
                    "💡 JOIN detected with duplicate columns. Use aliases: .select([\"{}.{} AS {}_{}\", \"{}.{} AS {}_{}\"])) or star selection: .select([\"{}.*, {}.*/)",
                    table_aliases[0], example_column, table_aliases[0], example_column,
                    table_aliases[1], example_column, table_aliases[1], example_column,
                    table_aliases[0], table_aliases[1]
                )
            } else {
                "💡 JOIN detected with duplicate columns. Use table aliases in your .select([...]) or use star selection for auto-deduplication".to_string()
            }
        } else if has_window_functions && error_msg.contains("resolved") {
            "💡 Window function error. Ensure all referenced columns are in .select([...]) or check your PARTITION BY/ORDER BY clauses".to_string()
        } else if has_aggs && !has_group_by {
            let non_agg_columns = self.get_non_aggregate_columns();
            if !non_agg_columns.is_empty() {
                format!(
                    "💡 Aggregations detected without GROUP BY. Use .group_by_all() or specify .group_by([{}])",
                    non_agg_columns.join(", ")
                )
            } else {
                "💡 Aggregations detected without GROUP BY. Use .group_by_all() or specify .group_by([...]) columns".to_string()
            }
        } else if has_aggs && has_group_by && error_msg.contains("resolved") {
            "💡 GROUP BY/SELECT mismatch. Ensure all non-aggregate SELECT columns are in GROUP BY, or use .group_by_all()".to_string()
        } else if has_star && error_msg.contains("duplicate") {
            "💡 Star selection with duplicate columns. This should auto-deduplicate - check for mixed star/explicit selection".to_string()
        } else {
            "💡 Check SQL syntax, column names, and table aliases. Use .df_schema() to see available columns".to_string()
        }
    }

    fn get_example_column_for_alias_suggestion(&self) -> String {
        // Try to find a common column name that might be duplicated
        let common_names = ["id", "key", "name", "code", "date", "time", "value"];
        
        // Check if any selected columns match common names
        for col in &self.raw_selected_columns {
            let col_lower = col.to_lowercase();
            for &common in &common_names {
                if col_lower.contains(common) {
                    return common.to_string();
                }
            }
        }
        
        // Fall back to extracting from first selected column
        if let Some(first_col) = self.raw_selected_columns.first() {
            if let Some(dot_pos) = first_col.rfind('.') {
                return first_col[dot_pos + 1..].to_string();
            } else if let Some(space_pos) = first_col.find(' ') {
                return first_col[..space_pos].to_string();
            } else {
                return first_col.clone();
            }
        }
        
        "column".to_string()
    }

    /// Get non-aggregate columns for GROUP BY suggestions
    fn get_non_aggregate_columns(&self) -> Vec<String> {
        self.raw_selected_columns
            .iter()
            .filter(|col| {
                let col_upper = col.to_uppercase();
                !col_upper.contains("SUM(") &&
                !col_upper.contains("COUNT(") &&
                !col_upper.contains("AVG(") &&
                !col_upper.contains("MIN(") &&
                !col_upper.contains("MAX(") &&
                !col_upper.contains("ROW_NUMBER(") &&
                !col_upper.contains("RANK(")
            })
            .map(|col| {
                // Extract clean column name for GROUP BY
                if let Some(alias_pos) = col.to_lowercase().find(" as ") {
                    format!("\"{}\"", col[alias_pos + 4..].trim())
                } else if let Some(dot_pos) = col.rfind('.') {
                    format!("\"{}\"", &col[dot_pos + 1..])
                } else {
                    format!("\"{}\"", col.trim())
                }
            })
            .collect()
    }
    
    /// Get available columns for error suggestions
    fn get_available_columns(&self) -> Vec<String> {
        let mut columns = Vec::new();
        
        // Add columns from main table schema
        let schema = self.df.schema();
        for field in schema.fields() {
            columns.push(format!("{}.{}", self.table_alias, field.name()));
        }
        
        // Add columns from joined tables
        for join in &self.joins {
            let join_schema = join.dataframe.df.schema();
            for field in join_schema.fields() {
                columns.push(format!("{}.{}", join.dataframe.table_alias, field.name()));
            }
        }
        
        columns
    }

    // ==============================================================

    /// Batch register all required tables for better performance
    async fn register_all_tables(&self, ctx: &SessionContext) -> ElusionResult<()> {
        let mut tables_to_register = Vec::new();
        
        tables_to_register.push((&self.table_alias, &self.df));
        
        if self.union_tables.is_none() {
            for join in &self.joins {
                tables_to_register.push((&join.dataframe.table_alias, &join.dataframe.df));
            }
        }
 
        if let Some(tables) = &self.union_tables {
            for (table_alias, df, _) in tables {
                if ctx.table(table_alias).await.is_err() {
                    tables_to_register.push((table_alias, df));
                }
            }
        }
        // Register all tables
        for (alias, df) in tables_to_register {
            register_df_as_table(ctx, alias, df).await
                .map_err(|e| ElusionError::SchemaError {
                    message: format!("Failed to register table '{}': {}", alias, e),
                    schema: Some(df.schema().to_string()),
                    suggestion: "💡 Check table schema compatibility".to_string()
                })?;
        }

        Ok(())
    }

    /// Determine if this is a complex query that should use spawn_blocking
    fn is_complex_query(&self) -> bool {
        self.joins.len() > 3 ||
        self.selected_columns.len() + self.aggregations.len() > 20 ||
        self.where_conditions.len() > 10 ||
        !self.ctes.is_empty() ||
        !self.set_operations.is_empty()
    }

    /// Estimate if the result will be large and should use spawn_blocking
    fn is_large_result_expected(&self) -> bool {
        // if many joins or aggregations, expect larger results
        self.joins.len() > 2 || 
        self.aggregations.len() > 5 ||
        self.limit_count.map_or(true, |limit| limit > 10000)
    }

    /// Display functions that display results to terminal
    pub async fn display(&self) -> ElusionResult<()> {
        self.df.clone().show().await.map_err(|e| 
            ElusionError::Custom(format!("Failed to display DataFrame: {}", e))
        )
    }
    /// Print a compact schema view - just column names and types
    pub fn df_schema(&self) {
        let schema = self.df.schema();
        
        println!("\n📋 Schema - table alias: '{}'", self.table_alias);
        println!("{}", "-".repeat(60));
        
        for (index, field) in schema.fields().iter().enumerate() {
            let data_type = match field.data_type() {
                arrow::datatypes::DataType::Utf8 => "String",
                arrow::datatypes::DataType::Int32 => "Int32",
                arrow::datatypes::DataType::Int64 => "Int64", 
                arrow::datatypes::DataType::Float32 => "Float32",
                arrow::datatypes::DataType::Float64 => "Float64",
                arrow::datatypes::DataType::Boolean => "Boolean",
                arrow::datatypes::DataType::Date32 => "Date",
                arrow::datatypes::DataType::Timestamp(_, _) => "Timestamp",
                _ => "Other"
            };
            
            println!("{:2}. {} ({})", 
                index + 1, 
                field.name(), 
                data_type
            );
        }
        println!();
    }

    /// Display the SQL query with proper CTE formatting
    pub fn display_query(&self) {
        let final_query = self.construct_sql();
        
        // Enhanced formatting that handles CTEs properly
        let formatted = final_query
            .replace("WITH ", "\nWITH ")          
            .replace(") SELECT ", ")\n\nSELECT ")  
            .replace(" SELECT ", "\nSELECT ")
            .replace(" FROM ", "\nFROM ")
            .replace(" WHERE ", "\nWHERE ")
            .replace(" GROUP BY ", "\nGROUP BY ")
            .replace(" HAVING ", "\nHAVING ")
            .replace(" ORDER BY ", "\nORDER BY ")
            .replace(" LIMIT ", "\nLIMIT ")
            .replace(" INNER JOIN ", "\n  INNER JOIN ")    
            .replace(" LEFT JOIN ", "\n  LEFT JOIN ")
            .replace(" RIGHT JOIN ", "\n  RIGHT JOIN ")
            .replace(" AS (", " AS (\n  ")              
            .replace("UNION ALL ", "\nUNION ALL\n")  
            .replace("UNION ", "\nUNION\n")
            .replace("EXCEPT ", "\nEXCEPT\n")  
            .replace("INTERSECT ", "\nINTERSECT\n");
        
        println!("📋 Generated SQL Query:");
        println!("{}", "=".repeat(60));
        println!("{}", formatted);
        println!("{}", "=".repeat(60));
    }

    /// Display query with execution plan info including CTE analysis
    pub fn display_query_with_info(&self) {
        let final_query = self.construct_sql();
        
        println!("📋 Query Analysis:");
        println!("{}", "=".repeat(50));
        println!("🔍 SQL Query:");
        
        // Better formatting for complex queries
        let formatted = final_query
            .replace("WITH ", "\nWITH ")
            .replace(") SELECT ", ")\n\nSELECT ")
            .replace(" SELECT ", "\nSELECT ")
            .replace(" FROM ", "\nFROM ")
            .replace(" WHERE ", "\nWHERE ")
            .replace(" GROUP BY ", "\nGROUP BY ")
            .replace(" HAVING ", "\nHAVING ")
            .replace(" ORDER BY ", "\nORDER BY ")
            .replace(" LIMIT ", "\nLIMIT ")
            .replace(" INNER JOIN ", "\n  INNER JOIN ")
            .replace(" LEFT JOIN ", "\n  LEFT JOIN ")
            .replace(" RIGHT JOIN ", "\n  RIGHT JOIN ");
        
        println!("{}", formatted);
        println!();
        
        // Enhanced query analysis including CTE detection
        let query_upper = final_query.to_uppercase();
        println!("📊 Query Info:");
        println!("   • Has CTEs: {}", query_upper.contains("WITH"));
        println!("   • Has JOINs: {}", query_upper.contains("JOIN"));
        println!("   • Has WHERE: {}", query_upper.contains("WHERE"));
        println!("   • Has GROUP BY: {}", query_upper.contains("GROUP BY"));
        println!("   • Has HAVING: {}", query_upper.contains("HAVING"));
        println!("   • Has ORDER BY: {}", query_upper.contains("ORDER BY"));
        println!("   • Has LIMIT: {}", query_upper.contains("LIMIT"));
        println!("   • Has UNION: {}", query_upper.contains("UNION"));
        
        // Count complexity including CTEs
        let cte_count = query_upper.matches("WITH").count();
        let join_count = query_upper.matches("JOIN").count();
        let function_count = final_query.matches('(').count();
        let union_count = query_upper.matches("UNION").count();
        
        println!("   • CTE count: {}", cte_count);
        println!("   • Join count: {}", join_count);
        println!("   • Union count: {}", union_count);
        println!("   • Function calls: ~{}", function_count);
        
        // Enhanced complexity calculation
        let complexity = match (cte_count, join_count, union_count, function_count) {
            (0, 0, 0, 0..=2) => "Simple",
            (0, 0..=2, 0, 3..=10) => "Moderate",
            (1, 0..=3, 0..=1, _) => "Moderate",
            (0..=1, 4..=5, 0..=2, _) => "Complex",
            _ => "Very Complex"
        };
        println!("   • Complexity: {}", complexity);
        
        // Additional insights for complex queries
        if cte_count > 0 {
            println!("   💡 Query uses CTEs - good for readability and performance");
        }
        if join_count > 3 {
            println!("   ⚠️  Many JOINs detected - consider performance implications");
        }
        if function_count > 15 {
            println!("   ⚠️  Many functions detected - verify index usage");
        }
        
        println!("{}", "=".repeat(50));
    }


    // ================== STATISTICS FUNCS =================== //

    // helper functions for union
    fn find_actual_column_name(&self, column: &str) -> Option<String> {
        self.df
            .schema()
            .fields()
            .iter()
            .find(|f| f.name().to_lowercase() == column.to_lowercase())
            .map(|f| f.name().to_string())
    }
    /// Compute basic statistics for specified columns
    async fn compute_column_stats(&self, columns: &[&str]) -> ElusionResult<ColumnStats> {
        let mut stats = ColumnStats::default();
        let ctx = Arc::new(SessionContext::new());

        // Register the current dataframe as a temporary table
        register_df_as_table(&ctx, &self.table_alias, &self.df).await?;

        for &column in columns {
            // Find the actual column name from schema
            let actual_column = self.find_actual_column_name(column)
                .ok_or_else(|| ElusionError::Custom(
                    format!("Column '{}' not found in schema", column)
                ))?;
            
            // Use the found column name in the normalized form
            let normalized_col = if actual_column.contains('.') {
                normalize_column_name(&actual_column)
            } else {
                normalize_column_name(&format!("{}.{}", self.table_alias, actual_column))
            };
    
            let sql = format!(
                "SELECT 
                    COUNT(*) as total_count,
                    COUNT({col}) as non_null_count,
                    AVG({col}::float) as mean,
                    MIN({col}) as min_value,
                    MAX({col}) as max_value,
                    STDDEV({col}::float) as std_dev
                FROM {}",
                normalize_alias(&self.table_alias),
                col = normalized_col
            );

            let result_df = ctx.sql(&sql).await.map_err(|e| {
                ElusionError::Custom(format!(
                    "Failed to compute statistics for column '{}': {}",
                    column, e
                ))
            })?;

            let batches = result_df.collect().await.map_err(ElusionError::DataFusion)?;
            
            if let Some(batch) = batches.first() {
                // Access columns directly instead of using row()
                let total_count = batch.column(0).as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| ElusionError::Custom("Failed to downcast total_count".to_string()))?
                    .value(0);
                
                let non_null_count = batch.column(1).as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| ElusionError::Custom("Failed to downcast non_null_count".to_string()))?
                    .value(0);
                
                let mean = batch.column(2).as_any().downcast_ref::<Float64Array>()
                    .ok_or_else(|| ElusionError::Custom("Failed to downcast mean".to_string()))?
                    .value(0);
                
                let min_value = ScalarValue::try_from_array(batch.column(3), 0)?;
                let max_value = ScalarValue::try_from_array(batch.column(4), 0)?;
                
                let std_dev = batch.column(5).as_any().downcast_ref::<Float64Array>()
                    .ok_or_else(|| ElusionError::Custom("Failed to downcast std_dev".to_string()))?
                    .value(0);

                stats.columns.push(ColumnStatistics {
                    name: column.to_string(),
                    total_count,
                    non_null_count,
                    mean: Some(mean),
                    min_value,
                    max_value,
                    std_dev: Some(std_dev),
                });
            }
        }

        Ok(stats)
    }

    /// Check for null values in specified columns
    async fn analyze_null_values(&self, columns: Option<&[&str]>) -> ElusionResult<NullAnalysis> {
        let ctx = Arc::new(SessionContext::new());
        register_df_as_table(&ctx, &self.table_alias, &self.df).await?;

        let columns = match columns {
            Some(cols) => cols.to_vec(),
            None => {
                self.df
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect()
            }
        };

        let mut null_counts = Vec::new();
        for column in columns {
            // Find the actual column name from schema
            let actual_column = self.find_actual_column_name(column)
                .ok_or_else(|| ElusionError::Custom(
                    format!("Column '{}' not found in schema", column)
                ))?;
                
            // Use the found column name in the normalized form
            let normalized_col = if actual_column.contains('.') {
                normalize_column_name(&actual_column)
            } else {
                normalize_column_name(&format!("{}.{}", self.table_alias, actual_column))
            };
    
            let sql = format!(
                "SELECT 
                    '{}' as column_name,
                    COUNT(*) as total_rows,
                    COUNT(*) - COUNT({}) as null_count,
                    (COUNT(*) - COUNT({})) * 100.0 / COUNT(*) as null_percentage
                FROM {}",
                column, normalized_col, normalized_col, normalize_alias(&self.table_alias)
            );

            let result_df = ctx.sql(&sql).await.map_err(|e| {
                ElusionError::Custom(format!(
                    "Failed to analyze null values for column '{}': {}",
                    column, e
                ))
            })?;

            let batches = result_df.collect().await.map_err(ElusionError::DataFusion)?;
            
            if let Some(batch) = batches.first() {
                let column_name = batch.column(0).as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| ElusionError::Custom("Failed to downcast column_name".to_string()))?
                    .value(0);

                let total_rows = batch.column(1).as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| ElusionError::Custom("Failed to downcast total_rows".to_string()))?
                    .value(0);

                let null_count = batch.column(2).as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| ElusionError::Custom("Failed to downcast null_count".to_string()))?
                    .value(0);

                let null_percentage = batch.column(3).as_any().downcast_ref::<Float64Array>()
                    .ok_or_else(|| ElusionError::Custom("Failed to downcast null_percentage".to_string()))?
                    .value(0);

                null_counts.push(NullCount {
                    column_name: column_name.to_string(),
                    total_rows,
                    null_count,
                    null_percentage,
                });
            }
        }

        Ok(NullAnalysis { counts: null_counts })
    }

    /// Compute correlation between two numeric columns
    async fn compute_correlation(&self, col1: &str, col2: &str) -> ElusionResult<f64> {
        let ctx = Arc::new(SessionContext::new());
        register_df_as_table(&ctx, &self.table_alias, &self.df).await?;

        let actual_col1 = self.find_actual_column_name(col1)
        .ok_or_else(|| ElusionError::Custom(
            format!("Column '{}' not found in schema", col1)
        ))?;

        let actual_col2 = self.find_actual_column_name(col2)
            .ok_or_else(|| ElusionError::Custom(
                format!("Column '{}' not found in schema", col2)
            ))?;
        
        // Use the found column names in normalized form
        let normalized_col1 = if actual_col1.contains('.') {
            normalize_column_name(&actual_col1)
        } else {
            normalize_column_name(&format!("{}.{}", self.table_alias, actual_col1))
        };
        
        let normalized_col2 = if actual_col2.contains('.') {
            normalize_column_name(&actual_col2)
        } else {
            normalize_column_name(&format!("{}.{}", self.table_alias, actual_col2))
        };
        
        let sql = format!(
            "SELECT corr({}::float, {}::float) as correlation 
            FROM {}",
            normalized_col1, normalized_col2, normalize_alias(&self.table_alias)
        );

        let result_df = ctx.sql(&sql).await.map_err(|e| {
            ElusionError::Custom(format!(
                "Failed to compute correlation between '{}' and '{}': {}",
                col1, col2, e
            ))
        })?;

        let batches = result_df.collect().await.map_err(ElusionError::DataFusion)?;
        
        if let Some(batch) = batches.first() {
            if let Some(array) = batch.column(0).as_any().downcast_ref::<Float64Array>() {
                if !array.is_null(0) {
                    return Ok(array.value(0));
                }
            }
        }

        Ok(0.0) // Return 0 if no correlation could be computed
    }


     /// Display statistical summary of specified columns
    pub async fn display_stats(&self, columns: &[&str]) -> ElusionResult<()> {
        let stats = self.compute_column_stats(columns).await?;
        
        println!("\n=== Column Statistics ===");
        println!("{:-<80}", "");
        
        for col_stat in stats.columns {
            println!("Column: {}", col_stat.name);
            println!("{:-<80}", "");
            println!("| {:<20} | {:>15} | {:>15} | {:>15} |", 
                "Metric", "Value", "Min", "Max");
            println!("{:-<80}", "");
            
            println!("| {:<20} | {:>15} | {:<15} | {:<15} |", 
                "Records", 
                col_stat.total_count,
                "-",
                "-");
                
            println!("| {:<20} | {:>15} | {:<15} | {:<15} |", 
                "Non-null Records", 
                col_stat.non_null_count,
                "-",
                "-");
                
            if let Some(mean) = col_stat.mean {
                println!("| {:<20} | {:>15.2} | {:<15} | {:<15} |", 
                    "Mean", 
                    mean,
                    "-",
                    "-");
            }
            
            if let Some(std_dev) = col_stat.std_dev {
                println!("| {:<20} | {:>15.2} | {:<15} | {:<15} |", 
                    "Standard Dev", 
                    std_dev,
                    "-",
                    "-");
            }
            
            println!("| {:<20} | {:>15} | {:<15} | {:<15} |", 
                "Value Range", 
                "-",
                format!("{}", col_stat.min_value),
                format!("{}", col_stat.max_value));
                
            println!("{:-<80}\n", "");
        }
        Ok(())
    }

    /// Display null value analysis
    pub async fn display_null_analysis(&self, columns: Option<&[&str]>) -> ElusionResult<()> {
        let analysis = self.analyze_null_values(columns).await?;
        
        println!("\n=== Null Value Analysis ===");
        println!("{:-<90}", "");
        println!("| {:<30} | {:>15} | {:>15} | {:>15} |", 
            "Column", "Total Rows", "Null Count", "Null Percentage");
        println!("{:-<90}", "");
        
        for count in analysis.counts {
            println!("| {:<30} | {:>15} | {:>15} | {:>14.2}% |", 
                count.column_name,
                count.total_rows,
                count.null_count,
                count.null_percentage);
        }
        println!("{:-<90}\n", "");
        Ok(())
    }

    /// Display correlation matrix for multiple columns
    pub async fn display_correlation_matrix(&self, columns: &[&str]) -> ElusionResult<()> {
        println!("\n=== Correlation Matrix ===");
        let col_width = 20;
        let total_width = (columns.len() + 1) * (col_width + 3) + 1;
        println!("{:-<width$}", "", width = total_width);
        
        // Print header with better column name handling
        print!("| {:<width$} |", "", width = col_width);
        for col in columns {
            let display_name = if col.len() > col_width {
                // Take first 12 chars and add "..." 
                format!("{}...", &col[..12])
            } else {
                col.to_string()
            };
            print!(" {:<width$} |", display_name, width = col_width);
        }
        println!();
        println!("{:-<width$}", "", width = total_width);
        
        // Calculate and print correlations with more decimal places
        for &col1 in columns {
            let display_name = if col1.len() > col_width {
                format!("{}...", &col1[..12])
            } else {
                col1.to_string()
            };
            print!("| {:<width$} |", display_name, width = col_width);
                    
            for &col2 in columns {
                let correlation = self.compute_correlation(col1, col2).await?;
                print!(" {:>width$.4} |", correlation, width = col_width);  // Changed to 4 decimal places
            }
            println!();
        }
        println!("{:-<width$}\n", "", width = total_width);
        Ok(())
    }

// ====================== WRITERS ==================== //

    /// Writes the DataFrame to a JSON file (always in overwrite mode)
    pub async fn write_to_json(
        &self,
        path: &str,
        pretty: bool,
    ) -> ElusionResult<()> {

        if !path.ends_with(".json") {
            return Err(ElusionError::Custom(
                "❌ Invalid file extension. Json files must end with '.json'".to_string()
            ));
        }

        if let Some(parent) = LocalPath::new(path).parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).map_err(|e| ElusionError::WriteError {
                    path: parent.display().to_string(),
                    operation: "create_directory".to_string(),
                    reason: e.to_string(),
                    suggestion: "💡 Check if you have permissions to create directories".to_string(),
                })?;
            }
        }

        if fs::metadata(path).is_ok() {
            fs::remove_file(path).or_else(|_| fs::remove_dir_all(path)).map_err(|e| 
                ElusionError::WriteError {
                    path: path.to_string(),
                    operation: "overwrite".to_string(),
                    reason: format!("❌ Failed to delete existing file: {}", e),
                    suggestion: "💡 Check file permissions and ensure no other process is using the file".to_string(),
                }
            )?;
        }

        let batches = self.df.clone().collect().await.map_err(|e| 
            ElusionError::InvalidOperation {
                operation: "Data Collection".to_string(),
                reason: format!("Failed to collect DataFrame: {}", e),
                suggestion: "💡 Verify DataFrame is not empty and contains valid data".to_string(),
            }
        )?;

        if batches.is_empty() {
            return Err(ElusionError::InvalidOperation {
                operation: "JSON Writing".to_string(),
                reason: "No data to write".to_string(),
                suggestion: "💡 Ensure DataFrame contains data before writing".to_string(),
            });
        }

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(|e| ElusionError::WriteError {
                path: path.to_string(),
                operation: "file_create".to_string(),
                reason: e.to_string(),
                suggestion: "💡 Check file permissions and path validity".to_string(),
            })?;

        let mut writer = BufWriter::new(file);
        
        // array opening bracket
        writeln!(writer, "[").map_err(|e| ElusionError::WriteError {
            path: path.to_string(),
            operation: "begin_json".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check disk space and write permissions".to_string(),
        })?;

        // Process each batch of records
        let mut first_row = true;
        let mut rows_written = 0;

        for batch in batches.iter() {
            let row_count = batch.num_rows();
            let column_count = batch.num_columns();
            
            // Skip empty batches
            if row_count == 0 || column_count == 0 {
                continue;
            }

            // Get column names
            let schema = batch.schema();
            let column_names: Vec<&str> = schema.fields().iter()
                .map(|f| f.name().as_str())
                .collect();

            // Process each row
            for row_idx in 0..row_count {
                if !first_row {
                    writeln!(writer, ",").map_err(|e| ElusionError::WriteError {
                        path: path.to_string(),
                        operation: "write_separator".to_string(),
                        reason: e.to_string(),
                        suggestion: "💡 Check disk space and write permissions".to_string(),
                    })?;
                }
                first_row = false;
                rows_written += 1;

                // Create a JSON object for the row
                let mut row_obj = serde_json::Map::new();
                
                // Add each column value to the row object
                for col_idx in 0..column_count {
                    let col_name = column_names[col_idx];
                    let array = batch.column(col_idx);
                    
                    // Convert arrow array value to serde_json::Value
                    let json_value = array_value_to_json(array, row_idx)?;
                    row_obj.insert(col_name.to_string(), json_value);
                }

                // Serialize the row to JSON
                let json_value = serde_json::Value::Object(row_obj);
                
                if pretty {
                    serde_json::to_writer_pretty(&mut writer, &json_value)
                        .map_err(|e| ElusionError::WriteError {
                            path: path.to_string(),
                            operation: format!("write_row_{}", rows_written),
                            reason: format!("JSON serialization error: {}", e),
                            suggestion: "💡 Check if row contains valid JSON data".to_string(),
                        })?;
                } else {
                    serde_json::to_writer(&mut writer, &json_value)
                        .map_err(|e| ElusionError::WriteError {
                            path: path.to_string(),
                            operation: format!("write_row_{}", rows_written),
                            reason: format!("JSON serialization error: {}", e),
                            suggestion: "💡 Check if row contains valid JSON data".to_string(),
                        })?;
                }
            }
        }

        // Write array closing bracket
        writeln!(writer, "\n]").map_err(|e| ElusionError::WriteError {
            path: path.to_string(),
            operation: "end_json".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check disk space and write permissions".to_string(),
        })?;

        // Ensure all data is written
        writer.flush().map_err(|e| ElusionError::WriteError {
            path: path.to_string(),
            operation: "flush".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Failed to flush data to file".to_string(),
        })?;

        println!("✅ Data successfully written to '{}'", path);
        
        if rows_written == 0 {
            println!("*** Warning ***: No rows were written to the file. Check if this is expected.");
        } else {
            println!("✅ Wrote {} rows to JSON file", rows_written);
        }

        Ok(())
    }


    /// Write the DataFrame to a Parquet file
    pub async fn write_to_parquet(
        &self,
        mode: &str,
        path: &str,
        options: Option<DataFrameWriteOptions>,
    ) -> ElusionResult<()> {

        if !path.ends_with(".parquet") {
            return Err(ElusionError::Custom(
                "❌ Invalid file extension. Parquet files must end with '.parquet'".to_string()
            ));
        }

        let write_options = options.unwrap_or_else(DataFrameWriteOptions::new);

        if let Some(parent) = LocalPath::new(path).parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).map_err(|e| ElusionError::WriteError {
                    path: parent.display().to_string(),
                    operation: "create_directory".to_string(),
                    reason: e.to_string(),
                    suggestion: "💡 Check if you have permissions to create directories".to_string(),
                })?;
            }
        }
        match mode {
            "overwrite" => {
                if fs::metadata(path).is_ok() {
                    fs::remove_file(path).or_else(|_| fs::remove_dir_all(path)).map_err(|e| {
                        ElusionError::WriteError {
                            path: path.to_string(),
                            operation: "overwrite".to_string(),
                            reason: format!("❌ Failed to delete existing file/directory: {}", e),
                            suggestion: "💡 Check file permissions and ensure no other process is using the file".to_string()
                        }
                    })?;
                }
                
                self.df.clone().write_parquet(path, write_options, None).await
                    .map_err(|e| ElusionError::WriteError {
                        path: path.to_string(),
                        operation: "overwrite".to_string(),
                        reason: e.to_string(),
                        suggestion: "💡 Check file permissions and path validity".to_string()
                    })?;
            }
        "append" => {
            let ctx = SessionContext::new();
            
            if !fs::metadata(path).is_ok() {
                self.df.clone().write_parquet(path, write_options, None).await
                    .map_err(|e| ElusionError::WriteError {
                        path: path.to_string(),
                        operation: "append".to_string(),
                        reason: format!("❌ Failed to create initial file: {}", e),
                        suggestion: "💡 Check directory permissions and path validity".to_string()
                    })?;
                return Ok(());
            }

            // Read existing parquet file
            let existing_df = ctx.read_parquet(path, ParquetReadOptions::default()).await
                .map_err(|e| ElusionError::WriteError {
                    path: path.to_string(),
                    operation: "read_existing".to_string(),
                    reason: e.to_string(),
                    suggestion: "💡 Verify the file is a valid Parquet file".to_string()
                })?;

            // Print schemas for debugging
            // println!("Existing schema: {:?}", existing_df.schema());
            // println!("New schema: {:?}", self.df.schema());
            
            // Print column names for both DataFrames
            // println!("Existing columns ({}): {:?}", 
            //     existing_df.schema().fields().len(),
            //     existing_df.schema().field_names());
            // println!("New columns ({}): {:?}", 
            //     self.df.schema().fields().len(),
            //     self.df.schema().field_names());

            // Register existing data with a table alias
            ctx.register_table("existing_data", Arc::new(
                MemTable::try_new(
                    existing_df.schema().clone().into(),
                    vec![existing_df.clone().collect().await.map_err(|e| ElusionError::WriteError {
                        path: path.to_string(),
                        operation: "collect_existing".to_string(),
                        reason: e.to_string(),
                        suggestion: "💡 Failed to collect existing data".to_string()
                    })?]
                ).map_err(|e| ElusionError::WriteError {
                    path: path.to_string(),
                    operation: "create_mem_table".to_string(),
                    reason: e.to_string(),
                    suggestion: "💡 Failed to create memory table".to_string()
                })?
            )).map_err(|e| ElusionError::WriteError {
                path: path.to_string(),
                operation: "register_existing".to_string(),
                reason: e.to_string(),
                suggestion: "💡 Failed to register existing data".to_string()
            })?;

            // new data with a table alias
            ctx.register_table("new_data", Arc::new(
                MemTable::try_new(
                    self.df.schema().clone().into(),
                    vec![self.df.clone().collect().await.map_err(|e| ElusionError::WriteError {
                        path: path.to_string(),
                        operation: "collect_new".to_string(),
                        reason: e.to_string(),
                        suggestion: "💡 Failed to collect new data".to_string()
                    })?]
                ).map_err(|e| ElusionError::WriteError {
                    path: path.to_string(),
                    operation: "create_mem_table".to_string(),
                    reason: e.to_string(),
                    suggestion: "💡 Failed to create memory table".to_string()
                })?
            )).map_err(|e| ElusionError::WriteError {
                path: path.to_string(),
                operation: "register_new".to_string(),
                reason: e.to_string(),
                suggestion: "💡 Failed to register new data".to_string()
            })?;

            // SQL with explicit column list
            let column_list = existing_df.schema()
                .fields()
                .iter()
                .map(|f| format!("\"{}\"", f.name()))  
                .collect::<Vec<_>>()
                .join(", ");

            //  UNION ALL with explicit columns
            let sql = format!(
                "SELECT {} FROM existing_data UNION ALL SELECT {} FROM new_data",
                column_list, column_list
            );
            // println!("Executing SQL: {}", sql);

            let combined_df = ctx.sql(&sql).await
                .map_err(|e| ElusionError::WriteError {
                    path: path.to_string(),
                    operation: "combine_data".to_string(),
                    reason: e.to_string(),
                    suggestion: "💡 Failed to combine existing and new data".to_string()
                })?;

                // temporary path for writing
                let temp_path = format!("{}.temp", path);

                // Write combined data to temporary file
                combined_df.write_parquet(&temp_path, write_options, None).await
                    .map_err(|e| ElusionError::WriteError {
                        path: temp_path.clone(),
                        operation: "write_combined".to_string(),
                        reason: e.to_string(),
                        suggestion: "💡 Failed to write combined data".to_string()
                    })?;

                // Remove original file
                fs::remove_file(path).map_err(|e| ElusionError::WriteError {
                    path: path.to_string(),
                    operation: "remove_original".to_string(),
                    reason: format!("❌ Failed to remove original file: {}", e),
                    suggestion: "💡 Check file permissions".to_string()
                })?;

                // Rename temporary file to original path
                fs::rename(&temp_path, path).map_err(|e| ElusionError::WriteError {
                    path: path.to_string(),
                    operation: "rename_temp".to_string(),
                    reason: format!("❌ Failed to rename temporary file: {}", e),
                    suggestion: "💡 Check file system permissions".to_string()
                })?;
            }
            _ => return Err(ElusionError::InvalidOperation {
                operation: mode.to_string(),
                reason: "Invalid write mode".to_string(),
                suggestion: "💡 Use 'overwrite' or 'append'".to_string()
            })
        }

        match mode {
            "overwrite" => println!("✅ Data successfully overwritten to '{}'", path),
            "append" => println!("✅ Data successfully appended to '{}'", path),
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

        if !path.ends_with(".csv") {
            return Err(ElusionError::Custom(
                "❌ Invalid file extension. CSV files must end with '.csv'".to_string()
            ));
        }

        csv_options.validate()?;
        
        if let Some(parent) = LocalPath::new(path).parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).map_err(|e| ElusionError::WriteError {
                    path: parent.display().to_string(),
                    operation: "create_directory".to_string(),
                    reason: e.to_string(),
                    suggestion: "💡 Check if you have permissions to create directories".to_string(),
                })?;
            }
        }

        match mode {
            "overwrite" => {
                // Remove existing file if it exists
                if fs::metadata(path).is_ok() {
                    fs::remove_file(path).or_else(|_| fs::remove_dir_all(path)).map_err(|e| 
                        ElusionError::WriteError {
                            path: path.to_string(),
                            operation: "overwrite".to_string(),
                            reason: format!("Failed to delete existing file: {}", e),
                            suggestion: "💡 Check file permissions and ensure no other process is using the file".to_string(),
                        }
                    )?;
                }

                let batches = self.df.clone().collect().await.map_err(|e| 
                    ElusionError::InvalidOperation {
                        operation: "Data Collection".to_string(),
                        reason: format!("Failed to collect DataFrame: {}", e),
                        suggestion: "💡 Verify DataFrame is not empty and contains valid data".to_string(),
                    }
                )?;

                if batches.is_empty() {
                    return Err(ElusionError::InvalidOperation {
                        operation: "CSV Writing".to_string(),
                        reason: "No data to write".to_string(),
                        suggestion: "💡 Ensure DataFrame contains data before writing".to_string(),
                    });
                }

                let file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(path)
                    .map_err(|e| ElusionError::WriteError {
                        path: path.to_string(),
                        operation: "file_create".to_string(),
                        reason: e.to_string(),
                        suggestion: "💡 Check file permissions and path validity".to_string(),
                    })?;

                let writer = BufWriter::new(file);
                let mut csv_writer = WriterBuilder::new()
                    .with_header(true)
                    .with_delimiter(csv_options.delimiter)
                    .with_escape(csv_options.escape)
                    .with_quote(csv_options.quote)
                    .with_double_quote(csv_options.double_quote)
                    .with_null(csv_options.null_value.clone())
                    .build(writer);

                for batch in batches.iter() {
                    csv_writer.write(batch).map_err(|e| ElusionError::WriteError {
                        path: path.to_string(),
                        operation: "write_data".to_string(),
                        reason: e.to_string(),
                        suggestion: "💡 Failed to write data batch".to_string(),
                    })?;
                }
                
                csv_writer.into_inner().flush().map_err(|e| ElusionError::WriteError {
                    path: path.to_string(),
                    operation: "flush".to_string(),
                    reason: e.to_string(),
                    suggestion: "💡 Failed to flush data to file".to_string(),
                })?;
            },
            "append" => {
                if !fs::metadata(path).is_ok() {
                    // If file doesn't exist in append mode, just write directly
                    let batches = self.df.clone().collect().await.map_err(|e| 
                        ElusionError::InvalidOperation {
                            operation: "Data Collection".to_string(),
                            reason: format!("Failed to collect DataFrame: {}", e),
                            suggestion: "💡 Verify DataFrame is not empty and contains valid data".to_string(),
                        }
                    )?;

                    if batches.is_empty() {
                        return Err(ElusionError::InvalidOperation {
                            operation: "CSV Writing".to_string(),
                            reason: "No data to write".to_string(),
                            suggestion: "💡 Ensure DataFrame contains data before writing".to_string(),
                        });
                    }

                    let file = OpenOptions::new()
                        .write(true)
                        .create(true)
                        .open(path)
                        .map_err(|e| ElusionError::WriteError {
                            path: path.to_string(),
                            operation: "file_create".to_string(),
                            reason: e.to_string(),
                            suggestion: "💡 Check file permissions and path validity".to_string(),
                        })?;

                    let writer = BufWriter::new(file);
                    let mut csv_writer = WriterBuilder::new()
                        .with_header(true)
                        .with_delimiter(csv_options.delimiter)
                        .with_escape(csv_options.escape)
                        .with_quote(csv_options.quote)
                        .with_double_quote(csv_options.double_quote)
                        .with_null(csv_options.null_value.clone())
                        .build(writer);

                    for batch in batches.iter() {
                        csv_writer.write(batch).map_err(|e| ElusionError::WriteError {
                            path: path.to_string(),
                            operation: "write_data".to_string(),
                            reason: e.to_string(),
                            suggestion: "💡 Failed to write data batch".to_string(),
                        })?;
                    }
                    csv_writer.into_inner().flush().map_err(|e| ElusionError::WriteError {
                        path: path.to_string(),
                        operation: "flush".to_string(),
                        reason: e.to_string(),
                        suggestion: "💡 Failed to flush data to file".to_string(),
                    })?;
                } else {
                    let ctx = SessionContext::new();
                    let existing_df = ctx.read_csv(
                        path,
                        CsvReadOptions::new()
                            .has_header(true)
                            .schema_infer_max_records(1000),
                    ).await?;

                    // Verify columns match before proceeding
                    let existing_cols: HashSet<_> = existing_df.schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().to_string())
                        .collect();
                    
                    let new_cols: HashSet<_> = self.df.schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().to_string())
                        .collect();

                    if existing_cols != new_cols {
                        return Err(ElusionError::WriteError {
                            path: path.to_string(),
                            operation: "column_check".to_string(),
                            reason: "Column mismatch between existing file and new data".to_string(),
                            suggestion: "💡 Ensure both datasets have the same columns".to_string()
                        });
                    }

                    ctx.register_table("existing_data", Arc::new(
                        MemTable::try_new(
                            existing_df.schema().clone().into(),
                            vec![existing_df.clone().collect().await.map_err(|e| ElusionError::WriteError {
                                path: path.to_string(),
                                operation: "collect_existing".to_string(),
                                reason: e.to_string(),
                                suggestion: "💡 Failed to collect existing data".to_string()
                            })?]
                        ).map_err(|e| ElusionError::WriteError {
                            path: path.to_string(),
                            operation: "create_mem_table".to_string(),
                            reason: e.to_string(),
                            suggestion: "💡 Failed to create memory table".to_string()
                        })?
                    )).map_err(|e| ElusionError::WriteError {
                        path: path.to_string(),
                        operation: "register_existing".to_string(),
                        reason: e.to_string(),
                        suggestion: "💡 Failed to register existing data".to_string()
                    })?;

                    ctx.register_table("new_data", Arc::new(
                        MemTable::try_new(
                            self.df.schema().clone().into(),
                            vec![self.df.clone().collect().await.map_err(|e| ElusionError::WriteError {
                                path: path.to_string(),
                                operation: "collect_new".to_string(),
                                reason: e.to_string(),
                                suggestion: "💡 Failed to collect new data".to_string()
                            })?]
                        ).map_err(|e| ElusionError::WriteError {
                            path: path.to_string(),
                            operation: "create_mem_table".to_string(),
                            reason: e.to_string(),
                            suggestion: "💡 Failed to create memory table".to_string()
                        })?
                    )).map_err(|e| ElusionError::WriteError {
                        path: path.to_string(),
                        operation: "register_new".to_string(),
                        reason: e.to_string(),
                        suggestion: "💡 Failed to register new data".to_string()
                    })?;

                    let column_list = existing_df.schema()
                        .fields()
                        .iter()
                        .map(|f| format!("\"{}\"", f.name()))  
                        .collect::<Vec<_>>()
                        .join(", ");

                    let sql = format!(
                        "SELECT {} FROM existing_data UNION ALL SELECT {} FROM new_data",
                        column_list, column_list
                    );

                    let combined_df = ctx.sql(&sql).await
                        .map_err(|e| ElusionError::WriteError {
                            path: path.to_string(),
                            operation: "combine_data".to_string(),
                            reason: e.to_string(),
                            suggestion: "💡 Failed to combine existing and new data".to_string()
                        })?;

                    let temp_path = format!("{}.temp", path);

                    // Clean up any existing temp file
                    if fs::metadata(&temp_path).is_ok() {
                        fs::remove_file(&temp_path).map_err(|e| ElusionError::WriteError {
                            path: temp_path.clone(),
                            operation: "cleanup_temp".to_string(),
                            reason: format!("Failed to delete temporary file: {}", e),
                            suggestion: "💡 Check file permissions and ensure no other process is using the file".to_string(),
                        })?;
                    }

                    let batches = combined_df.collect().await.map_err(|e| 
                        ElusionError::InvalidOperation {
                            operation: "Data Collection".to_string(),
                            reason: format!("Failed to collect DataFrame: {}", e),
                            suggestion: "💡 Verify DataFrame is not empty and contains valid data".to_string(),
                        }
                    )?;

                    if batches.is_empty() {
                        return Err(ElusionError::InvalidOperation {
                            operation: "CSV Writing".to_string(),
                            reason: "No data to write".to_string(),
                            suggestion: "💡 Ensure DataFrame contains data before writing".to_string(),
                        });
                    }

                    // Write to temporary file
                    {
                        let file = OpenOptions::new()
                            .write(true)
                            .create(true)
                            .truncate(true)
                            .open(&temp_path)
                            .map_err(|e| ElusionError::WriteError {
                                path: temp_path.clone(),
                                operation: "file_open".to_string(),
                                reason: e.to_string(),
                                suggestion: "💡 Check file permissions and path validity".to_string(),
                            })?;

                        let writer = BufWriter::new(file);
                        let mut csv_writer = WriterBuilder::new()
                            .with_header(true)
                            .with_delimiter(csv_options.delimiter)
                            .with_escape(csv_options.escape)
                            .with_quote(csv_options.quote)
                            .with_double_quote(csv_options.double_quote)
                            .with_null(csv_options.null_value.clone())
                            .build(writer);

                        for batch in batches.iter() {
                            csv_writer.write(batch).map_err(|e| ElusionError::WriteError {
                                path: temp_path.clone(),
                                operation: "write_data".to_string(),
                                reason: e.to_string(),
                                suggestion: "💡 Failed to write data batch".to_string(),
                            })?;
                        }

                        csv_writer.into_inner().flush().map_err(|e| ElusionError::WriteError {
                            path: temp_path.clone(),
                            operation: "flush".to_string(),
                            reason: e.to_string(),
                            suggestion: "💡 Check disk space and write permissions".to_string(),
                        })?;
                    } // Writer is dropped here

                    // Remove original file first if it exists
                    if fs::metadata(path).is_ok() {
                        fs::remove_file(path).map_err(|e| ElusionError::WriteError {
                            path: path.to_string(),
                            operation: "remove_original".to_string(),
                            reason: format!("Failed to remove original file: {}", e),
                            suggestion: "💡 Check file permissions".to_string()
                        })?;
                    }

                    // Now rename temp file to original path
                    fs::rename(&temp_path, path).map_err(|e| ElusionError::WriteError {
                        path: path.to_string(),
                        operation: "rename_temp".to_string(),
                        reason: format!("Failed to rename temporary file: {}", e),
                        suggestion: "💡 Check file system permissions".to_string()
                    })?;
                }
            },
            _ => return Err(ElusionError::InvalidOperation {
                operation: mode.to_string(),
                reason: "Invalid write mode".to_string(),
                suggestion: "💡 Use 'overwrite' or 'append'".to_string()
            })
        }

        match mode {
            "overwrite" => println!("✅ Data successfully overwritten to '{}'", path),
            "append" => println!("✅ Data successfully appended to '{}'", path),
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
                (true, WriteMode::Default)
            }
            "append" => {
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

    //=========== EXCEL WRITING ================================

    #[cfg(feature = "excel")]
    pub async fn write_to_excel(
        &self,
        path: &str,
        sheet_name: Option<&str>
    ) -> ElusionResult<()> {
        crate::features::excel::write_to_excel_impl(self, path, sheet_name).await
    }

    #[cfg(not(feature = "excel"))]
    pub async fn write_to_excel(
        &self,
        _path: &str,
        _sheet_name: Option<&str>
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom("*** Warning ***: Excel feature not enabled. Add feature excel under [dependencies]".to_string()))
    }
    // ============== AZURE WRITING ======================
    #[cfg(feature = "azure")]
    pub async fn write_parquet_to_azure_with_sas(
        &self,
        mode: &str,
        url: &str,
        sas_token: &str,
    ) -> ElusionResult<()> {
        crate::features::azure::write_parquet_to_azure_with_sas_impl(self, mode, url, sas_token).await
    }

    #[cfg(not(feature = "azure"))]
    pub async fn write_parquet_to_azure_with_sas(
        &self,
        _mode: &str,
        _url: &str,
        _sas_token: &str,
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom("*** Warning ***: Azure feature not enabled. Add feature under [dependencies]".to_string()))
    }

    #[cfg(feature = "azure")]
    pub async fn write_json_to_azure_with_sas(
        &self,
        url: &str,
        sas_token: &str,
        pretty: bool,
    ) -> ElusionResult<()> {
        crate::features::azure::write_json_to_azure_with_sas_impl(self, url, sas_token, pretty).await
    }

    #[cfg(not(feature = "azure"))]
    pub async fn write_json_to_azure_with_sas(
        &self,
        _url: &str,
        _sas_token: &str,
        _pretty: bool,
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom("*** Warning ***: Azure feature not enabled. Add feature under [dependencies]".to_string()))
    }

     // ============== AZURE READING ======================
    #[cfg(feature = "azure")]
    pub async fn from_azure_with_sas_token(
        url: &str,
        sas_token: &str,
        filter_keyword: Option<&str>, 
        alias: &str,
    ) -> ElusionResult<Self> {
        crate::features::azure::from_azure_with_sas_token_impl(url, sas_token, filter_keyword, alias).await
    }

    #[cfg(not(feature = "azure"))]
    pub async fn from_azure_with_sas_token(
        _url: &str,
        _sas_token: &str,
        _filter_keyword: Option<&str>, 
        _alias: &str,
    ) -> ElusionResult<Self> {
        Err(ElusionError::Custom("*** Warning ***: Azure feature not enabled. Add feature under [dependencies]".to_string()))
    }

    // ========= FABRIC - ONE LAKE ================

    #[cfg(feature = "fabric")]
    pub async fn from_fabric(
        abfss_path: &str,
        file_path: &str,
        alias: &str,
    ) -> ElusionResult<CustomDataFrame> {
        crate::features::fabric::load_from_fabric_abfss_impl(
            abfss_path,
            file_path,
            alias,
        ).await
    }

    #[cfg(not(feature = "fabric"))]
    pub async fn from_fabric(
        _abfss_path: &str,
        _file_path: &str,
        _alias: &str,
    ) -> ElusionResult<CustomDataFrame> {
        Err(ElusionError::Custom("*** Warning ***: fabric feature not enabled. Add 'fabric' feature under [dependencies]".to_string()))
    }

    #[cfg(feature = "fabric")]
    pub async fn from_fabric_with_service_principal(
        tenant_id: &str,
        client_id: &str,
        client_secret: &str,
        abfss_path: &str,
        file_path: &str,
        alias: &str,
    ) -> ElusionResult<CustomDataFrame> {
        crate::features::fabric::load_from_fabric_abfss_with_service_principal_impl(
            tenant_id,
            client_id,
            client_secret,
            abfss_path,
            file_path,
            alias,
        ).await
    }

    #[cfg(not(feature = "fabric"))]
    pub async fn from_fabric_with_service_principal(
        _tenant_id: &str,
        _client_id: &str,
        _client_secret: &str,
        _abfss_path: &str,
        _file_path: &str,
        _alias: &str,
    ) -> ElusionResult<CustomDataFrame> {
        Err(ElusionError::Custom("*** Warning ***: fabric feature not enabled. Add 'fabric' feature under [dependencies]".to_string()))
    }

    // Write Parquet to fabric using ABFSS path
    #[cfg(feature = "fabric")]
    pub async fn write_parquet_to_fabric(
        &self,
        abfss_path: &str,
        file_path: &str,
    ) -> ElusionResult<()> {
        crate::features::fabric::write_parquet_to_fabric_abfss_impl(
            self,
            abfss_path,
            file_path,
        ).await
    }

    #[cfg(not(feature = "fabric"))]
    pub async fn write_parquet_to_fabric(
        &self,
        _abfss_path: &str,
        _file_path: &str,
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom("*** Warning ***: fabric feature not enabled. Add 'fabric' feature under [dependencies]".to_string()))
    }

    #[cfg(feature = "fabric")]
    pub async fn write_parquet_to_fabric_with_service_principal(
        &self,
        tenant_id: &str,
        client_id: &str,
        client_secret: &str,
        abfss_path: &str,
        file_path: &str,
    ) -> ElusionResult<()> {
        crate::features::fabric::write_parquet_to_fabric_abfss_with_service_principal_impl(
            self,
            tenant_id,
            client_id,
            client_secret,
            abfss_path,
            file_path,
        ).await
    }

    #[cfg(not(feature = "fabric"))]
    pub async fn write_parquet_to_fabric_with_service_principal(
        &self,
        _tenant_id: &str,
        _client_id: &str,
        _client_secret: &str,
        _abfss_path: &str,
        _file_path: &str,
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom("*** Warning ***: fabric feature not enabled. Add 'fabric' feature under [dependencies]".to_string()))
    }

    // ================== FTP COnnector ========================== //
    #[cfg(feature = "ftp")]
    pub async fn from_ftp(
        server: &str,
        username: &str,
        password: &str,
        remote_path: &str,
        alias: &str
    ) -> ElusionResult<Self> {
        crate::features::ftp::from_ftp_impl(
            server,
            username,
            password,
            remote_path,
            alias
        ).await
    }
    
    #[cfg(not(feature = "ftp"))]
    pub async fn from_ftp(
        _server: &str,
        _username: &str,
        _password: &str,
        _remote_path: &str,
        _alias: &str
    ) -> ElusionResult<Self> {
        Err(ElusionError::Custom("*** Warning ***: ftp feature not enabled. Add 'ftp' feature under [dependencies]".to_string()))
    }

    #[cfg(feature = "ftp")]
    pub async fn from_ftps(
        server: &str,
        username: &str,
        password: &str,
        remote_path: &str,
        alias: &str
    ) -> ElusionResult<Self> {
        crate::features::ftp::from_ftps_impl(
            server,
            username,
            password,
            remote_path,
            alias
        ).await
    }
    
    #[cfg(not(feature = "ftp"))]
    pub async fn from_ftps(
        _server: &str,
        _username: &str,
        _password: &str,
        _remote_path: &str,
        _alias: &str
    ) -> ElusionResult<Self> {
        Err(ElusionError::Custom("*** Warning ***: ftp feature not enabled. Add 'ftp' feature under [dependencies]".to_string()))
    }

    #[cfg(feature = "ftp")]
    pub async fn from_ftp_with_port(
        server: &str,
        port: u16,
        username: &str,
        password: &str,
        remote_path: &str,
        alias: &str
    ) -> ElusionResult<Self> {
        crate::features::ftp::from_ftp_with_port_impl(
            server,
            port,
            username,
            password,
            remote_path,
            alias
        ).await
    }
    
    #[cfg(not(feature = "ftp"))]
    pub async fn from_ftp_with_port(
        _server: &str,
        _port: u16,
        _username: &str,
        _password: &str,
        _remote_path: &str,
        _alias: &str
    ) -> ElusionResult<Self> {
        Err(ElusionError::Custom("*** Warning ***: ftp feature not enabled. Add 'ftp' feature under [dependencies]".to_string()))
    }

    #[cfg(feature = "ftp")]
    pub async fn from_ftp_with_directory(
        server: &str,
        username: &str,
        password: &str,
        directory: &str,
        remote_path: &str,
        alias: &str
    ) -> ElusionResult<Self> {
        crate::features::ftp::from_ftp_with_directory_impl(
            server,
            username,
            password,
            directory,
            remote_path,
            alias
        ).await
    }
    
    #[cfg(not(feature = "ftp"))]
    pub async fn from_ftp_with_directory(
        _server: &str,
        _username: &str,
        _password: &str,
        _directory: &str,
        _remote_path: &str,
        _alias: &str
    ) -> ElusionResult<Self> {
        Err(ElusionError::Custom("*** Warning ***: ftp feature not enabled. Add 'ftp' feature under [dependencies]".to_string()))
    }

    /// Load all files from an FTP folder and union them if they have compatible schemas
    /// Supports CSV, Excel, JSON, Parquet, and XML files
    #[cfg(feature = "ftp")]
    pub async fn from_ftp_folder(
        server: &str,
        username: &str,
        password: &str,
        port: Option<u16>,
        folder_path: &str,
        file_extensions: Option<Vec<&str>>,
        result_alias: &str,
    ) -> ElusionResult<Self> {
        crate::features::ftp::from_ftp_folder_impl(
            server,
            username,
            password,
            port,
            false,
            folder_path,
            file_extensions,
            result_alias
        ).await
    }
    
    #[cfg(not(feature = "ftp"))]
    pub async fn from_ftp_folder(
        _server: &str,
        _username: &str,
        _password: &str,
        _port: Option<u16>,
        _folder_path: &str,
        _file_extensions: Option<Vec<&str>>,
        _result_alias: &str,
    ) -> ElusionResult<Self> {
        Err(ElusionError::Custom("*** Warning ***: ftp feature not enabled. Add 'ftp' feature under [dependencies]".to_string()))
    }

    /// Load all files from an FTPS folder and union them if they have compatible schemas
    /// Supports CSV, Excel, JSON, Parquet, and XML files
    #[cfg(feature = "ftp")]
    pub async fn from_ftps_folder(
        server: &str,
        username: &str,
        password: &str,
        port: Option<u16>,
        folder_path: &str,
        file_extensions: Option<Vec<&str>>,
        result_alias: &str,
    ) -> ElusionResult<Self> {
        crate::features::ftp::from_ftp_folder_impl(
            server,
            username,
            password,
            port,
            true,
            folder_path,
            file_extensions,
            result_alias
        ).await
    }
    
    #[cfg(not(feature = "ftp"))]
    pub async fn from_ftps_folder(
        _server: &str,
        _username: &str,
        _password: &str,
        _port: Option<u16>,
        _folder_path: &str,
        _file_extensions: Option<Vec<&str>>,
        _result_alias: &str,
    ) -> ElusionResult<Self> {
        Err(ElusionError::Custom("*** Warning ***: ftp feature not enabled. Add 'ftp' feature under [dependencies]".to_string()))
    }

    /// Load all files from an FTPS folder and union them if they have compatible schemas and andds column with file names
    /// Supports CSV, Excel, JSON, Parquet, and XML files
    #[cfg(feature = "ftp")]
    pub async fn from_ftp_folder_with_filename_column(
        server: &str,
        username: &str,
        password: &str,
        port: Option<u16>,
        folder_path: &str,
        file_extensions: Option<Vec<&str>>,
        result_alias: &str,
    ) -> ElusionResult<Self> {
        crate::features::ftp::from_ftp_folder_with_filename_column_impl(
            server,
            username,
            password,
            port,
            false,
            folder_path,
            file_extensions,
            result_alias
        ).await
    }
    
    #[cfg(not(feature = "ftp"))]
    pub async fn from_ftp_folder_with_filename_column(
        _server: &str,
        _username: &str,
        _password: &str,
        _port: Option<u16>,
        _folder_path: &str,
        _file_extensions: Option<Vec<&str>>,
        _result_alias: &str,
    ) -> ElusionResult<Self> {
        Err(ElusionError::Custom("*** Warning ***: ftp feature not enabled. Add 'ftp' feature under [dependencies]".to_string()))
    }

    /// Load all files from an FTPS folder and union them if they have compatible schemas and andds column with file names
    /// Supports CSV, Excel, JSON, Parquet, and XML files
    #[cfg(feature = "ftp")]
    pub async fn from_ftps_folder_with_filename_column(
        server: &str,
        username: &str,
        password: &str,
        port: Option<u16>,
        folder_path: &str,
        file_extensions: Option<Vec<&str>>,
        result_alias: &str,
    ) -> ElusionResult<Self> {
        crate::features::ftp::from_ftp_folder_with_filename_column_impl(
            server,
            username,
            password,
            port,
            true,
            folder_path,
            file_extensions,
            result_alias
        ).await
    }
    
    #[cfg(not(feature = "ftp"))]
    pub async fn from_ftps_folder_with_filename_column(
        _server: &str,
        _username: &str,
        _password: &str,
        _port: Option<u16>,
        _folder_path: &str,
        _file_extensions: Option<Vec<&str>>,
        _result_alias: &str,
    ) -> ElusionResult<Self> {
        Err(ElusionError::Custom("*** Warning ***: ftp feature not enabled. Add 'ftp' feature under [dependencies]".to_string()))
    }

    /// Write DataFrame result to FTP server as CSV
    #[cfg(feature = "ftp")]
    pub async fn write_csv_to_ftp(
        &self,
        server: &str,
        username: &str,
        password: &str,
        remote_path: &str
    ) -> ElusionResult<()> {
        crate::features::ftp::write_csv_to_ftp_impl(
            self,
            server,
            username,
            password,
            remote_path
        ).await
    }
    
    #[cfg(not(feature = "ftp"))]
    pub async fn write_csv_to_ftp(
        &self,
        _server: &str,
        _username: &str,
        _password: &str,
        _remote_path: &str
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom("*** Warning ***: ftp feature not enabled. Add 'ftp' feature under [dependencies]".to_string()))
    }

    /// Write DataFrame result to FTP server as EXCEL
    #[cfg(feature = "ftp")]
    pub async fn write_excel_to_ftp(
        &self,
        server: &str,
        username: &str,
        password: &str,
        remote_path: &str,
        sheet_name: Option<&str>
    ) -> ElusionResult<()> {
        crate::features::ftp::write_excel_to_ftp_impl(
            self,
            server,
            username,
            password,
            remote_path,
            sheet_name
        ).await
    }
    
    #[cfg(not(feature = "ftp"))]
    pub async fn write_excel_to_ftp(
        &self,
        _server: &str,
        _username: &str,
        _password: &str,
        _remote_path: &str,
        _sheet_name: Option<&str>
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom("*** Warning ***: ftp feature not enabled. Add 'ftp' feature under [dependencies]".to_string()))
    }

    /// Write DataFrame result to FTP server as Parquet
    #[cfg(feature = "ftp")]
    pub async fn write_parquet_to_ftp(
        &self,
        server: &str,
        username: &str,
        password: &str,
        remote_path: &str
    ) -> ElusionResult<()> {
        crate::features::ftp::write_parquet_to_ftp_impl(
            self,
            server,
            username,
            password,
            remote_path
        ).await
    }
    
    #[cfg(not(feature = "ftp"))]
    pub async fn write_parquet_to_ftp(
        &self,
        _server: &str,
        _username: &str,
        _password: &str,
        _remote_path: &str
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom("*** Warning ***: ftp feature not enabled. Add 'ftp' feature under [dependencies]".to_string()))
    }

    /// Write DataFrame result to FTP server as JSON
    #[cfg(feature = "ftp")]
    pub async fn write_json_to_ftp(
        &self,
        server: &str,
        username: &str,
        password: &str,
        remote_path: &str,
        pretty: bool
    ) -> ElusionResult<()> {
        crate::features::ftp::write_json_to_ftp_impl(
            self,
            server,
            username,
            password,
            remote_path,
            pretty
        ).await
    }
    
    #[cfg(not(feature = "ftp"))]
    pub async fn write_json_to_ftp(
        &self,
        _server: &str,
        _username: &str,
        _password: &str,
        _remote_path: &str,
        _pretty: bool
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom("*** Warning ***: ftp feature not enabled. Add 'ftp' feature under [dependencies]".to_string()))
    }

    //=================== LOCAL LOADERS ============================= //
   
    /// LOAD function for XML files
    pub fn load_xml<'a>(file_path: &'a str, alias: &'a str) -> BoxFuture<'a, ElusionResult<AliasedDataFrame>> {
        Box::pin(async move {
            load_xml_with_mode(file_path, alias, XmlProcessingMode::Auto).await
        })
    }
    /// LOAD function for CSV file type
    pub async fn load_csv(file_path: &str, alias: &str) -> ElusionResult<AliasedDataFrame> {
        load_csv_with_type_handling(file_path, alias).await
    }
        

    /// LOAD function for Parquet file type
    pub fn load_parquet<'a>(
        file_path: &'a str,
        alias: &'a str,
    ) -> BoxFuture<'a, ElusionResult<AliasedDataFrame>> {
        Box::pin(async move {
            let ctx = SessionContext::new();

            if !LocalPath::new(file_path).exists() {
                return Err(ElusionError::WriteError {
                    path: file_path.to_string(),
                    operation: "read".to_string(),
                    reason: "File not found".to_string(),
                    suggestion: "💡 Check if the file path is correct".to_string(),
                });
            }

             println!("🔄 Starting Parquet loading process...");

             if let Ok(metadata) = std::fs::metadata(file_path) {
                let file_size = metadata.len();
                println!("📏 Parquet file size: {} bytes ({:.2} MB)", 
                    file_size, file_size as f64 / 1024.0 / 1024.0);
            }

             let read_start = std::time::Instant::now();
             let df = match ctx.read_parquet(file_path, ParquetReadOptions::default()).await {
                Ok(df) => {
                    let read_elapsed = read_start.elapsed();
                    let schema = df.schema();
                    let column_count = schema.fields().len();
                    
                    println!("✅ Parquet file read successfully in {:?}", read_elapsed);
                    println!("📊 Schema detected: {} columns with native types", column_count);
                    
                    // Show column types (first 8 columns to avoid overwhelming output)
                    // let column_info: Vec<String> = schema.fields().iter()
                    //     .take(8)
                    //     .map(|field| format!("{}: {}", field.name(), field.data_type()))
                    //     .collect();
                    
                    // println!("📋 Column types: [{}{}]", 
                    //     column_info.join(", "),
                    //     if schema.fields().len() > 8 { 
                    //         format!(" ... +{} more", schema.fields().len() - 8) 
                    //     } else { 
                    //         String::new() 
                    //     });
                    
                    df
                }
                Err(err) => {
                    let read_elapsed = read_start.elapsed();
                    println!("❌ Failed to read Parquet file after {:?}: {}", read_elapsed, err);
                    return Err(ElusionError::DataFusion(err));
                }
            };
            
            let batches = df.clone().collect().await.map_err(ElusionError::DataFusion)?;
            let schema = df.schema().clone();
            let mem_table = MemTable::try_new(schema.clone().into(), vec![batches])
                .map_err(|e| ElusionError::SchemaError {
                    message: e.to_string(),
                    schema: Some(schema.to_string()),
                    suggestion: "💡 Check if the parquet file schema is valid".to_string(),
                })?;

            let normalized_alias = normalize_alias_write(alias).into_owned();

            ctx.register_table(&normalized_alias, Arc::new(mem_table))
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: "Table Registration".to_string(),
                    reason: e.to_string(),
                    suggestion: "💡 Try using a different alias name".to_string(),
                })?;

            let aliased_df = ctx.table(alias).await
                .map_err(|_| ElusionError::InvalidOperation {
                    operation: "Table Creation".to_string(),
                    reason: format!("Failed to create table with alias '{}'", alias),
                    suggestion: "💡 Check if the alias is valid and unique".to_string(),
                })?;

            let total_elapsed = read_start.elapsed();
                println!("🎉 Parquet DataFrame loading completed successfully in {:?} for table alias: '{}'", 
            total_elapsed, alias);

            Ok(AliasedDataFrame {
                dataframe: aliased_df,
                alias: alias.to_string(),
            })
        })
    }

    pub fn load_json<'a>(
        file_path: &'a str,
        alias: &'a str,
    ) -> BoxFuture<'a, ElusionResult<AliasedDataFrame>> {
        Box::pin(async move {
            println!("🔄 Processing JSON records...");

            let file = File::open(file_path).map_err(|e| ElusionError::WriteError {
                path: file_path.to_string(),
                operation: "read".to_string(),
                reason: e.to_string(),
                suggestion: "💡 Check if the file exists and you have proper permissions".to_string(),
            })?;
            
            let file_size = file.metadata().map_err(|e| ElusionError::WriteError {
                path: file_path.to_string(),
                operation: "metadata reading".to_string(),
                reason: e.to_string(),
                suggestion: "💡 Check file permissions and disk status".to_string(),
            })?.len();
            
            println!("📏 File size: {} bytes ({:.2} MB)", file_size, file_size as f64 / 1024.0 / 1024.0);
            // Larger buffer for big files
            let reader = BufReader::with_capacity(128 * 1024, file); 
            let stream = Deserializer::from_reader(reader).into_iter::<Value>();
            
            let mut all_data: Vec<HashMap<String, Value>> = Vec::new();
            let mut processed_count = 0;
            let start_time = std::time::Instant::now();
            
            
            for (index, value) in stream.enumerate() {
            
                if index % 500 == 0 && index > 0 {
                    let elapsed = start_time.elapsed();
                    let rate = index as f64 / elapsed.as_secs_f64();
                    println!("Processed {} records in {:?} ({:.1} records/sec)", index, elapsed, rate);
                }
                
                match value {
                    Ok(json_value) => {
                        match json_value {
                            Value::Object(map) => {
                                // Handle single JSON object (your case is unlikely this)
                                let hash_map: HashMap<String, Value> = map.into_iter().collect();
                                all_data.push(hash_map);
                                processed_count += 1;
                            },
                            Value::Array(array) => {
                                // Handle large JSON array (THIS IS YOUR CASE!)
                                let array_size = array.len();
                            //  println!("Processing large array with {} items", array_size);
                                
                                // Reserve capacity to prevent reallocations
                                all_data.reserve(array_size);
                                
                                // memory management
                                let batch_size = 1000;
                                for (batch_start, batch) in array.chunks(batch_size).enumerate() {
                                    if batch_start % 10 == 0 && batch_start > 0 {
                                        let items_processed = batch_start * batch_size;
                                        let progress = (items_processed as f64 / array_size as f64) * 100.0;
                                        println!("📦 Array progress: {}/{} items ({:.1}%)", 
                                            items_processed, array_size, progress);
                                    }
                                    
                                    for item in batch {
                                        if let Value::Object(map) = item {
                                            //  Map to HashMap 
                                            let hash_map: HashMap<String, Value> = map.clone().into_iter().collect();
                                            all_data.push(hash_map);
                                            processed_count += 1;
                                        }
                                    }
                                }
                                
                                println!("✅ Completed processing array with {} items", array_size);
                            },
                            _ => {
                                println!("⚠️  Skipping non-object/non-array JSON value at index {}", index);
                                continue;
                            }
                        }
                    },
                    Err(e) => {
                        if e.is_eof() {
                            println!("📄 Reached end of JSON file at record {}", index);
                            break;
                        } else {
                            println!("❌ JSON parsing error at record {}: {}", index, e);
                            continue;
                        }
                    }
                }
            }
            
            let total_elapsed = start_time.elapsed();
            println!("✅ JSON parsing completed: {} records in {:?}", processed_count, total_elapsed);
            
            if all_data.is_empty() {
                return Err(ElusionError::InvalidOperation {
                    operation: "JSON processing".to_string(),
                    reason: "No valid JSON data found".to_string(),
                    suggestion: "💡 Check if the JSON file contains valid object data".to_string(),
                });
            }
            
        //  println!("🔧 Inferring schema from {} records...", all_data.len());
        //  let schema_start = std::time::Instant::now();
            let schema = infer_schema_from_json(&all_data);
        //   let schema_elapsed = schema_start.elapsed();
        //  println!(" Schema inferred with {} fields in {:?}", schema.fields().len(), schema_elapsed);
            
            println!("🔧 Building record batch...");
            let batch_start = std::time::Instant::now();
            let record_batch = build_record_batch(&all_data, schema.clone())
                .map_err(|e| ElusionError::SchemaError {
                    message: format!("Failed to build RecordBatch: {}", e),
                    schema: Some(schema.to_string()),
                    suggestion: "💡 Check if the JSON data structure is consistent".to_string(),
                })?;
            let batch_elapsed = batch_start.elapsed();
            println!("📊 Record batch created with {} rows in {:?}", record_batch.num_rows(), batch_elapsed);
            
            let ctx = SessionContext::new();
            let mem_table = MemTable::try_new(schema.clone(), vec![vec![record_batch]])
                .map_err(|e| ElusionError::SchemaError {
                    message: format!("Failed to create MemTable: {}", e),
                    schema: Some(schema.to_string()),
                    suggestion: "💡 Verify data types and schema compatibility".to_string(),
                })?;
            
            ctx.register_table(alias, Arc::new(mem_table))
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: "Table registration".to_string(),
                    reason: format!("Failed to register table: {}", e),
                    suggestion: "💡 Try using a different alias or check table compatibility".to_string(),
                })?;
            
            let df = ctx.table(alias).await.map_err(|e| ElusionError::InvalidOperation {
                operation: "Table creation".to_string(),
                reason: format!("Failed to create table: {}", e),
                suggestion: "💡 Verify table creation parameters and permissions".to_string(),
            })?;
            
            let total_time = start_time.elapsed();
            println!("🎉 JSON DataFrame loading completed successfully in {:?} for table alias: {}", total_time, alias);
            
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
    ) -> BoxFuture<'a, ElusionResult<AliasedDataFrame>> {
        Box::pin(async move {
            let ctx = SessionContext::new();

            // path manager
            let path_manager = DeltaPathManager::new(file_path);

            println!("🔄 Opening Delta table and reading metadata...");
            let table_start = std::time::Instant::now();
            // Open Delta table using path manager
            let table = open_table(&path_manager.table_path())
            .await
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Delta Table Opening".to_string(),
                reason: e.to_string(),
                suggestion: "💡 Ensure the path points to a valid Delta table".to_string(),
            })?;

            let table_elapsed = table_start.elapsed();
            println!("✅ Delta table opened successfully in {:?}", table_elapsed);
 
            let version = table.version();
            println!("📊 Delta table version: {}", version);

            println!("🔍 Discovering Delta table files...");
            let files_start = std::time::Instant::now();
            
            let file_paths: Vec<String> = {
                let raw_uris = table.get_file_uris()
                    .map_err(|e| ElusionError::InvalidOperation {
                        operation: "Delta File Listing".to_string(),
                        reason: e.to_string(),
                        suggestion: "💡 Check Delta table permissions and integrity".to_string(),
                    })?;
                
                raw_uris.map(|uri| path_manager.normalize_uri(&uri))
                    .collect()
            };

            let files_elapsed = files_start.elapsed();
            let file_count = file_paths.len();
            println!("📁 Found {} Delta files in {:?}", file_count, files_elapsed);
            
            println!("🔄 Reading Delta data files as Parquet...");
            let read_start = std::time::Instant::now();
            // ParquetReadOptions
            let parquet_options = ParquetReadOptions::new()
                // .schema(&combined_schema)
                // .table_partition_cols(partition_columns.clone())
                .parquet_pruning(false)
                .skip_metadata(false);

           

            let df = ctx.read_parquet(file_paths, parquet_options).await?;

            let read_elapsed = read_start.elapsed();
            println!("✅ Delta data read successfully in {:?}", read_elapsed);

            println!("🔄 Loading data into memory and building table...");
            let collect_start = std::time::Instant::now();

            let batches = df.clone().collect().await?;
            // println!("Number of batches: {}", batches.len());
            // for (i, batch) in batches.iter().enumerate() {
            //     println!("Batch {} row count: {}", i, batch.num_rows());
            // }
            let schema = df.schema().clone().into();
            // Build M  emTable
            let mem_table = MemTable::try_new(schema, vec![batches])?;

            let collect_elapsed = collect_start.elapsed();
            println!("✅ Memory table created in {:?}", collect_elapsed);

            let normalized_alias = normalize_alias_write(alias).into_owned();

            ctx.register_table(&normalized_alias, Arc::new(mem_table))
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: "Table Registration".to_string(),
                    reason: e.to_string(),
                    suggestion: "💡 Try using a different alias name".to_string(),
                })?;

            let aliased_df = ctx.table(&normalized_alias).await
                .map_err(|_| ElusionError::InvalidOperation {
                    operation: "Table Creation".to_string(),
                    reason: format!("Failed to create table with alias '{}'", alias),
                    suggestion: "💡 Check if the alias is valid and unique".to_string(),
                })?;

            let total_elapsed = table_start.elapsed();
            
            println!("🎉 Delta table loading completed successfully in {:?} for table alias: '{}'", 
                total_elapsed, alias);

            Ok(AliasedDataFrame {
                dataframe: aliased_df,
                alias: alias.to_string(),
            })
        })
    }
   

    // ============== LOADING LOCAL FILES ===========================

    /// Unified load function that determines the file type based on extension
    pub async fn load(
        file_path: &str,
        alias: &str,
    ) -> ElusionResult<AliasedDataFrame> {
        let path_manager = DeltaPathManager::new(file_path);
        if path_manager.is_delta_table() {
            let aliased_df = Self::load_delta(file_path, alias).await?;
            // Apply lowercase transformation
            let df_lower = lowercase_column_names(aliased_df.dataframe).await?;
            return Ok(AliasedDataFrame {
                dataframe: df_lower,
                alias: alias.to_string(),
            });
        }

        let ext = file_path
            .split('.')
            .last()
            .unwrap_or_default()
            .to_lowercase();

        let aliased_df = match ext.as_str() {
            "csv" => Self::load_csv(file_path, alias).await?,
            "json" => Self::load_json(file_path, alias).await?,
            "parquet" => Self::load_parquet(file_path, alias).await?,
            "xml" => Self::load_xml(file_path, alias).await?,
            "xlsx" | "xls" => crate::features::excel::load_excel(file_path, alias).await?,
            "" => return Err(ElusionError::InvalidOperation {
                operation: "File Loading".to_string(),
                reason: format!("Directory is not a Delta table and has no recognized extension: {file_path}"),
                suggestion: "💡 Provide a file with a supported extension (.csv, .json, .parquet, .xlsx, .xls, .xml) or a valid Delta table directory".to_string(),
            }),
            other => return Err(ElusionError::InvalidOperation {
                operation: "File Loading".to_string(),
                reason: format!("Unsupported file extension: {other}"),
                suggestion: "💡 Use one of the supported file types: .csv, .json, .parquet, .xlsx, .xls, .xml or Delta table".to_string(),
            }),
        };

        let df_lower = lowercase_column_names(aliased_df.dataframe).await?;
        Ok(AliasedDataFrame {
            dataframe: df_lower,
            alias: alias.to_string(),
        })
    }

    // ==================== LOADING LOCAL FILES FROM FOLDERS ===============================
    /// Load all files from a local folder and union them if they have compatible schemas
    /// Supports CSV, Excel, JSON, XML and Parquet files
    pub async fn load_folder(
        folder_path: &str,
        file_extensions: Option<Vec<&str>>, 
        result_alias: &str,
    ) -> ElusionResult<Self> {
        use std::fs;
        use std::path::Path;
        
        let folder_path_obj = Path::new(folder_path);
        if !folder_path_obj.exists() {
            return Err(ElusionError::WriteError {
                path: folder_path.to_string(),
                operation: "read".to_string(),
                reason: "Folder not found".to_string(),
                suggestion: "💡 Check if the folder path is correct".to_string(),
            });
        }
        
        if !folder_path_obj.is_dir() {
            return Err(ElusionError::InvalidOperation {
                operation: "Local Folder Loading".to_string(),
                reason: "Path is not a directory".to_string(),
                suggestion: "💡 Provide a valid directory path".to_string(),
            });
        }
        
        let entries = fs::read_dir(folder_path)
            .map_err(|e| ElusionError::WriteError {
                path: folder_path.to_string(),
                operation: "read".to_string(),
                reason: format!("Failed to read directory: {}", e),
                suggestion: "💡 Check directory permissions".to_string(),
            })?;
        
        let mut dataframes = Vec::new();
        
        for entry in entries {
            let entry = entry.map_err(|e| ElusionError::WriteError {
                path: folder_path.to_string(),
                operation: "read".to_string(),
                reason: format!("Failed to read directory entry: {}", e),
                suggestion: "💡 Check directory permissions".to_string(),
            })?;
            
            let file_path = entry.path();

            if !file_path.is_file() {
                continue;
            }
            
            let file_name = file_path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("")
                .to_string();

            if file_name.starts_with('.') {
                continue;
            }
            
            // Skip if file extensions filter is specified and file doesn't match
            if let Some(ref extensions) = file_extensions {
                let file_ext = file_name
                    .split('.')
                    .last()
                    .unwrap_or("")
                    .to_lowercase();
                
                if !extensions.iter().any(|ext| ext.to_lowercase() == file_ext) {
                    continue;
                }
            }
            
            let file_path_str = file_path.to_str().ok_or_else(|| ElusionError::InvalidOperation {
                operation: "Local Folder Loading".to_string(),
                reason: format!("Invalid file path: {:?}", file_path),
                suggestion: "💡 Ensure file paths contain valid UTF-8 characters".to_string(),
            })?;
            
            match file_name.split('.').last().unwrap_or("").to_lowercase().as_str() {
                "csv" => {
                    let file_size = std::fs::metadata(file_path_str)
                        .map(|m| m.len())
                        .unwrap_or(0);
                        
                    if file_size > 500_000_000 { 
                        println!("📊 Large CSV detected ({} bytes), using streaming loader", file_size);
                        
                        match load_csv_smart(file_path_str, "local_data").await {
                            Ok(aliased_df) => {
                                println!("✅ Loaded large CSV: {}", file_name);
                                
                                // Normalize column names to lowercase and create CustomDataFrame
                                let normalized_df = lowercase_column_names(aliased_df.dataframe).await?;
                                
                                let df = CustomDataFrame {
                                    df: normalized_df,
                                    table_alias: "local_csv".to_string(),
                                    from_table: "local_csv".to_string(),
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
                                };
                                dataframes.push(df); 
                            },
                            Err(e) => {
                                eprintln!("⚠️ Failed to load large CSV file {}: {}", file_name, e);
                                continue;
                            }
                        }
                    } else {
                        // Use regular loader for smaller files
                        match Self::load_csv(file_path_str, "local_data").await {
                            Ok(aliased_df) => {
                                println!("✅ Loaded CSV: {}", file_name);
                                
                                // Normalize column names to lowercase and create CustomDataFrame
                                let normalized_df = lowercase_column_names(aliased_df.dataframe).await?;
                                
                                let df = CustomDataFrame {
                                    df: normalized_df,
                                    table_alias: "local_csv".to_string(),
                                    from_table: "local_csv".to_string(),
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
                                };
                                dataframes.push(df);  
                            },
                            Err(e) => {
                                eprintln!("⚠️ Failed to load CSV file {}: {}", file_name, e);
                                continue;
                            }
                        }
                    }
                },
                "xlsx" | "xls" => {
                    match crate::features::excel::load_excel(file_path_str, "local_data").await {
                        Ok(aliased_df) => {
                            println!("✅ Loaded Excel: {}", file_name);
                            
                            // Normalize column names to lowercase and create CustomDataFrame
                            let normalized_df = lowercase_column_names(aliased_df.dataframe).await?;
                            
                            let df = CustomDataFrame {
                                df: normalized_df,
                                table_alias: "local_excel".to_string(),
                                from_table: "local_excel".to_string(),
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
                            };
                            dataframes.push(df);
                        },
                        Err(e) => {
                            eprintln!("⚠️ Failed to load Excel file {}: {}", file_name, e);
                            continue;
                        }
                    }
                },
                "json" => {
                    match Self::load_json(file_path_str, "local_data").await {
                        Ok(aliased_df) => {
                            println!("✅ Loaded JSON: {}", file_name);
                            
                            // Normalize column names to lowercase and create CustomDataFrame
                            let normalized_df = lowercase_column_names(aliased_df.dataframe).await?;
                            
                            let df = CustomDataFrame {
                                df: normalized_df,
                                table_alias: "local_json".to_string(),
                                from_table: "local_json".to_string(),
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
                            };
                            dataframes.push(df);
                        },
                        Err(e) => {
                            eprintln!("⚠️ Failed to load JSON file {}: {}", file_name, e);
                            continue;
                        }
                    }
                },
                "parquet" => {
                    match Self::load_parquet(file_path_str, "local_data").await {
                        Ok(aliased_df) => {
                            println!("✅ Loaded Parquet: {}", file_name);
                            
                            // Normalize column names to lowercase and create CustomDataFrame
                            let normalized_df = lowercase_column_names(aliased_df.dataframe).await?;
                            
                            let df = CustomDataFrame {
                                df: normalized_df,
                                table_alias: "local_parquet".to_string(),
                                from_table: "local_parquet".to_string(),
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
                            };
                            dataframes.push(df);
                        },
                        Err(e) => {
                            eprintln!("⚠️ Failed to load Parquet file {}: {}", file_name, e);
                            continue;
                        }
                    }
                },
                "xml" => {
                    match Self::load_xml(file_path_str, "local_data").await {
                        Ok(aliased_df) => {
                            println!("✅ Loaded XML: {}", file_name);
                            
                            let normalized_df = lowercase_column_names(aliased_df.dataframe).await?;
                            
                            let df = CustomDataFrame {
                                df: normalized_df,
                                table_alias: "local_xml".to_string(),
                                from_table: "local_xml".to_string(),
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
                            };
                            dataframes.push(df);
                        },
                        Err(e) => {
                            eprintln!("⚠️ Failed to load XML file {}: {}", file_name, e);
                            continue;
                        }
                    }
                },
                _ => {
                    println!("⏭️ Skipping unsupported file type: {}", file_name);
                }
            }
        }
        
        if dataframes.is_empty() {
            return Err(ElusionError::InvalidOperation {
                operation: "Local Folder Loading".to_string(),
                reason: "No supported files found or all files failed to load".to_string(),
                suggestion: "💡 Check folder path and ensure it contains CSV, Excel, JSON, or Parquet files".to_string(),
            });
        }
        
        // If only one file, return it directly
        if dataframes.len() == 1 {
            println!("📄 Single file loaded, returning as-is");
            return dataframes.into_iter().next().unwrap().elusion(result_alias).await;
        }
        
        // Check schema compatibility by column names AND types
        println!("🔍 Checking schema compatibility for {} files (names + types)...", dataframes.len());
        
        let first_schema = dataframes[0].df.schema();
        let mut compatible_schemas = true;
        let mut schema_issues = Vec::new();
        
        // Print first file schema for reference
        println!("📋 File 1 schema:");
        for (i, field) in first_schema.fields().iter().enumerate() {
            println!("   Column {}: '{}' ({})", i + 1, field.name(), field.data_type());
        }
        
        for (file_idx, df) in dataframes.iter().enumerate().skip(1) {
            let current_schema = df.df.schema();
            
            println!("📋 File {} schema:", file_idx + 1);
            for (i, field) in current_schema.fields().iter().enumerate() {
                println!("   Column {}: '{}' ({})", i + 1, field.name(), field.data_type());
            }
            
            // Check if column count matches
            if first_schema.fields().len() != current_schema.fields().len() {
                compatible_schemas = false;
                schema_issues.push(format!("File {} has {} columns, but first file has {}", 
                    file_idx + 1, current_schema.fields().len(), first_schema.fields().len()));
                continue;
            }
            
            // Check if column names and types match (case insensitive names)
            for (col_idx, first_field) in first_schema.fields().iter().enumerate() {
                if let Some(current_field) = current_schema.fields().get(col_idx) {
                    // Check column name (case insensitive)
                    if first_field.name().to_lowercase() != current_field.name().to_lowercase() {
                        compatible_schemas = false;
                        schema_issues.push(format!("File {} column {} name is '{}', but first file has '{}'", 
                            file_idx + 1, col_idx + 1, current_field.name(), first_field.name()));
                    }
                    
                    // Check column type
                    if first_field.data_type() != current_field.data_type() {
                        compatible_schemas = false;
                        schema_issues.push(format!("File {} column {} ('{}') type is {:?}, but first file has {:?}", 
                            file_idx + 1, col_idx + 1, current_field.name(), 
                            current_field.data_type(), first_field.data_type()));
                    }
                }
            }
        }
        
        if !compatible_schemas {
            println!("⚠️ Schema compatibility issues found:");
            for issue in &schema_issues {
                println!("   {}", issue);
            }
            
            println!("🔧 Reordering columns by name to match first file...");
            
            // Get the column order from the first file
            let first_file_columns: Vec<String> = first_schema.fields()
                .iter()
                .map(|field| field.name().clone())
                .collect();
            
            println!("📋 Target column order: {:?}", first_file_columns);
            
            let mut reordered_dataframes = Vec::new();
            
            for (i, df) in dataframes.clone().into_iter().enumerate() {
                // Select columns in the same order as first file
                let column_refs: Vec<&str> = first_file_columns.iter().map(|s| s.as_str()).collect();
                let reordered_df = df.select_vec(column_refs);
                
                // Create temporary alias
                let temp_alias = format!("reordered_file_{}", i + 1);
                match reordered_df.elusion(&temp_alias).await {
                    Ok(standardized_df) => {
                        println!("✅ Reordered file {} columns", i + 1);
                        reordered_dataframes.push(standardized_df);
                    },
                    Err(e) => {
                        eprintln!("⚠️ Failed to reorder file {} columns: {}", i + 1, e);
                        continue;
                    }
                }
            }
            
            if reordered_dataframes.is_empty() {
                println!("📄 Column reordering failed, returning first file only");
                return dataframes.into_iter().next().unwrap().elusion(result_alias).await;
            }
            
            dataframes = reordered_dataframes;
            println!("✅ All files reordered to match first file column order");
        } else {
            println!("✅ All schemas are compatible!");
        }
        
        println!("🔗 Unioning {} files with compatible schemas...", dataframes.len());
        
        let total_files = dataframes.len();
        let mut result = dataframes.clone().into_iter().next().unwrap();
        
        // Union with remaining dataframes using union_all to keep all data
        for (i, df) in dataframes.into_iter().enumerate().skip(1) {
            result = result.union_all(df).await
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: "Local Folder Union All".to_string(),
                    reason: format!("Failed to union file {}: {}", i + 1, e),
                    suggestion: "💡 Check that all files have compatible schemas".to_string(),
                })?;
            
            println!("✅ Unioned file {}/{}", i + 1, total_files - 1);
        }
        
        println!("🎉 Successfully combined {} files using UNION ALL", total_files);

        result.elusion(result_alias).await
    }

    /// Load all files from local folder and add filename as a column
    /// Same as load_folder but adds a "filename" column to track source files
    pub async fn load_folder_with_filename_column(
        folder_path: &str,
        file_extensions: Option<Vec<&str>>,
        result_alias: &str,
    ) -> ElusionResult<Self> {
        use std::fs;
        use std::path::Path;
        
        // Check if folder exists
        let folder_path_obj = Path::new(folder_path);
        if !folder_path_obj.exists() {
            return Err(ElusionError::WriteError {
                path: folder_path.to_string(),
                operation: "read".to_string(),
                reason: "Folder not found".to_string(),
                suggestion: "💡 Check if the folder path is correct".to_string(),
            });
        }
        
        if !folder_path_obj.is_dir() {
            return Err(ElusionError::InvalidOperation {
                operation: "Local Folder Loading with Filename".to_string(),
                reason: "Path is not a directory".to_string(),
                suggestion: "💡 Provide a valid directory path".to_string(),
            });
        }
        
        // Read directory contents
        let entries = fs::read_dir(folder_path)
            .map_err(|e| ElusionError::WriteError {
                path: folder_path.to_string(),
                operation: "read".to_string(),
                reason: format!("Failed to read directory: {}", e),
                suggestion: "💡 Check directory permissions".to_string(),
            })?;
        
        let mut dataframes = Vec::new();
        
        for entry in entries {
            let entry = entry.map_err(|e| ElusionError::WriteError {
                path: folder_path.to_string(),
                operation: "read".to_string(),
                reason: format!("Failed to read directory entry: {}", e),
                suggestion: "💡 Check directory permissions".to_string(),
            })?;
            
            let file_path = entry.path();
            
            // Skip if not a file
            if !file_path.is_file() {
                continue;
            }
            
            let file_name = file_path.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("")
                .to_string();
            
            // Skip hidden files
            if file_name.starts_with('.') {
                continue;
            }
            
            // Skip if file extensions filter is specified and file doesn't match
            if let Some(ref extensions) = file_extensions {
                let file_ext = file_name
                    .split('.')
                    .last()
                    .unwrap_or("")
                    .to_lowercase();
                
                if !extensions.iter().any(|ext| ext.to_lowercase() == file_ext) {
                    continue;
                }
            }
            
            let file_path_str = file_path.to_str().ok_or_else(|| ElusionError::InvalidOperation {
                operation: "Local Folder Loading with Filename".to_string(),
                reason: format!("Invalid file path: {:?}", file_path),
                suggestion: "💡 Ensure file paths contain valid UTF-8 characters".to_string(),
            })?;
            
            // Load file based on extension and add filename column
            let mut loaded_df = None;
            
            match file_name.split('.').last().unwrap_or("").to_lowercase().as_str() {
                "csv" => {

            let file_size = std::fs::metadata(file_path_str)
                .map(|m| m.len())
                .unwrap_or(0);

                if file_size > 500_000_000 { 
                    println!("📊 Large CSV detected ({}), using streaming loader", file_name);

                match load_csv_smart(file_path_str, "local_data").await {
                    Ok(aliased_df) => {
                        println!("✅ Loaded large CSV: {}", file_name);
                            let normalized_df = lowercase_column_names(aliased_df.dataframe).await?;
                        
                        let df = CustomDataFrame {
                            df: normalized_df,
                            table_alias: "local_csv".to_string(),
                            from_table: "local_csv".to_string(),
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
                        };
                        loaded_df = Some(df);
                    },
                    Err(e) => {
                        eprintln!("⚠️ Failed to load large CSV file {}: {}", file_name, e);
                        continue;
                        }
                    }
                } else {
                    match Self::load_csv(file_path_str, "local_data").await {
                    Ok(aliased_df) => {
                        println!("✅ Loaded CSV: {}", file_name);
                        
                        let normalized_df = lowercase_column_names(aliased_df.dataframe).await?;
                        
                        let df = CustomDataFrame {
                            df: normalized_df,
                            table_alias: "local_csv".to_string(),
                            from_table: "local_csv".to_string(),
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
                        };
                        loaded_df = Some(df);
                    },
                    Err(e) => {
                        eprintln!("⚠️ Failed to load CSV file {}: {}", file_name, e);
                        continue;
                        }
                    }
                }
            },
                "xlsx" | "xls" => {
                    match crate::features::excel::load_excel(file_path_str, "local_data").await {
                        Ok(aliased_df) => {
                            println!("✅ Loaded Excel: {}", file_name);
                            
                            // Normalize column names to lowercase and create CustomDataFrame
                            let normalized_df = lowercase_column_names(aliased_df.dataframe).await?;
                            
                            let df = CustomDataFrame {
                                df: normalized_df,
                                table_alias: "local_excel".to_string(),
                                from_table: "local_excel".to_string(),
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
                            };
                            loaded_df = Some(df);
                        },
                        Err(e) => {
                            eprintln!("⚠️ Failed to load Excel file {}: {}", file_name, e);
                            continue;
                        }
                    }
                },
                "json" => {
                    match Self::load_json(file_path_str, "local_data").await {
                        Ok(aliased_df) => {
                            println!("✅ Loaded JSON: {}", file_name);
                            
                            // Normalize column names to lowercase and create CustomDataFrame
                            let normalized_df = lowercase_column_names(aliased_df.dataframe).await?;
                            
                            let df = CustomDataFrame {
                                df: normalized_df,
                                table_alias: "local_json".to_string(),
                                from_table: "local_json".to_string(),
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
                            };
                            loaded_df = Some(df);
                        },
                        Err(e) => {
                            eprintln!("⚠️ Failed to load JSON file {}: {}", file_name, e);
                            continue;
                        }
                    }
                },
                "parquet" => {
                    match Self::load_parquet(file_path_str, "local_data").await {
                        Ok(aliased_df) => {
                            println!("✅ Loaded Parquet: {}", file_name);
                            
                            // Normalize column names to lowercase and create CustomDataFrame
                            let normalized_df = lowercase_column_names(aliased_df.dataframe).await?;
                            
                            let df = CustomDataFrame {
                                df: normalized_df,
                                table_alias: "local_parquet".to_string(),
                                from_table: "local_parquet".to_string(),
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
                            };
                            loaded_df = Some(df);
                        },
                        Err(e) => {
                            eprintln!("⚠️ Failed to load Parquet file {}: {}", file_name, e);
                            continue;
                        }
                    }
                },
                "xml" => {
                    match Self::load_xml(file_path_str, "local_data").await {
                        Ok(aliased_df) => {
                            println!("✅ Loaded XML: {}", file_name);
                            
                            // Normalize column names to lowercase and create CustomDataFrame
                            let normalized_df = lowercase_column_names(aliased_df.dataframe).await?;
                            
                            let df = CustomDataFrame {
                                df: normalized_df,
                                table_alias: "local_xml".to_string(),
                                from_table: "local_xml".to_string(),
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
                            };
                            dataframes.push(df);
                        },
                        Err(e) => {
                            eprintln!("⚠️ Failed to load XML file {}: {}", file_name, e);
                            continue;
                        }
                    }
                },
                _ => {
                    println!("⏭️ Skipping unsupported file type: {}", file_name);
                }
            }
            
            // Add filename column to the loaded dataframe
            if let Some(mut df) = loaded_df {
                // Add filename as a new column using select with literal value
                df = df.select_vec(vec![
                    &format!("'{}' AS filename_added", file_name), 
                    "*"
                ]);
                
                // Execute the selection to create the dataframe with filename column
                let temp_alias = format!("file_with_filename_{}", dataframes.len());
                match df.elusion(&temp_alias).await {
                    Ok(filename_df) => {
                        println!("✅ Added filename column to {}", file_name);
                        dataframes.push(filename_df);
                    },
                    Err(e) => {
                        eprintln!("⚠️ Failed to add filename to {}: {}", file_name, e);
                        continue;
                    }
                }
            }
        }
        
        if dataframes.is_empty() {
            return Err(ElusionError::InvalidOperation {
                operation: "Local Folder Loading with Filename".to_string(),
                reason: "No supported files found or all files failed to load".to_string(),
                suggestion: "💡 Check folder path and ensure it contains supported files".to_string(),
            });
        }
        
        // If only one file, return it directly
        if dataframes.len() == 1 {
            println!("📄 Single file loaded with filename column");
            return dataframes.into_iter().next().unwrap().elusion(result_alias).await;
        }
        
        // Check schema compatibility (all files should now have filename as first column)
        println!("🔍 Checking schema compatibility for {} files with filename columns...", dataframes.len());
        
        let first_schema = dataframes[0].df.schema();
        let mut compatible_schemas = true;
        let mut schema_issues = Vec::new();
        
        for (file_idx, df) in dataframes.iter().enumerate().skip(1) {
            let current_schema = df.df.schema();
            
            // Check if column count matches
            if first_schema.fields().len() != current_schema.fields().len() {
                compatible_schemas = false;
                schema_issues.push(format!("File {} has {} columns, but first file has {}", 
                    file_idx + 1, current_schema.fields().len(), first_schema.fields().len()));
                continue;
            }
            
            // Check if column names match (should be identical now with filename column)
            for (col_idx, first_field) in first_schema.fields().iter().enumerate() {
                if let Some(current_field) = current_schema.fields().get(col_idx) {
                    if first_field.name().to_lowercase() != current_field.name().to_lowercase() {
                        compatible_schemas = false;
                        schema_issues.push(format!("File {} column {} name is '{}', but first file has '{}'", 
                            file_idx + 1, col_idx + 1, current_field.name(), first_field.name()));
                    }
                }
            }
        }
        
        if !compatible_schemas {
            println!("⚠️ Schema compatibility issues found:");
            for issue in &schema_issues {
                println!("   {}", issue);
            }
            
            // Reorder columns by name to match first file
            println!("🔧 Reordering columns by name to match first file...");
            
            let first_file_columns: Vec<String> = first_schema.fields()
                .iter()
                .map(|field| field.name().clone())
                .collect();
            
            println!("📋 Target column order: {:?}", first_file_columns);
            
            let mut reordered_dataframes = Vec::new();
            
            for (i, df) in dataframes.clone().into_iter().enumerate() {
                let column_refs: Vec<&str> = first_file_columns.iter().map(|s| s.as_str()).collect();
                let reordered_df = df.select_vec(column_refs);
                
                let temp_alias = format!("reordered_file_{}", i + 1);
                match reordered_df.elusion(&temp_alias).await {
                    Ok(standardized_df) => {
                        println!("✅ Reordered file {} columns", i + 1);
                        reordered_dataframes.push(standardized_df);
                    },
                    Err(e) => {
                        eprintln!("⚠️ Failed to reorder file {} columns: {}", i + 1, e);
                        continue;
                    }
                }
            }
            
            if reordered_dataframes.is_empty() {
                println!("📄 Column reordering failed, returning first file only");
                return dataframes.into_iter().next().unwrap().elusion(result_alias).await;
            }
            
            dataframes = reordered_dataframes;
            println!("✅ All files reordered to match first file column order");
        } else {
            println!("✅ All schemas are compatible!");
        }
        
        // Union the compatible dataframes
        println!("🔗 Unioning {} files with filename tracking...", dataframes.len());
        
        let total_files = dataframes.len();
        let mut result = dataframes.clone().into_iter().next().unwrap();
        
        // Union with remaining dataframes using union_all to keep all data
        for (i, df) in dataframes.into_iter().enumerate().skip(1) {
            result = result.union_all(df).await
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: "Local Folder Union with Filename".to_string(),
                    reason: format!("Failed to union file {}: {}", i + 1, e),
                    suggestion: "💡 Check that all files have compatible schemas".to_string(),
                })?;
            
            println!("✅ Unioned file {}/{}", i + 1, total_files - 1);
        }
        
        println!("🎉 Successfully combined {} files with filename tracking", total_files);

        result.elusion(result_alias).await
    }

    //============== DASHBOARD ===============
    
    #[cfg(feature = "dashboard")]
    pub async fn plot_line(
        &self,
        date_col: &str,
        value_col: &str,
        show_markers: bool,
        title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        crate::features::dashboard::plot_line_impl(self, date_col, value_col, show_markers, title).await
    }

    #[cfg(not(feature = "dashboard"))]
    pub async fn plot_line(
        &self,
        _date_col: &str,
        _value_col: &str,
        _show_markers: bool,
        _title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        Err(ElusionError::Custom("*** Warning ***: Dashboard feature not enabled. Add feature dashboard under [dependencies]".to_string()))
    }

    #[cfg(feature = "dashboard")]
    pub async fn plot_time_series(
        &self,
        date_col: &str,
        value_col: &str,
        show_markers: bool,
        title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        crate::features::dashboard::plot_time_series_impl(self, date_col, value_col, show_markers, title).await
    }

    #[cfg(not(feature = "dashboard"))]
    pub async fn plot_time_series(
        &self,
        _date_col: &str,
        _value_col: &str,
        _show_markers: bool,
        _title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        Err(ElusionError::Custom("*** Warning ***: Dashboard feature not enabled. Add feature dashboard under [dependencies]".to_string()))
    }

    #[cfg(feature = "dashboard")]
    pub async fn plot_bar(
        &self,
        x_col: &str,
        y_col: &str,
        title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        crate::features::dashboard::plot_bar_impl(self, x_col, y_col, title).await
    }

    #[cfg(not(feature = "dashboard"))]
    pub async fn plot_bar(
        &self,
        _x_col: &str,
        _y_col: &str,
        _title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        Err(ElusionError::Custom("*** Warning ***: Dashboard feature not enabled. Add feature dashboard under [dependencies]".to_string()))
    }

    #[cfg(feature = "dashboard")]
    pub async fn plot_scatter(
        &self,
        x_col: &str,
        y_col: &str,
        marker_size: Option<usize>,
    ) -> ElusionResult<PlotlyPlot> {
        crate::features::dashboard::plot_scatter_impl(self, x_col, y_col, marker_size).await
    }

    #[cfg(not(feature = "dashboard"))]
    pub async fn plot_scatter(
        &self,
        _x_col: &str,
        _y_col: &str,
        _marker_size: Option<usize>,
    ) -> ElusionResult<PlotlyPlot> {
        Err(ElusionError::Custom("*** Warning ***: Dashboard feature not enabled. Add feature dashboard under [dependencies]".to_string()))
    }

    #[cfg(feature = "dashboard")]
    pub async fn plot_histogram(
        &self,
        col: &str,
        title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        crate::features::dashboard::plot_histogram_impl(self, col, title).await
    }

    #[cfg(not(feature = "dashboard"))]
    pub async fn plot_histogram(
        &self,
        _col: &str,
        _title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        Err(ElusionError::Custom("*** Warning ***: Dashboard feature not enabled. Add feature dashboard under [dependencies]".to_string()))
    }

    #[cfg(feature = "dashboard")]
    pub async fn plot_box(
        &self,
        value_col: &str,
        group_by_col: Option<&str>,
        title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        crate::features::dashboard::plot_box_impl(self, value_col, group_by_col, title).await
    }

    #[cfg(not(feature = "dashboard"))]
    pub async fn plot_box(
        &self,
        _value_col: &str,
        _group_by_col: Option<&str>,
        _title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        Err(ElusionError::Custom("*** Warning ***: Dashboard feature not enabled. Add feature dashboard under [dependencies]".to_string()))
    }

    #[cfg(feature = "dashboard")]
    pub async fn plot_pie(
        &self,
        label_col: &str,
        value_col: &str,
        title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        crate::features::dashboard::plot_pie_impl(self, label_col, value_col, title).await
    }

    #[cfg(not(feature = "dashboard"))]
    pub async fn plot_pie(
        &self,
        _label_col: &str,
        _value_col: &str,
        _title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        Err(ElusionError::Custom("*** Warning ***: Dashboard feature not enabled. Add feature dashboard under [dependencies]".to_string()))
    }

    #[cfg(feature = "dashboard")]
    pub async fn plot_donut(
        &self,
        label_col: &str,
        value_col: &str,
        title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        crate::features::dashboard::plot_donut_impl(self, label_col, value_col, title).await
    }

    #[cfg(not(feature = "dashboard"))]
    pub async fn plot_donut(
        &self,
        _label_col: &str,
        _value_col: &str,
        _title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        Err(ElusionError::Custom("*** Warning ***: Dashboard feature not enabled. Add feature dashboard under [dependencies]".to_string()))
    }

    /// Create a waterfall chart showing cumulative changes
    #[cfg(feature = "dashboard")]
    pub async fn plot_waterfall(
        &self,
        x_col: &str,
        y_col: &str,
        title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        crate::features::dashboard::plot_waterfall_impl(self, x_col, y_col, title).await
    }

    #[cfg(not(feature = "dashboard"))]
    pub async fn plot_waterfall(
        &self,
        _x_col: &str,
        _y_col: &str,
        _title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        Err(ElusionError::Custom("*** Warning ***: Dashboard feature not enabled. Add feature dashboard under [dependencies]".to_string()))
    }

    #[cfg(feature = "dashboard")]
    pub async fn create_report(
        plots: Option<&[(&PlotlyPlot, &str)]>,
        tables: Option<&[(&CustomDataFrame, &str)]>,
        report_title: &str,
        filename: &str,
        layout_config: Option<ReportLayout>,
        table_options: Option<TableOptions>,
    ) -> ElusionResult<()> {
        crate::features::dashboard::create_report_impl(
            plots, tables, report_title, filename, layout_config, table_options
        ).await
    }

    #[cfg(not(feature = "dashboard"))]
    pub async fn create_report(
        _plots: Option<&[(&PlotlyPlot, &str)]>,
        _tables: Option<&[(&CustomDataFrame, &str)]>,
        _report_title: &str,
        _filename: &str,
        _layout_config: Option<ReportLayout>,
        _table_options: Option<TableOptions>,
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom("*** Warning ***: Dashboard feature not enabled.".to_string()))
    }

    /// Export PLOT to PNG
    #[cfg(feature = "dashboard")]
    pub async fn export_plot_to_png(
        plot: &PlotlyPlot,
        filename: &str,
        width: u32,
        height: u32,
    ) -> ElusionResult<()> {
        crate::features::dashboard::export_plot_to_png_impl(plot, filename, width, height).await
    }

    #[cfg(not(feature = "dashboard"))]
    pub async fn export_plot_to_png(
        _plot: &PlotlyPlot,
        _filename: &str,
        _width: u32,
        _height: u32,
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom("*** Warning ***: Dashboard feature not enabled. Add feature dashboard under [dependencies]".to_string()))
    }

    /// Export report to PDF
    #[cfg(feature = "dashboard")]
    pub async fn export_report_to_pdf(
        plots: Option<&[(&PlotlyPlot, &str)]>,
        tables: Option<&[(&CustomDataFrame, &str)]>,
        report_title: &str,
        pdf_filename: &str,
        layout_config: Option<ReportLayout>,
        table_options: Option<TableOptions>,
    ) -> ElusionResult<()> {
        crate::features::dashboard::export_report_to_pdf_impl(
            plots, tables, report_title, pdf_filename, layout_config, table_options
        ).await
    }

    #[cfg(not(feature = "dashboard"))]
    pub async fn export_report_to_pdf(
        _plots: Option<&[(&PlotlyPlot, &str)]>,
        _tables: Option<&[(&CustomDataFrame, &str)]>,
        _report_title: &str,
        _pdf_filename: &str,
        _layout_config: Option<ReportLayout>,
        _table_options: Option<TableOptions>,
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom("*** Warning ***: Dashboard feature not enabled. Add feature dashboard under [dependencies]".to_string()))
    }


    // ============== CSV STREAMING =================

    /// Simple streaming version of elusion - processes chunks without OOM
    /// Just like regular elusion() but doesn't load everything into memory
    pub async fn elusion_streaming(&self, alias: &str) -> ElusionResult<()> {

        if alias.trim().is_empty() {
            return Err(ElusionError::InvalidOperation {
                operation: "Elusion Streaming".to_string(),
                reason: "Alias cannot be empty".to_string(),
                suggestion: "💡 Provide a valid table alias".to_string()
            });
        }

        println!("🚀 Executing streaming query for '{}'...", alias);
        
        let sql = self.construct_sql();
      //  println!("Generated SQL: {}", sql);
        
        let mut stream = match self.stream().await {
            Ok(stream) => stream,
            Err(e) => {
                println!("❌ Failed to create stream: {}", e);
                return Err(e);
            }
        };
        
        let mut chunk_count = 0;
        let mut total_rows = 0;
        let start_time = std::time::Instant::now();
        let mut has_shown_sample = false;
        
        println!("🔃 Starting to process stream...");
        
        while let Some(batch_result) = stream.next().await {
            let batch = match batch_result {
                Ok(batch) => batch,
                Err(e) => {
                    println!("❌ Error processing batch {}: {}", chunk_count + 1, e);
                    return Err(ElusionError::InvalidOperation {
                        operation: "Stream Processing".to_string(),
                        reason: format!("Failed to process batch: {}", e),
                        suggestion: "💡 Check query syntax and data integrity".to_string()
                    });
                }
            };
            
            chunk_count += 1;
            let batch_rows = batch.num_rows();
            total_rows += batch_rows;
            
            if batch_rows > 0 {
                println!("📦 Chunk {}: {} rows", chunk_count, batch_rows);
            } else {
                println!("📦 Chunk {}: {} rows (empty)", chunk_count, batch_rows);
            }
        
            // Show progress summary periodically
            if chunk_count <= 3 || chunk_count % 100 == 0 {
                let elapsed = start_time.elapsed();
                let rows_per_sec = if elapsed.as_secs() > 0 {
                    total_rows as f64 / elapsed.as_secs_f64()
                } else {
                    0.0
                };
                println!("🔁 Progress: {} chunks processed | {} total rows | {:.0} rows/sec", 
                    chunk_count, total_rows, rows_per_sec);
            }

            if !has_shown_sample && batch_rows > 0 {
                println!("📋 Sample results ({} rows shown):", batch_rows.min(15));
                match self.display_sample(&batch) {
                    Ok(_) => has_shown_sample = true,
                    Err(e) => println!("⚠️ Could not display sample: {}", e)
                }
            }
                
            // small pause for large datasets
            if chunk_count % 1000 == 0 {
                println!("💤 Brief pause after {} chunks to prevent system overload", chunk_count);
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
        
        let total_time = start_time.elapsed();
        
        if total_rows == 0 {
            println!("⚠️ Query completed but returned 0 rows");
            println!("💡 This could indicate:");
            println!("   - Filters eliminated all data");
            println!("   - Empty source dataset");
            println!("   - Query logic issue");
            println!("🔍 Generated SQL was: {}", sql);
        } else {
            println!("✅ Streaming complete: {} chunks, {} rows in {:?}", 
                chunk_count, total_rows, total_time);
        }
        
        Ok(())
    }

    /// Helper to display sample from first chunk
    fn display_sample(&self, batch: &RecordBatch) -> ElusionResult<()> {
        if batch.num_rows() == 0 {
            println!("📋 Empty batch (0 rows)");
            return Ok(());
        }
        
        // Determine sample size based on result set size
        let sample_size = if batch.num_rows() <= 20 {
            batch.num_rows()
        } else {
            15 
        };
        
        let sample_batch = batch.slice(0, sample_size);
        
        let formatted = pretty_format_batches(&[sample_batch])
            .map_err(|e| ElusionError::Custom(format!("Display error: {}", e)))?;
        
        if batch.num_rows() <= 20 {
            println!("📑 Complete result ({} rows):", batch.num_rows());
        } else {
            println!("📋 Sample result ({} of {} rows shown):", sample_size, batch.num_rows());
        }
        
        println!("{}", formatted);
        
        if batch.num_rows() > sample_size {
            println!("... ({} more rows in this chunk)", batch.num_rows() - sample_size);
        }
        
        Ok(())
    }

     /// streaming iterator over the DataFrame - TRUE streaming
    pub async fn stream(&self) -> ElusionResult<SendableRecordBatchStream> {
        let ctx = SessionContext::new();
        
        // Register tables with error handling
        if let Err(e) = self.register_all_tables(&ctx).await {
            println!("❌ Failed to register tables: {}", e);
            return Err(e);
        }
        
        let sql = self.construct_sql();
        //println!("🔍 Executing SQL: {}", sql);
        
        // Create DataFrame from SQL
        let df = ctx.sql(&sql).await.map_err(|e| {
            println!("❌ SQL parsing failed: {}", e);
            ElusionError::InvalidOperation {
                operation: "SQL Parsing".to_string(),
                reason: format!("Failed to parse SQL: {}", e),
                suggestion: "💡 Check SQL syntax, table aliases, and column names".to_string()
            }
        })?;
        
        println!("✅ Query parsed successfully, creating execution stream...");
        
        // Execute stream directly from DataFrame
        df.execute_stream().await.map_err(|e| {
            println!("❌ Stream execution failed: {}", e);
            ElusionError::InvalidOperation {
                operation: "Stream Execution".to_string(),
                reason: format!("Failed to execute stream: {}", e),
                suggestion: "💡 Check data integrity and memory availability".to_string()
            }
        })
    }
    
    /// Streaming data with closure
    pub async fn stream_process<F, Fut>(
        &self, 
        mut processor: F
    ) -> ElusionResult<()> 
    where
        F: FnMut(RecordBatch) -> Fut,
        Fut: std::future::Future<Output = ElusionResult<()>>,
    {
        let mut stream = self.stream().await?;
        
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(|e| ElusionError::InvalidOperation {
                operation: "Stream Processing".to_string(),
                reason: format!("Failed to get batch from stream: {}", e),
                suggestion: "💡 Check data integrity and memory availability".to_string()
            })?;
            
            processor(batch).await?;
        }
        
        Ok(())
    }

    //============ STREAMING WRITE ============

    /// Streaming elusion that writes results using your existing writers
    pub async fn elusion_streaming_write(
        &self, 
        alias: &str,
        output_path: &str,
        write_mode: &str  // "overwrite" or "append"
    ) -> ElusionResult<()> {
        if alias.trim().is_empty() {
            return Err(ElusionError::InvalidOperation {
                operation: "Elusion Streaming Write".to_string(),
                reason: "Alias cannot be empty".to_string(),
                suggestion: "💡 Provide a valid table alias".to_string()
            });
        }

        println!("📄 Streaming query results to: {}", output_path);
        
        // Determine file type from extension
        let ext = output_path.split('.').last().unwrap_or_default().to_lowercase();
        
        match ext.as_str() {
            "csv" => {
                self.stream_to_csv(alias, output_path, write_mode).await
            },
            "json" => {
                self.stream_to_json(alias, output_path, write_mode).await
            },
            "parquet" => {
                // Parquet doesn't support true streaming, so we'll batch it
                self.stream_to_parquet_batched(alias, output_path, write_mode).await
            },
            _ => {
                Err(ElusionError::InvalidOperation {
                    operation: "Streaming Write".to_string(),
                    reason: format!("Unsupported file extension: {}", ext),
                    suggestion: "💡 Use .csv, .json, or .parquet extensions".to_string()
                })
            }
        }
    }

    /// Stream to CSV using your existing CSV writer logic
    async fn stream_to_csv(&self, _alias: &str, output_path: &str, mode: &str) -> ElusionResult<()> {
        // overwrite
        if mode == "overwrite" && std::fs::metadata(output_path).is_ok() {
            std::fs::remove_file(output_path).map_err(|e| ElusionError::WriteError {
                path: output_path.to_string(),
                operation: "overwrite".to_string(),
                reason: e.to_string(),
                suggestion: "💡 Check file permissions".to_string()
            })?;
        }
        
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(mode == "append")
            .truncate(mode == "overwrite")
            .open(output_path)
            .map_err(|e| ElusionError::WriteError {
                path: output_path.to_string(),
                operation: "file_create".to_string(),
                reason: e.to_string(),
                suggestion: "💡 Check path and permissions".to_string()
            })?;

        let mut writer = std::io::BufWriter::new(file);

        let mut stream = self.stream().await?;

        let mut is_first_batch = true;
        let mut total_rows = 0;
        let mut chunk_count = 0;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(|e| ElusionError::DataFusion(e))?;
            chunk_count += 1;
            total_rows += batch.num_rows();

            // Write header only for first batch (and only if not appending to existing file)
            let write_header = is_first_batch && (mode == "overwrite" || !std::fs::metadata(output_path).is_ok());
            
            let mut csv_writer = WriterBuilder::new()
                .with_header(write_header)
                .build(&mut writer);

            csv_writer.write(&batch).map_err(|e| ElusionError::WriteError {
                path: output_path.to_string(),
                operation: "write_batch".to_string(),
                reason: e.to_string(),
                suggestion: "💡 Check disk space and permissions".to_string()
            })?;

            if chunk_count % 100 == 0 {
                println!("📦 Written {} chunks ({} rows) to CSV", chunk_count, total_rows);
            }

            is_first_batch = false;
        }

        writer.flush().map_err(|e| ElusionError::WriteError {
            path: output_path.to_string(),
            operation: "flush".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Failed to flush data".to_string()
        })?;

        println!("✅ Streaming CSV write complete: {} rows in {} chunks", total_rows, chunk_count);
        Ok(())
    }

    /// Stream to JSON 
    async fn stream_to_json(&self, _alias: &str, output_path: &str, mode: &str) -> ElusionResult<()> {
        
        if mode == "overwrite" && std::fs::metadata(output_path).is_ok() {
            std::fs::remove_file(output_path).map_err(|e| ElusionError::WriteError {
                path: output_path.to_string(),
                operation: "overwrite".to_string(),
                reason: e.to_string(),
                suggestion: "💡 Check file permissions".to_string()
            })?;
        }

        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(mode == "overwrite")
            .open(output_path)
            .map_err(|e| ElusionError::WriteError {
                path: output_path.to_string(),
                operation: "file_create".to_string(),
                reason: e.to_string(),
                suggestion: "💡 Check path and permissions".to_string()
            })?;

        let mut writer = std::io::BufWriter::new(file);
        let mut stream = self.stream().await?;
        let mut is_first_row = true;
        let mut total_rows = 0;

        // Start JSON array
        writeln!(writer, "[").map_err(|e| ElusionError::WriteError {
            path: output_path.to_string(),
            operation: "write_start".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check disk space".to_string()
        })?;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(|e| ElusionError::DataFusion(e))?;
            total_rows += batch.num_rows();

            // Process each row in the batch
            for row_idx in 0..batch.num_rows() {
                if !is_first_row {
                    writeln!(writer, ",").map_err(|e| ElusionError::WriteError {
                        path: output_path.to_string(),
                        operation: "write_separator".to_string(),
                        reason: e.to_string(),
                        suggestion: "💡 Check disk space".to_string()
                    })?;
                }
                is_first_row = false;

                // Convert row to JSON (use your existing array_value_to_json logic)
                let mut row_obj = serde_json::Map::new();
                for col_idx in 0..batch.num_columns() {
                    let schema = batch.schema();
                    let field = schema.field(col_idx); 
                    let array = batch.column(col_idx);
                    let json_value = array_value_to_json(array, row_idx)?;
                    row_obj.insert(field.name().clone(), json_value);
                }

                let json_value = serde_json::Value::Object(row_obj);
                serde_json::to_writer(&mut writer, &json_value).map_err(|e| ElusionError::WriteError {
                    path: output_path.to_string(),
                    operation: "write_json".to_string(),
                    reason: e.to_string(),
                    suggestion: "💡 Check JSON serialization".to_string()
                })?;
            }
        }

        // Close JSON array
        writeln!(writer, "\n]").map_err(|e| ElusionError::WriteError {
            path: output_path.to_string(),
            operation: "write_end".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check disk space".to_string()
        })?;

        writer.flush().map_err(|e| ElusionError::WriteError {
            path: output_path.to_string(),
            operation: "flush".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Failed to flush data".to_string()
        })?;

        println!("✅ Streaming JSON write complete: {} rows", total_rows);
        Ok(())
    }

    /// For Parquet, we'll collect in larger batches to maintain efficiency
    async fn stream_to_parquet_batched(&self, _alias: &str, output_path: &str, mode: &str) -> ElusionResult<()> {
        println!("⚠️  Parquet streaming: collecting batches of 50k rows for efficiency");
        
        let mut stream = self.stream().await?;
        let mut accumulated_batches = Vec::new();
        let mut total_rows = 0;
        let batch_limit = 50000; //  50k rows before writing
        
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(|e| ElusionError::DataFusion(e))?;
            total_rows += batch.num_rows();
            accumulated_batches.push(batch);
            
            // Write when we hit the batch limit
            if total_rows >= batch_limit {
                self.write_parquet_batches(&accumulated_batches, output_path, mode).await?;
                accumulated_batches.clear();
                total_rows = 0;
                println!("📦 Written batch to Parquet ({}k+ rows)", batch_limit / 1000);
            }
        }
        
        // Write remaining batches
        if !accumulated_batches.is_empty() {
            self.write_parquet_batches(&accumulated_batches, output_path, mode).await?;
        }
        
        println!("✅ Streaming Parquet write complete");
        Ok(())
    }

    /// Helper to write accumulated batches to Parquet
    async fn write_parquet_batches(&self, batches: &[RecordBatch], output_path: &str, mode: &str) -> ElusionResult<()> {

        let ctx = SessionContext::new();
        let schema = batches[0].schema();
        let mem_table = MemTable::try_new(schema.into(), vec![batches.to_vec()])
            .map_err(|e| ElusionError::Custom(format!("Failed to create temp table: {}", e)))?;
        
        ctx.register_table("temp_batches", Arc::new(mem_table))
            .map_err(|e| ElusionError::DataFusion(e))?;
        
        let temp_df = ctx.table("temp_batches").await.map_err(|e| ElusionError::DataFusion(e))?;
        
        let temp_custom_df = CustomDataFrame {
            df: temp_df,
            table_alias: "temp".to_string(),
            from_table: "temp".to_string(),
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
        };
        
        temp_custom_df.write_to_parquet(mode, output_path, None).await
    }


}
