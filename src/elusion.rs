pub mod prelude;
mod features;
mod helper_funcs;
mod custom_error;
mod normalizers;
mod csvwrite;
mod sqlbuilder;

// =========== DataFusion
use regex::Regex;
use datafusion::prelude::*;
use futures::future::BoxFuture;
use datafusion::datasource::MemTable;
use std::sync::Arc;
use arrow::datatypes::{DataType as ArrowDataType};
use arrow::array::{ArrayRef, Array, Float64Array,Int64Array};
 
use arrow::record_batch::RecordBatch;
use arrow::csv::writer::WriterBuilder;

// ========= CSV defects
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

// ======== PIVOT
use arrow::compute;
use arrow::array::StringArray;

// dashboard
pub use features::dashboard::{ReportLayout, TableOptions};
use crate::prelude::PlotlyPlot;

// ======== STATISTICS
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

//======= csv 
use crate::features::csv::load_csv_with_type_handling;
use crate::csvwrite::csvwriteops::CsvWriteOptions;

// ======= optimizers
use crate::sqlbuilder::sqlbuild::SqlBuilder;

// // Generic struct for DataFrame row representation
// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct DataFrameRow {
//     pub fields: HashMap<String, String>,
// }

// impl DataFrameRow {
//     pub fn new() -> Self {
//         Self {
//             fields: HashMap::new(),
//         }
//     }
    
//     pub fn insert(&mut self, key: String, value: String) {
//         self.fields.insert(key, value);
//     }
    
//     pub fn get(&self, key: &str) -> Option<&String> {
//         self.fields.get(key)
//     }
// }

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
            needs_normalization: self.needs_normalization.clone(),
            raw_selected_columns: self.raw_selected_columns.clone(),   
            raw_group_by_columns: self.raw_group_by_columns.clone(),  
            raw_where_conditions: self.raw_where_conditions.clone(),  
            raw_having_conditions: self.raw_having_conditions.clone(),
            raw_join_conditions: self.raw_join_conditions.clone(),
            raw_aggregations: self.raw_aggregations.clone()
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

    //=========== SHARE POINT
    #[cfg(feature = "sharepoint")]
    pub async fn load_from_sharepoint(
        tenant_id: &str,
        client_id: &str,
        site_url: &str,
        file_path: &str,
        alias: &str,
    ) -> ElusionResult<Self> {
        crate::features::sharepoint::load_from_sharepoint_impl(
            tenant_id, client_id, site_url, file_path, alias
        ).await
    }

    #[cfg(not(feature = "sharepoint"))]
    pub async fn load_from_sharepoint(
        _tenant_id: &str,
        _client_id: &str,
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
        tenant_id: &str,
        client_id: &str,
        site_url: &str,
        folder_path: &str,
        file_extensions: Option<Vec<&str>>,
        result_alias: &str,
    ) -> ElusionResult<Self> {
        crate::features::sharepoint::load_folder_from_sharepoint_impl(
            tenant_id, client_id, site_url, folder_path, file_extensions, result_alias
        ).await
    }

    #[cfg(not(feature = "sharepoint"))]
    pub async fn load_folder_from_sharepoint(
        _tenant_id: &str, 
        _client_id: &str, 
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
        tenant_id: &str,
        client_id: &str,
        site_url: &str,
        folder_path: &str,
        file_extensions: Option<Vec<&str>>,
        result_alias: &str,
    ) -> ElusionResult<Self> {
        crate::features::sharepoint::load_folder_from_sharepoint_with_filename_column_impl(
            tenant_id, client_id, site_url, folder_path, file_extensions, result_alias
        ).await
    }

    #[cfg(not(feature = "sharepoint"))]
    pub async fn load_folder_from_sharepoint_with_filename_column(
        _tenant_id: &str, 
        _client_id: &str, 
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
    // pub fn join<const N: usize>(
    //     mut self,
    //     other: CustomDataFrame,
    //     conditions: [&str; N],  // Keeping single array of conditions with "="
    //     join_type: &str
    // ) -> Self {
    //     let condition = conditions.iter()
    //         .map(|&cond| normalize_condition(cond))  // Keeping the "=" in condition
    //         .collect::<Vec<_>>()
    //         .join(" AND ");
    
    //     self.joins.push(Join {
    //         dataframe: other,
    //         condition,
    //         join_type: join_type.to_string(),
    //     });
    //     self
    // }
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
                if is_simple_column(s) {
                    normalize_column_name(s)
                } else if s.contains(" AS ") {
                    // Handle expressions with aliases
                    let expr_part = s.split(" AS ")
                        .next()
                        .unwrap_or(s);
                    normalize_expression(expr_part, &self.table_alias)
                } else {
                    // Handle expressions without aliases
                    normalize_expression(s, &self.table_alias)
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
        // Store raw
        for expr in expressions.iter() {
            self.raw_selected_columns.push(expr.to_string());
        }
        self.needs_normalization = true;

        // Execute original logic (PRESERVED EXACTLY)
        for expr in expressions.iter() {
            // Add to SELECT clause
            self.selected_columns.push(normalize_expression(expr, &self.table_alias));

            // If GROUP BY is used, extract the expression part (before AS)
            // and add it to GROUP BY columns
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
    /// Supports various date/time operations and formats
    pub fn datetime_functions<const N: usize>(mut self, expressions: [&str; N]) -> Self {
        // Store raw
        for expr in expressions.iter() {
            self.raw_selected_columns.push(expr.to_string());
        }
        self.needs_normalization = true;

        for expr in expressions.iter() {
            // Add to SELECT clause
            self.selected_columns.push(normalize_expression(expr, &self.table_alias));

            // If GROUP BY is used, extract the expression part (before AS)
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
        })
    }

    /// Handle various set operations including 
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
                    if col.contains(" AS ") {
                        col.split(" AS ")
                            .nth(1)
                            .unwrap_or(col)
                            .trim_matches('"')
                            .trim()
                            .to_string()
                    } else {
                        col.trim_matches('"')
                            .split('.')
                            .last()
                            .unwrap_or(col)
                            .trim_matches('"')
                            .to_string()
                    }
                })
                .collect()
        };
        
      //  println!("Debug: selected_cols after processing: {:?}", selected_cols);
      //  println!("Debug: columns to fill: {:?}", columns);
        
        // Handle both NULL and string "null" values by converting them to actual NULLs first
        let fill_expressions: Vec<String> = selected_cols
            .iter()
            .map(|col_name| {
                if columns.contains(&col_name.as_str()) {
                   // println!("Debug: Processing fill_down for column: {}", col_name);
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
                    //println!("Debug: NOT processing fill_down for column: {} (not in fill list)", col_name);
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
            return base_sql; // Invalid format, return unchanged
        }
        
        let columns_str = parts[0];
        let fill_value = parts[1];
        let columns: Vec<&str> = columns_str.split(',').collect();
        
        let all_columns = if self.selected_columns.is_empty() {
            self.df.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
        } else {
            self.selected_columns
                .iter()
                .map(|col| {
                    if col.contains(" AS ") {
                        col.split(" AS ")
                            .nth(1)
                            .unwrap_or(col)
                            .trim_matches('"')
                            .trim()
                            .to_string()
                    } else {
                        col.trim_matches('"')
                            .split('.')
                            .last()
                            .unwrap_or(col)
                            .trim_matches('"')
                            .to_string()
                    }
                })
                .collect()
        };
        
        let select_expressions: Vec<String> = all_columns
            .iter()
            .map(|col_name| {
                let quoted_col = format!("\"{}\"", col_name);
                let normalized_col = col_name.trim().replace(" ", "_").to_lowercase();
                
                // Check if this column should have nulls filled
                let should_fill = columns.iter().any(|&target_col| {
                    let normalized_target = target_col.trim().replace(" ", "_").to_lowercase();
                    normalized_col == normalized_target
                });
                
                if should_fill {
                    format!(
                        r#"CASE 
                            WHEN {0} IS NULL OR 
                                 TRIM({0}) = '' OR 
                                 UPPER(TRIM({0})) = 'NULL' OR
                                 UPPER(TRIM({0})) = 'NA' OR
                                 UPPER(TRIM({0})) = 'N/A' OR
                                 UPPER(TRIM({0})) = 'NONE' OR
                                 TRIM({0}) = '-' OR
                                 TRIM({0}) = '?' OR
                                 TRIM({0}) = 'NaN' OR
                                 UPPER(TRIM({0})) = 'NAN'
                            THEN '{1}'
                            ELSE {0}
                        END AS {0}"#,
                        quoted_col, fill_value
                    )
                } else {
                    quoted_col
                }
            })
            .collect();
        
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
        // Store original expressions with AS clauses 
        self.original_expressions = columns
            .iter()
            .filter(|&col| col.contains(" AS "))
            .map(|&s| s.to_string())
            .collect();

        // raw columns for lazy processing
        self.raw_selected_columns.extend(columns.iter().map(|&s| s.to_string()));
        self.needs_normalization = true;

        let mut all_columns = self.selected_columns.clone();
        
        if !self.group_by_columns.is_empty() {
            for col in columns {
                if is_expression(col) {
                    if is_aggregate_expression(col) {
                        all_columns.push(normalize_expression(col, &self.table_alias));
                    } else {
                        self.group_by_columns.push(col.to_string());
                        all_columns.push(normalize_expression(col, &self.table_alias));
                    }
                } else {
                    let normalized_col = normalize_column_name(col);
                    if self.group_by_columns.contains(&normalized_col) {
                        all_columns.push(normalized_col);
                    } else {
                        self.group_by_columns.push(normalized_col.clone());
                        all_columns.push(normalized_col);
                    }
                }
            }
        } else {
            // Handle non-GROUP BY case (PRESERVED EXACTLY)
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
                columns
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

        // Remove duplicates while preserving order (PRESERVED)
        let mut seen = HashSet::new();
        self.selected_columns = all_columns
            .into_iter()
            .filter(|x| seen.insert(x.clone()))
            .collect();

        self
    }

    /// Extract JSON properties from a column containing JSON strings
    pub fn json<'a, const N: usize>(mut self, columns: [&'a str; N]) -> Self {
        let mut json_expressions = Vec::new();
        
        for expr in columns.iter() {
            // Parse the expression: "column.'$jsonPath' AS alias"
            // let parts: Vec<&str> = expr.split(" AS ").collect();
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
                search_pattern, column_name,
                column_name, 
                search_pattern, column_name, search_pattern.len(),
                column_name, search_pattern, column_name, search_pattern.len(),
                column_name, search_pattern, column_name, search_pattern.len(),
                column_name, search_pattern, column_name, search_pattern.len(),
                column_name, search_pattern, column_name, search_pattern.len(),
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
            // let parts: Vec<&str> = expr.split(" AS ").collect();
            let re = Regex::new(r"(?i)\s+AS\s+").unwrap();
            let parts: Vec<&str> = re.split(expr).collect();

            if parts.len() != 2 {
                continue; // skip invalid expressions
            }
            
            let path_part = parts[0].trim();
            let alias = parts[1].trim().to_lowercase();
            
            if !path_part.contains(".'$") {
                continue; // Skip invalid expressions
            }
            
            let col_path_parts: Vec<&str> = path_part.split(".'$").collect();
            let column_name = col_path_parts[0].trim();
            let filter_expr = col_path_parts[1].trim_end_matches('\'');
         
            let filter_parts: Vec<&str> = filter_expr.split(':').collect();
            
            let sql_expr: String;
            
            if filter_parts.len() == 2 {
                // Format: "column.'$ValueField:IdField=IdValue' AS alias"
                let value_field = filter_parts[0].trim();
                let condition = filter_parts[1].trim();
                
                let condition_parts: Vec<&str> = condition.split('=').collect();
                if condition_parts.len() != 2 {
                    continue; // Skip invalid expressions
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
                    column_name, id_field, id_value, value_field,
                    column_name, id_field, id_value, value_field,
                    column_name, id_field, id_value, value_field,
                    column_name, id_field, id_value, value_field,
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


    // /// Convert DataFrame to Vec<DataFrameRow> for easy manipulation
    // pub async fn convert_to_vec(&self) -> ElusionResult<Vec<DataFrameRow>> {
    //     let batches = self.df.clone().collect().await
    //         .map_err(|e| ElusionError::InvalidOperation {
    //             operation: "DataFrame to Vec".to_string(),
    //             reason: format!("Failed to collect DataFrame: {}", e),
    //             suggestion: "💡 Verify DataFrame contains valid data".to_string(),
    //         })?;

    //     if batches.is_empty() {
    //         return Ok(Vec::new());
    //     }

    //     let mut rows = Vec::new();
    //     let schema = self.df.schema();

    //     for batch in batches.iter() {
    //         let row_count = batch.num_rows();
            
    //         for row_idx in 0..row_count {
    //             let mut row = DataFrameRow::new();
                
    //             for (col_idx, field) in schema.fields().iter().enumerate() {
    //                 let array = batch.column(col_idx);
    //                 let col_name = field.name().to_lowercase(); 

    //                 let value = if array.is_null(row_idx) {
    //                     "".to_string()
    //                 } else {
    //                     match array_value_to_json(array, row_idx)? {
    //                         serde_json::Value::Null => "".to_string(),
    //                         serde_json::Value::String(s) => s,
    //                         other => other.to_string().trim_matches('"').to_string(),
    //                     }
    //                 };

    //                 row.insert(col_name, value); 
    //             }
                
    //             rows.push(row);
    //         }
    //     }

    //     Ok(rows)
    // }

    // /// Convert DataFrame to Vec<HashMap<String, String>> 
    // pub async fn convert_to_vec_hashmap(&self) -> ElusionResult<Vec<HashMap<String, String>>> {
    //     let rows = self.convert_to_vec().await?;
    //     Ok(rows.into_iter().map(|row| row.fields).collect())
    // }

    /// Construct the SQL query based on the current state, including joins
    /// Optimized SQL construction with SqlBuilder and reduced allocations
    fn construct_sql(&self) -> String {
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
        
        // Build the main query
        builder.select(&select_parts)
               .from_table(&self.from_table, Some(&self.table_alias))
               .joins(&self.joins)
               .where_clause(&self.where_conditions)
               .group_by(&self.group_by_columns)
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
        let total_parts = self.aggregations.len() 
                         + self.selected_columns.len() 
                         + self.window_functions.len();
        
        let mut select_parts = Vec::with_capacity(total_parts);

        if !self.group_by_columns.is_empty() {
            // Add aggregations first
            select_parts.extend_from_slice(&self.aggregations);

            // Add GROUP BY columns and selected columns (avoid duplicates)
            for col in &self.selected_columns {
                if !select_parts.contains(col) {
                    select_parts.push(col.clone());
                }
            }
        } else {
            // No GROUP BY - add all parts
            select_parts.extend_from_slice(&self.aggregations);
            select_parts.extend_from_slice(&self.selected_columns);
        }

        // Add window functions last
        select_parts.extend_from_slice(&self.window_functions);
        
        select_parts
    }
    
    /// Execute the constructed SQL and return a new CustomDataFrame
    pub async fn elusion(&self, alias: &str) -> ElusionResult<Self> {
        // Pre-validate inputs to fail fast
        if alias.trim().is_empty() {
            return Err(ElusionError::InvalidOperation {
                operation: "Elusion".to_string(),
                reason: "Alias cannot be empty".to_string(),
                suggestion: "💡 Provide a valid table alias".to_string()
            });
        }

        let ctx = Arc::new(SessionContext::new());

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
        println!("{:?}", final_sql);

        // Execute the SQL query
        let df = ctx.sql(&final_sql).await
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "SQL Execution".to_string(),
                reason: format!("Failed to execute SQL: {}", e),
                suggestion: "💡 Verify SQL syntax and table/column references".to_string()
            })?;

        // Using spawn_blocking for potentially expensive data collection
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
                suggestion: "💡 Check if query returns valid data, or check if your file have unreadable column values".to_string()
            })?
        } else {
            let batches = df.clone().collect().await
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: "Data Collection".to_string(),
                    reason: format!("Failed to collect results: {}", e),
                    suggestion: "💡 Check if query returns valid data, or check if your file have unreadable column values".to_string()
                })?;
            (batches, df.schema().clone())
        };

        let result_mem_table = MemTable::try_new(schema.clone().into(), vec![batches])
            .map_err(|e| ElusionError::SchemaError {
                message: format!("Failed to create result table: {}", e),
                schema: Some(schema.to_string()),
                suggestion: "💡 Verify result schema compatibility".to_string()
            })?;

        ctx.register_table(alias, Arc::new(result_mem_table))
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Result Registration".to_string(),
                reason: format!("Failed to register result table: {}", e),
                suggestion: "💡 Try using a different alias name".to_string()
            })?;

        let result_df = ctx.table(alias).await
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Result Retrieval".to_string(),
                reason: format!("Failed to retrieve final result: {}", e),
                suggestion: "💡 Check if result table was properly registered".to_string()
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
        })
    }

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

    //=================== LOCAL LOADERS ============================= //

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

            let df = match ctx.read_parquet(file_path, ParquetReadOptions::default()).await {
                Ok(df) => {
                    df
                }
                Err(err) => {
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
    ) -> BoxFuture<'a, ElusionResult<AliasedDataFrame>> {
        Box::pin(async move {
            // Open file with BufReader for efficient reading
            let file = File::open(file_path).map_err(|e| ElusionError::WriteError {
                path: file_path.to_string(),
                operation: "read".to_string(),
                reason: e.to_string(),
                suggestion: "💡 heck if the file exists and you have proper permissions".to_string(),
            })?;
            
            let file_size = file.metadata().map_err(|e| ElusionError::WriteError {
                path: file_path.to_string(),
                operation: "metadata reading".to_string(),
                reason: e.to_string(),
                suggestion: "💡 Check file permissions and disk status".to_string(),
            })?.len() as usize;
                
            let reader = BufReader::with_capacity(32 * 1024, file); // 32KB buffer
            let stream = Deserializer::from_reader(reader).into_iter::<Value>();
            
            let mut all_data = Vec::with_capacity(file_size / 3); // Pre-allocate with estimated size
            
            // Process the first value to determine if it's an array or object
            let mut stream = stream.peekable();
            match stream.peek() {
                Some(Ok(Value::Array(_))) => {
                    for value in stream {
                        match value {
                            Ok(Value::Array(array)) => {
                                for item in array {
                                    if let Value::Object(map) = item {
                                        let mut base_map = map.clone();
                                        
                                        if let Some(Value::Array(fields)) = base_map.remove("fields") {
                                            for field in fields {
                                                let mut row = base_map.clone();
                                                if let Value::Object(field_obj) = field {
                                                    for (key, val) in field_obj {
                                                        row.insert(format!("field_{}", key), val);
                                                    }
                                                }
                                                all_data.push(row.into_iter().collect());
                                            }
                                        } else {
                                            all_data.push(base_map.into_iter().collect());
                                        }
                                    }
                                }
                            }
                            Ok(_) => continue,
                            Err(e) => return Err(ElusionError::InvalidOperation {
                                operation: "JSON parsing".to_string(),
                                reason: format!("Failed to parse JSON array: {}", e),
                                suggestion: "💡 Ensure the JSON file is properly formatted and contains valid data".to_string(),
                            }),
                        }
                    }
                }
                Some(Ok(Value::Object(_))) => {
                    for value in stream {
                        if let Ok(Value::Object(map)) = value {
                            let mut base_map = map.clone();
                            if let Some(Value::Array(fields)) = base_map.remove("fields") {
                                for field in fields {
                                    let mut row = base_map.clone();
                                    if let Value::Object(field_obj) = field {
                                        for (key, val) in field_obj {
                                            row.insert(format!("field_{}", key), val);
                                        }
                                    }
                                    all_data.push(row.into_iter().collect());
                                }
                            } else {
                                all_data.push(base_map.into_iter().collect());
                            }
                        }
                    }
                }
                Some(Err(e)) => return Err(ElusionError::InvalidOperation {
                    operation: "JSON parsing".to_string(),
                    reason: format!("Invalid JSON format: {}", e),
                    suggestion: "💡 Check if the JSON file is well-formed and valid".to_string(),
                }),
                _ => return Err(ElusionError::InvalidOperation {
                    operation: "JSON reading".to_string(),
                    reason: "Empty or invalid JSON file".to_string(),
                    suggestion: "💡 Ensure the JSON file contains valid data in either array or object format".to_string(),
                }),
            }

            if all_data.is_empty() {
                return Err(ElusionError::InvalidOperation {
                    operation: "JSON processing".to_string(),
                    reason: "No valid JSON data found".to_string(),
                    suggestion: "💡 Check if the JSON file contains the expected data structure".to_string(),
                });
            }

            let schema = infer_schema_from_json(&all_data);
            let record_batch = build_record_batch(&all_data, schema.clone())
                .map_err(|e| ElusionError::SchemaError {
                    message: format!("Failed to build RecordBatch: {}", e),
                    schema: Some(schema.to_string()),
                    suggestion: "💡 Check if the JSON data structure is consistent".to_string(),
                })?;

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

            // Open Delta table using path manager
            let table = open_table(&path_manager.table_path())
            .await
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Delta Table Opening".to_string(),
                reason: e.to_string(),
                suggestion: "💡 Ensure the path points to a valid Delta table".to_string(),
            })?;

            
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
            "xlsx" | "xls" => crate::features::excel::load_excel(file_path, alias).await?,
            "" => return Err(ElusionError::InvalidOperation {
                operation: "File Loading".to_string(),
                reason: format!("Directory is not a Delta table and has no recognized extension: {file_path}"),
                suggestion: "💡 Provide a file with a supported extension (.csv, .json, .parquet, .xlsx, .xls) or a valid Delta table directory".to_string(),
            }),
            other => return Err(ElusionError::InvalidOperation {
                operation: "File Loading".to_string(),
                reason: format!("Unsupported file extension: {other}"),
                suggestion: "💡 Use one of the supported file types: .csv, .json, .parquet, .xlsx, .xls or Delta table".to_string(),
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
    /// Supports CSV, Excel, JSON, and Parquet files
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
                            };
                            dataframes.push(df);
                        },
                        Err(e) => {
                            eprintln!("⚠️ Failed to load CSV file {}: {}", file_name, e);
                            continue;
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
                            };
                            dataframes.push(df);
                        },
                        Err(e) => {
                            eprintln!("⚠️ Failed to load Parquet file {}: {}", file_name, e);
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
                            };
                            loaded_df = Some(df);
                        },
                        Err(e) => {
                            eprintln!("⚠️ Failed to load CSV file {}: {}", file_name, e);
                            continue;
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
                            };
                            loaded_df = Some(df);
                        },
                        Err(e) => {
                            eprintln!("⚠️ Failed to load Parquet file {}: {}", file_name, e);
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
   
}

