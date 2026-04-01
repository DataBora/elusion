use datafusion::prelude::SessionContext;
use crate::CustomDataFrame;
use crate::custom_error::cust_error::{ElusionError, ElusionResult};
use crate::helper_funcs::registertable::register_df_as_table;
use crate::project::context::NodeRegistry;
use std::sync::Arc;
use datafusion::datasource::MemTable;

pub async fn run_sql(
    query: &str,
    alias: &str,
    registry: NodeRegistry,
) -> ElusionResult<CustomDataFrame> {
    let ctx = SessionContext::new();

    // Register all resolved nodes from registry as tables
    for (name, node) in &registry.resolved {
        register_df_as_table(&ctx, name, &node.df.df).await
            .map_err(|e| ElusionError::Custom(format!(
                "❌ Failed to register '{}' for SQL execution: {}", name, e
            )))?;
    }

    // Execute the SQL
    let df = ctx.sql(query).await
        .map_err(|e| ElusionError::Custom(format!(
            "❌ SQL execution failed in node '{}': {}", alias, e
        )))?;

    // Collect and register as CustomDataFrame
    let batches = df.clone().collect().await
        .map_err(|e| ElusionError::Custom(format!(
            "❌ Failed to collect SQL results for '{}': {}", alias, e
        )))?;

    let schema = df.schema().clone();

    let mem_table = MemTable::try_new(schema.clone().into(), vec![batches])
        .map_err(|e| ElusionError::Custom(format!(
            "❌ Failed to create result table for '{}': {}", alias, e
        )))?;

    ctx.register_table(alias, Arc::new(mem_table))
        .map_err(|e| ElusionError::Custom(format!(
            "❌ Failed to register result for '{}': {}", alias, e
        )))?;

    let result_df = ctx.table(alias).await
        .map_err(|e| ElusionError::Custom(format!(
            "❌ Failed to retrieve result for '{}': {}", alias, e
        )))?;

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
        query: query.to_string(),
        aggregated_df: Some(df),
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