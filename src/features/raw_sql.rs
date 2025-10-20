use crate::prelude::*;
use crate::register_df_as_table;

#[macro_export]
macro_rules! sql {
    ($sql:expr, $alias:expr, $($df:expr),+) => {
        {
            async {
                $crate::features::raw_sql::execute_raw_sql($sql, $alias, &[$(&$df),+]).await
            }
        }
    };
}

pub async fn execute_raw_sql(
    sql: &str,
    alias: &str,
    dataframes: &[&crate::CustomDataFrame],
) -> ElusionResult<crate::CustomDataFrame> {
   
    let ctx = Arc::new(SessionContext::new());

    for df in dataframes {
        register_df_as_table(&ctx, &df.table_alias, &df.df).await?;
    }

    let result_df = ctx
        .sql(sql)
        .await
        .map_err(ElusionError::DataFusion)?;

    let batches = result_df
        .clone()
        .collect()
        .await
        .map_err(ElusionError::DataFusion)?;


    let result_mem_table = MemTable::try_new(
        result_df.schema().clone().into(),
        vec![batches],
    )
    .map_err(ElusionError::DataFusion)?;

    ctx.register_table(alias, Arc::new(result_mem_table))
        .map_err(ElusionError::DataFusion)?;

    let final_df = ctx
        .table(alias)
        .await
        .map_err(|e| {
            ElusionError::Custom(format!(
                "Failed to retrieve result table '{}': {}",
                alias, e
            ))
        })?;

    Ok(crate::CustomDataFrame {
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
        query: sql.to_string(),
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