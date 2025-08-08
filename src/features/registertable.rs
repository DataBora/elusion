use crate::prelude::*;

pub async fn register_df_as_table(
        ctx: &SessionContext,
        table_name: &str,
        df: &DataFrame,
    ) -> ElusionResult<()> {
        let batches = df.clone().collect().await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Data Collection".to_string(),
            reason: format!("Failed to collect DataFrame: {}", e),
            suggestion: "💡 Check if DataFrame contains valid data".to_string()
        })?;

        let schema = df.schema();

        let mem_table = MemTable::try_new(schema.clone().into(), vec![batches])
        .map_err(|e| ElusionError::SchemaError {
            message: format!("Failed to create in-memory table: {}", e),
            schema: Some(schema.to_string()),
            suggestion: "💡 Verify schema compatibility and data types".to_string()
        })?;

        ctx.register_table(table_name, Arc::new(mem_table))
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Table Registration".to_string(),
            reason: format!("Failed to register table '{}': {}", table_name, e),
            suggestion: "💡 Check if table name is unique and valid".to_string()
        })?;

        Ok(())
    }