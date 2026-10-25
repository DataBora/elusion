use crate::prelude::*;

/// Extract a Value from a DataFrame based on column name and row index
pub async fn extract_value_from_df(df: &CustomDataFrame, column_name: &str, row_index: usize) -> ElusionResult<String>{

    let ctx = SessionContext::new();

    let batches = df.df.clone().collect().await 
        .map_err(|e| ElusionError::InvalidOperation { 
            operation: "Data Colleciton".to_string(), 
            reason: format!("Failed to collect DataFrame: {}", e), 
            suggestion: "💡 Check if DataFrame contains valid data".to_string() 
        })?;

    let schema = df.df.schema();
    let mem_table = MemTable::try_new(schema.clone().into(), vec![batches])
        .map_err(|e| ElusionError::SchemaError { 
            message: format!("Failed to create in-memory table: {}", e), 
            schema: Some(schema.to_string()), 
            suggestion: "💡 Verify schema compatibility and data types".to_string()
        })?;

    ctx.register_table("temp_extract", Arc::new(mem_table))
        .map_err(|e| ElusionError::InvalidOperation { 
            operation: "Table Registration".to_string(), 
            reason: format!("Failed to register Table: {}", e), 
            suggestion: "💡 Check if table is unique or valid".to_string() 
        })?;

    let value_df = ctx.sql(&format!("SELECT\"{}\" FROM temp_extract LIMIT 1 OFFSET {}", column_name, row_index)).await
        .map_err(|e| ElusionError::InvalidOperation { 
            operation: "SQL Execution".to_string(), 
            reason: format!("Failed to Execute SQL: {}", e), 
            suggestion: "💡 Verify column name exists in DataFrame".to_string() 
        })?;

    let batches = value_df.collect().await
        .map_err(|e| ElusionError::InvalidOperation { 
            operation: "Result Collection".to_string(), 
            reason: format!("Failed to collect Result: {}", e), 
            suggestion: "💡 Check if Query returns valid data".to_string() 
        })?;

    if batches.is_empty() || batches[0].num_rows() == 0 {
        return Err(ElusionError::Custom(format!("No data found for column '{}' at row {}", column_name, row_index)));
    }

    let col = batches[0].column(0);
    // Format any Arrow type as plain text (numbers, dates, Utf8View...), nulls as empty string
    let value = if col.is_null(0) {
        "".to_string()
    } else {
        datafusion::arrow::util::display::array_value_to_string(col, 0)
            .map_err(|e| ElusionError::Custom(format!("Failed to format value of column '{}': {}", column_name, e)))?
    };

    Ok(value)
}
