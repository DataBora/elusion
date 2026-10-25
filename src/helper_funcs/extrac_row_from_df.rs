use crate::prelude::*;
/// Extract row from a DataFrame as a HashMap based on row index
pub async fn extract_row_from_df(df: &CustomDataFrame, row_index: usize) -> ElusionResult<HashMap<String, String>> {
    let ctx = SessionContext::new();
   
    let batches = df.df.clone().collect().await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Data Collection".to_string(),
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
            reason: format!("Failed to register table: {}", e),
            suggestion: "💡 Check if table name is unique and valid".to_string()
        })?;
    
    let row_df = ctx.sql(&format!("SELECT * FROM temp_extract LIMIT 1 OFFSET {}", row_index)).await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "SQL Execution".to_string(),
            reason: format!("Failed to execute SQL: {}", e),
            suggestion: "💡 Verify DataFrame is valid".to_string()
        })?;
    
    let batches = row_df.collect().await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Result Collection".to_string(),
            reason: format!("Failed to collect result: {}", e),
            suggestion: "💡 Check if query returns valid data".to_string()
        })?;
    
    if batches.is_empty() || batches[0].num_rows() == 0 {
        return Err(ElusionError::Custom(format!("No data found at row {}", row_index)));
    }
    
    let mut row_values = HashMap::new();
    let batch = &batches[0];
    
    for (i, field) in schema.fields().iter().enumerate() {
        let col = batch.column(i);
        // Format any Arrow type as plain text (numbers, dates, Utf8View...), nulls as empty string
        let value = if col.is_null(0) {
            "".to_string()
        } else {
            datafusion::arrow::util::display::array_value_to_string(col, 0)
                .map_err(|e| ElusionError::Custom(format!("Failed to format value of column '{}': {}", field.name(), e)))?
        };
        row_values.insert(field.name().to_string(), value);
    }
    
    Ok(row_values)
}