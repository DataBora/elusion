use chrono::{NaiveDate, Datelike};
use datafusion::datasource::MemTable;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use std::sync::Arc;
use arrow::array::{Array, ArrayRef, Date32Array, StringArray};
use uuid::Uuid;


pub async fn preprocess_date_column(
    df: DataFrame,
    column_name: &str,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    // Clone the DataFrame for `select_columns` to avoid moving the original `df`
    let df_clone = df.clone();

    // Select the column as a StringArray
    let batches = df_clone
        .select_columns(&[column_name])
        .map_err(|_| format!("Failed to select column '{}'", column_name))?
        .collect()
        .await?;

    let mut date_values: Vec<Option<i32>> = Vec::new();

    // Parse dates in each batch
    for batch in batches {
        let column = batch.column(0); // We only have one column here
        let string_array = column
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| format!("Column '{}' is not a StringArray", column_name))?;

        for value in string_array.iter() {
            if let Some(date_str) = value {
                // Use `parse_date_with_formats` to parse date strings
                match parse_date_with_formats(date_str) {
                    Some(date) => date_values.push(Some(date.num_days_from_ce())),
                    None => date_values.push(None),
                }
            } else {
                date_values.push(None);
            }
        }
    }

    // Convert parsed dates to a Date32Array
    let date_array: ArrayRef = Arc::new(Date32Array::from(date_values));

    // Get the schema of the original DataFrame
    let df_schema = df.schema(); // Use the original `df` here

    let arrow_schema = Arc::new(df_schema.as_arrow().clone());

    // Collect existing columns and replace the target column
    let mut columns = vec![];
    let batches = df.collect().await?; // Collect data as RecordBatches

    for (i, field) in arrow_schema.fields().iter().enumerate() {
        if field.name() == column_name {
            // Replace the target column with the new date array
            columns.push(date_array.clone());
        } else {
            // Retain other columns
            columns.push(batches[0].column(i).clone());
        }
    }

    // Create a new RecordBatch with the updated columns
    let record_batch = RecordBatch::try_new(arrow_schema.clone(), columns)?;

    // Create a MemTable using the new RecordBatch
    let mem_table = MemTable::try_new(arrow_schema.clone(), vec![vec![record_batch]])?;
    let session = SessionContext::new();

    // Generate a unique alias for the MemTable
    let unique_alias = format!("temp_{}", Uuid::new_v4());
    session.register_table(&unique_alias, Arc::new(mem_table))?;

    // Return the updated DataFrame
    session.table(&unique_alias).await.map_err(|e| e.into())
}



fn parse_date_with_formats(date_str: &str) -> Option<NaiveDate> {
    let formats = vec![
        "%Y-%m-%d", 
        "%d.%m.%Y",
        "%m/%d/%Y", 
        "%d-%b-%Y",
        "%a, %d %b %Y",
        "%Y/%m/%d",
        "%Y/%m",
        "%Y-%m-%dT%H:%M:%S%z",
        "%d%b%Y",
    ];

    for format in formats {
        if let Ok(date) = NaiveDate::parse_from_str(date_str, format) {
            return Some(date);
        }
    }

    // Log a warning for debugging purposes if no format matches
    eprintln!("Warning: Failed to parse date '{}'. Returning None.", date_str);
    None
}


// fn parse_date_with_formats(date_str: &str) -> Result<NaiveDate, ParseError> {
//     let formats = vec![
//         "%Y-%m-%d", 
//         "%d.%m.%Y",
//         "%m/%d/%Y", 
//         "%d-%b-%Y",
//         "%a, %d %b %Y",
//         "%Y/%m/%d",
//         "%Y/%m",
//         "%Y-%m-%dT%H:%M:%S%z",
//         "%d%b%Y"
//     ];

//     let mut last_error: Option<ParseError> = None;

//     for format in formats {
//         match NaiveDate::parse_from_str(date_str, format) {
//             Ok(date) => return Ok(date), 
//             Err(err) => last_error = Some(err),
//         }
//     }

//     Err(last_error.unwrap_or_else(|| {
//         panic!(
//             "No parse error was generated while attempting to parse '{}'. This is unexpected behaviour...SORRY :( .",
//             date_str
//         )
//     }))
// }

