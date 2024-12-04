use std::sync::Arc;
use datafusion::arrow::datatypes::{Schema, Field, DataType as ArrowDataType};
use datafusion::common::DFSchema;
use datafusion::logical_expr::ExprSchemable;
use crate::datatypes::datatypes::SQLDataType;
use datafusion::prelude::*;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::col;
use futures::future::BoxFuture;
use datafusion::datasource::MemTable;
// pub trait CsvLoader {
//     // Change the lifetime parameter to be more flexible
//     fn load<'a>(&'a self, csv_path: &'a str, schema: Arc<Schema>) -> BoxFuture<'a, Result<DataFrame, DataFusionError>>;
// }

// impl CsvLoader for str {
//     fn load<'a>(&'a self, csv_path: &'a str, schema: Arc<Schema>) -> BoxFuture<'a, Result<DataFrame, DataFusionError>> {
//         Box::pin(async move {
//             let ctx = SessionContext::new();

//             // Register CSV file with custom options to preserve case
//             let options = CsvReadOptions::new()
//                 .has_header(true)
//                 .file_extension(".csv");

//             ctx.register_csv("csv_table", csv_path, options).await?;

//             let mut df = ctx.table("csv_table").await?;

//             // Get the actual column names from the DataFrame
//             let df_columns: Vec<String> = df.schema().fields()
//                 .iter()
//                 .map(|field| field.name().to_string())
//                 .collect();

//             // Validate that all columns in the schema are present in the DataFrame
//             let missing_columns: Vec<String> = schema.fields().iter()
//                 .filter(|field| !df_columns.contains(&field.name().to_string()))
//                 .map(|field| field.name().to_string())
//                 .collect();

//             if !missing_columns.is_empty() {
//                 return Err(DataFusionError::Configuration(format!(
//                     "Missing columns in DataFrame: {:?}",
//                     missing_columns
//                 )));
//             }

//             // Convert schema to DFSchema
//             let df_schema = DFSchema::try_from(schema.clone())?;

//             // Map the columns to cast them to the expected schema types
//             let mut exprs = Vec::new(); // Start with an empty vector to collect expressions

//             for field in schema.fields().iter() {
//                 let schema_col_name = field.name();

//                 // Find the exact match in DataFrame columns
//                 let matching_col = df_columns.iter()
//                     .find(|&col| col == schema_col_name)
//                     .ok_or_else(|| DataFusionError::Configuration(
//                         format!("Column '{}' not found in the CSV file.", schema_col_name)
//                     ))?;

//                 let arrow_type = field.data_type().clone();
//                 let col_expr = col(matching_col);

//                 // Attempt to cast the column to the appropriate type based on the schema
//                 match col_expr.cast_to(&arrow_type, &df_schema) {
//                     Ok(cast_expr) => {
//                         // Log success and add the casted expression
//                         // println!(
//                         //     "Casting column '{}', expected type: {:?}, result: {:?}",
//                         //     schema_col_name, arrow_type, cast_expr
//                         // );
//                         exprs.push(cast_expr);
//                     }
//                     Err(e) => {
//                         // Log the error and skip this column
//                         println!("Error casting column '{}': {:?}", schema_col_name, e);
//                     }
//                 }
//             }

//             // Ensure at least one valid column is selected
//             if exprs.is_empty() {
//                 return Err(DataFusionError::Plan(
//                     "No valid columns were casted. Check the schema and data.".to_string(),
//                 ));
//             }

//             // Select the casted columns
//             df = df.select(exprs)?;

//             // println!("DataFrame schema after processing: {:?}", df.schema());

//             Ok(df)
//         })
//     }
// }

// pub struct CustomDataFrame {
//     pub df: DataFrame
// }

// impl CustomDataFrame {
//     pub fn new(df: DataFrame) -> Self {
//         CustomDataFrame { df }
//     }
// }

// impl CsvLoader for CustomDataFrame {
//     fn load<'a>(&'a self, csv_path: &'a str, schema: Arc<Schema>) -> BoxFuture<'a, Result<DataFrame, DataFusionError>> {
//         Box::pin(async move {
            
//             let df = csv_path.load(csv_path, schema).await?; 
//             Ok(df)
//         })
//     }
// }

pub fn create_schema_from_str(columns: Vec<(&str, &str, bool)>) -> Schema {
    let fields = columns.into_iter().map(|(name, sql_type_str, nullable)| {
        let sql_type = SQLDataType::from_str(sql_type_str);
        let arrow_type: ArrowDataType = sql_type.into();
        Field::new(name, arrow_type, nullable)
    }).collect::<Vec<_>>();

    Schema::new(fields)
}

//------------------------------- PROBA

pub trait CsvLoader {
    fn load<'a>(
        &'a self,
        csv_path: &'a str,
        schema: Arc<Schema>,
        alias: &'a str,
    ) -> BoxFuture<'a, Result<AliasedDataFrame, DataFusionError>>;
}

pub struct AliasedDataFrame {
    pub dataframe: DataFrame,
    pub alias: String,
}

impl CsvLoader for str {
    fn load<'a>(
        &'a self,
        csv_path: &'a str,
        schema: Arc<Schema>,
        alias: &'a str,
    ) -> BoxFuture<'a, Result<AliasedDataFrame, DataFusionError>> {
        Box::pin(async move {
            let ctx = SessionContext::new();

            // Read CSV into DataFrame
            let df = ctx
                .read_csv(
                    csv_path,
                    CsvReadOptions::new()
                        .schema(&schema)
                        .has_header(true)
                        .file_extension(".csv"),
                )
                .await?;

            // Register the DataFrame with the user-provided alias
            ctx.register_table(alias, Arc::new(MemTable::try_new(schema.clone(), vec![df.collect().await?])?))
                .expect("Failed to register DataFrame alias");

            // Retrieve the aliased table for use
            let aliased_df = ctx.table(alias).await.expect("Failed to retrieve aliased table");

            Ok(AliasedDataFrame {
                dataframe: aliased_df,
                alias: alias.to_string(),
            })
        })
}
}