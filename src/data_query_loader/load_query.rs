use crate::datatypes::datatypes::SQLDataType;
use crate::data_query_loader::csv_detect_defect::{csv_detect_defect_utf8, convert_invalid_utf8};
use crate::AggregationBuilder;
use crate::data_query_loader::parse_dates::parse_date_with_formats;

use datafusion::logical_expr::{Expr, col, SortExpr};
use regex::Regex;
use datafusion::prelude::*;
use datafusion::error::DataFusionError; 
use futures::future::BoxFuture;
use datafusion::datasource::MemTable;
use std::sync::Arc;
use datafusion::arrow::datatypes::{Field, DataType as ArrowDataType, Schema};
use chrono::NaiveDate;
use arrow::array::{StringArray, Date32Array, Date64Array, Float64Array, Int32Array, ArrayRef, Array};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaBuilder;
// use log::debug;
#[derive(Clone)]
pub struct CustomDataFrame {
    pub df: DataFrame,
    pub table_alias: String,
    query: String,
    selected_columns: Vec<String>,
    alias_map: Vec<(String, Expr)>,
    aggregated_df: Option<DataFrame>
}


pub struct AliasedDataFrame {
    pub dataframe: DataFrame,
    pub alias: String,
}

fn col_with_relation(relation: &str, column: &str) -> Expr {
        if column.contains('.') {
            col(column) // Already qualified
        } else {
            col(&format!("{}.{}", relation, column)) // Add table alias
        }
    }

fn validate_schema(schema: &Schema, df: &DataFrame) {
        let df_fields = df.schema().fields();
    
        for field in schema.fields() {
            if !df_fields.iter().any(|f| f.name() == field.name()) {
                panic!(
                    "Column '{}' not found in the loaded CSV file. Available columns: {:?}",
                    field.name(),
                    df_fields.iter().map(|f| f.name()).collect::<Vec<_>>()
                );
            }
        }
    }

fn normalize_column_name(name: &str) -> String {
        name.to_lowercase() // or .to_uppercase() based on convention
    }
    
impl CustomDataFrame {
    
    /// Unified `new` method for loading and schema definition
    pub async fn new<'a>(
        file_path: &'a str,
        columns: Vec<(&'a str, &'a str, bool)>,
        alias: &'a str,
    ) -> Self {
        let schema = Arc::new(Self::create_schema_from_str(columns));

        // Load the file into a DataFrame
        let aliased_df = Self::load(file_path, schema.clone(), alias)
            .await
            .expect("Failed to load file");

        CustomDataFrame {
            df: aliased_df.dataframe,
            table_alias: aliased_df.alias,
            query: String::new(),
            selected_columns: Vec::new(),
            alias_map: Vec::new(),
            aggregated_df: None,
        }
    }

     /// Utility function to create schema from user-defined column info
     fn create_schema_from_str(columns: Vec<(&str, &str, bool)>) -> Schema {
        let fields = columns
            .into_iter()
            .map(|(name, sql_type_str, nullable)| {
                let sql_type = SQLDataType::from_str(sql_type_str);
                // If the type is DATE, map it to Utf8 initially
                let arrow_type = if matches!(sql_type, SQLDataType::Date) {
                    ArrowDataType::Utf8
                } else {
                    sql_type.into()
                };
                Field::new(&normalize_column_name(name), arrow_type, nullable)
            })
            .collect::<Vec<_>>();
    
        Schema::new(fields)
    }

    /// Internal unified `load` function for any supported file type
    pub fn load<'a>(
        file_path: &'a str,
        schema: Arc<Schema>,
        alias: &'a str,
    ) -> BoxFuture<'a, Result<AliasedDataFrame, DataFusionError>> {
        Box::pin(async move {
            let ctx = SessionContext::new();
            // let mut config = SessionConfig::new();
            // config = config.with_batch_size(8192); 
            // let ctx = SessionContext::new_with_config(config);

            let file_extension = file_path
                .split('.')
                .last()
                .unwrap_or_else(|| panic!("Unable to determine file type for path: {}", file_path))
                .to_lowercase();

            // Detect and fix invalid UTF-8
            if let Err(err) = csv_detect_defect_utf8(file_path) {
                eprintln!(
                    "Invalid UTF-8 data detected in file '{}': {}. Attempting in-place conversion...",
                    file_path, err
                );
                convert_invalid_utf8(file_path).expect("Failed to convert invalid UTF-8 data in-place.");
            }

            // Read and validate the CSV
            let df = match file_extension.as_str() {
                "csv" => {
                    let result = ctx
                        .read_csv(
                            file_path,
                            CsvReadOptions::new()
                                .schema(&schema)
                                .has_header(true)
                                .file_extension(".csv"),
                        )
                        .await;

                    match result {
                        Ok(df) => {
                            validate_schema(&schema, &df); // Validate schema here
                            df
                        }
                        Err(err) => {
                            eprintln!(
                                "Error reading CSV file '{}': {}. Ensure the file is UTF-8 encoded and free of corrupt data.",
                                file_path, err
                            );
                            return Err(err);
                        }
                    }
                }
                _ => panic!("Unsupported file type: {}", file_extension),
            };

            let batches = df.collect().await?;
            let mut schema_builder = SchemaBuilder::new(); // Initialize SchemaBuilder
            let mut new_batches = Vec::new();

            // Temporary vector to store updated fields (to avoid consuming schema_builder)
            let mut updated_fields = Vec::new();

            // Step 1: Process each batch
            for batch in &batches {
                let mut columns = Vec::new();

                // Step 2: Iterate through each column and process date fields
                for (i, field) in schema.fields().iter().enumerate() {
                    if field.data_type() == &ArrowDataType::Utf8 && field.name().contains("date") {
                        let column = batch.column(i);
                        let string_array = column
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .expect("Column is not a StringArray");
                        
                        // println!("Original column values for '{}':", field.name());
                        // for value in string_array.iter() {
                        //     println!("{:?}", value);
                        // }
                        // Convert string dates to Date32
                        let date_values = string_array
                            .iter()
                            .map(|value| value.and_then(|v| parse_date_with_formats(v)))
                            .collect::<Vec<_>>();

                        // println!("Converted Date32 values: {:?}", date_values);


                        let date_array: ArrayRef = Arc::new(Date32Array::from(date_values));
                        columns.push(date_array);

                        // Collect the updated field with Date32 type
                        updated_fields.push(Field::new(field.name(), ArrowDataType::Date32, field.is_nullable()));
                    } else {
                        // Retain other columns
                        columns.push(batch.column(i).clone());

                        // Collect the original field as-is
                        updated_fields.push(field.as_ref().clone());
                    }
                }

                // Create RecordBatch with the intermediate schema (not finalized yet)
                let temp_schema = Arc::new(Schema::new(updated_fields.clone()));
                let new_batch = RecordBatch::try_new(temp_schema.clone(), columns)?;
                new_batches.push(new_batch);

                // Clear updated fields for next batch to avoid duplication
                updated_fields.clear();
            }

            // Step 3: Finalize the schema using the updated fields
            for field in schema.fields() {
                if field.data_type() == &ArrowDataType::Utf8 && field.name().contains("date") {
                    schema_builder.push(Field::new(field.name(), ArrowDataType::Date32, field.is_nullable()));
                } else {
                    schema_builder.push(field.as_ref().clone());
                }
            }
            let final_schema = Arc::new(schema_builder.finish());

            // for batch in &new_batches {
            //     println!("Final updated batch data:");
            //     for column in batch.columns() {
            //         println!("{:?}", column);
            //     }
            // }

            let partitions: Vec<Vec<RecordBatch>> = new_batches.into_iter().map(|batch| vec![batch]).collect();
            let mem_table = MemTable::try_new(final_schema.clone(), partitions)?;

            
            ctx.register_table(alias, Arc::new(mem_table))?;
            
            println!("Registering table with alias: {}", alias);
            println!("Loading file: {}", file_path);
            println!("Loaded schema: {:?}", final_schema);

            // Register the table with the context
            // ctx.register_table(
            //     alias,
            //     Arc::new(MemTable::try_new(schema.clone(), vec![df.collect().await?])?),
            // )
            // .expect("Failed to register DataFrame alias");

            // // Return the aliased DataFrame
            // let aliased_df = ctx
            //     .table(alias)
            //     .await
            //     .expect("Failed to retrieve aliased table");

            let aliased_df = ctx.table(alias).await.expect("Failed to retrieve aliased table");
            Ok(AliasedDataFrame {
                dataframe: aliased_df,
                alias: alias.to_string(),
            })
        })
    }
    


    pub fn display_query_plan(&self) {
        println!("Generated Logical Plan:");
        println!("{:?}", self.df.logical_plan());
       
    
    }
    
    // ---------------------  SQL QUERIES ------------------//
    /// FROM function for handling multiple DataFrames
    pub fn from(mut self, table_aliases: Vec<(DataFrame, &str)>) -> Self {
        if table_aliases.is_empty() {
            panic!("At least one table alias must be provided.");
        }
    
        // Build the FROM clause with aliases
        let from_clause = table_aliases
            .iter()
            .map(|(_, alias)| alias.to_string())
            .collect::<Vec<_>>()
            .join(", ");
    
        self.query = format!("FROM {}", from_clause);
    
        // Use the first DataFrame as the main DataFrame
        if let Some((df, alias)) = table_aliases.into_iter().next() {
            self.df = df; // Take ownership of the first DataFrame
            self.table_alias = alias.to_string(); // Set the alias
        }
    
        self
    }

    pub fn select(mut self, columns: Vec<&str>) -> Self {
        let mut expressions: Vec<Expr> = Vec::new();
        let mut selected_columns: Vec<String> = Vec::new();
    
        for col in columns {
            if let Some((alias, _)) = self.alias_map.iter().find(|(_, expr)| match expr {
                Expr::Column(column) if column.name == col => true,
                _ => false,
            }) {
                // Replace with alias if column is part of aggregation
                expressions.push(col_with_relation(&self.table_alias, alias));
                selected_columns.push(alias.clone());
            } else {
                // Retain regular columns
                expressions.push(col_with_relation(&self.table_alias, &normalize_column_name(col)));

                selected_columns.push(col.to_string());
            }
        }
    
        self.selected_columns = selected_columns;
        self.df = self.df.select(expressions).expect("Failed to apply SELECT.");
    
        // Update query string
        self.query = format!(
            "SELECT {} FROM {}",
            self.selected_columns
                .iter()
                .map(|col| normalize_column_name(col))
                .collect::<Vec<_>>()
                .join(", "),
            self.table_alias
        );
    
        self
    }
    
    
    
    pub fn aggregation(mut self, aggregations: Vec<AggregationBuilder>) -> Self {
        for builder in aggregations {
            let expr = builder.build_expr(&self.table_alias); // Build aggregation expression
            let alias = builder
                .agg_alias
                .clone()
                .unwrap_or_else(|| format!("{:?}", expr)); // Use alias or default to expression
    
            // Add the alias and expression to alias_map
            self.alias_map.push((alias.clone(), expr.clone()));
    
            // Include the alias in the list of selected columns for display
            self.selected_columns.push(alias);
        }
    
        self
    }



    pub fn group_by(mut self, group_columns: Vec<&str>) -> Self {
        let group_exprs: Vec<Expr> = group_columns
            .iter()
            .map(|col| col_with_relation(&self.table_alias, col))
            .collect();
    
        let aggregate_exprs: Vec<Expr> = self
            .alias_map
            .iter()
            .map(|(_, expr)| expr.clone())
            .collect();
    
        self.df = self
            .df
            .aggregate(group_exprs, aggregate_exprs)
            .expect("Failed to apply GROUP BY.");
    
        self.query = format!(
            "{} GROUP BY {}",
            self.query.trim_end(),
            group_columns.join(", ")
        );

        self
    }
    

    pub fn order_by(mut self, columns: Vec<&str>, ascending: Vec<bool>) -> Self {
        assert!(
            columns.len() == ascending.len(),
            "The number of columns and sort directions must match"
        );

        let sort_exprs: Vec<SortExpr> = columns
            .iter()
            .zip(ascending.iter())
            .map(|(col, &asc)| {
                SortExpr {
                    expr: col_with_relation(&self.table_alias, col),
                    asc,
                    nulls_first: true,
                }
            })
            .collect();

        self.df = self
            .df
            .sort(sort_exprs)
            .expect("Failed to apply ORDER BY.");

        self.query = format!(
            "{} ORDER BY {}",
            self.query.trim_end(),
            columns
                .iter()
                .zip(ascending.iter())
                .map(|(col, &asc)| format!("{} {}", col, if asc { "ASC" } else { "DESC" }))
                .collect::<Vec<_>>()
                .join(", ")
        );

        self
    }

    pub fn limit(mut self, count: usize) -> Self {
        self.query = format!("{} LIMIT {}", self.query, count);

        self.df = self.df.limit(0, Some(count)).expect("Failed to apply LIMIT.");
        self
    }
    
    
    
    /// WHERE clause
    pub fn filter(mut self, condition: &str) -> Self {
        println!("Condition passed to filter: '{}'", condition);
    
        self.query = format!("{} WHERE {}", self.query.trim_end(), condition);
    
        // Parse the condition
        let expr = self.parse_condition_for_filter(condition);
        self.df = self.df.filter(expr).expect("Failed to apply WHERE filter");
    
        self
    }

    pub fn having(mut self, condition: &str) -> Self {
        if self.aggregated_df.is_none() {
            panic!("HAVING must be applied after aggregation. Ensure `group_by` and `aggregation` are called before `having`.");
        }

        if let Some(ref mut aggregated_df) = self.aggregated_df {
            // Parse the condition using alias_map for aggregated columns
            let expr = Self::parse_condition_for_having(condition, &self.alias_map);

            println!("DEBUG: Applying HAVING condition: {:?}", expr);

            // Apply the condition to the aggregated DataFrame
            *aggregated_df = aggregated_df
                .clone()
                .filter(expr)
                .expect("Failed to apply HAVING filter.");

            println!(
                "DEBUG: Schema after HAVING: {:?}",
                aggregated_df.schema()
            );

            self.query = format!("{} HAVING {}", self.query.trim_end(), condition);
        } else {
            panic!("Aggregated DataFrame not available for HAVING.");
        }

        self
    }
    
    
    
    

    /// JOIN clause
    // pub fn join(
    //     mut self,
    //     other_df: AliasedDataFrame,
    //     join_keys: Vec<(&str, &str)>,
    //     join_type: JoinType,
    // ) -> Self {
    //     let join_condition: Vec<String> = join_keys
    //         .iter()
    //         .map(|(left, right)| {
    //             let original_left = self
    //                 .alias_map
    //                 .iter()
    //                 .find(|(alias, _)| alias == *left)
    //                 .map(|(_, expr)| expr.to_string())
    //                 .unwrap_or_else(|| format!("{}.{}", self.table_alias, left));
    
    //             format!(
    //                 "{} = {}.{}",
    //                 original_left,
    //                 other_df.alias,
    //                 right
    //             )
    //         })
    //         .collect();
    
    //     self.query = format!(
    //         "{} {} JOIN {} ON {}",
    //         self.query,
    //         match join_type {
    //             JoinType::Inner => "INNER",
    //             JoinType::Left => "LEFT",
    //             JoinType::Right => "RIGHT",
    //             JoinType::Full => "FULL",
    //             JoinType::LeftSemi => "LEFT SEMI",
    //             JoinType::RightSemi => "RIGHT SEMI",
    //             JoinType::LeftAnti => "LEFT ANTI",
    //             JoinType::RightAnti => "RIGHT ANTI",
    //             JoinType::LeftMark => "LEFT MARK",
    //         },
    //         other_df.alias,
    //         join_condition.join(" AND ")
    //     );
    
    //     let (left_cols, right_cols): (Vec<&str>, Vec<&str>) = join_keys.iter().cloned().unzip();
    
    //     self.df = self
    //         .df
    //         .join(
    //             other_df.dataframe,
    //             join_type,
    //             &left_cols,
    //             &right_cols,
    //             None,
    //         )
    //         .expect("Failed to apply JOIN.");
    
    //     self
    // }

    pub fn join(
        mut self,
        other: CustomDataFrame,    // Use CustomDataFrame directly
        condition: &str,
        join_type: &str,
    ) -> Self {
        let join_type_enum = match join_type.to_uppercase().as_str() {
            "INNER" => JoinType::Inner,
            "LEFT" => JoinType::Left,
            "RIGHT" => JoinType::Right,
            "FULL" => JoinType::Full,
            "LEFT SEMI" => JoinType::LeftSemi,
            "RIGHT SEMI" => JoinType::RightSemi,
            "LEFT ANTI" => JoinType::LeftAnti,
            "RIGHT ANTI" => JoinType::RightAnti,
            "LEFT MARK" => JoinType::LeftMark,
            _ => panic!("Unsupported join type: {}", join_type),
        };  
    
        // Parse condition: e.g., "sales.CustomerKey == customers.CustomerKey"
        let condition_parts: Vec<&str> = condition.split("==").map(|s| s.trim()).collect();
        if condition_parts.len() != 2 {
            panic!("Invalid join condition format. Use: 'table.column == table.column'");
        }

        let left_col = condition_parts[0];
        let right_col = condition_parts[1];

        // Update query string
        self.query = format!(
            "{} {} JOIN {} ON {} = {}",
            self.query.trim_end(),
            join_type.to_uppercase(),
            other.table_alias,
            left_col,
            right_col
        );

        // Extract column names without table prefixes for join keys
        let left_column = left_col.split('.').last().unwrap();
        let right_column = right_col.split('.').last().unwrap();

        // Perform the join
        self.df = self
            .df
            .join(
                other.df.clone(),
                join_type_enum,
                &[left_column],
                &[right_column],
                None,
            )
            .expect("Failed to apply JOIN.");

        self
    }
    
    

    /// Helper function to parse a qualified column (e.g., "table.column")
    // fn parse_qualified_column(qualified_column: &str) -> (&str, &str) {
    //     let parts: Vec<&str> = qualified_column.split('.').collect();
    //     if parts.len() != 2 {
    //         panic!(
    //             "Invalid column format '{}'. Expected 'table.column'",
    //             qualified_column
    //         );
    //     }
    //     (parts[0], parts[1])
    // }

    

    
    // WINDOW function
    pub fn window(
        mut self,
        func: &str,
        column: &str,
        partition_by: Vec<&str>,
        order_by: Vec<&str>,
    ) -> Self {
        let original_column = self
            .alias_map
            .iter()
            .find(|(alias, _)| alias == column)
            .map(|(_, expr)| expr.to_string())
            .unwrap_or_else(|| format!("{}.{}", self.table_alias, column));
    
        let partition_str = if !partition_by.is_empty() {
            format!(
                "PARTITION BY {}",
                partition_by
                    .iter()
                    .map(|&col| {
                        self.alias_map
                            .iter()
                            .find(|(alias, _)| alias == col)
                            .map(|(_, expr)| expr.to_string())
                            .unwrap_or_else(|| format!("{}.{}", self.table_alias, col))
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        } else {
            String::new()
        };
    
        let order_str = if !order_by.is_empty() {
            format!(
                "ORDER BY {}",
                order_by
                    .iter()
                    .map(|&col| {
                        self.alias_map
                            .iter()
                            .find(|(alias, _)| alias == col)
                            .map(|(_, expr)| expr.to_string())
                            .unwrap_or_else(|| format!("{}.{}", self.table_alias, col))
                    })
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        } else {
            String::new()
        };
    
        self.query = format!(
            "{} {}({}) OVER ({} {})",
            self.query,
            func.to_uppercase(),
            original_column,
            partition_str,
            order_str
        );
    
        self
    }
    


    fn parse_condition_for_filter(&self, condition: &str) -> Expr {
        let re = Regex::new(r"^(.+?)\s*(=|!=|>|<|>=|<=)\s*(.+)$").unwrap();
        let condition_trimmed = condition.trim();
    
        if !re.is_match(condition_trimmed) {
            panic!(
                "Invalid FILTER condition format: '{}'. Expected format: 'column operator value'",
                condition_trimmed
            );
        }
    
        let caps = re.captures(condition_trimmed).expect("Invalid FILTER condition format!");
        let column = caps.get(1).unwrap().as_str().trim();
        let operator = caps.get(2).unwrap().as_str().trim();
        let value = caps.get(3).unwrap().as_str().trim().trim_matches('\'');
    
        let column_expr = col_with_relation(&self.table_alias, column);
    
        // Determine if value is numeric or string
        match value.parse::<f64>() {
            Ok(num_value) => match operator {
                "=" => column_expr.eq(lit(num_value)),
                "!=" => column_expr.not_eq(lit(num_value)),
                ">" => column_expr.gt(lit(num_value)),
                "<" => column_expr.lt(lit(num_value)),
                ">=" => column_expr.gt_eq(lit(num_value)),
                "<=" => column_expr.lt_eq(lit(num_value)),
                _ => panic!("Unsupported operator in FILTER condition: '{}'", operator),
            },
            Err(_) => match operator {
                "=" => column_expr.eq(lit(value)),
                "!=" => column_expr.not_eq(lit(value)),
                ">" => column_expr.gt(lit(value)),
                "<" => column_expr.lt(lit(value)),
                ">=" => column_expr.gt_eq(lit(value)),
                "<=" => column_expr.lt_eq(lit(value)),
                _ => panic!("Unsupported operator in FILTER condition: '{}'", operator),
            },
        }
    }
    
    
    fn parse_condition_for_having(
        condition: &str,
        alias_map: &[(String, Expr)],
    ) -> Expr {
        let re = Regex::new(r"^(.+?)\s*(=|!=|>|<|>=|<=)\s*(.+)$").unwrap();
        let condition_trimmed = condition.trim();
    
        if !re.is_match(condition_trimmed) {
            panic!(
                "Invalid HAVING condition format: '{}'. Expected format: 'column operator value'",
                condition_trimmed
            );
        }
    
        let caps = re.captures(condition_trimmed).expect("Invalid HAVING condition format!");
        let column = caps.get(1).unwrap().as_str().trim();
        let operator = caps.get(2).unwrap().as_str().trim();
        let value = caps.get(3).unwrap().as_str().trim();
    
        // Resolve aggregated columns by mapping aliases to their original expressions
        let column_expr = alias_map
            .iter()
            .find(|(alias, _)| alias == column)
            .map(|(_, expr)| expr.clone())
            .unwrap_or_else(|| panic!("Invalid column '{}' in HAVING. Must be an aggregation alias.", column));
    
        // Parse the numeric or string value for comparison
        let parsed_value = if let Ok(num) = value.parse::<f64>() {
            lit(num) // Numeric value
        } else {
            lit(value) // String value
        };
    
        // Construct the HAVING condition
        match operator {
            "=" => column_expr.eq(parsed_value),
            "!=" => column_expr.not_eq(parsed_value),
            ">" => column_expr.gt(parsed_value),
            "<" => column_expr.lt(parsed_value),
            ">=" => column_expr.gt_eq(parsed_value),
            "<=" => column_expr.lt_eq(parsed_value),
            _ => panic!("Unsupported operator in HAVING condition: '{}'", operator),
        }
    }   
    
    

    pub fn display_query(&self) {
        println!("Generated SQL Query: {}", self.query);

    }

    /// Display the DataFrame
    // pub fn display(&self) -> BoxFuture<'_, Result<(), DataFusionError>> {
    //     let df = self.aggregated_df.as_ref().unwrap_or(&self.df);
    
    //     Box::pin(async move {
    //         let batches = df.clone().collect().await?;
    //         let schema = df.schema();
    
    //         // Filter the columns to display: retain selected columns and alias map
    //         let column_names: Vec<String> = self
    //             .selected_columns
    //             .iter()
    //             .filter(|col| {
    //                 // Include only aggregated or explicitly selected columns
    //                 self.alias_map
    //                     .iter()
    //                     .any(|(alias, _)| alias == *col)
    //                     || schema.fields().iter().any(|field| field.name() == *col)
    //             })
    //             .map(|col| {
    //                 // Prefer alias if exists
    //                 self.alias_map
    //                     .iter()
    //                     .find(|(alias, _)| alias == col)
    //                     .map(|(alias, _)| alias.clone())
    //                     .unwrap_or_else(|| col.clone())
    //             })
    //             .collect();
    
    //         // Print the column headers
    //         let header_row = column_names
    //             .iter()
    //             .map(|name| format!("{:<30}", name))
    //             .collect::<Vec<String>>()
    //             .join(" | ");
    //         println!("{}", header_row);
    
    //         // Print underscores below the headers
    //         let separator_row = column_names
    //             .iter()
    //             .map(|_| format!("{}", "-".repeat(30)))
    //             .collect::<Vec<String>>()
    //             .join(" | ");
    //         println!("{}", separator_row);
    
    //         // Iterate over batches and rows, but stop after 100 rows
    //         let mut row_count = 0;
    //         'outer: for batch in &batches {
    //             for row in 0..batch.num_rows() {
    //                 if row_count >= 100 {
    //                     break 'outer;
    //                 }
    
    //                 let mut row_data = Vec::new();
    //                 for col_name in &column_names {
    //                     // Match column names to schema fields
    //                     if let Some(col_index) = schema.fields().iter().position(|field| {
    //                         field.name() == col_name || field.name() == col_name.split(" AS ").next().unwrap()
    //                     }) {
    //                         let column = batch.column(col_index);
    
    //                         // Extract values based on column type
    //                         let value = if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
    //                             array.value(row).to_string()
    //                         } else if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
    //                             array.value(row).to_string()
    //                         } else if let Some(array) = column.as_any().downcast_ref::<Float64Array>() {
    //                             format!("{:.2}", array.value(row))
    //                         } else if let Some(array) = column.as_any().downcast_ref::<arrow::array::BooleanArray>() {
    //                             array.value(row).to_string()
    //                         } else if let Some(array) = column.as_any().downcast_ref::<arrow::array::Decimal128Array>() {
    //                             array.value(row).to_string()
    //                         } else if let Some(array) = column.as_any().downcast_ref::<Date32Array>() {
    //                             let days_since_epoch = array.value(row);
    //                             NaiveDate::from_ymd_opt(1970, 1, 1)
    //                                 .and_then(|epoch| epoch.checked_add_days(chrono::Days::new(days_since_epoch as u64)))
    //                                 .map(|valid_date| valid_date.to_string())
    //                                 .unwrap_or_else(|| "Invalid date".to_string())
    //                         } else if let Some(array) = column.as_any().downcast_ref::<Date64Array>() {
    //                             let millis_since_epoch = array.value(row);
    //                             let days_since_epoch = millis_since_epoch / (1000 * 60 * 60 * 24);
    //                             NaiveDate::from_ymd_opt(1970, 1, 1)
    //                                 .and_then(|epoch| epoch.checked_add_days(chrono::Days::new(days_since_epoch as u64)))
    //                                 .map(|valid_date| valid_date.to_string())
    //                                 .unwrap_or_else(|| "Invalid date".to_string())
    //                         } else {
    //                             "Unsupported Type".to_string()
    //                         };
    
    //                         row_data.push(value);
    //                     } else {
    //                         row_data.push("Column not found".to_string());
    //                     }
    //                 }
    
    //                 let formatted_row = row_data
    //                     .iter()
    //                     .map(|v| format!("{:<30}", v))
    //                     .collect::<Vec<String>>()
    //                     .join(" | ");
    //                 println!("{}", formatted_row);
    
    //                 row_count += 1;
    //             }
    //         }
    
    //         if row_count == 0 {
    //             println!("No data to display.");
    //         } else if row_count < 100 {
    //             println!("\nDisplayed all available rows (less than 100).");
    //         } else {
    //             println!("\nDisplayed the first 100 rows.");
    //         }
    
    //         Ok(())
    //     })
    // }
    
    pub fn display(&self) -> BoxFuture<'_, Result<(), DataFusionError>> {
        let df = self.aggregated_df.as_ref().unwrap_or(&self.df);
    
        Box::pin(async move {
            let batches = df.clone().collect().await?;
            let schema = df.schema();
            
            // Get schema column names and prepare for display
            let column_names = schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>();
            
            // Print the column headers
            let header_row = column_names
                .iter()
                .map(|name| format!("{:<20}", name))
                .collect::<Vec<String>>()
                .join(" | ");
            println!("{}", header_row);
            
            let separator_row = column_names
                .iter()
                .map(|_| format!("{}", "-".repeat(20)))
                .collect::<Vec<String>>()
                .join(" | ");
            println!("{}", separator_row);
            
            // Iterate over batches and rows, printing values
            let mut row_count = 0;
            'outer: for batch in &batches {
                for row in 0..batch.num_rows() {
                    if row_count >= 100 {
                        break 'outer;
                    }
    
                    let mut row_data = Vec::new();
                    for column in batch.columns() {
                        let value = if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
                            array.value(row).to_string()
                        } else if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
                            array.value(row).to_string()
                        } else if let Some(array) = column.as_any().downcast_ref::<Date32Array>() {
                            let days_since_epoch = array.value(row);
                            NaiveDate::from_num_days_from_ce_opt(1970 * 365 + days_since_epoch) // Safe date handling
                                .map(|d| d.to_string())
                                .unwrap_or_else(|| "Invalid date".to_string())
                        } else {
                            "Unsupported Type".to_string()
                        };
    
                        row_data.push(value);
                    }
    
                    let formatted_row = row_data
                        .iter()
                        .map(|v| format!("{:<20}", v))
                        .collect::<Vec<String>>()
                        .join(" | ");
                    println!("{}", formatted_row);
    
                    row_count += 1;
                }
            }
    
            if row_count == 0 {
                println!("No data to display.");
            } else if row_count < 100 {
                println!("\nDisplayed all available rows (less than 100).");
            } else {
                println!("\nDisplayed the first 100 rows.");
            }
    
            Ok(())
        })
    }
    
    
}

