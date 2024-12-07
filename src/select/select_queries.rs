
use crate::AggregationBuilder;

use arrow::array::Date64Array;
use datafusion::prelude::*;
use datafusion::error::DataFusionError; 
use futures::future::BoxFuture;
use datafusion::logical_expr::{Expr, col, SortExpr};
use arrow::array::{Date32Array,  Float64Array, Int32Array, StringArray};
use chrono::NaiveDate;
use regex::Regex;
use crate::loaders::csv_loader::AliasedDataFrame;

// use log::debug;



pub struct CustomDataFrame {
    pub df: DataFrame,
    pub table_alias: String,
    query: String,
    selected_columns: Vec<String>,
    alias_map: Vec<(String, Expr)>
}


fn col_with_relation(relation: &str, column: &str) -> Expr {
    if column.contains('.') {
        col(column) // Already qualified
    } else {
        col(&format!("{}.{}", relation, column)) // Add table alias
    }
}



impl CustomDataFrame {
    /// Create a new CustomDataFrame with an optional alias
    pub fn new(aliased_df: AliasedDataFrame) -> Self {
        CustomDataFrame {
            df: aliased_df.dataframe,
            table_alias: aliased_df.alias,
            query: String::new(),
            selected_columns: Vec::new(),
            alias_map: Vec::new()
        }
    }

   

    pub fn display_query_plan(&self) {
        println!("Generated Logical Plan:");
        println!("{:?}", self.df.logical_plan());
       
    
    }
    
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

    /// SELECT clause
    // pub fn select(mut self, columns: Vec<&str>) -> Self {
    //     let mut expressions: Vec<Expr> = vec![];
    //     let mut final_columns: Vec<String> = vec![];
    
    //     for &col in &columns {
    //         // Check if the column is already aliased
    //         if let Some((alias, expr)) = self.alias_map.iter().find(|(_, expr)| match expr {
    //             Expr::Alias(alias_struct) => match *alias_struct.expr {
    //                 Expr::Column(ref column) => column.name() == col,
    //                 _ => false,
    //             },
    //             _ => false,
    //         }) {
    //             // If it's in alias_map, replace the column with the alias
    //             expressions.push(expr.clone());
    //             final_columns.push(alias.clone());
    //         } else {
    //             // Otherwise, include the original column
    //             expressions.push(col_with_relation(&self.table_alias, col));
    //             final_columns.push(col.to_string());
    //         }
    //     }
    
    //     // Add any aggregated columns (from alias_map) that are not already included
    //     for (alias, expr) in &self.alias_map {
    //         if !final_columns.contains(alias) {
    //             expressions.push(expr.clone());
    //             final_columns.push(alias.clone());
    //         }
    //     }
    
    //     // Apply the select operation
    //     self.df = self
    //         .df
    //         .select(expressions.clone())
    //         .expect("Failed to apply SELECT.");
    
    //     // Update selected columns and query
    //     self.selected_columns = final_columns.clone();
    
    //     self.query = format!(
    //         "SELECT {} FROM {}",
    //         final_columns.join(", "),
    //         self.table_alias
    //     );
    
    //     self
    // }

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
                expressions.push(col_with_relation(&self.table_alias, col));
                selected_columns.push(col.to_string());
            }
        }
    
        self.selected_columns = selected_columns;
        self.df = self.df.select(expressions).expect("Failed to apply SELECT.");
    
        // Update query string
        self.query = format!(
            "SELECT {} FROM {}",
            self.selected_columns.join(", "),
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
        let expr = self.parse_condition(condition);
        self.df = self.df.filter(expr).expect("Failed to apply WHERE filter");
    
        self
    }
    
    pub fn having(mut self, condition: &str) -> Self {
        println!("Condition passed to HAVING: '{}'", condition);
    
        self.query = format!("{} HAVING {}", self.query.trim_end(), condition);
    
        // Parse the condition
        let expr = self.parse_condition(condition);
        self.df = self.df.filter(expr).expect("Failed to apply HAVING filter");
    
        self
    }
  

    /// JOIN clause
    pub fn join(
        mut self,
        other_df: AliasedDataFrame,
        join_keys: Vec<(&str, &str)>,
        join_type: JoinType,
    ) -> Self {
        let join_condition: Vec<String> = join_keys
            .iter()
            .map(|(left, right)| {
                let original_left = self
                    .alias_map
                    .iter()
                    .find(|(alias, _)| alias == *left)
                    .map(|(_, expr)| expr.to_string())
                    .unwrap_or_else(|| format!("{}.{}", self.table_alias, left));
    
                format!(
                    "{} = {}.{}",
                    original_left,
                    other_df.alias,
                    right
                )
            })
            .collect();
    
        self.query = format!(
            "{} {} JOIN {} ON {}",
            self.query,
            match join_type {
                JoinType::Inner => "INNER",
                JoinType::Left => "LEFT",
                JoinType::Right => "RIGHT",
                JoinType::Full => "FULL",
                JoinType::LeftSemi => "LEFT SEMI",
                JoinType::RightSemi => "RIGHT SEMI",
                JoinType::LeftAnti => "LEFT ANTI",
                JoinType::RightAnti => "RIGHT ANTI",
                JoinType::LeftMark => "LEFT MARK",
            },
            other_df.alias,
            join_condition.join(" AND ")
        );
    
        let (left_cols, right_cols): (Vec<&str>, Vec<&str>) = join_keys.iter().cloned().unzip();
    
        self.df = self
            .df
            .join(
                other_df.dataframe,
                join_type,
                &left_cols,
                &right_cols,
                None,
            )
            .expect("Failed to apply JOIN.");
    
        self
    }
    

    
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
    


    fn parse_condition(&self, condition: &str) -> Expr {
        let re = Regex::new(r"^(.+?)\s*(=|!=|>|<|>=|<=)\s*(.+)$").unwrap();
    
        // Ensure the condition is trimmed and matches the expected format
        let condition_trimmed = condition.trim();
        if !re.is_match(condition_trimmed) {
            panic!("Invalid condition format: '{}'. Expected format: 'column operator value'", condition_trimmed);
        }
    
        let caps = re.captures(condition_trimmed).expect("Invalid condition format!");
    
        let column = caps.get(1).unwrap().as_str().trim();
        let operator = caps.get(2).unwrap().as_str().trim();
        let value = caps.get(3).unwrap().as_str().trim().trim_matches('\'');
    
        match operator {
            "=" => col(column).eq(lit(value)),
            "!=" => col(column).not_eq(lit(value)),
            ">" => col(column).gt(lit(value)),
            "<" => col(column).lt(lit(value)),
            ">=" => col(column).gt_eq(lit(value)),
            "<=" => col(column).lt_eq(lit(value)),
            _ => panic!("Unsupported operator in condition: '{}'", operator),
        }
    }
    
    

    pub fn display_query(&self) {
        println!("Generated SQL Query: {}", self.query);

    }

    /// Display the DataFrame
    pub fn display(&self) -> BoxFuture<'_, Result<(), DataFusionError>> {
        Box::pin(async move {
            let df = &self.df;
            let batches = df.clone().collect().await?;
            let schema = df.schema();
    
            // Filter the columns to display: retain selected columns and alias map
            let column_names: Vec<String> = self
                .selected_columns
                .iter()
                .filter(|col| {
                    // Include only aggregated or explicitly selected columns
                    self.alias_map
                        .iter()
                        .any(|(alias, _)| alias == *col)
                        || schema.fields().iter().any(|field| field.name() == *col)
                })
                .map(|col| {
                    // Prefer alias if exists
                    self.alias_map
                        .iter()
                        .find(|(alias, _)| alias == col)
                        .map(|(alias, _)| alias.clone())
                        .unwrap_or_else(|| col.clone())
                })
                .collect();
    
            // Print the column headers
            let header_row = column_names
                .iter()
                .map(|name| format!("{:<30}", name))
                .collect::<Vec<String>>()
                .join(" | ");
            println!("{}", header_row);
    
            // Print underscores below the headers
            let separator_row = column_names
                .iter()
                .map(|_| format!("{}", "-".repeat(30)))
                .collect::<Vec<String>>()
                .join(" | ");
            println!("{}", separator_row);
    
            // Iterate over each batch and print rows
            for batch in batches {
                for row in 0..batch.num_rows() {
                    let mut row_data = Vec::new();
    
                    for col_name in &column_names {
                        // Match column names to schema fields
                        if let Some(col_index) = schema.fields().iter().position(|field| {
                            field.name() == col_name || field.name() == col_name.split(" AS ").next().unwrap()
                        }) {
                            let column = batch.column(col_index);
    
                            // Extract values based on column type
                            let value = if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
                                array.value(row).to_string()
                            } else if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
                                array.value(row).to_string()
                            } else if let Some(array) = column.as_any().downcast_ref::<Float64Array>() {
                                format!("{:.2}", array.value(row))
                            } else if let Some(array) = column.as_any().downcast_ref::<Date32Array>() {
                                let days_since_epoch = array.value(row);
                                NaiveDate::from_ymd_opt(1970, 1, 1)
                                    .and_then(|epoch| epoch.checked_add_days(chrono::Days::new(days_since_epoch as u64)))
                                    .map(|valid_date| valid_date.to_string())
                                    .unwrap_or_else(|| "Invalid date".to_string())
                            } else if let Some(array) = column.as_any().downcast_ref::<Date64Array>() {
                                let millis_since_epoch = array.value(row);
                                let days_since_epoch = millis_since_epoch / (1000 * 60 * 60 * 24);
                                NaiveDate::from_ymd_opt(1970, 1, 1)
                                    .and_then(|epoch| epoch.checked_add_days(chrono::Days::new(days_since_epoch as u64)))
                                    .map(|valid_date| valid_date.to_string())
                                    .unwrap_or_else(|| "Invalid date".to_string())
                            } else {
                                "Unsupported Type".to_string()
                            };
    
                            row_data.push(value);
                        } else {
                            row_data.push("Column not found".to_string());
                        }
                    }
    
                    let formatted_row = row_data
                        .iter()
                        .map(|v| format!("{:<30}", v))
                        .collect::<Vec<String>>()
                        .join(" | ");
                    println!("{}", formatted_row);
                }
            }
    
            Ok(())
        })
    }
    
    
}

