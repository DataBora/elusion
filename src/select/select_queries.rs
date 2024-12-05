use arrow::array::Date64Array;
use datafusion::prelude::*;
use datafusion::error::DataFusionError; 
use futures::future::BoxFuture;
use datafusion::logical_expr::{Expr, col, SortExpr};
use arrow::array::{Date32Array,  Float64Array, Int32Array, StringArray};
use chrono::NaiveDate;
use regex::Regex;
use crate::loaders::csv_loader::AliasedDataFrame;
use datafusion::functions_aggregate::expr_fn::{sum, min, max, avg,stddev, count, count_distinct, corr, approx_percentile_cont, first_value,grouping,nth_value }; //last_value , median

pub struct CustomDataFrame {
    df: DataFrame,
    alias: String,
    query: String,
    // selected_columns: Vec<String>,
    alias_map: Vec<(String, Expr)>
}

use datafusion::prelude::Column;

fn col_with_relation(relation: &str, column: &str) -> Expr {
    if column.contains('.') {
        // Directly construct a column without modifying it
        Expr::Column(Column::from(column))
    } else {
        // Construct a fully qualified column
        let qualified_name = format!("{}.{}", relation, column);
        Expr::Column(Column::from(qualified_name.as_str()))
    }
}


impl CustomDataFrame {
    /// Create a new CustomDataFrame with an optional alias
    pub fn new(aliased_df: AliasedDataFrame) -> Self {
        CustomDataFrame {
            df: aliased_df.dataframe,
            alias: aliased_df.alias,
            query: String::new(),
            // selected_columns: Vec::new(),
            alias_map: Vec::new()
        }
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
            self.alias = alias.to_string(); // Set the alias
        }
    
        self
    }
    

    /// SELECT clause
    pub fn select(mut self, columns: Vec<&str>) -> Self {
        let mut expressions = Vec::new();
    
        for &col in &columns {
            if let Some(alias_start) = col.to_uppercase().find(" AS ") {
                // Extract the original column and alias
                let original_expression = &col[..alias_start].trim();
                let alias = col[alias_start + 4..].trim();
    
                // Parse and append only the original column name
                let parsed_expr = self.parse_aggregate_function(original_expression);
                expressions.push(parsed_expr.alias(alias.to_string()));
            } else {
                // If no alias, parse normally
                let parsed_expr = self.parse_aggregate_function(col);
                expressions.push(parsed_expr);
            }
        }
    
        println!("Parsed SELECT expressions: {:?}", expressions);
    
        self.df = self.df.select(expressions).expect("Failed to apply SELECT.");
        self.query = format!(
            "SELECT {} FROM {}",
            columns.join(", "),
            self.alias
        );
    
        self
    }
    
    
    
    
    
    
    
    //GROUP BY clause
    pub fn group_by(mut self, group_columns: Vec<&str>) -> Self {
        let group_exprs: Vec<Expr> = group_columns
            .iter()
            .map(|&col| col_with_relation(self.alias.as_str(), col))
            .collect();
    
        self.query = format!(
            "{} GROUP BY {}",
            self.query.trim_end(),
            group_columns
                .iter()
                .map(|&col| format!("{}.{}", self.alias, col))
                .collect::<Vec<_>>()
                .join(", ")
        );
    
        self.df = self
            .df
            .aggregate(group_exprs, vec![])
            .expect("Failed to apply GROUP BY.");
    
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
    

    
    
    
    

    /// ORDER BY clause
    pub fn order_by(mut self, columns: Vec<&str>, ascending: Vec<bool>) -> Self {
        assert!(
            columns.len() == ascending.len(),
            "The number of columns and sort directions must match"
        );
    
        let sort_exprs: Vec<SortExpr> = columns
            .iter()
            .zip(ascending.iter())
            .map(|(&col, &asc)| {
                SortExpr {
                    expr: col_with_relation(self.alias.as_str(), col),
                    asc,
                    nulls_first: true,
                }
            })
            .collect();
    
        self.query = format!(
            "{} ORDER BY {}",
            self.query.trim_end(),
            columns
                .iter()
                .zip(ascending.iter())
                .map(|(&col, &asc)| format!("{} {}", col, if asc { "ASC" } else { "DESC" }))
                .collect::<Vec<_>>()
                .join(", ")
        );
    
        self.df = self.df.sort(sort_exprs).expect("Failed to apply ORDER BY.");
    
        self
    }
    
    

    /// LIMIT clause
    pub fn limit(mut self, count: usize) -> Self {
        self.query = format!("{} LIMIT {}", self.query, count);
    
        self.df = self.df.limit(0, Some(count)).expect("Failed to apply LIMIT.");
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
                    .unwrap_or_else(|| format!("{}.{}", self.alias, left));
    
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
            .unwrap_or_else(|| format!("{}.{}", self.alias, column));
    
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
                            .unwrap_or_else(|| format!("{}.{}", self.alias, col))
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
                            .unwrap_or_else(|| format!("{}.{}", self.alias, col))
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
    

    
    //-------------- PARSING FUNCTIONS ---------------

   

    fn parse_aggregate_function(&self, aggregate: &str) -> Expr {
        println!("Parsing aggregate function: {}", aggregate);
    
        // Regex to capture function, column, and optional alias
        let re = Regex::new(
            r"^(?i)(MIN|MAX|AVG|MEDIAN|SUM|COUNT|COUNT_DISTINCT|STDEV|CORR|FIRST_VALUE|NTH_VALUE|APPROX_PERCENTILE_CONT)\(([^)]+)\)(?:\s+AS\s+(.+))?$",
        )
        .expect("Failed to compile regex");
    
        if let Some(caps) = re.captures(aggregate) {
            let func = caps.get(1).unwrap().as_str().to_uppercase(); // Extract function name
            let column = caps.get(2).unwrap().as_str().trim(); // Extract column name
            // let alias = caps.get(3).map(|m| m.as_str().trim().to_string()); // Extract alias if present
    
            println!("Function: {}, Column: {}", func, column);
    
            // Use col_with_relation to ensure the column has the correct table relation
            let qualified_column_expr = col_with_relation(self.alias.as_str(), column);
    
            // Build the aggregate expression
            let agg_expr = match func.as_str() {
                "SUM" => sum(qualified_column_expr),
                "MIN" => min(qualified_column_expr),
                "MAX" => max(qualified_column_expr),
                "AVG" => avg(qualified_column_expr),
                "MEDIAN" => approx_percentile_cont(qualified_column_expr, lit(0.5), None),
                "STDDEV" => stddev(qualified_column_expr),
                "COUNT" => count(qualified_column_expr),
                "COUNT_DISTINCT" => count_distinct(qualified_column_expr),
                "CORR" => {
                    let args: Vec<&str> = column.split(',').map(|s| s.trim()).collect();
                    if args.len() == 2 {
                        corr(
                            col_with_relation(self.alias.as_str(), args[0]),
                            col_with_relation(self.alias.as_str(), args[1]),
                        )
                    } else {
                        panic!("CORR requires two columns, e.g., CORR(col1, col2).");
                    }
                }
                "APPROX_PERCENTILE_CONT" => {
                    let args: Vec<&str> = column.split(',').map(|s| s.trim()).collect();
                    if args.len() == 2 {
                        approx_percentile_cont(
                            col_with_relation(self.alias.as_str(), args[0]),
                            lit(args[1].parse::<f64>().expect("Invalid percentile")),
                            None,
                        )
                    } else if args.len() == 3 {
                        approx_percentile_cont(
                            col_with_relation(self.alias.as_str(), args[0]),
                            lit(args[1].parse::<f64>().expect("Invalid percentile")),
                            Some(col_with_relation(self.alias.as_str(), args[2])),
                        )
                    } else {
                        panic!("APPROX_PERCENTILE_CONT requires 2 or 3 arguments.");
                    }
                }
                "FIRST_VALUE" => {
                    let args: Vec<&str> = column.split(',').map(|s| s.trim()).collect();
                    if args.len() == 1 {
                        first_value(col_with_relation(self.alias.as_str(), args[0]), None)
                    } else if args.len() == 2 {
                        first_value(
                            col_with_relation(self.alias.as_str(), args[0]),
                            Some(vec![SortExpr {
                                expr: col_with_relation(self.alias.as_str(), args[1]),
                                asc: true,
                                nulls_first: true,
                            }]),
                        )
                    } else {
                        panic!("FIRST_VALUE requires 1 or 2 arguments.");
                    }
                }
                "NTH_VALUE" => {
                    let args: Vec<&str> = column.split(',').map(|s| s.trim()).collect();
                    if args.len() == 3 {
                        nth_value(
                            col_with_relation(self.alias.as_str(), args[0]),
                            args[1].parse::<i64>().expect("Invalid nth value"),
                            vec![SortExpr {
                                expr: col_with_relation(self.alias.as_str(), args[2]),
                                asc: true,
                                nulls_first: true,
                            }],
                        )
                    } else {
                        panic!("NTH_VALUE requires 3 arguments.");
                    }
                }
                "GROUPING" => grouping(qualified_column_expr),
                _ => panic!("Unsupported aggregate function: {}", func),
            };
    
            // Apply alias if provided
            if let Some(alias) = caps.get(3) {
                agg_expr.alias(alias.as_str().to_string())
            } else {
                agg_expr
            }
        } else {
            // Treat as plain column
            col_with_relation(self.alias.as_str(), aggregate)
        }
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
    
            // Collect data from the DataFrame
            let batches = df.clone().collect().await?;
            let schema = df.schema();
    
            // Map schema column names to their aliases where applicable
            let column_names: Vec<String> = schema
                .fields()
                .iter()
                .map(|field| {
                    // Check if the column has an alias in the alias_map
                    self.alias_map
                        .iter()
                        .find(|(_, expr)| expr.to_string() == *field.name())
                        .map(|(alias, _)| alias.clone())
                        .unwrap_or_else(|| field.name().clone())
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
                        // Find the column index by name
                        if let Some(col_index) = schema.fields().iter().position(|field| {
                            // Match either original name or alias
                            self.alias_map
                                .iter()
                                .find(|(alias, expr)| alias == col_name || expr.to_string() == *field.name())
                                .is_some()
                        }) {
                            let column = batch.column(col_index);
    
                            // Match column type and extract values
                            let value = if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
                                array.value(row).to_string()
                            } else if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
                                array.value(row).to_string()
                            } else if let Some(array) = column.as_any().downcast_ref::<Float64Array>() {
                                format!("{:.2}", array.value(row))
                            } else if let Some(array) = column.as_any().downcast_ref::<Date32Array>() {
                                let days_since_epoch = array.value(row);
                                match NaiveDate::from_ymd_opt(1970, 1, 1)
                                    .and_then(|epoch| epoch.checked_add_days(chrono::Days::new(days_since_epoch as u64)))
                                {
                                    Some(valid_date) => valid_date.to_string(),
                                    None => "Invalid date".to_string(),
                                }
                            } else if let Some(array) = column.as_any().downcast_ref::<Date64Array>() {
                                let millis_since_epoch = array.value(row);
                                let days_since_epoch = millis_since_epoch / (1000 * 60 * 60 * 24);
                                match NaiveDate::from_ymd_opt(1970, 1, 1)
                                    .and_then(|epoch| epoch.checked_add_days(chrono::Days::new(days_since_epoch as u64)))
                                {
                                    Some(valid_date) => valid_date.to_string(),
                                    None => "Invalid date".to_string(),
                                }
                            } else {
                                "Unsupported Type".to_string()
                            };
    
                            row_data.push(value);
                        } else {
                            row_data.push("Column not found".to_string());
                        }
                    }
    
                    // Print the formatted row
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



