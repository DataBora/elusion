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
    selected_columns: Vec<String>,
}

impl CustomDataFrame {
    /// Create a new CustomDataFrame with an optional alias
    pub fn new(aliased_df: AliasedDataFrame) -> Self {
        CustomDataFrame {
            df: aliased_df.dataframe,
            alias: aliased_df.alias,
            query: String::new(),
            selected_columns: Vec::new(),
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
        let mut aliases = vec![]; // Create a mutable aliases vector
        
        let expressions: Vec<Expr> = columns.iter().map(|&col| {
            let expr = self.parse_aggregate_function(col);
    
            if let Some(alias_start) = col.to_uppercase().find(" AS ") {
                // Extract and push alias
                let alias = col[alias_start + 4..].trim().to_string();
                aliases.push(alias.clone());
                expr.alias(alias)
            } else {
                // Push plain column name if no alias
                aliases.push(col.to_string());
                expr
            }
        }).collect();
    
        println!("Expressions: {:?}", expressions);
        println!("Parsed columns with aliases: {:?}", aliases);
    
        self.df = self.df.select(expressions).expect("Failed to apply SELECT.");
    
        // Use `clone` to avoid moving `aliases`
        self.selected_columns = aliases.clone();
    
        // Update self.query with the SELECT clause if not already present
        if !self.query.to_uppercase().starts_with("SELECT") {
            self.query = format!("SELECT {} {}", aliases.join(", "), self.query);
        }
    
        self
    }
    
    

     /// GROUP BY clause
    //  pub fn group_by(mut self, group_columns: Vec<&str>) -> Self {
    //     // Create grouping expressions using original column names
    //     let group_exprs: Vec<Expr> = group_columns
    //         .iter()
    //         .map(|&col_name| col(col_name)) // Use original column names
    //         .collect();
    
    //     // Prepare aggregate expressions for non-grouped columns
    //     let aggregate_exprs: Vec<Expr> = self
    //         .selected_columns
    //         .iter()
    //         .filter(|col_name| !group_columns.contains(&col_name.as_str()))
    //         .map(|col_name| self.parse_aggregate_function(col_name))
    //         .collect();
    
    //     // Construct the SQL GROUP BY clause
    //     self.query = format!(
    //         "{} GROUP BY {}",
    //         self.query,
    //         group_columns
    //             .iter()
    //             .map(|&col| format!("{}.{}", self.alias, col))
    //             .collect::<Vec<_>>()
    //             .join(", ")
    //     );
    
    //     // Apply grouping and aggregation to the DataFrame
    //     self.df = self
    //         .df
    //         .aggregate(group_exprs, aggregate_exprs)
    //         .expect("Failed to apply GROUP BY.");
    
    //     self
    // }
    pub fn group_by(mut self, group_columns: Vec<&str>) -> Self {
        let group_exprs: Vec<Expr> = group_columns.iter().map(|&col_name| col(col_name)).collect();
        let aggregate_exprs: Vec<Expr> = self.selected_columns.iter()
            .filter(|col_name| !group_columns.contains(&col_name.as_str()))
            .map(|col_name| self.parse_aggregate_function(col_name))
            .collect();
        self.query = format!(
            "{} GROUP BY {}",
            self.query,
            group_columns.iter()
                .map(|&col| format!("{}.{}", self.alias, col))
                .collect::<Vec<_>>()
                .join(", ")
        );
    
        // Include aggregate functions in the query
        if !aggregate_exprs.is_empty() {
            let aggregates_sql: Vec<String> = aggregate_exprs.iter().map(|expr| format!("{:?}", expr)).collect();
            self.query = format!("SELECT {}, {} {}", group_columns.join(", "), aggregates_sql.join(", "), self.query);
        }
    
        self.df = self.df.aggregate(group_exprs, aggregate_exprs).expect("Failed to apply GROUP BY.");
        self
    }
    
    
    
    
    
    

    /// WHERE clause
    pub fn filter(mut self, condition: &str) -> Self {
        let column_name = condition.split_whitespace().next().unwrap(); // Get the column being filtered
        if !self.selected_columns.contains(&column_name.to_string()) {
            // Add the column to selected columns if it's not already included
            self.query = format!("SELECT {}, {} ", column_name, self.query);
        }
    
        self.query = format!("{} WHERE {}", self.query, condition);
    
        let expr = self.parse_condition(condition);
        self.df = self.df.filter(expr).expect("Failed to apply WHERE filter");
        self
    }
    
    

    /// ORDER BY clause
    pub fn order_by(mut self, columns: Vec<&str>, ascending: Vec<bool>) -> Self {
        assert!(
            columns.len() == ascending.len(),
            "The number of columns and sort directions must match"
        );
    
        let table_name = &self.alias;
    
        // Construct the SQL representation for the ORDER BY clause
        let column_order: Vec<String> = columns
            .iter()
            .zip(&ascending)
            .map(|(col, asc)| {
                let qualified_col = if table_name.is_empty() {
                    col.to_string()
                } else {
                    format!("{}.{}", table_name, col)
                };
                format!("{} {}", qualified_col, if *asc { "ASC" } else { "DESC" })
            })
            .collect();
    
        self.query = format!("{} ORDER BY {}", self.query, column_order.join(", "));
    
        // Create SortExprs for DataFusion
        let sort_exprs: Vec<SortExpr> = columns
            .into_iter()
            .zip(ascending.into_iter())
            .map(|(col, asc)| {
                SortExpr {
                    expr: Expr::Column(format!("{}.{}", table_name, col).into()), // Use Expr::Column
                    asc,
                    nulls_first: true, // Adjust this based on your requirements
                }
            })
            .collect();
    
        // Apply sorting directly on the DataFrame
        self.df = self
            .df
            .sort(sort_exprs)
            .expect("Failed to apply ORDER BY.");
    
        self
    }
    
    
    

    /// LIMIT clause
    pub fn limit(mut self, count: usize) -> Self {
        self.query = format!("{} LIMIT {}", self.query, count);
    
        self.df = self.df.limit(0, Some(count)).expect("Failed to apply LIMIT.");
        self
    }
    
   

    /// HAVING clause
    pub fn having(mut self, condition: &str) -> Self {
        self.query = format!("{} HAVING {}", self.query, condition);
    
        let qualified_condition = condition.replace(".", &format!("{}.", self.alias));
    
        let expr = self.parse_condition(&qualified_condition);
    
        self.df = self
            .df
            .filter(expr)
            .expect("Failed to apply HAVING filter");
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
                format!(
                    "{}.{} = {}.{}",
                    self.alias,
                    left,
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
            .join(other_df.dataframe, join_type, &left_cols, &right_cols, None)
            .expect(&format!(
                "Failed to apply JOIN between {} and {}.",
                self.alias, other_df.alias
            ));
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
        let qualified_column = format!("{}.{}", self.alias, column);
    
        let partition_str = if !partition_by.is_empty() {
            format!(
                "PARTITION BY {}",
                partition_by
                    .iter()
                    .map(|&col| format!("{}.{}", self.alias, col))
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
                    .map(|&col| format!("{}.{}", self.alias, col))
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
            qualified_column,
            partition_str,
            order_str
        );
    
        // No modification to the DataFrame since DataFusion may not fully support window functions
        self
    }
    
    //-------------- PARSING FUNCTIONS ---------------

    fn parse_aggregate_function(&self, aggregate: &str) -> Expr {
        println!("Parsing aggregate function: {}", aggregate);
    
        // Updated regex to capture function, column, and alias (optional)
        let re = Regex::new(
            r"^(?i)(MIN|MAX|AVG|MEDIAN|SUM|COUNT|COUNT_DISTINCT|STDEV|CORR|FIRST_VALUE|NTH_VALUE|APPROX_PERCENTILE_CONT)\(([^)]+)\)(?:\s+AS\s+(.+))?$",
        )
        .expect("Failed to compile regex");
    
        if let Some(caps) = re.captures(aggregate) {
            let func = caps.get(1).unwrap().as_str().to_uppercase(); // Extract function name
            let column = caps.get(2).unwrap().as_str().trim(); // Extract column name
            let alias = caps.get(3).map(|m| m.as_str().trim().to_string()); // Extract alias if present
    
            println!("Function: {}, Column: {}, Alias: {:?}", func, column, alias);
    
            // Build the appropriate aggregate expression
            let agg_expr = match func.as_str() {
                "SUM" => sum(col(column)),
                "MIN" => min(col(column)),
                "MAX" => max(col(column)),
                "AVG" => avg(col(column)),
                "MEDIAN" => approx_percentile_cont(col(column), lit(0.5), None),
                "STDDEV" => stddev(col(column)),
                "COUNT" => count(col(column)),
                "COUNT_DISTINCT" => count_distinct(col(column)),
                "CORR" => {
                    let args: Vec<&str> = column.split(',').map(|s| s.trim()).collect();
                    if args.len() == 2 {
                        corr(col(args[0]), col(args[1]))
                    } else {
                        panic!("CORR requires two columns, e.g., CORR(col1, col2).");
                    }
                }
                "APPROX_PERCENTILE_CONT" => {
                    let args: Vec<&str> = column.split(',').map(|s| s.trim()).collect();
                    if args.len() == 2 {
                        approx_percentile_cont(
                            col(args[0]),
                            lit(args[1].parse::<f64>().expect("Invalid percentile")),
                            None,
                        )
                    } else if args.len() == 3 {
                        approx_percentile_cont(
                            col(args[0]),
                            lit(args[1].parse::<f64>().expect("Invalid percentile")),
                            Some(col(args[2])),
                        )
                    } else {
                        panic!("APPROX_PERCENTILE_CONT requires 2 or 3 arguments.");
                    }
                }
                "FIRST_VALUE" => {
                    let args: Vec<&str> = column.split(',').map(|s| s.trim()).collect();
                    if args.len() == 1 {
                        first_value(col(args[0]), None)
                    } else if args.len() == 2 {
                        first_value(
                            col(args[0]),
                            Some(vec![SortExpr {
                                expr: col(args[1]),
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
                            col(args[0]),
                            args[1].parse::<i64>().expect("Invalid nth value"),
                            vec![SortExpr {
                                expr: col(args[2]),
                                asc: true,
                                nulls_first: true,
                            }],
                        )
                    } else {
                        panic!("NTH_VALUE requires 3 arguments.");
                    }
                }
                "GROUPING" => grouping(col(column)),
                _ => panic!("Unsupported aggregate function: {}", func),
            };
    
            // Apply alias if provided
            if let Some(alias_name) = alias {
                agg_expr.alias(alias_name)
            } else {
                agg_expr
            }
        } else {
            // Not an aggregate function, treat as a plain column
            println!("Treating as plain column: {}", aggregate);
            col(aggregate)
        }
    }
    
    
    
    

    fn parse_condition(&self, condition: &str) -> Expr {
        let re = Regex::new(r"^(.+?)\s*(=|!=|>|<|>=|<=)\s*(.+)$").unwrap();
        let caps = re.captures(condition).expect("Invalid condition format");

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
            _ => panic!("Unsupported operator in condition"),
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
    
            // Retrieve column names (fall back to schema fields if selected columns are empty)
            let column_names = if self.selected_columns.is_empty() {
                schema.fields().iter().map(|field| field.name().clone()).collect::<Vec<_>>()
            } else {
                self.selected_columns.clone()
            };
    
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
                        if let Some(col_index) = schema.fields()
                            .iter()
                            .position(|field| field.name() == col_name) {
                            
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
                                    .and_then(|epoch| epoch.checked_add_days(chrono::Days::new(days_since_epoch as u64))) {
                                    Some(valid_date) => valid_date.to_string(),
                                    None => "Invalid date".to_string(),
                                }
                            } else if let Some(array) = column.as_any().downcast_ref::<Date64Array>() {
                                let millis_since_epoch = array.value(row);
                                let days_since_epoch = millis_since_epoch / (1000 * 60 * 60 * 24);
                                match NaiveDate::from_ymd_opt(1970, 1, 1)
                                    .and_then(|epoch| epoch.checked_add_days(chrono::Days::new(days_since_epoch as u64))) {
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



