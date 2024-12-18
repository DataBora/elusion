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
use arrow::array::{StringArray, Date32Array,Date64Array, Float64Array, Decimal128Array, Int32Array, Int64Array, ArrayRef, Array};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaBuilder;

#[derive(Clone)]
pub struct CustomDataFrame {
    pub df: DataFrame,
    pub table_alias: String,

    from_table: String,
    selected_columns: Vec<String>,
    alias_map: Vec<(String, Expr)>,
    aggregations: Vec<(String, Expr)>,
    group_by_columns: Vec<String>,
    where_conditions: Vec<String>,
    having_conditions: Vec<String>,
    order_by_columns: Vec<(String, bool)>,
    limit_count: Option<usize>,

    // Store JOIN clauses
    joins: Vec<JoinClause>,

    // Store WINDOW definitions
    window_functions: Vec<WindowDefinition>,

    // Store WITH CTEs
    ctes: Vec<CTEDefinition>,

    // Store SUBQUERY information
    subquery_source: Option<Box<CustomDataFrame>>,

    // Store SET operations: UNION, INTERSECT, EXCEPT
    set_operations: Vec<SetOperation>,

    query: String,
    aggregated_df: Option<DataFrame>,
}

// Structures to hold additional info
#[derive(Clone)]
struct JoinClause {
    join_type: JoinType,
    table: String,
    alias: String,
    on_left: String,
    on_right: String,
}

#[derive(Clone)]
struct WindowDefinition {
    func: String,
    column: String,
    partition_by: Vec<String>,
    order_by: Vec<String>,
    alias: Option<String>,
}

#[derive(Clone)]
struct CTEDefinition {
    name: String,
    cte_df: CustomDataFrame,
}

#[derive(Clone)]
enum SetOperationType {
    Union,
    Intersect,
    Except
}

#[derive(Clone)]
struct SetOperation {
    op_type: SetOperationType,
    df: CustomDataFrame,
    all: bool,
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
    name.to_lowercase()
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
                query: String::new(),
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
            let mut schema_builder = SchemaBuilder::new();
            let mut new_batches = Vec::new();
            let mut updated_fields = Vec::new();

            // Process each batch for date conversion
            for batch in &batches {
                let mut columns = Vec::new();

                for (i, field) in schema.fields().iter().enumerate() {
                    if field.data_type() == &ArrowDataType::Utf8 && field.name().contains("date") {
                        let column = batch.column(i);
                        let string_array = column
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .expect("Column is not a StringArray");
                        
                        let date_values = string_array
                            .iter()
                            .map(|value| value.and_then(|v| parse_date_with_formats(v)))
                            .collect::<Vec<_>>();

                        let date_array: ArrayRef = Arc::new(Date32Array::from(date_values));
                        columns.push(date_array);
                        updated_fields.push(Field::new(field.name(), ArrowDataType::Date32, field.is_nullable()));
                    } else {
                        // Retain other columns
                        columns.push(batch.column(i).clone());
                        updated_fields.push(field.as_ref().clone());
                    }
                }

                let temp_schema = Arc::new(Schema::new(updated_fields.clone()));
                let new_batch = RecordBatch::try_new(temp_schema.clone(), columns)?;
                new_batches.push(new_batch);
                updated_fields.clear();
            }

            // Finalize the schema
            for field in schema.fields() {
                if field.data_type() == &ArrowDataType::Utf8 && field.name().contains("date") {
                    schema_builder.push(Field::new(field.name(), ArrowDataType::Date32, field.is_nullable()));
                } else {
                    schema_builder.push(field.as_ref().clone());
                }
            }
            let final_schema = Arc::new(schema_builder.finish());

            let partitions: Vec<Vec<RecordBatch>> = new_batches.into_iter().map(|batch| vec![batch]).collect();
            let mem_table = MemTable::try_new(final_schema.clone(), partitions)?;

            ctx.register_table(alias, Arc::new(mem_table))?;

            println!("Registering table with alias: {}", alias);
            println!("Loading file: {}", file_path);
            println!("Loaded schema: {:?}", final_schema);

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

    // ---------------------  STORING TRANSFORMATIONS ---------------------//
    pub fn from_subquery(mut self, sub_df: CustomDataFrame, alias: &str) -> Self {
        // Treat sub_df as a subquery source
        self.subquery_source = Some(Box::new(sub_df));
        self.table_alias = alias.to_string();
        self.from_table = alias.to_string();
        self
    }

    // WITH CTE
    pub fn with_cte(mut self, name: &str, cte_df: CustomDataFrame) -> Self {
        self.ctes.push(CTEDefinition {
            name: name.to_string(),
            cte_df,
        });
        self
    }

    // Set operations: UNION, INTERSECT, EXCEPT
    pub fn union(mut self, other: CustomDataFrame, all: bool) -> Self {
        self.set_operations.push(SetOperation {
            op_type: SetOperationType::Union,
            df: other,
            all,
        });
        self
    }

    pub fn intersect(mut self, other: CustomDataFrame, all: bool) -> Self {
        self.set_operations.push(SetOperation {
            op_type: SetOperationType::Intersect,
            df: other,
            all,
        });
        self
    }

    pub fn except(mut self, other: CustomDataFrame, all: bool) -> Self {
        self.set_operations.push(SetOperation {
            op_type: SetOperationType::Except,
            df: other,
            all,
        });
        self
    }

    
    pub fn select(mut self, columns: Vec<&str>) -> Self {
        let mut expressions: Vec<Expr> = Vec::new();
        let mut selected_columns: Vec<String> = Vec::new();

        for c in columns {
            // Check if c is an aggregation alias in self.alias_map
            if let Some((alias, _expr)) = self.alias_map.iter().find(|(a, _)| a == c) {
                // This is an aggregated column, it now exists at top-level without table alias
                expressions.push(col(alias.as_str())); // use alias.as_str() to get &str
                selected_columns.push(alias.clone());
            } else {
                // Normal column from the original table
                let col_name = normalize_column_name(c);
                expressions.push(col_with_relation(&self.table_alias, &col_name));
                selected_columns.push(c.to_string());
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
            let expr = builder.build_expr(&self.table_alias);
            let alias = builder
                .agg_alias
                .clone()
                .unwrap_or_else(|| format!("{:?}", expr));
            self.alias_map.push((alias.clone(), expr.clone()));
            self.aggregations.push((alias, expr));
        }
        self
    }

    pub fn group_by(mut self, group_columns: Vec<&str>) -> Self {
        let group_exprs: Vec<Expr> = group_columns
            .iter()
            .map(|&col_name| col(col_name))
            .collect();
    
        let aggregate_exprs: Vec<Expr> = self
            .aggregations
            .iter()
            .map(|(_, expr)| expr.clone())
            .collect();
    
        self.df = self
            .df
            .aggregate(group_exprs, aggregate_exprs)
            .expect("Failed to apply GROUP BY.");
    
        self.aggregated_df = Some(self.df.clone());
        self
    }
    
    pub fn order_by(mut self, columns: Vec<&str>, ascending: Vec<bool>) -> Self {
        assert!(
            columns.len() == ascending.len(),
            "The number of columns and sort directions must match"
        );
    
        let mut sort_exprs = Vec::new();
    
        // Note the pattern `(&col_name, &asc)` instead of `(col_name, &asc)`
        // `columns.iter()` yields &&str, destructuring `&col_name` gives &str.
        for (&col_name, &asc) in columns.iter().zip(ascending.iter()) {
            // Check if col_name is an aggregation alias or a group-by column
            let is_agg_alias = self.aggregations.iter().any(|(alias, _)| alias == col_name);
            let is_group_col = self.group_by_columns.iter().any(|gc| gc == col_name);
            
            let expr = if is_agg_alias || is_group_col {
                // Aggregated or grouped columns should not have table alias
                col(col_name)
            } else {
                // Normal column: just use col(col_name)
                col(col_name)
            };
    
            sort_exprs.push(SortExpr {
                expr,
                asc,
                nulls_first: true,
            });
        }
    
        self.df = self.df.sort(sort_exprs).expect("Failed to apply ORDER BY.");
    
        // Since we consumed columns and ascending, they can't be used directly
        // outside the loop if we need their original values. If we need them after,
        // consider a different approach. If not, this is fine as is.
        for (c, a) in columns.into_iter().zip(ascending.into_iter()) {
            self.order_by_columns.push((c.to_string(), a));
        }
    
        self
    }
    
    

    pub fn limit(mut self, count: usize) -> Self {
        self.limit_count = Some(count);
        self.df = self.df.limit(0, Some(count)).expect("Failed to apply LIMIT.");
        self.aggregated_df = None; // force display to use df
        self
    }
    
    

    pub fn filter(mut self, condition: &str) -> Self {
        let expr = self.parse_condition_for_filter(condition);
        self.df = self.df.filter(expr).expect("Failed to apply WHERE filter");
        self.where_conditions.push(condition.to_string());
        self
    }

    pub fn having(mut self, condition: &str) -> Self {
        if self.aggregations.is_empty() {
            panic!("HAVING must be applied after aggregation and group_by.");
        }
    
        // Parse the HAVING condition using aliases
        let expr = Self::parse_condition_for_having(condition, &self.alias_map);
    
        // Now that aggregated_df is Some(df), we operate on that:
        let agg_df = self.aggregated_df.as_ref().expect("Aggregated DataFrame not set after group_by()");
        let new_agg_df = agg_df
            .clone()
            .filter(expr)
            .expect("Failed to apply HAVING filter.");
    
        self.aggregated_df = Some(new_agg_df);
        self.having_conditions.push(condition.to_string());
        self
    }
    

    pub fn join(
        mut self,
        other: CustomDataFrame,
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

        let condition_parts: Vec<&str> = condition.split("==").map(|s| s.trim()).collect();
        if condition_parts.len() != 2 {
            panic!("Invalid join condition format. Use: 'table.column == table.column'");
        }

        let left_col = condition_parts[0];
        let right_col = condition_parts[1];

        let left_column = left_col.split('.').last().unwrap();
        let right_column = right_col.split('.').last().unwrap();

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

        // Record join for SQL reconstruction
        self.joins.push(JoinClause {
            join_type: join_type_enum,
            table: other.from_table,
            alias: other.table_alias,
            on_left: left_col.to_string(),
            on_right: right_col.to_string(),
        });

        self
    }

    pub fn window(
        mut self,
        func: &str,
        column: &str,
        partition_by: Vec<&str>,
        order_by: Vec<&str>,
        alias: Option<&str>,
    ) -> Self {
        // Record window function
        self.window_functions.push(WindowDefinition {
            func: func.to_string(),
            column: column.to_string(),
            partition_by: partition_by.into_iter().map(|s| s.to_string()).collect(),
            order_by: order_by.into_iter().map(|s| s.to_string()).collect(),
            alias: alias.map(|s| s.to_string()),
        });
        // Note: Applying the actual window function to the DataFrame needs calling df.window(...) or similar.
        // For now, we just record for SQL generation.
        self
    }

    fn parse_condition_for_filter(&self, condition: &str) -> Expr {
        let re = Regex::new(r"^(.+?)\s*(=|!=|>|<|>=|<=)\s*(.+)$").unwrap();
        let condition_trimmed = condition.trim();

        if !re.is_match(condition_trimmed) {
            panic!(
                "Invalid FILTER condition format: '{}'. Expected 'column operator value'",
                condition_trimmed
            );
        }

        let caps = re.captures(condition_trimmed).expect("Invalid FILTER condition format!");
        let column = caps.get(1).unwrap().as_str().trim();
        let operator = caps.get(2).unwrap().as_str().trim();
        let value = caps.get(3).unwrap().as_str().trim().trim_matches('\'');

        let column_expr = col_with_relation(&self.table_alias, column);

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
        // Condition is something like "total_sales > 1000"
        // First parse: column = "total_sales", operator = ">", value = "1000"
        let re = Regex::new(r"^(.+?)\s*(=|!=|>|<|>=|<=)\s*(.+)$").unwrap();
        let caps = re.captures(condition).expect("Invalid HAVING format");
        let column = caps.get(1).unwrap().as_str().trim();
        let operator = caps.get(2).unwrap().as_str().trim();
        let value_str = caps.get(3).unwrap().as_str().trim();
    
        let column_expr = if let Some((alias, _)) = alias_map.iter().find(|(a, _)| a == column) {
            // If the column matches an aggregation alias, just use col(alias)
            col(alias.as_str())
        } else {
            // Otherwise, treat column as is
            col(column)
        };
    
        let parsed_value = match value_str.parse::<f64>() {
            Ok(num) => lit(num),
            Err(_) => lit(value_str),
        };
    
        match operator {
            "=" => column_expr.eq(parsed_value),
            "!=" => column_expr.not_eq(parsed_value),
            ">" => column_expr.gt(parsed_value),
            "<" => column_expr.lt(parsed_value),
            ">=" => column_expr.gt_eq(parsed_value),
            "<=" => column_expr.lt_eq(parsed_value),
            _ => panic!("Unsupported operator in HAVING: '{}'", operator),
        }
    }
    
    fn build_ctes(&self) -> String {
        if self.ctes.is_empty() {
            "".to_string()
        } else {
            let cte_strs: Vec<String> = self.ctes.iter().map(|cte| {
                format!("{} AS ({})", cte.name, cte.cte_df.build_query())
            }).collect();
            format!("WITH {}", cte_strs.join(", "))
        }
    }

    fn build_from_clause(&self) -> String {
        if let Some(sub) = &self.subquery_source {
            format!("FROM ({}) {}", sub.build_query(), self.from_table)
        } else {
            format!("FROM {}", self.from_table)
        }
    }

    fn build_joins(&self) -> String {
        let mut join_str = String::new();
        for jc in &self.joins {
            let jt = match jc.join_type {
                JoinType::Inner => "INNER JOIN",
                JoinType::Left => "LEFT JOIN",
                JoinType::Right => "RIGHT JOIN",
                JoinType::Full => "FULL JOIN",
                JoinType::LeftSemi => "LEFT SEMI JOIN",
                JoinType::RightSemi => "RIGHT SEMI JOIN",
                JoinType::LeftAnti => "LEFT ANTI JOIN",
                JoinType::RightAnti => "RIGHT ANTI JOIN",
                JoinType::LeftMark => "LEFT MARK JOIN",
            };
            join_str.push_str(&format!(" {} {} ON {} = {}", jt, jc.alias, jc.on_left, jc.on_right));
        }
        join_str
    }

    fn build_window_functions(&self) -> String {
        // Window functions typically appear in SELECT clause or as separate columns.
        // We'll append them as selected columns with aliases if provided.
        // Example: SELECT ..., func(column) OVER (PARTITION BY ... ORDER BY ...) AS alias
        // For simplicity, just return them as additional columns:
        if self.window_functions.is_empty() {
            "".to_string()
        } else {
            let funcs: Vec<String> = self.window_functions.iter().map(|w| {
                let mut w_str = format!("{}({}) OVER (", w.func.to_uppercase(), w.column);
                if !w.partition_by.is_empty() {
                    w_str.push_str(&format!("PARTITION BY {}", w.partition_by.join(", ")));
                }
                if !w.order_by.is_empty() {
                    if !w.partition_by.is_empty() {
                        w_str.push_str(" ");
                    }
                    w_str.push_str(&format!("ORDER BY {}", w.order_by.join(", ")));
                }
                w_str.push(')');
                if let Some(a) = &w.alias {
                    w_str.push_str(&format!(" AS {}", a));
                }
                w_str
            }).collect();
            funcs.join(", ")
        }
    }

    fn build_set_operations(&self) -> String {
        let mut s = String::new();
        for op in &self.set_operations {
            let op_str = match op.op_type {
                SetOperationType::Union => {
                    if op.all {
                        "UNION ALL"
                    } else {
                        "UNION"
                    }
                },
                SetOperationType::Intersect => {
                    if op.all {
                        "INTERSECT ALL"
                    } else {
                        "INTERSECT"
                    }
                },
                SetOperationType::Except => {
                    if op.all {
                        "EXCEPT ALL"
                    } else {
                        "EXCEPT"
                    }
                }
            };
            s.push_str(&format!(" {} ({})", op_str, op.df.build_query()));
        }
        s
    }

    fn build_select_clause(&self) -> String {
        // Combine normal selected columns, aggregated columns, and window functions.
        // If we have aggregation, select group_by_columns + aggregation aliases
        let mut cols = if self.aggregations.is_empty() {
            if self.selected_columns.is_empty() {
                vec!["*".to_string()]
            } else {
                self.selected_columns.clone()
            }
        } else {
            let mut c = self.group_by_columns.clone();
            c.extend(self.aggregations.iter().map(|(alias, _)| alias.clone()));
            if c.is_empty() {
                vec!["*".to_string()]
            } else {
                c
            }
        };

        if !self.window_functions.is_empty() {
            // Add window functions as additional columns
            let wfuncs = self.build_window_functions();
            if !wfuncs.is_empty() {
                cols.push(wfuncs);
            }
        }

        format!("SELECT {}", cols.join(", "))
    }
    /// Build the query string based on recorded transformations
    fn build_query(&self) -> String {
        let cte_str = self.build_ctes();

        let select_clause = self.build_select_clause();
        let from_clause = self.build_from_clause();
        let join_clause = self.build_joins();

        let mut sql = if cte_str.is_empty() {
            format!("{} {}{}", select_clause, from_clause, join_clause)
        } else {
            format!("{} {} {}{}", cte_str, select_clause, from_clause, join_clause)
        };

        if !self.where_conditions.is_empty() {
            sql.push_str(&format!(" WHERE {}", self.where_conditions.join(" AND ")));
        }

        if !self.group_by_columns.is_empty() {
            sql.push_str(&format!(" GROUP BY {}", self.group_by_columns.join(", ")));
        }

        if !self.having_conditions.is_empty() {
            sql.push_str(&format!(" HAVING {}", self.having_conditions.join(" AND ")));
        }

        if !self.order_by_columns.is_empty() {
            let order_exprs = self.order_by_columns.iter()
                .map(|(col, asc)| format!("{} {}", col, if *asc { "ASC" } else { "DESC" }))
                .collect::<Vec<_>>().join(", ");
            sql.push_str(&format!(" ORDER BY {}", order_exprs));
        }

        if let Some(count) = self.limit_count {
            sql.push_str(&format!(" LIMIT {}", count));
        }

        let sets = self.build_set_operations();
        if !sets.is_empty() {
            sql.push_str(&sets);
        }

        sql
    }

    pub fn display_query(&self) {
        let final_query = self.build_query();
        println!("Generated SQL Query: {}", final_query);
    }

    pub fn display(&self) -> BoxFuture<'_, Result<(), DataFusionError>> {
        let df = self.aggregated_df.as_ref().unwrap_or(&self.df);
    
        Box::pin(async move {
            let batches = df.clone().collect().await?;
            let schema = df.schema();
    
            let column_names = schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>();
    
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
    
            let mut row_count = 0;
            'outer: for batch in &batches {
                for row in 0..batch.num_rows() {
                    if row_count >= 100 {
                        break 'outer;
                    }
    
                    let mut row_data = Vec::new();
                    for (col_idx, column) in batch.columns().iter().enumerate() {
                        let field = schema.field(col_idx);
                        let value = if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
                            array.value(row).to_string()
                        } 
                         // Boolean
                         else if let Some(array) = column.as_any().downcast_ref::<arrow::array::BooleanArray>() {
                            array.value(row).to_string()
                        }
                        
                        // Integers
                        else if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
                            array.value(row).to_string()
                        } else if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
                            array.value(row).to_string()
                        } else if let Some(array) = column.as_any().downcast_ref::<arrow::array::UInt32Array>() {
                            array.value(row).to_string()
                        } else if let Some(array) = column.as_any().downcast_ref::<arrow::array::UInt64Array>() {
                            array.value(row).to_string()
                        }
                        
                        // Floats
                        else if let Some(array) = column.as_any().downcast_ref::<Float64Array>() {
                            format!("{:.4}", array.value(row))
                        } 
                        else if let Some(array) = column.as_any().downcast_ref::<arrow::array::Float32Array>() {
                            format!("{:.4}", array.value(row))
                        }
                        else if let Some(array) = column.as_any().downcast_ref::<Decimal128Array>() {
                            if let ArrowDataType::Decimal128(precision, scale) = field.data_type() {
                                let raw_value = array.value(row);
                                let negative = raw_value < 0;
                                let abs_value = raw_value.abs();
                                let mut digits_str = abs_value.to_string();
                                let digits_len = digits_str.len();
                                let scale_usize = *scale as usize;
                                let precision_usize = *precision as usize;
                        
                                // If the number of digits is less than the scale, pad with leading zeros.
                                // Example: scale=4, digits="12" => needed_zeros=2 => "0.0012"
                                if scale_usize > 0 {
                                    if digits_len > scale_usize {
                                        let point_pos = digits_len - scale_usize;
                                        digits_str.insert(point_pos, '.');
                                    } else {
                                        let needed_zeros = scale_usize - digits_len;
                                        let zero_padding = "0".repeat(needed_zeros);
                                        digits_str = format!("0.{}{}", zero_padding, digits_str);
                                    }
                                }
                        
                                if negative {
                                    digits_str.insert(0, '-');
                                }
                        
                                // Count total digits (excluding decimal point and sign)
                                let total_digits = digits_str.chars().filter(|c| c.is_ascii_digit()).count();
                        
                                // If total digits exceed precision, truncate from the right.
                                // Example: precision=5, number="123456.78" => too many digits, truncate extra from right.
                                if total_digits > precision_usize {
                                    // Determine how many characters to remove. Only digits are counted, so we must find digits from the right.
                                    let excess = total_digits - precision_usize;
                                    let mut digit_count = 0;
                                    let mut chars: Vec<char> = digits_str.chars().collect();
                                    while digit_count < excess {
                                        // Remove characters from the end that are digits, skipping decimal and sign
                                        if let Some(ch) = chars.pop() {
                                            if ch.is_ascii_digit() {
                                                digit_count += 1;
                                            } else {
                                                // If the popped char is not a digit, put it back 
                                                // since we only count digits for precision
                                                chars.push(ch);
                                                break;
                                            }
                                        } else {
                                            break;
                                        }
                                    }
                        
                                    // If we end with a trailing '.', remove it
                                    if chars.last() == Some(&'.') {
                                        chars.pop();
                                    }
                        
                                    digits_str = chars.into_iter().collect();
                                }
                        
                                digits_str
                            } else {
                                array.value(row).to_string()
                            }
                        }
                        
                        
                        
                        //DATE 32   
                        else if let Some(array) = column.as_any().downcast_ref::<Date32Array>() {
                            let days_since_epoch = array.value(row);
                            NaiveDate::from_num_days_from_ce_opt(1970 * 365 + days_since_epoch)
                                .map(|d| d.to_string())
                                .unwrap_or_else(|| "Invalid date".to_string())
                        }  
                        //DAte 64
                        // Date64 (milliseconds since epoch)
                        else if let Some(array) = column.as_any().downcast_ref::<Date64Array>() {
                            let millis_since_epoch = array.value(row);
                            let days_since_epoch = millis_since_epoch / (1000 * 60 * 60 * 24);
                            NaiveDate::from_num_days_from_ce_opt(1970 * 365 + days_since_epoch as i32)
                                .map(|d| d.to_string())
                                .unwrap_or_else(|| "Invalid date".to_string())
                        }
                        
                        
                      // Timestamps
                    //   else if let Some(array) = column.as_any().downcast_ref::<arrow::array::TimestampNanosecondArray>() {
                    //     // Timestamp is in nanoseconds
                    //     let nanos = array.value(row);
                    //     chrono::NaiveDateTime::from_timestamp_nanos(nanos)
                    //         .map(|d| d.to_string())
                    //         .unwrap_or_else(|| "Invalid timestamp".to_string())
                    // } else if let Some(array) = column.as_any().downcast_ref::<arrow::array::TimestampMicrosecondArray>() {
                    //     // Timestamp is in microseconds
                    //     let micros = array.value(row);
                    //     chrono::NaiveDateTime::from_timestamp_micros(micros)
                    //         .map(|d| d.to_string())
                    //         .unwrap_or_else(|| "Invalid timestamp".to_string())
                    // } else if let Some(array) = column.as_any().downcast_ref::<arrow::array::TimestampMillisecondArray>() {
                    //     // Timestamp is in milliseconds
                    //     let millis = array.value(row);
                    //     chrono::NaiveDateTime::from_timestamp_millis(millis)
                    //         .map(|d| d.to_string())
                    //         .unwrap_or_else(|| "Invalid timestamp".to_string())
                    // } else if let Some(array) = column.as_any().downcast_ref::<arrow::array::TimestampSecondArray>() {
                    //     // Timestamp is in seconds
                    //     let sec = array.value(row);
                    //     chrono::NaiveDateTime::from_timestamp_opt(sec, 0)
                    //         .map(|d| d.to_string())
                    //         .unwrap_or_else(|| "Invalid timestamp".to_string())
                    // }
                    


                        // Binary data
                        else if let Some(array) = column.as_any().downcast_ref::<arrow::array::BinaryArray>() {
                            let bytes = array.value(row);
                            format!("0x{}", hex::encode(bytes))
                        } else if let Some(array) = column.as_any().downcast_ref::<arrow::array::LargeBinaryArray>() {
                            let bytes = array.value(row);
                            format!("0x{}", hex::encode(bytes))
                        }
                        // Large string
                        else if let Some(array) = column.as_any().downcast_ref::<arrow::array::LargeStringArray>() {
                            array.value(row).to_string()
                        }    

                        else {
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
