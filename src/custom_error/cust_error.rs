use crate::prelude::*;
use crate::normalizers::normalize::STRING_LITERAL_PATTERN;
use crate::normalizers::normalize::AS_PATTERN;
use crate::normalizers::normalize::FUNCTION_PATTERN;
use crate::normalizers::normalize::SQL_KEYWORDS;
use crate::normalizers::normalize::TABLE_COLUMN_PATTERN;
use crate::normalizers::normalize::AGGREGATE_FUNCTIONS;
use crate::normalizers::normalize::SIMPLE_COLUMN_PATTERN;
use crate::normalizers::normalize::POSTGRES_CAST_PATTERN;
use crate::normalizers::normalize::DATETIME_FUNCTIONS;
use crate::normalizers::normalize::STRING_FUNCTIONS;

#[derive(Debug)]
pub enum ElusionError {
    MissingColumn {
        column: String,
        available_columns: Vec<String>,
    },
    InvalidDataType {
        column: String,
        expected: String,
        found: String,
    },
    DuplicateColumn {
        column: String,
        locations: Vec<String>,
    },
    InvalidOperation {
        operation: String,
        reason: String,
        suggestion: String,
    },
    SchemaError {
        message: String,
        schema: Option<String>,
        suggestion: String,
    },
    JoinError {
        message: String,
        left_table: String,
        right_table: String,
        suggestion: String,
    },
    GroupByError {
        message: String,
        invalid_columns: Vec<String>,
        suggestion: String,
        function_context: Option<String>,
    },
    WriteError {
        path: String,
        operation: String,
        reason: String,
        suggestion: String,
    },
    PartitionError {
        message: String,
        partition_columns: Vec<String>,
        suggestion: String,
    },
    AggregationError {
        message: String,
        function: String,
        column: String,
        suggestion: String,
    },
    OrderByError {
        message: String,
        columns: Vec<String>,
        suggestion: String,
    },
    WindowFunctionError {
        message: String,
        function: String,
        details: String,
        suggestion: String,
    },
    LimitError {
        message: String,
        value: u64,
        suggestion: String,
    },
    SetOperationError {
        operation: String,
        reason: String,
        suggestion: String,
    },
    DataFusion(DataFusionError),
    Io(std::io::Error),
    Custom(String),
}

impl fmt::Display for ElusionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ElusionError::MissingColumn { column, available_columns } => {
                let suggestion = suggest_similar_column(column, available_columns);
                write!(
                    f,
                    "🔍 Column Not Found: '{}'\n\
                     📋 Available columns are: {}\n\
                     💡 Did you mean '{}'?\n\
                     🔧 Check for typos or use .display_schema() to see all available columns.",
                    column,
                    available_columns.join(", "),
                    suggestion
                )
            },
            ElusionError::InvalidDataType { column, expected, found } => write!(
                f,
                "📊 Type Mismatch in column '{}'\n\
                 ❌ Found: {}\n\
                 ✅ Expected: {}\n\
                 💡 Try: .with_column(\"{}\", cast(\"{}\", {}));",
                column, found, expected, column, column, expected
            ),
            ElusionError::DuplicateColumn { column, locations } => write!(
                f,
                "🔄 Duplicate Column: '{}'\n\
                 📍 Found in: {}\n\
                 💡 Try using table aliases or renaming columns:\n\
                 .select([\"table1.{} as table1_{}\", \"table2.{} as table2_{}\"])",
                column,
                locations.join(", "),
                column, column, column, column
            ),
            ElusionError::InvalidOperation { operation, reason, suggestion } => write!(
                f,
                "⚠️ Invalid Operation: {}\n\
                 ❌ Problem: {}\n\
                 💡 Suggestion: {}",
                operation, reason, suggestion
            ),
            ElusionError::SchemaError { message, schema, suggestion } => {
                let schema_info = schema.as_ref().map_or(
                    String::new(),
                    |s| format!("\n📋 Current Schema:\n{}", s)
                );
                write!(
                    f,
                    "🏗️ Schema Error: {}{}\n\
                     💡 Suggestion: {}",
                    message, schema_info, suggestion
                )
            },
            ElusionError::JoinError { message, left_table, right_table, suggestion } => write!(
                f,
                "🤝 Join Error:\n\
                 ❌ {}\n\
                 📌 Left Table: {}\n\
                 📌 Right Table: {}\n\
                 💡 Suggestion: {}",
                message, left_table, right_table, suggestion
            ),
            ElusionError::GroupByError { message, invalid_columns, suggestion, function_context } => {
                let function_info = if let Some(context) = function_context {
                    format!("\n🔧 Function Context: {}", context)
                } else {
                    String::new()
                };
                
                write!(
                    f,
                    "📊 Group By Error: {}\n\
                    ❌ Invalid columns: {}{}\n\
                    💡 Suggestion: {}",
                    message,
                    invalid_columns.join(", "),
                    function_info,
                    suggestion
                )
            },
            ElusionError::WriteError { path, operation, reason, suggestion } => write!(
                f,
                "💾 Write Error during {} operation\n\
                 📍 Path: {}\n\
                 ❌ Problem: {}\n\
                 💡 Suggestion: {}",
                operation, path, reason, suggestion
            ),
            ElusionError::DataFusion(err) => write!(
                f,
                "⚡ DataFusion Error: {}\n\
                 💡 Don't worry! Here's what you can try:\n\
                 1. Check your column names and types\n\
                 2. Verify your SQL syntax\n\
                 3. Use .display_schema() to see available columns\n\
                 4. Try breaking down complex operations into smaller steps",
                err
            ),
            ElusionError::Io(err) => write!(
                f,
                "📁 I/O Error: {}\n\
                 💡 Quick fixes to try:\n\
                 1. Check if the file/directory exists\n\
                 2. Verify your permissions\n\
                 3. Ensure the path is correct\n\
                 4. Close any programs using the file",
                err
            ),
            ElusionError::PartitionError { message, partition_columns, suggestion } => write!(
                f,
                "📦 Partition Error: {}\n\
                 ❌ Affected partition columns: {}\n\
                 💡 Suggestion: {}",
                message,
                partition_columns.join(", "),
                suggestion
            ),
            ElusionError::AggregationError { message, function, column, suggestion } => write!(
                f,
                "📊 Aggregation Error in function '{}'\n\
                 ❌ Problem with column '{}': {}\n\
                 💡 Suggestion: {}",
                function, column, message, suggestion
            ),
            ElusionError::OrderByError { message, columns, suggestion } => write!(
                f,
                "🔄 Order By Error: {}\n\
                 ❌ Problem with columns: {}\n\
                 💡 Suggestion: {}",
                message,
                columns.join(", "),
                suggestion
            ),
            ElusionError::WindowFunctionError { message, function, details, suggestion } => write!(
                f,
                "🪟 Window Function Error in '{}'\n\
                 ❌ Problem: {}\n\
                 📝 Details: {}\n\
                 💡 Suggestion: {}",
                function, message, details, suggestion
            ),
            ElusionError::LimitError { message, value, suggestion } => write!(
                f,
                "🔢 Limit Error: {}\n\
                 ❌ Invalid limit value: {}\n\
                 💡 Suggestion: {}",
                message, value, suggestion
            ),
            ElusionError::SetOperationError { operation, reason, suggestion } => write!(
                f,
                "🔄 Set Operation Error in '{}'\n\
                 ❌ Problem: {}\n\
                 💡 Suggestion: {}",
                operation, reason, suggestion
            ),
            ElusionError::Custom(err) => write!(f, "💫 {}", err),
        }
    }
}

impl From<DataFusionError> for ElusionError {
    fn from(err: DataFusionError) -> Self {
        match &err {
            DataFusionError::SchemaError(schema_err, _context) => {
                let error_msg = schema_err.to_string();
                
                if error_msg.contains("Column") && error_msg.contains("not found") {
                    if let Some(col_name) = extract_column_name_from_error(&error_msg) {
                        return ElusionError::MissingColumn {
                            column: col_name,
                            available_columns: extract_available_columns_from_error(&error_msg),
                        };
                    }
                }
                
                if error_msg.contains("Cannot cast") {
                    if let Some((col, expected, found)) = extract_type_info_from_error(&error_msg) {
                        return ElusionError::InvalidDataType {
                            column: col,
                            expected,
                            found,
                        };
                    }
                }

                if error_msg.contains("Schema") {
                    return ElusionError::SchemaError {
                        message: error_msg,
                        schema: None,
                        suggestion: "💡 Check column names and data types in your schema".to_string(),
                    };
                }

                ElusionError::DataFusion(err)
            },
            DataFusionError::Plan(plan_err) => {
                let error_msg = plan_err.to_string();
                
                if error_msg.contains("Duplicate column") {
                    if let Some((col, locs)) = extract_duplicate_column_info(&error_msg) {
                        return ElusionError::DuplicateColumn {
                            column: col,
                            locations: locs,
                        };
                    }
                }

                if error_msg.contains("JOIN") {
                    return ElusionError::JoinError {
                        message: error_msg.clone(),
                        left_table: "unknown".to_string(),
                        right_table: "unknown".to_string(),
                        suggestion: "💡 Check join conditions and table names".to_string(),
                    };
                }

                ElusionError::DataFusion(err)
            },
            DataFusionError::Execution(exec_err) => {
                let error_msg = exec_err.to_string();

                if error_msg.contains("aggregate") || error_msg.contains("SUM") || 
                error_msg.contains("AVG") || error_msg.contains("COUNT") {
                 if let Some((func, col)) = extract_aggregation_error(&error_msg) {
                     return ElusionError::AggregationError {
                         message: error_msg.clone(),
                         function: func,
                         column: col,
                         suggestion: "💡 Verify aggregation function syntax and column data types".to_string(),
                     };
                 }
             }
                if error_msg.contains("GROUP BY") {
                    let missing_col = extract_missing_column(&error_msg).unwrap_or("unknown".to_string());
                    let function_context = detect_function_usage_in_error(&error_msg, &missing_col);
                    
                    return ElusionError::GroupByError {
                        message: error_msg.clone(),
                        invalid_columns: if missing_col != "unknown" { vec![missing_col.clone()] } else { Vec::new() },
                        function_context: function_context.clone(),
                        suggestion: generate_enhanced_groupby_suggestion(&missing_col, function_context.as_deref()),
                    };
                }

                if error_msg.contains("PARTITION BY") {
                    return ElusionError::PartitionError {
                        message: error_msg.clone(),
                        partition_columns: Vec::new(),
                        suggestion: "💡 Check partition column names and data types".to_string(),
                    };
                }

                if error_msg.contains("ORDER BY") {
                    return ElusionError::OrderByError {
                        message: error_msg.clone(),
                        columns: Vec::new(),
                        suggestion: "💡 Verify column names and sort directions".to_string(),
                    };
                }

                if error_msg.contains("OVER") || error_msg.contains("window") {
                    if let Some((func, details)) = extract_window_function_error(&error_msg) {
                        return ElusionError::WindowFunctionError {
                            message: error_msg.clone(),
                            function: func,
                            details,
                            suggestion: "💡 Check window function syntax and parameters".to_string(),
                        };
                    }
                }

                if error_msg.contains("LIMIT") {
                    return ElusionError::LimitError {
                        message: error_msg.clone(),
                        value: 0,
                        suggestion: "💡 Ensure limit value is a positive integer".to_string(),
                    };
                }

                if error_msg.contains("UNION") || error_msg.contains("INTERSECT") || error_msg.contains("EXCEPT") {
                    return ElusionError::SetOperationError {
                        operation: "Set Operation".to_string(),
                        reason: error_msg.clone(),
                        suggestion: "💡 Ensure both sides of the operation have compatible schemas".to_string(),
                    };
                }

                ElusionError::DataFusion(err)
            },
            DataFusionError::NotImplemented(msg) => {
                ElusionError::InvalidOperation {
                    operation: "Operation not supported".to_string(),
                    reason: msg.clone(),
                    suggestion: "💡 Try using an alternative approach or check documentation for supported features".to_string(),
                }
            },
            DataFusionError::Internal(msg) => {
                ElusionError::Custom(format!("Internal error: {}. Please report this issue.", msg))
            },
            _ => ElusionError::DataFusion(err)
        }
    }
}

fn extract_window_function_error(err: &str) -> Option<(String, String)> {
    let re = Regex::new(r"Window function '([^']+)' error: (.+)").ok()?;
    let caps = re.captures(err)?;
    Some((
        caps.get(1)?.as_str().to_string(),
        caps.get(2)?.as_str().to_string(),
    ))
}

fn extract_aggregation_error(err: &str) -> Option<(String, String)> {
    let re = Regex::new(r"Aggregate function '([^']+)' error on column '([^']+)'").ok()?;
    let caps = re.captures(err)?;
    Some((
        caps.get(1)?.as_str().to_string(),
        caps.get(2)?.as_str().to_string(),
    ))
}
// Helper functions for error parsing
fn extract_column_name_from_error(err: &str) -> Option<String> {
    let re = Regex::new(r"Column '([^']+)'").ok()?;
    re.captures(err)?.get(1).map(|m| m.as_str().to_string())
}

fn extract_available_columns_from_error(err: &str) -> Vec<String> {
    if let Some(re) = Regex::new(r"Available fields are: \[(.*?)\]").ok() {
        if let Some(caps) = re.captures(err) {
            if let Some(fields) = caps.get(1) {
                return fields.as_str()
                    .split(',')
                    .map(|s| s.trim().trim_matches('\'').to_string())
                    .collect();
            }
        }
    }
    Vec::new()
}

fn extract_type_info_from_error(err: &str) -> Option<(String, String, String)> {
    let re = Regex::new(r"Cannot cast column '([^']+)' from ([^ ]+) to ([^ ]+)").ok()?;
    let caps = re.captures(err)?;
    Some((
        caps.get(1)?.as_str().to_string(),
        caps.get(3)?.as_str().to_string(),
        caps.get(2)?.as_str().to_string(),
    ))
}

fn extract_duplicate_column_info(err: &str) -> Option<(String, Vec<String>)> {
    let re = Regex::new(r"Duplicate column '([^']+)' in schema: \[(.*?)\]").ok()?;
    let caps = re.captures(err)?;
    Some((
        caps.get(1)?.as_str().to_string(),
        caps.get(2)?
            .as_str()
            .split(',')
            .map(|s| s.trim().to_string())
            .collect()
    ))
}

// Helper function to suggest similar column names using basic string similarity
fn suggest_similar_column(target: &str, available: &[String]) -> String {
    available
        .iter()
        .map(|col| (col, string_similarity(target, col)))
        .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
        .map(|(col, _)| col.clone())
        .unwrap_or_else(|| "".to_string())
}

// Simple string similarity function (you might want to use a proper crate like 'strsim' in production)
fn string_similarity(s1: &str, s2: &str) -> f64 {
    let s1_lower = s1.to_lowercase();
    let s2_lower = s2.to_lowercase();
    
    // Check for exact prefix match
    if s1_lower.starts_with(&s2_lower) || s2_lower.starts_with(&s1_lower) {
        return 0.9;
    }
    
    // Check for common substring
    let common_len = s1_lower.chars()
        .zip(s2_lower.chars())
        .take_while(|(c1, c2)| c1 == c2)
        .count() as f64;
    
    if common_len > 0.0 {
        return common_len / s1_lower.len().max(s2_lower.len()) as f64;
    }
    
    // Fall back to character frequency similarity
    let max_len = s1_lower.len().max(s2_lower.len()) as f64;
    let common_chars = s1_lower.chars()
        .filter(|c| s2_lower.contains(*c))
        .count() as f64;
    
    common_chars / max_len
}

impl Error for ElusionError {}

impl From<std::io::Error> for ElusionError {
    fn from(err: std::io::Error) -> Self {
        ElusionError::Io(err)
    }
}

pub type ElusionResult<T> = Result<T, ElusionError>;

pub fn extract_table_from_join_error(error: &str) -> Option<String> {

    for cap in STRING_LITERAL_PATTERN.captures_iter(error) {
        if let Some(quoted_text) = cap.get(1) {
            let text = quoted_text.as_str();
            // Filter out common non-table words
            if !SQL_KEYWORDS.contains(&text.to_uppercase().as_str()) && 
               !text.chars().all(|c| c.is_numeric()) {
                return Some(text.to_string());
            }
        }
    }
    
    if let Some(cap) = TABLE_COLUMN_PATTERN.captures(error) {
        if let Some(table_part) = cap.get(1) {
            let table = table_part.as_str();
            // Return if it's not a SQL keyword
            if !SQL_KEYWORDS.contains(&table.to_uppercase().as_str()) {
                return Some(table.to_string());
            }
        }
    }
    
    let error_lower = error.to_lowercase();
    if error_lower.contains("table") && error_lower.contains("not found") {
        // Try to find the table name near "not found"
        if let Some(start) = error_lower.find("table") {
            let remaining = &error[start..];
            if let Some(cap) = STRING_LITERAL_PATTERN.captures(remaining) {
                if let Some(table_name) = cap.get(1) {
                    return Some(table_name.as_str().to_string());
                }
            }
        }
    }
    
    None
}

pub fn extract_column_from_agg_error(error: &str) -> Option<String> {
    
    if let Some(cap) = FUNCTION_PATTERN.captures(error) {
        if let Some(func_name) = cap.get(1) {
            if AGGREGATE_FUNCTIONS.contains(&func_name.as_str().to_uppercase().as_str()) {
                if let Some(args) = cap.get(2) {
                    let arg_str = args.as_str().trim();
                    
                    // Check if it's a table.column reference
                    if let Some(table_col_cap) = TABLE_COLUMN_PATTERN.captures(arg_str) {
                        if let Some(column_part) = table_col_cap.get(2) {
                            return Some(column_part.as_str().to_string());
                        }
                    }
                    
                    // Check if it's a simple column
                    if SIMPLE_COLUMN_PATTERN.is_match(arg_str) && 
                       !SQL_KEYWORDS.contains(&arg_str.to_uppercase().as_str()) {
                        return Some(arg_str.to_string());
                    }
                }
            }
        }
    }
    
    for cap in STRING_LITERAL_PATTERN.captures_iter(error) {
        if let Some(quoted_text) = cap.get(1) {
            let text = quoted_text.as_str();
            // Check if it looks like a column name (not a SQL keyword or pure number)
            if SIMPLE_COLUMN_PATTERN.is_match(text) && 
               !SQL_KEYWORDS.contains(&text.to_uppercase().as_str()) &&
               !text.chars().all(|c| c.is_numeric()) {
                return Some(text.to_string());
            }
        }
    }
    
    if let Some(cap) = TABLE_COLUMN_PATTERN.captures(error) {
        if let Some(column_part) = cap.get(2) {
            return Some(column_part.as_str().to_string());
        }
    }
    
    if let Some(cap) = POSTGRES_CAST_PATTERN.captures(error) {
        if let Some(column_expr) = cap.get(1) {
            let expr = column_expr.as_str();
            // If it's table.column, extract just the column part
            if let Some(dot_pos) = expr.rfind('.') {
                return Some(expr[dot_pos + 1..].to_string());
            } else {
                return Some(expr.to_string());
            }
        }
    }
    
    None
}

pub fn extract_function_from_error(error: &str) -> Option<String> {

    if let Some(cap) = FUNCTION_PATTERN.captures(error) {
        if let Some(func_name) = cap.get(1) {
            let func = func_name.as_str().to_uppercase();
            
            // Check if it's an aggregate function
            if AGGREGATE_FUNCTIONS.contains(&func.as_str()) {
                return Some(func);
            }
            
            // Check if it's a datetime function
            if DATETIME_FUNCTIONS.contains(&func.as_str()) {
                return Some(func);
            }
        }
    }
    
    // Fallback: look for any aggregate function names in the error text
    for &func in AGGREGATE_FUNCTIONS.iter() {
        if error.to_uppercase().contains(func) {
            return Some(func.to_string());
        }
    }
    
    // Check datetime functions too
    for &func in DATETIME_FUNCTIONS.iter() {
        if error.to_uppercase().contains(func) {
            return Some(func.to_string());
        }
    }
    
    None
}


    pub fn extract_missing_column(error: &str) -> Option<String> {
        let error_lower = error.to_lowercase();
        
        // Pattern 1: "Expression X could not be resolved"
        if error_lower.contains("expression") && error_lower.contains("could not be resolved") {
            if let Some(start) = error_lower.find("expression ") {
                let remaining = &error[start + 11..];
                if let Some(end) = remaining.find(" could not be resolved") {
                    let expr = remaining[..end].trim();
                    
                    if let Some(cap) = TABLE_COLUMN_PATTERN.captures(expr) {
                        if let Some(column_part) = cap.get(2) {
                            return Some(column_part.as_str().to_string());
                        }
                    }
                    
                    if SIMPLE_COLUMN_PATTERN.is_match(expr) {
                        return Some(expr.to_string());
                    }
                }
            }
        }
        
        if error_lower.contains("no field named") {
            if let Some(start) = error_lower.find("no field named") {
                let remaining = &error[start..];
                // Look for quoted field name
                if let Some(cap) = regex::Regex::new(r"'([^']+)'").unwrap().captures(remaining) {
                    return Some(cap.get(1)?.as_str().to_string());
                }
            }
        }
        
        if error_lower.contains("over") && error_lower.contains("could not be resolved") {
            // Look for pattern like "PARTITION BY region" or "ORDER BY mesto"
            if let Some(cap) = regex::Regex::new(r"(partition by|order by)\s+([a-zA-Z_][a-zA-Z0-9_]*)").unwrap().captures(&error_lower) {
                return Some(cap.get(2)?.as_str().to_string());
            }
        }
        
        None
    }

    pub fn extract_column_from_duplicate_error(error: &str) -> Option<String> {

        if error.to_lowercase().contains("duplicate") && error.to_lowercase().contains("field name") {
            if let Some(start) = error.to_lowercase().find("field name") {
                let remaining = &error[start + 10..]; 
                
                if let Some(cap) = TABLE_COLUMN_PATTERN.captures(remaining) {
                    if let Some(column_part) = cap.get(2) {
                        return Some(column_part.as_str().to_string());
                    }
                }
                
                if let Some(cap) = SIMPLE_COLUMN_PATTERN.captures(remaining) {
                    let potential_column = cap.get(0)?.as_str();
                    if !SQL_KEYWORDS.contains(&potential_column.to_uppercase().as_str()) {
                        return Some(potential_column.to_string());
                    }
                }
            }
        }
        
        None
    }

    pub fn extract_column_from_projection_error(error: &str) -> Option<String> {
        //  "expression \"table.column AS alias\" at position X"
        if error.contains("expression") && error.contains("at position") {
            // Look for quoted expressions
            for cap in STRING_LITERAL_PATTERN.captures_iter(error) {
                if let Some(quoted_expr) = cap.get(1) {
                    let expr = quoted_expr.as_str();
                    
                    // Check if it contains AS clause using your AS_PATTERN
                    if AS_PATTERN.is_match(expr) {
                        // Split by AS and get the alias part after AS
                        if let Some(as_match) = AS_PATTERN.find(expr) {
                            let alias_part = expr[as_match.end()..].trim();
                            if SIMPLE_COLUMN_PATTERN.is_match(alias_part) {
                                return Some(alias_part.to_string());
                            }
                        }
                    } else {
                        // No AS clause, extract column from table.column
                        if let Some(table_col_cap) = TABLE_COLUMN_PATTERN.captures(expr) {
                            if let Some(column_part) = table_col_cap.get(2) {
                                return Some(column_part.as_str().to_string());
                            }
                        }
                    }
                }
            }
        }
        
        None
    }

    pub fn generate_enhanced_groupby_suggestion(missing_column: &str, function_context: Option<&str>) -> String {
        if let Some(context) = function_context {
            let function_type = if context.contains("string function") {
                "string function"
            } else if context.contains("datetime function") {
                "datetime function"  
            } else if context.contains("CASE expression") {
                "CASE expression"
            } else {
                "function"
            };
            
            format!(
                "Column '{}' is referenced in a {} but missing from GROUP BY.\n\
                \n\
                🔧 Solutions:\n\
                1️⃣ Add '{}' to .select([...]) then use .group_by_all()\n\
                    Example: .select([\"existing_cols\", \"{}\"]).group_by_all()\n\
                \n\
                2️⃣ Add '{}' manually to .group_by([...])\n\
                \n\
                3️⃣ Use manual GROUP BY for complex function dependencies\n\
                    Example: .group_by([\"col1\", \"col2\", \"{}\"])",
                missing_column, function_type, missing_column, missing_column, missing_column, missing_column
            )
        } else {
            "💡 Use .group_by_all() to automatically include all SELECT columns in GROUP BY, or manually add missing columns to .group_by([...])".to_string()
        }
    }

    pub fn detect_function_usage_in_error(error: &str, missing_column: &str) -> Option<String> {
        let error_upper = error.to_uppercase();
        let column_upper = missing_column.to_uppercase();
        
        for &func in STRING_FUNCTIONS.iter() {
            let patterns = [
                format!("{}({})", func, column_upper),
                format!("{}({}", func, column_upper),  
                format!("{}(.*{}.*)", func, column_upper), 
            ];
            
            for pattern in &patterns {
                if error_upper.contains(pattern) {
                    return Some(format!("Column '{}' is used in {}() string function", missing_column, func));
                }
            }
        }
        
        for &func in DATETIME_FUNCTIONS.iter() {
            let patterns = [
                format!("{}({})", func, column_upper),
                format!("{}({}", func, column_upper),
                format!("{}(.*{}.*)", func, column_upper),
            ];
            
            for pattern in &patterns {
                if error_upper.contains(pattern) {
                    return Some(format!("Column '{}' is used in {}() datetime function", missing_column, func));
                }
            }
        }
        
        if error_upper.contains("CASE") && error_upper.contains(&column_upper) {
            return Some(format!("Column '{}' is used in CASE expression", missing_column));
        }
        
        for &func in AGGREGATE_FUNCTIONS.iter() {
            let patterns = [
                format!("{}({})", func, column_upper),
                format!("{}({}", func, column_upper),
            ];
            
            for pattern in &patterns {
                if error_upper.contains(pattern) {
                    return Some(format!("Column '{}' is used in {}() aggregate function", missing_column, func));
                }
            }
        }
        
        None
    }

    pub fn extract_window_function_columns(error: &str) -> Vec<String> {
        let mut columns = Vec::new();
        let error_upper = error.to_uppercase();
        
        if let Some(cap) = regex::Regex::new(r"PARTITION BY\s+([a-zA-Z_][a-zA-Z0-9_]*)").unwrap().captures(&error_upper) {
            if let Some(col) = cap.get(1) {
                columns.push(col.as_str().to_lowercase());
            }
        }
        
        if let Some(cap) = regex::Regex::new(r"ORDER BY\s+([a-zA-Z_][a-zA-Z0-9_]*)").unwrap().captures(&error_upper) {
            if let Some(col) = cap.get(1) {
                let col_name = col.as_str().to_lowercase();
                if !columns.contains(&col_name) {
                    columns.push(col_name);
                }
            }
        }
        
        if columns.is_empty() {
            if let Some(col) = extract_missing_column(error) {
                columns.push(col);
            }
        }
        
        columns
    }

    pub fn extract_window_function_name(error: &str) -> Option<String> {
        let error_upper = error.to_uppercase();
        
        // Common window functions
        let window_functions = [
            "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE", "PERCENT_RANK", "CUME_DIST",
            "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE"
        ];
        
        for func in &window_functions {
            if error_upper.contains(&format!("{}(", func)) {
                return Some(func.to_string());
            }
        }
        
        if error_upper.contains("OVER") {
            for func in ["SUM", "AVG", "COUNT", "MIN", "MAX"] {
                if error_upper.contains(&format!("{}(", func)) {
                    return Some(format!("{} (window)", func));
                }
            }
        }
        
        Some("WINDOW_FUNCTION".to_string())
    }

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_extract_with_existing_patterns() {
 
        assert_eq!(
            extract_function_from_error("SUM(s.orderquantity) failed"),
            Some("SUM".to_string())
        );
        
        assert_eq!(
            extract_column_from_agg_error("SUM(customer.total_amount) type error"),
            Some("total_amount".to_string())
        );
        
        assert_eq!(
            extract_table_from_join_error("Table 'customers' not found in join"),
            Some("customers".to_string())
        );
        
        assert_eq!(
            extract_missing_column("Expression s.customerkey could not be resolved from available columns"),
            Some("customerkey".to_string())
        );
    }
}