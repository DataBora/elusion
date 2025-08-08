use crate::prelude::*;

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
            ElusionError::GroupByError { message, invalid_columns, suggestion } => write!(
                f,
                "📊 Group By Error: {}\n\
                 ❌ Invalid columns: {}\n\
                 💡 Suggestion: {}",
                message,
                invalid_columns.join(", "),
                suggestion
            ),
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
                    return ElusionError::GroupByError {
                        message: error_msg.clone(),
                        invalid_columns: Vec::new(),
                        suggestion: "💡 Ensure all non-aggregated columns are included in GROUP BY".to_string(),
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