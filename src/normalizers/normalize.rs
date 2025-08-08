
use crate::prelude::*;

pub async fn lowercase_column_names(df: DataFrame) -> ElusionResult<DataFrame> {
    let schema = df.schema();
   
    // Create a SELECT statement that renames all columns to lowercase
    let columns: Vec<String> = schema.fields()
        .iter()
        .map(|f| format!("\"{}\" as \"{}\"", f.name(),  f.name().trim().replace(" ", "_").to_lowercase()))
        .collect();

    let ctx = SessionContext::new();
    
    // Register original DataFrame with proper schema conversion
    let batches = df.clone().collect().await?;
    let mem_table = MemTable::try_new(schema.clone().into(), vec![batches])?;
    ctx.register_table("temp_table", Arc::new(mem_table))?;
    
    // Create new DataFrame with lowercase columns
    let sql = format!("SELECT {} FROM temp_table", columns.join(", "));
    ctx.sql(&sql).await.map_err(|e| ElusionError::Custom(format!("Failed to lowercase column names: {}", e)))
}

/// Normalizes an alias by trimming whitespace and converting it to lowercase.
pub  fn normalize_alias_write(alias: &str) -> String {
    alias.trim().to_lowercase()
}

/// Normalizes column name by trimming whitespace and properly quoting table aliases and column names.
pub  fn normalize_column_name(name: &str) -> String {
    // Case-insensitive check for " AS " with more flexible whitespace handling
    let name_upper = name.to_uppercase();
    if name_upper.contains(" AS ") {
       
        // let pattern = regex::Regex::new(r"(?i)\s+AS\s+") //r"(?i)^(.*?)\s+AS\s+(.+?)$"
        // .unwrap();

        let pattern = match regex::Regex::new(r"(?i)\s+AS\s+") {
            Ok(re) => re,
            Err(e) => {
                // Log error and return a safe default
                eprintln!("Column parsing error in SELECT() function: {}", e);
                return name.to_string();
            }
        };

        let parts: Vec<&str> = pattern.split(name).collect();
        
        if parts.len() >= 2 {
            let column = parts[0].trim();
            let alias = parts[1].trim();
            
            if let Some(pos) = column.find('.') {
                let table = &column[..pos];
                let col = &column[pos + 1..];
                format!("\"{}\".\"{}\" AS \"{}\"",
                    table.trim().to_lowercase(),
                    col.trim().to_lowercase(),
                    alias.to_lowercase())
            } else {
                format!("\"{}\" AS \"{}\"",
                    column.trim().to_lowercase(),
                    alias.to_lowercase())
            }
        } else {

            if let Some(pos) = name.find('.') {
                let table = &name[..pos];
                let column = &name[pos + 1..];
                format!("\"{}\".\"{}\"", 
                    table.trim().to_lowercase(), 
                    column.trim().replace(" ", "_").to_lowercase())
            } else {
                format!("\"{}\"", 
                    name.trim().replace(" ", "_").to_lowercase())
            }
        }
    } else {

        if let Some(pos) = name.find('.') {
            let table = &name[..pos];
            let column = &name[pos + 1..];
            format!("\"{}\".\"{}\"", 
                table.trim().to_lowercase(), 
                column.trim().replace(" ", "_").to_lowercase())
        } else {
            format!("\"{}\"", 
                name.trim().replace(" ", "_").to_lowercase())
        }
    }
}
/// Normalizes an alias by trimming whitespace and converting it to lowercase.
pub  fn normalize_alias(alias: &str) -> String {
    // alias.trim().to_lowercase()
    format!("\"{}\"", alias.trim().to_lowercase())
}

/// Normalizes a condition string by properly quoting table aliases and column names.
pub  fn normalize_condition(condition: &str) -> String {
    // let re = Regex::new(r"(\b\w+)\.(\w+\b)").unwrap();
    let re = Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b").unwrap();
    
    re.replace_all(condition.trim(), "\"$1\".\"$2\"").to_string().to_lowercase()
}

pub  fn normalize_condition_filter(condition: &str) -> String {
    // let re = Regex::new(r"(\b\w+)\.(\w+\b)").unwrap();
    let re = Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b").unwrap();
    
    // re.replace_all(condition.trim(), "\"$1\".\"$2\"").to_string()
    re.replace_all(condition.trim(), |caps: &regex::Captures| {
        let table = &caps[1];
        let column = &caps[2];
        format!("\"{}\".\"{}\"", table, column.to_lowercase())
    }).to_string()
}

/// Normalizes an expression by properly quoting table aliases and column names.
/// Example:
/// - "SUM(s.OrderQuantity) AS total_quantity" becomes "SUM(\"s\".\"OrderQuantity\") AS total_quantity"
/// Normalizes an expression by properly quoting table aliases and column names.
pub  fn normalize_expression(expr: &str, table_alias: &str) -> String {
    let parts: Vec<&str> = expr.splitn(2, " AS ").collect();
    
    if parts.len() == 2 {
        let expr_part = parts[0].trim();
        let alias_part = parts[1].trim();
        
        let normalized_expr = if is_aggregate_expression(expr_part) {
            normalize_aggregate_expression(expr_part, table_alias)
        } else if is_datetime_expression(expr_part) {
            normalize_datetime_expression(expr_part)
        } else {
            normalize_simple_expression(expr_part, table_alias)
        };

        format!("{} AS \"{}\"", 
            normalized_expr.to_lowercase(), 
            alias_part.replace(" ", "_").to_lowercase())
    } else {
        if is_aggregate_expression(expr) {
            normalize_aggregate_expression(expr, table_alias).to_lowercase()
        } else if is_datetime_expression(expr) {
            normalize_datetime_expression(expr).to_lowercase()
        } else {
            normalize_simple_expression(expr, table_alias).to_lowercase()
        }
    }
}

pub fn normalize_aggregate_expression(expr: &str, table_alias: &str) -> String {
    let re = Regex::new(r"^([A-Za-z_][A-Za-z0-9_]*)\s*\((.*)\)$").unwrap();
    if let Some(caps) = re.captures(expr.trim()) {
        let func_name = &caps[1];
        let args = &caps[2];
        let normalized_args = args.split(',')
            .map(|arg| normalize_simple_expression(arg.trim(), table_alias))
            .collect::<Vec<_>>()
            .join(", ");
        format!("{}({})", func_name.to_lowercase(), normalized_args.to_lowercase())
    } else {
        expr.to_lowercase()
    }
}

pub fn normalize_simple_expression(expr: &str, table_alias: &str) -> String {
    let col_re = Regex::new(r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<column>[A-Za-z_][A-Za-z0-9_]*)").unwrap();
    let func_re = Regex::new(r"^([A-Za-z_][A-Za-z0-9_]*)\s*\((.*)\)$").unwrap();
    let operator_re = Regex::new(r"([\+\-\*\/])").unwrap();
 
    if let Some(caps) = func_re.captures(expr) {
        let func_name = &caps[1];
        let args = &caps[2];
        
        let normalized_args = args.split(',')
            .map(|arg| normalize_simple_expression(arg.trim(), table_alias))
            .collect::<Vec<_>>()
            .join(", ");
            
        format!("{}({})", func_name.to_lowercase(), normalized_args.to_lowercase())
    } else if operator_re.is_match(expr) {
        let mut result = String::new();
        let mut parts = operator_re.split(expr).peekable();
        
        while let Some(part) = parts.next() {
            let trimmed = part.trim();
            if col_re.is_match(trimmed) {
                result.push_str(&trimmed.to_lowercase());
            } else if is_simple_column(trimmed) {
                result.push_str(&format!("\"{}\".\"{}\"", 
                    table_alias.to_lowercase(), 
                    trimmed.to_lowercase()));
            } else {
                result.push_str(&trimmed.to_lowercase());
            }
            
            if parts.peek().is_some() {
                if let Some(op) = expr.chars().skip_while(|c| !"+-*/%".contains(*c)).next() {
                    result.push_str(&format!(" {} ", op));
                }
            }
        }
        result
    } else if col_re.is_match(expr) {
        col_re.replace_all(expr, "\"$1\".\"$2\"")
            .to_string()
            .to_lowercase()
    } else if is_simple_column(expr) {
        format!("\"{}\".\"{}\"", 
            table_alias.to_lowercase(), 
            expr.trim().replace(" ", "_").to_lowercase())
    } else {
        expr.to_lowercase()
    }
}

/// Helper function to determine if a string is an expression.
pub fn is_expression(s: &str) -> bool {
    // Check for presence of arithmetic operators or function-like patterns
    let operators = ['+', '-', '*', '/', '%' ,'(', ')', ',', '.'];
    let has_operator = s.chars().any(|c| operators.contains(&c));
    let has_function = Regex::new(r"\b[A-Za-z_][A-Za-z0-9_]*\s*\(").unwrap().is_match(s);
    has_operator || has_function
}

/// Returns true if the string contains only alphanumeric characters and underscores.
pub fn is_simple_column(s: &str) -> bool {
    let re = Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$").unwrap();
    re.is_match(s)
}

/// Helper function to determine if an expression is an aggregate.
pub fn is_aggregate_expression(expr: &str) -> bool {
    let aggregate_functions = [
    //agg funcs
    "SUM", "AVG", "MAX", "MIN", "MEAN", "MEDIAN","COUNT", "LAST_VALUE", "FIRST_VALUE",  
    "GROUPING", "STRING_AGG", "ARRAY_AGG","VAR", "VAR_POP", "VAR_POPULATION", "VAR_SAMP", "VAR_SAMPLE",  
    "BIT_AND", "BIT_OR", "BIT_XOR", "BOOL_AND", "BOOL_OR",
    //scalar funcs unnecessary here but willl use it else where
    "ABS", "FLOOR", "CEIL", "SQRT", "ISNAN", "ISZERO",  "PI", "POW", "POWER", "RADIANS", "RANDOM", "ROUND",  
   "FACTORIAL", "ACOS", "ACOSH", "ASIN", "ASINH",  "COS", "COSH", "COT", "DEGREES", "EXP","SIN", "SINH", "TAN", "TANH", "TRUNC", "CBRT", "ATAN", "ATAN2", "ATANH", "GCD", "LCM", "LN",  "LOG", "LOG10", "LOG2", "NANVL", "SIGNUM"
   ];
   
   aggregate_functions.iter().any(|&func| expr.to_uppercase().starts_with(func))
 
}

pub fn is_datetime_expression(expr: &str) -> bool {
    // List of all datetime functions to check for
    let datetime_functions = [
        "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "DATE_BIN", "DATE_FORMAT",
        "DATE_PART", "DATE_TRUNC", "DATEPART", "DATETRUNC", "FROM_UNIXTIME", "MAKE_DATE",
        "NOW", "TO_CHAR", "TO_DATE", "TO_LOCAL_TIME", "TO_TIMESTAMP", "TO_TIMESTAMP_MICROS",
        "TO_TIMESTAMP_MILLIS", "TO_TIMESTAMP_NANOS", "TO_TIMESTAMP_SECONDS", "TO_UNIXTIME", "TODAY"
    ];

    datetime_functions.iter().any(|&func| expr.to_uppercase().starts_with(func))
}

/// Normalizes datetime expressions by quoting column names with double quotes.
pub fn normalize_datetime_expression(expr: &str) -> String {
    let re = Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*)\.(?P<column>[A-Za-z_][A-Za-z0-9_]*)\b").unwrap();

    let expr_with_columns = re.replace_all(expr, |caps: &regex::Captures| {
        format!("\"{}\"", caps["column"].to_lowercase())
    }).to_string();

    expr_with_columns.to_lowercase()
}

/// window functions normalization
pub fn normalize_window_function(expression: &str) -> String {
    let parts: Vec<&str> = expression.splitn(2, " OVER ").collect();
    if parts.len() != 2 {
        return expression.to_lowercase();
    }

    let function_part = parts[0].trim();
    let over_part = parts[1].trim();

    let func_regex = Regex::new(r"^(\w+)\((.*)\)$").unwrap();

    let (normalized_function, maybe_args) = if let Some(caps) = func_regex.captures(function_part) {
        let func_name = &caps[1];
        let arg_list_str = &caps[2];

        let raw_args: Vec<&str> = arg_list_str.split(',').map(|s| s.trim()).collect();
        
        let normalized_args: Vec<String> = raw_args
            .iter()
            .map(|arg| normalize_function_arg(arg))
            .collect();

        (func_name.to_lowercase(), Some(normalized_args))
    } else {
        (function_part.to_lowercase(), None)
    };

    let rebuilt_function = if let Some(args) = maybe_args {
        format!("{}({})", normalized_function, args.join(", ").to_lowercase())
    } else {
        normalized_function
    };

    let re_cols = Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b").unwrap();
    let normalized_over = re_cols.replace_all(over_part, "\"$1\".\"$2\"")
        .to_string()
        .to_lowercase();

    format!("{} OVER {}", rebuilt_function, normalized_over)
}

/// Helper: Normalize one argument if it looks like a table.column reference.
pub fn normalize_function_arg(arg: &str) -> String {
    let re_table_col = Regex::new(r"^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)$").unwrap();

    if let Some(caps) = re_table_col.captures(arg) {
        let table = &caps[1];
        let col = &caps[2];
        format!("\"{}\".\"{}\"", 
            table.to_lowercase(), 
            col.to_lowercase())
    } else {
        arg.to_lowercase()
    }
}