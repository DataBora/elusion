use crate::prelude::*;

static AS_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\s+AS\s+").expect("Failed to compile AS regex")
});

static TABLE_COLUMN_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b")
        .expect("Failed to compile table.column regex")
});

static STRING_LITERAL_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"'([^']*)'")
        .expect("Failed to compile table.column regex")
});

static FUNCTION_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^([A-Za-z_][A-Za-z0-9_]*)\s*\((.*)\)$")
        .expect("Failed to compile function regex")
});

static SIMPLE_COLUMN_PATTERN: Lazy<Regex> = Lazy::new(|| {
    //Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$")
      Regex::new(r"\b[A-Za-z_][A-Za-z0-9_]*\b").expect("Failed to compile simple column regex")
});

static OPERATOR_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"([\+\-\*\/])")
        .expect("Failed to compile operator regex")
});

static WINDOW_FUNCTION_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^(\w+)\((.*)\)$")
        .expect("Failed to compile window function regex")
});

// PostgreSQL-style casting pattern (::TYPE) - handles both simple columns and table.column
static POSTGRES_CAST_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)::(TEXT|VARCHAR|INTEGER|BIGINT|FLOAT|DOUBLE|BOOLEAN|DATE|TIMESTAMP)")
        .expect("Failed to compile PostgreSQL cast regex")
});

static AGGREGATE_FUNCTIONS: Lazy<std::collections::HashSet<&'static str>> = Lazy::new(|| {
    [
        "SUM", "AVG", "MAX", "MIN", "MEAN", "MEDIAN", "COUNT", "LAST_VALUE", "FIRST_VALUE",
        "GROUPING", "STRING_AGG", "ARRAY_AGG", "VAR", "VAR_POP", "VAR_POPULATION", "VAR_SAMP", "VAR_SAMPLE",
        "BIT_AND", "BIT_OR", "BIT_XOR", "BOOL_AND", "BOOL_OR",
        "ABS", "FLOOR", "CEIL", "SQRT", "ISNAN", "ISZERO", "PI", "POW", "POWER", "RADIANS", "RANDOM", "ROUND",
        "FACTORIAL", "ACOS", "ACOSH", "ASIN", "ASINH", "COS", "COSH", "COT", "DEGREES", "EXP",
        "SIN", "SINH", "TAN", "TANH", "TRUNC", "CBRT", "ATAN", "ATAN2", "ATANH", "GCD", "LCM", "LN",
        "LOG", "LOG10", "LOG2", "NANVL", "SIGNUM"
    ].into_iter().collect()
});

static DATETIME_FUNCTIONS: Lazy<std::collections::HashSet<&'static str>> = Lazy::new(|| {
    [
        "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "DATE_BIN", "DATE_FORMAT",
        "DATE_PART", "DATE_TRUNC", "DATEPART", "DATETRUNC", "FROM_UNIXTIME", "MAKE_DATE",
        "NOW", "TO_CHAR", "TO_DATE", "TO_LOCAL_TIME", "TO_TIMESTAMP", "TO_TIMESTAMP_MICROS",
        "TO_TIMESTAMP_MILLIS", "TO_TIMESTAMP_NANOS", "TO_TIMESTAMP_SECONDS", "TO_UNIXTIME", "TODAY"
    ].into_iter().collect()
});

#[allow(dead_code)]
static SQL_KEYWORDS: Lazy<std::collections::HashSet<&'static str>> = Lazy::new(|| {
    [
        "TEXT", "INTEGER", "BIGINT", "VARCHAR", "FLOAT", "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP",
        "SELECT", "FROM", "WHERE", "GROUP", "ORDER", "BY", "HAVING", "JOIN", "INNER", "LEFT", 
        "RIGHT", "OUTER", "FULL", "LEFT SEMI", "RIGHT SEMI", "LEFT ANTI", "RIGHT ANTI", "LEFT MARK" ,
        "ON", "AS", "AND", "OR", "NOT", "NULL", "TRUE", "FALSE", "CASE", 
        "WHEN", "THEN", "ELSE", "END", "IN", "EXISTS", "BETWEEN", "LIKE", "IS", "DISTINCT",
        "ASC", "DESC", "LIMIT", "OFFSET", "UNION", "INTERSECT", "EXCEPT", "ALL", "ANY", "SOME", "POSITION", 
    ].into_iter().collect()
});

/// Advanced function argument parser that handles nested parentheses, quotes, and complex expressions
fn parse_function_arguments(args: &str) -> Vec<String> {
    if args.trim().is_empty() {
        return Vec::new();
    }
    
    #[cfg(test)]
    eprintln!("DEBUG: Parsing function arguments: '{}'", args);
    
    let mut result = Vec::new();
    let mut current_arg = String::new();
    let mut paren_depth = 0;
    let mut in_quotes = false;
    let mut quote_char = '\0';
    let mut escaped = false;
    
    for ch in args.chars() {
        if escaped {
            current_arg.push(ch);
            escaped = false;
            continue;
        }
        
        match ch {
            '\\' if in_quotes => {
                escaped = true;
                current_arg.push(ch);
            },
            '\'' | '"' if !in_quotes => {
                in_quotes = true;
                quote_char = ch;
                current_arg.push(ch);
            },
            c if in_quotes && c == quote_char => {
                in_quotes = false;
                quote_char = '\0';
                current_arg.push(ch);
            },
            '(' if !in_quotes => {
                paren_depth += 1;
                current_arg.push(ch);
            },
            ')' if !in_quotes => {
                paren_depth -= 1;
                current_arg.push(ch);
            },
            ',' if !in_quotes && paren_depth == 0 => {
                let trimmed = current_arg.trim();
                if !trimmed.is_empty() {
                    #[cfg(test)]
                    eprintln!("DEBUG: Found argument: '{}'", trimmed);
                    result.push(trimmed.to_string());
                }
                current_arg.clear();
            },
            _ => {
                current_arg.push(ch);
            }
        }
    }
    
    let trimmed = current_arg.trim();
    if !trimmed.is_empty() {
        #[cfg(test)]
        eprintln!("DEBUG: Final argument: '{}'", trimmed);
        result.push(trimmed.to_string());
    }
    
    #[cfg(test)]
    eprintln!("DEBUG: Total arguments parsed: {:?}", result);
    
    result
}

/// Converts PostgreSQL-style casting (::TYPE) to standard SQL CAST() function
fn normalize_postgres_casting(expr: &str) -> String {
    POSTGRES_CAST_PATTERN.replace_all(expr, |caps: &regex::Captures| {
        let column = &caps[1];
        let data_type = &caps[2];
        format!("CAST({} AS {})", column, data_type)
    }).to_string()
}

/// Enhanced expression normalization with better parsing
pub fn normalize_expression(expr: &str, table_alias: &str) -> String {

    if expr.to_uppercase().contains(" OVER ") {
        return normalize_window_function(expr);
    }
    
    let expr_with_standard_cast = normalize_postgres_casting(expr);
    
    #[cfg(test)]
    eprintln!("DEBUG: Processing expression '{}', looking for AS patterns", expr_with_standard_cast);
    
    let all_matches: Vec<_> = AS_PATTERN.find_iter(&expr_with_standard_cast).collect();
    
    #[cfg(test)]
    eprintln!("DEBUG: Found {} AS matches", all_matches.len());
    
    for as_match in all_matches.iter().rev() {
        let before_as = &expr_with_standard_cast[..as_match.start()];
        let after_as = &expr_with_standard_cast[as_match.end()..];
        
        #[cfg(test)]
        eprintln!("DEBUG: Checking AS match - before_as='{}', after_as='{}'", before_as, after_as);
        
        let mut paren_depth = 0;
        let mut in_quotes = false;
        let mut quote_char = '\0';
        
        for ch in before_as.chars() {
            match ch {
                '\'' | '"' if !in_quotes => {
                    in_quotes = true;
                    quote_char = ch;
                },
                c if in_quotes && c == quote_char => {
                    in_quotes = false;
                    quote_char = '\0';
                },
                '(' if !in_quotes => {
                    paren_depth += 1;
                },
                ')' if !in_quotes => {
                    paren_depth -= 1;
                },
                _ => {}
            }
        }
        
        #[cfg(test)]
        eprintln!("DEBUG: Final paren_depth={}", paren_depth);
        
        if paren_depth == 0 {
            let expr_part = before_as.trim();
            let alias_part = after_as.trim();
            
            let normalized_expr = if is_aggregate_expression(expr_part) {
                normalize_aggregate_expression(expr_part, table_alias)
            } else if is_datetime_expression(expr_part) {
                normalize_datetime_expression(expr_part, table_alias)  
            } else {
                normalize_simple_expression(expr_part, table_alias)
            };
            
            #[cfg(test)]
            eprintln!("DEBUG: Before lowercasing: '{}'", normalized_expr);

            let final_result = format!("{} as \"{}\"", 
                normalized_expr.to_lowercase(), 
                alias_part.replace(' ', "_").to_lowercase());
                
            #[cfg(test)]
            eprintln!("DEBUG: Final expression result: '{}'", final_result);
            
            return final_result;
        }
    }
    
    #[cfg(test)]
    eprintln!("DEBUG: No top-level AS found, using fallback splitn logic");
    
    let parts: Vec<&str> = expr_with_standard_cast.splitn(2, " AS ").collect();
    if parts.len() == 2 {
        #[cfg(test)]
        eprintln!("DEBUG: Fallback found AS, expr_part='{}', alias_part='{}'", parts[0].trim(), parts[1].trim());
        let expr_part = parts[0].trim();
        let alias_part = parts[1].trim();
        let normalized_expr = if is_aggregate_expression(expr_part) {
            normalize_aggregate_expression(expr_part, table_alias)
        } else if is_datetime_expression(expr_part) {
            normalize_datetime_expression(expr_part, table_alias)  
        } else {
            normalize_simple_expression(expr_part, table_alias)
        };
        return format!(
            "{} as \"{}\"", 
            normalized_expr.to_lowercase(),
            alias_part.replace(" ", "_").to_lowercase()
        );
    }
    
    let result = if is_aggregate_expression(&expr_with_standard_cast) {
        normalize_aggregate_expression(&expr_with_standard_cast, table_alias)
    } else if is_datetime_expression(&expr_with_standard_cast) {
        normalize_datetime_expression(&expr_with_standard_cast, table_alias)  // Fixed: added table_alias
    } else {
        normalize_simple_expression(&expr_with_standard_cast, table_alias)
    };
    
    result.to_lowercase()
}

/// Enhanced aggregate expression normalization with proper argument parsing
pub fn normalize_aggregate_expression(expr: &str, table_alias: &str) -> String {
    if OPERATOR_PATTERN.is_match(expr) {
        return normalize_expression_with_operators(expr, table_alias);
    }

    if let Some(caps) = FUNCTION_PATTERN.captures(expr.trim()) {
        let func_name = &caps[1];
        let args = &caps[2];
        
        let arg_parts = parse_function_arguments(args);
        let mut normalized_args = Vec::with_capacity(arg_parts.len());
        
        for arg in arg_parts {
            normalized_args.push(normalize_simple_expression(&arg, table_alias));
        }
        
        format!("{}({})", func_name.to_lowercase(), normalized_args.join(", ").to_lowercase())
    } else {
        normalize_simple_expression(expr, table_alias)
    }
}

/// Enhanced simple expression normalization with better function handling
pub fn normalize_simple_expression(expr: &str, table_alias: &str) -> String {
    let expr_trimmed = expr.trim();
    
    let expr_with_standard_cast = normalize_postgres_casting(expr_trimmed);
    
    if expr_with_standard_cast.to_uppercase().starts_with("CASE") {
        return normalize_case_expression(&expr_with_standard_cast, table_alias);
    }
    
    if let Some(caps) = FUNCTION_PATTERN.captures(&expr_with_standard_cast) {
        let func_name = &caps[1];
        let args = &caps[2];
        
        #[cfg(test)]
        eprintln!("DEBUG: Function detected - name: '{}', args: '{}'", func_name, args);
        
        // Special handling for POSITION function which uses IN instead of comma
        if func_name.to_uppercase() == "POSITION" {
            let upper_args = args.to_uppercase();
            if let Some(in_pos) = upper_args.find(" IN ") {
                let search_expr = args[..in_pos].trim();
                let column_expr = args[in_pos + 4..].trim();
                
                #[cfg(test)]
                eprintln!("DEBUG: POSITION function - search: '{}', column: '{}'", search_expr, column_expr);
                
                // Handle the search expression (usually a string literal)
                let normalized_search = if search_expr.starts_with('\'') && search_expr.ends_with('\'') {
                    search_expr.to_string()
                } else {
                    normalize_simple_expression(search_expr, table_alias)
                };
                
                // Handle the column expression
                let normalized_column = if TABLE_COLUMN_PATTERN.is_match(column_expr) {
                    TABLE_COLUMN_PATTERN
                        .replace_all(column_expr, "\"$1\".\"$2\"")
                        .to_string()
                        .to_lowercase()
                } else if is_simple_column(column_expr) {
                    format!("\"{}\".\"{}\"", table_alias.to_lowercase(), column_expr.to_lowercase())
                } else {
                    normalize_simple_expression(column_expr, table_alias)
                };
                
                return format!("{}({} in {})", 
                    func_name.to_lowercase(), 
                    normalized_search, 
                    normalized_column);
            }
        }
        
        // Special handling for CAST function which uses AS instead of comma
        if func_name.to_uppercase() == "CAST" {
            let upper_args = args.to_uppercase();
            if let Some(as_pos) = upper_args.find(" AS ") {
                let expression = args[..as_pos].trim();
                let datatype = args[as_pos + 4..].trim();
                
                #[cfg(test)]
                eprintln!("DEBUG: CAST function - expression: '{}', datatype: '{}'", expression, datatype);
                
                let normalized_expr = if TABLE_COLUMN_PATTERN.is_match(expression) {
                    TABLE_COLUMN_PATTERN
                        .replace_all(expression, "\"$1\".\"$2\"")
                        .to_string()
                        .to_lowercase()
                } else if is_simple_column(expression) {
                    format!("\"{}\".\"{}\"", table_alias.to_lowercase(), expression.to_lowercase())
                } else {
                    normalize_simple_expression(expression, table_alias)
                };
                
                return format!("{}({} as {})", 
                    func_name.to_lowercase(), 
                    normalized_expr, 
                    datatype.to_lowercase());
            }
        }
        
        // Use advanced argument parser for other functions
        let arg_parts = parse_function_arguments(args);
        let mut normalized_args = Vec::with_capacity(arg_parts.len());
        
        for arg in arg_parts {
            let arg_trimmed = arg.trim();
            
            if arg_trimmed.starts_with('\'') && arg_trimmed.ends_with('\'') {
                // String literal - preserve as-is
                normalized_args.push(arg_trimmed.to_string());
            } else if arg_trimmed.starts_with('"') && arg_trimmed.ends_with('"') {
                // Quoted identifier - preserve as-is
                normalized_args.push(arg_trimmed.to_string());
            } else if arg_trimmed.chars().all(|c| c.is_ascii_digit() || c == '.' || c == '-') {
                // Numeric literal - preserve as-is
                normalized_args.push(arg_trimmed.to_string());
            } else if matches!(arg_trimmed.to_uppercase().as_str(), 
                              "TEXT" | "INTEGER" | "BIGINT" | "VARCHAR" | "FLOAT" | "DOUBLE" | 
                              "BOOLEAN" | "DATE" | "TIMESTAMP") {
                // Data type - lowercase
                normalized_args.push(arg_trimmed.to_lowercase());
            } else if FUNCTION_PATTERN.is_match(arg_trimmed) {
                // Nested function - recursively normalize
                #[cfg(test)]
                eprintln!("DEBUG: Processing nested function: '{}'", arg_trimmed);
                let nested_result = normalize_simple_expression(arg_trimmed, table_alias);
                #[cfg(test)]
                eprintln!("DEBUG: Nested function result: '{}'", nested_result);
                normalized_args.push(nested_result);
            } else if TABLE_COLUMN_PATTERN.is_match(arg_trimmed) {
                // This must come BEFORE is_simple_column check to catch table.column patterns
                let normalized = TABLE_COLUMN_PATTERN
                    .replace_all(arg_trimmed, "\"$1\".\"$2\"")
                    .to_string()
                    .to_lowercase();
                normalized_args.push(normalized);
            } else if is_simple_column(arg_trimmed) {
                // Simple column without table prefix - add the main table alias
                normalized_args.push(format!("\"{}\".\"{}\"", 
                    table_alias.to_lowercase(), 
                    arg_trimmed.to_lowercase()));
            } else if OPERATOR_PATTERN.is_match(arg_trimmed) {
                // Operator expression - handle recursively
                normalized_args.push(normalize_expression_with_operators(arg_trimmed, table_alias));
            } else {
                // Fallback - just lowercase
                #[cfg(test)]
                eprintln!("DEBUG: Argument '{}' fell through to default case", arg_trimmed);
                normalized_args.push(arg_trimmed.to_lowercase());
            }
        }
        
        let final_result = format!("{}({})", func_name.to_lowercase(), normalized_args.join(", "));
        
        #[cfg(test)]
        eprintln!("DEBUG: Final function result: '{}'", final_result);
        
        final_result
    } else if OPERATOR_PATTERN.is_match(&expr_with_standard_cast) {
        normalize_expression_with_operators(&expr_with_standard_cast, table_alias)
    } else if TABLE_COLUMN_PATTERN.is_match(&expr_with_standard_cast) {
        TABLE_COLUMN_PATTERN
            .replace_all(&expr_with_standard_cast, "\"$1\".\"$2\"")
            .to_string()
            .to_lowercase()
    } else if is_simple_column(&expr_with_standard_cast) {
        format!("\"{}\".\"{}\"", 
            table_alias.to_lowercase(), 
            expr_with_standard_cast.replace(' ', "_").to_lowercase())
    } else {
        expr_with_standard_cast.to_lowercase()
    }
}

/// Enhanced window function normalization with better argument parsing
pub fn normalize_window_function(expression: &str) -> String {
    let expr_with_standard_cast = normalize_postgres_casting(expression);
    
    if let Some(over_pos) = expr_with_standard_cast.to_uppercase().find(" OVER ") {
        let function_part = expr_with_standard_cast[..over_pos].trim();
        let over_part = expr_with_standard_cast[over_pos + 6..].trim();

        let (normalized_function, maybe_args) = if let Some(caps) = WINDOW_FUNCTION_PATTERN.captures(function_part) {
            let func_name = &caps[1];
            let arg_list_str = &caps[2];

            let raw_args = parse_function_arguments(arg_list_str);
            let mut normalized_args = Vec::with_capacity(raw_args.len());
            
            for arg in raw_args {
                normalized_args.push(normalize_function_arg(&arg));
            }

            (func_name.to_lowercase(), Some(normalized_args))
        } else {
            (function_part.to_lowercase(), None)
        };

        let rebuilt_function = if let Some(args) = maybe_args {
            format!("{}({})", normalized_function, args.join(", ").to_lowercase())
        } else {
            normalized_function
        };

        let normalized_over = TABLE_COLUMN_PATTERN
            .replace_all(over_part, "\"$1\".\"$2\"")
            .to_string()
            .to_lowercase();

        format!("{} over {}", rebuilt_function, normalized_over)
    } else {
        expr_with_standard_cast.to_lowercase()
    }
}

/// Enhanced function argument normalization
pub fn normalize_function_arg(arg: &str) -> String {

    let arg_with_standard_cast = normalize_postgres_casting(arg);
    
    if let Some(caps) = TABLE_COLUMN_PATTERN.captures(&arg_with_standard_cast) {
        let table = &caps[1];
        let col = &caps[2];
        format!("\"{}\".\"{}\"", 
            table.to_lowercase(), 
            col.to_lowercase())
    } else {
        arg_with_standard_cast.to_lowercase()
    }
}

fn normalize_expression_with_operators(expr: &str, table_alias: &str) -> String {
    let mut result = String::with_capacity(expr.len() * 2);
    let parts: Vec<&str> = OPERATOR_PATTERN.split(expr).collect();
    let operators: Vec<&str> = OPERATOR_PATTERN.find_iter(expr).map(|m| m.as_str()).collect();
    
    for (i, part) in parts.iter().enumerate() {
        let trimmed = part.trim();
        
        if TABLE_COLUMN_PATTERN.is_match(trimmed) {
            result.push_str(&TABLE_COLUMN_PATTERN
                .replace_all(trimmed, "\"$1\".\"$2\"")
                .to_string()
                .to_lowercase());
        } else if FUNCTION_PATTERN.is_match(trimmed) {
            if is_aggregate_expression(trimmed) {
                result.push_str(&normalize_aggregate_expression(trimmed, table_alias));
            } else {
                result.push_str(&normalize_simple_expression(trimmed, table_alias));
            }
        } else if is_simple_column(trimmed) {
            result.push_str(&format!("\"{}\".\"{}\"", 
                table_alias.to_lowercase(), 
                trimmed.to_lowercase()));
        } else {
            result.push_str(&trimmed.to_lowercase());
        }
        
        if let Some(op) = operators.get(i) {
            result.push_str(&format!(" {} ", op));
        }
    }
    
    result
}

fn normalize_case_expression(expr: &str, table_alias: &str) -> String {
    let mut result = expr.to_string();
    
    result = TABLE_COLUMN_PATTERN
        .replace_all(&result, "\"$1\".\"$2\"")
        .to_string();

    let simple_column_in_context = Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*)\b").unwrap();
    
    result = simple_column_in_context.replace_all(&result, |caps: &regex::Captures| {
        let word = &caps[1];
        let word_upper = word.to_uppercase();
        
        let full_match = caps.get(0).unwrap();
        let match_start = full_match.start();
        let match_end = full_match.end();
        
        let before = &result[..match_start];
        let after = &result[match_end..];
        
        let quotes_before = before.matches('\'').count();
        let inside_quotes = quotes_before % 2 == 1;
        
        if word.starts_with('"') || 
           inside_quotes ||
           SQL_KEYWORDS.contains(word_upper.as_str()) ||
           word.chars().all(|c| c.is_ascii_digit() || c == '.') ||
           after.trim_start().starts_with('(') {
            word.to_string()
        } else if is_simple_column(word) {
            format!("\"{}\".\"{}\"", table_alias.to_lowercase(), word.to_lowercase())
        } else {
            word.to_lowercase()
        }
    }).to_string();
    
    result.to_lowercase()
}

pub async fn lowercase_column_names(df: DataFrame) -> ElusionResult<DataFrame> {
    let schema = df.schema();
   
    let mut columns = Vec::with_capacity(schema.fields().len());
    
    for field in schema.fields() {
        let name = field.name();
        let normalized = normalize_field_name(name);
        columns.push(format!("\"{}\" as \"{}\"", name, normalized));
    }

    let ctx = SessionContext::new();
    
    let batches = df.clone().collect().await?;
    let mem_table = MemTable::try_new(schema.clone().into(), vec![batches])?;
    ctx.register_table("temp_table", Arc::new(mem_table))?;
    
    let sql = format!("SELECT {} FROM temp_table", columns.join(", "));
    ctx.sql(&sql).await.map_err(|e| ElusionError::Custom(format!("Failed to lowercase column names: {}", e)))
}

fn normalize_field_name(name: &str) -> Cow<str> {
    let trimmed = name.trim();
    
    if trimmed.chars().all(|c| c.is_ascii_lowercase() || c == '_') && !trimmed.contains(' ') {
        Cow::Borrowed(trimmed)
    } else {
        Cow::Owned(trimmed.replace(' ', "_").to_lowercase())
    }
}

pub fn normalize_alias_write(alias: &str) -> Cow<str> {
    let trimmed = alias.trim();
    
    if trimmed.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') {
        Cow::Borrowed(trimmed)
    } else {
        Cow::Owned(trimmed.to_lowercase())
    }
}

pub fn normalize_column_name(name: &str) -> String {
    let name_upper = name.to_uppercase();
    
    if name_upper.contains(" AS ") {
        let parts: Vec<&str> = AS_PATTERN.split(name).collect();
        
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
            format_table_column(name)
        }
    } else {
        format_table_column(name)
    }
}

#[inline]
fn format_table_column(name: &str) -> String {
    if let Some(pos) = name.find('.') {
        let table = &name[..pos];
        let column = &name[pos + 1..];
        format!("\"{}\".\"{}\"", 
            table.trim().to_lowercase(), 
            column.trim().replace(' ', "_").to_lowercase())
    } else {
        format!("\"{}\"", 
            name.trim().replace(' ', "_").to_lowercase())
    }
}

pub fn normalize_alias(alias: &str) -> String {
    format!("\"{}\"", alias.trim().to_lowercase())
}

pub fn normalize_condition(condition: &str) -> String {
    TABLE_COLUMN_PATTERN
        .replace_all(condition.trim(), "\"$1\".\"$2\"")
        .to_string()
        .to_lowercase()
}

pub fn normalize_condition_filter(condition: &str) -> String {
    let trimmed = condition.trim();
   // eprintln!("Original condition: '{}'", trimmed);

    let sql_keywords: HashSet<&str> = [
        "SELECT", "FROM", "WHERE", "IS", "NOT", "IN", "NULL", "LIKE","OR", "AND"
    ].into_iter().collect();

    let mut result = String::new();
    let mut last_end = 0;

    for cap in STRING_LITERAL_PATTERN.find_iter(trimmed) {
        let before = &trimmed[last_end..cap.start()];

        // replacing table.column with temp markers
        let matches: Vec<_> = TABLE_COLUMN_PATTERN.captures_iter(before).collect();
        let mut temp_replacements: HashMap<String, String> = HashMap::new();
        let mut modified_before = before.to_string();
        let mut offset: isize = 0;
        for (i, match_cap) in matches.iter().enumerate() {
            let marker = format!("1TEMP{}", i);
            let table = match_cap.get(1).unwrap().as_str();
            let column = match_cap.get(2).unwrap().as_str().to_lowercase();
            let quoted = format!("\"{}\".\"{}\"", table, column);
            temp_replacements.insert(marker.clone(), quoted);
            let start = (match_cap.get(0).unwrap().start() as isize + offset) as usize;
            let end = (match_cap.get(0).unwrap().end() as isize + offset) as usize;
            modified_before.replace_range(start..end, &marker);
            offset += marker.len() as isize - (end - start) as isize;
        }

        // replacing simple columns
        let replaced = SIMPLE_COLUMN_PATTERN.replace_all(&modified_before, |caps: &regex::Captures| {
            let column = caps.get(0).unwrap().as_str();
            let upper_column = column.to_uppercase();
            if sql_keywords.contains(upper_column.as_str()) {
                column.to_string()
            } else {
                format!("\"{}\"", column.to_lowercase())
            }
        }).to_string();

        // replacing back temp markers
        let mut final_replaced = replaced;
        for (marker, quoted) in temp_replacements {
            final_replaced = final_replaced.replace(&marker, &quoted);
        }

        result.push_str(&final_replaced);
        result.push_str(cap.as_str());
        last_end = cap.end();
    }

    let remaining = &trimmed[last_end..];

    // Same process for remaining
    let matches: Vec<_> = TABLE_COLUMN_PATTERN.captures_iter(remaining).collect();
    let mut temp_replacements: HashMap<String, String> = HashMap::new();
    let mut modified_remaining = remaining.to_string();
    let mut offset: isize = 0;
    for (i, match_cap) in matches.iter().enumerate() {
        let marker = format!("1TEMP{}", i);
        let table = match_cap.get(1).unwrap().as_str();
        let column = match_cap.get(2).unwrap().as_str().to_lowercase();
        let quoted = format!("\"{}\".\"{}\"", table, column);
        temp_replacements.insert(marker.clone(), quoted);
        let start = (match_cap.get(0).unwrap().start() as isize + offset) as usize;
        let end = (match_cap.get(0).unwrap().end() as isize + offset) as usize;
        modified_remaining.replace_range(start..end, &marker);
        offset += marker.len() as isize - (end - start) as isize;
    }

    let replaced = SIMPLE_COLUMN_PATTERN.replace_all(&modified_remaining, |caps: &regex::Captures| {
        let column = caps.get(0).unwrap().as_str();
        let upper_column = column.to_uppercase();
        if sql_keywords.contains(upper_column.as_str()) {
            column.to_string()
        } else {
            format!("\"{}\"", column.to_lowercase())
        }
    }).to_string();

    let mut final_replaced = replaced;
    for (marker, quoted) in temp_replacements {
        final_replaced = final_replaced.replace(&marker, &quoted);
    }

    result.push_str(&final_replaced);

   // eprintln!("Final normalized condition: '{}'", result);

    result
}

pub fn is_expression(s: &str) -> bool {
    s.chars().any(|c| matches!(c, '+' | '-' | '*' | '/' | '%' | '(' | ')' | ',' | '.')) ||
    s.contains("(")
}

pub fn is_simple_column(s: &str) -> bool {
    let matches_pattern = SIMPLE_COLUMN_PATTERN.is_match(s);
    let upper_s = s.to_uppercase();
    let is_keyword = SQL_KEYWORDS.contains(upper_s.as_str());
    let is_complex = s.contains('(') || s.contains(')') || s.to_lowercase().contains(" as ") || OPERATOR_PATTERN.is_match(s);

    let has_table_prefix = s.contains('.');

    // eprintln!("REAL FUNCTION DEBUG: is_simple_column('{}') - matches_pattern: {}, upper: '{}', is_keyword: {}, is_complex: {}", 
    //     s, matches_pattern, upper_s, is_keyword, is_complex);
    
    let result = matches_pattern && !is_keyword && !is_complex && !has_table_prefix;
    // eprintln!("REAL FUNCTION DEBUG: final result = {} && !{} && !{} && !{} = {}", 
    //     matches_pattern, is_keyword, is_complex, has_table_prefix, result);
    
    result
}

pub fn is_aggregate_expression(expr: &str) -> bool {
    let upper_expr = expr.to_uppercase();
    
    if let Some(paren_pos) = upper_expr.find('(') {
        let func_name = upper_expr[..paren_pos].trim();
        AGGREGATE_FUNCTIONS.contains(func_name)
    } else {
        AGGREGATE_FUNCTIONS.iter().any(|&func| upper_expr.starts_with(func))
    }
}

pub fn is_datetime_expression(expr: &str) -> bool {
    let upper_expr = expr.to_uppercase();
    
    if let Some(paren_pos) = upper_expr.find('(') {
        let func_name = upper_expr[..paren_pos].trim();
        DATETIME_FUNCTIONS.contains(func_name)
    } else {
        DATETIME_FUNCTIONS.iter().any(|&func| upper_expr.starts_with(func))
    }
}

pub fn normalize_datetime_expression(expr: &str, table_alias: &str) -> String {
    let expr_with_standard_cast = normalize_postgres_casting(expr);
    
    if let Some(caps) = FUNCTION_PATTERN.captures(&expr_with_standard_cast) {
        let func_name = &caps[1];
        let args = &caps[2];
        
        let arg_parts = parse_function_arguments(args);
        let mut normalized_args = Vec::with_capacity(arg_parts.len());
        
        for arg in arg_parts {
            let arg_trimmed = arg.trim();
            
            if arg_trimmed.starts_with('\'') && arg_trimmed.ends_with('\'') {
                normalized_args.push(arg_trimmed.to_string());
            } else if arg_trimmed.chars().all(|c| c.is_ascii_digit() || c == '.' || c == '-') {
                normalized_args.push(arg_trimmed.to_string());
            } else if TABLE_COLUMN_PATTERN.is_match(arg_trimmed) {
                let normalized = TABLE_COLUMN_PATTERN
                    .replace_all(arg_trimmed, "\"$1\".\"$2\"")
                    .to_string()
                    .to_lowercase();
                normalized_args.push(normalized);
            } else if is_simple_column(arg_trimmed) {
                normalized_args.push(format!("\"{}\".\"{}\"", 
                    table_alias.to_lowercase(), 
                    arg_trimmed.to_lowercase()));
            } else if FUNCTION_PATTERN.is_match(arg_trimmed) {
                let nested_result = normalize_datetime_expression(arg_trimmed, table_alias);
                normalized_args.push(nested_result);
            } else if OPERATOR_PATTERN.is_match(arg_trimmed) {
                let operator_result = normalize_expression_with_operators(arg_trimmed, table_alias);
                normalized_args.push(operator_result);
            } else {
                normalized_args.push(arg_trimmed.to_lowercase());
            }
        }
        
        format!("{}({})", func_name.to_lowercase(), normalized_args.join(", "))
    } else {
        if TABLE_COLUMN_PATTERN.is_match(&expr_with_standard_cast) {
            TABLE_COLUMN_PATTERN
                .replace_all(&expr_with_standard_cast, "\"$1\".\"$2\"")
                .to_string()
                .to_lowercase()
        } else if is_simple_column(&expr_with_standard_cast) {
            format!("\"{}\".\"{}\"", 
                table_alias.to_lowercase(), 
                expr_with_standard_cast.to_lowercase())
        } else {
            expr_with_standard_cast.to_lowercase()
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_function_arguments() {

        assert_eq!(
            parse_function_arguments("a, b, c"),
            vec!["a", "b", "c"]
        );
        
        assert_eq!(
            parse_function_arguments("godina::TEXT, '-', LPAD(mesec, 10, '0')"),
            vec!["godina::TEXT", "'-'", "LPAD(mesec, 10, '0')"]
        );
        
        assert_eq!(
            parse_function_arguments("SUM(amount), CONCAT(fname, ' ', lname), COUNT(*)"),
            vec!["SUM(amount)", "CONCAT(fname, ' ', lname)", "COUNT(*)"]
        );
        
        assert_eq!(
            parse_function_arguments("'Hello, World', 'Another, String'"),
            vec!["'Hello, World'", "'Another, String'"]
        );
    }
    
    #[test]
    fn test_postgres_casting_normalization() {
        assert_eq!(
            normalize_postgres_casting("godina::TEXT"),
            "CAST(godina AS TEXT)"
        );
        
        assert_eq!(
            normalize_postgres_casting("amount::BIGINT"),
            "CAST(amount AS BIGINT)"
        );
        
        assert_eq!(
            normalize_postgres_casting("CONCAT(godina::TEXT, '-', mesec::INTEGER)"),
            "CONCAT(CAST(godina AS TEXT), '-', CAST(mesec AS INTEGER))"
        );
    }
    
    #[test]
    fn test_enhanced_expression_normalization() {

        let result = normalize_expression(
            "CONCAT(godina::TEXT, '-', LPAD(mesec, 10, '0')) AS year_month", 
            "arch"
        );
        
        assert!(result.contains("lpad"));
        assert!(result.contains("year_month"));
        assert!(!result.contains("::")); 
    }
    
    #[test]
    fn test_simple_column_detection() {

        let test_cases = vec![
            ("region", true),
            ("region_rank", true), 
            ("test_column", true),
            ("TEXT", false),
            ("INTEGER", false),
            ("SELECT", false),
        ];
        
        for (input, expected) in test_cases {
            let result = is_simple_column(input);
            println!("is_simple_column('{}') = {} (expected: {})", input, result, expected);
            
            if result != expected {
                let matches_pattern = SIMPLE_COLUMN_PATTERN.is_match(input);
                let upper_input = input.to_uppercase();
                let is_keyword = SQL_KEYWORDS.contains(upper_input.as_str());
                
                println!("  DEBUG: matches_pattern={}, upper='{}', is_keyword={}", 
                    matches_pattern, upper_input, is_keyword);
                println!("  DEBUG: SQL_KEYWORDS.len()={}", SQL_KEYWORDS.len());
                println!("  DEBUG: First few keywords: {:?}", 
                    SQL_KEYWORDS.iter().take(3).collect::<Vec<_>>());
            }
            
            assert_eq!(result, expected, 
                "is_simple_column('{}') should return {}", input, expected);
        }
        
        let result = normalize_simple_expression("region_rank", "pipeline_ranked");
        println!("Simple region_rank result: {}", result);
        assert_eq!(result, "\"pipeline_ranked\".\"region_rank\"");
    }
    
    #[test]
    fn test_benchmark_failing_cases() {
        let result1 = normalize_expression(
            "CONCAT(region, ' - Rank ', CAST(region_rank AS TEXT)) AS region_rank_label", 
            "pipeline_ranked"
        );
        
        println!("Result1: {}", result1);

        assert!(result1.contains("concat("));
        assert!(result1.contains("cast("));
        assert!(result1.contains(" as \"region_rank_label\""));
        assert!(result1.contains("\"pipeline_ranked\".\"region_rank\"")); 
        assert!(result1.contains("text)"));  
        
        let expected_pattern = "cast(\"pipeline_ranked\".\"region_rank\" as text)";
        assert!(result1.contains(expected_pattern), 
            "Expected '{}' in result: {}", expected_pattern, result1);
        
        let result3 = normalize_expression(
            "CONCAT(TRIM(region), ' - Rank ', TRIM(region_rank)) AS region_rank_label",
            "pipeline_ranked"
        );
        
        println!("Result3 (TRIM): {}", result3);
        
        let result4 = normalize_expression(
            "CONCAT(TRIM(region), ' _', TRIM(region_rank)) AS region_rank_label",
            "pipeline_ranked"
        );
        
        println!("Result4 (TRIM with underscore): {}", result4);
        
        let result2 = normalize_expression(
            "CASE WHEN region_rank <= 5 THEN 'TOP_5' ELSE 'OTHER' END AS performance_tier",
            "pipeline_ranked"
        );
        
        println!("Result2: {}", result2);
        
        assert!(result2.contains("case when"));
        assert!(result2.contains(" as \"performance_tier\""));
        assert!(!result2.contains("_as_"));  // No mangled alias
    }

    #[test]
    fn test_basic_filter_query() {
        // Test the exact pattern from your basic filter query
        let expressions = vec![
            "customer_name",
            "order_date", 
            "billable_value"
        ];
        
        println!("=== BASIC SELECT TEST ===");
        for expr in expressions {
            let result = normalize_expression(expr, "s");
            println!("'{}' -> '{}'", expr, result);
            
            assert!(result.contains("\"s\"."), "Missing table prefix in: {}", result);
        }
        
        let filters = vec![
            "order_date > '2021-07-04'",
            "billable_value > 100.0",
            "cusTomer_name == 'Customer IRRVL'"  
        ];
        
        println!("\n=== FILTER CONDITIONS TEST ===");
        for filter in filters {
            let result = normalize_condition_filter(filter);
            println!("'{}' -> '{}'", filter, result);
        }
    }
    
    #[test]
    fn test_complex_select_with_functions() {
      
        let expressions = vec![
            "customer_name",
            "order_date",
            "ABS(billable_value) AS abs_billable_value",
            "ROUND(SQRT(billable_value), 2) AS SQRT_billable_value", 
            "billable_value * 2 AS double_billable_value",
            "billable_value / 100 AS percentage_billable"
        ];
        
        println!("=== COMPLEX SELECT TEST ===");
        for expr in expressions {
            let result = normalize_expression(expr, "s");
            println!("'{}' -> '{}'", expr, result);
            
            if expr.contains(" AS ") {
                assert!(result.contains(" as \""), "Missing AS clause in: {}", result);
                assert!(!result.contains("  "), "Double spaces in: {}", result);
            }
            
            if !expr.contains("(") && !expr.contains(" AS ") {
                assert!(result.contains("\"s\"."), "Missing table prefix in simple column: {}", result);
            }
            
            if expr.contains("billable_value") {
                assert!(result.contains("\"s\".\"billable_value\""), "Missing table prefix on column reference: {}", result);
            }
        }
    }
    
    #[test]
    fn test_aggregation_expressions() {
        let agg_expressions = vec![
            "ROUND(AVG(ABS(billable_value)), 2) AS avg_abs_billable",
            "SUM(billable_value) AS total_billable",
            "MAX(ABS(billable_value)) AS max_abs_billable", 
            "SUM(billable_value) * 2 AS double_total_billable",
            "SUM(billable_value) / 100 AS percentage_total_billable"
        ];
        
        println!("=== AGGREGATION EXPRESSIONS TEST ===");
        for expr in agg_expressions {

            if expr.contains("SUM(billable_value) * 2") {
                let expr_part = "SUM(billable_value) * 2";
                println!("DEBUG: Testing operator detection on '{}'", expr_part);
                println!("DEBUG: OPERATOR_PATTERN.is_match = {}", OPERATOR_PATTERN.is_match(expr_part));
                println!("DEBUG: is_aggregate_expression = {}", is_aggregate_expression(expr_part));
                
                let direct_result = normalize_expression_with_operators(expr_part, "s");
                println!("DEBUG: Direct operator handler result: '{}'", direct_result);
            }
            
            let result = normalize_expression(expr, "s");
            println!("'{}' -> '{}'", expr, result);
            
            assert!(result.contains(" as \""), "Missing AS clause in: {}", result);
            assert!(result.ends_with("\""), "Missing end quote in: {}", result);
            
            assert!(result.contains("\"s\".\"billable_value\""), "Missing table prefix: {}", result);
            
            if expr.contains("ROUND") {
                assert!(result.contains("round("), "Function not lowercased: {}", result);
            }
            if expr.contains("SUM") {
                assert!(result.contains("sum("), "Function not lowercased: {}", result);
            }
        }
    }
    
    #[test]
    fn test_column_alias_patterns() {
        let column_expressions = vec![
            "column_1 AS Site",
            "column_2 AS Location", 
            "column_3 AS Centre",
            "column_4 as Breafast_Net",
            "column_5 AS Breafast_Gross",
            "column_6 AS Lunch_Net",
            "column_7 AS Lunch_Gross",
            "column_8 AS Dinner_Net", 
            "column_9 AS Dinner_Gross"
        ];
        
        println!("=== COLUMN ALIAS TEST ===");
        for expr in column_expressions {
            let result = normalize_expression(expr, "s");
            println!("'{}' -> '{}'", expr, result);
            
            assert!(result.contains("\"s\"."), "Missing table prefix: {}", result);
            assert!(result.contains(" as \""), "Missing AS clause: {}", result);
            assert!(result.ends_with("\""), "Missing end quote: {}", result);
            
            if expr.contains("Breafast_Net") {
                assert!(result.contains("\"breafast_net\""), "Alias not normalized: {}", result);
            }
        }
    }
    
    #[test]
    fn test_string_functions_real_patterns() {
        let string_functions = vec![
            "TRIM(c.EmailAddress) AS trimmed_email",
            "UPPER(c.FirstName) AS upper_first_name",
            "LOWER(c.LastName) AS lower_last_name",
            "LENGTH(c.EmailAddress) AS email_length",
            "CONCAT(c.FirstName, ' ', c.LastName) AS full_name",
            "SUBSTRING(p.ProductName, 1, 5) AS product_substr",
            "REPLACE(c.EmailAddress, '@adventure-works.com', '@newdomain.com') AS new_email",
            "LPAD(c.CustomerKey::TEXT, 10, '0') AS padded_customer_id",
            "SPLIT_PART(c.EmailAddress, '@', 1) AS email_username",
            "POSITION('APOTEKA' IN naziv_ustanove) AS apoteka_pos",
            "CASE WHEN naziv_proizvoda LIKE '%SENI%' THEN 'SENI_PRODUCT' ELSE 'OTHER' END AS product_category",
            "CONCAT(region, ' - Rank ', CAST(region_rank AS TEXT)) AS region_rank_label",
            "CASE WHEN region_rank <= 5 THEN 'TOP_5' ELSE 'OTHER' END AS performance_tier"
        ];
        
        println!("=== STRING FUNCTIONS TEST ===");
        for expr in string_functions {
            let result = normalize_expression(expr, "main");
            println!("'{}' -> '{}'", expr, result);
            
            assert!(result.contains(" as \""), "Missing AS clause: {}", result);
            
            if expr.contains("c.EmailAddress") {
                assert!(result.contains("\"c\".\"emailaddress\""), "Table.column not preserved: {}", result);
            }
            
            if expr.contains("::TEXT") {
                assert!(result.contains("cast("), "PostgreSQL casting not converted: {}", result);
                assert!(result.contains(" as text"), "CAST syntax incorrect: {}", result);
            }
        }
    }
    
    #[test]
    fn test_window_functions_real_patterns() {
        let window_functions = vec![
            "SUM(s.OrderQuantity) OVER (PARTITION BY c.CustomerKey ORDER BY s.OrderDate) as running_total",
            "ROW_NUMBER() OVER (ORDER BY c.CustomerKey) AS customer_index",
            "DENSE_RANK() OVER (PARTITION BY c.CustomerKey ORDER BY s.OrderDate) AS dense_rnk",
            "LAG(s.OrderQuantity, 1, 0) OVER (PARTITION BY c.CustomerKey ORDER BY s.OrderDate) AS prev_qty",
            "FIRST_VALUE(s.OrderQuantity) OVER (PARTITION BY c.CustomerKey ORDER BY s.OrderDate) AS first_qty"
        ];
        
        println!("=== WINDOW FUNCTIONS TEST ===");
        for expr in window_functions {
            let result = normalize_window_function(expr);
            println!("'{}' -> '{}'", expr, result);
            
            assert!(result.contains(" over "), "OVER not lowercased: {}", result);
            
            if expr.contains("s.OrderQuantity") {
                assert!(result.contains("\"s\".\"orderquantity\""), "Table.column not preserved: {}", result);
            }
            
            if expr.contains("PARTITION BY") {
                assert!(result.contains("partition by"), "PARTITION BY not lowercased: {}", result);
            }
        }
    }
    
    #[test]
    fn test_datetime_expressions_real_patterns() {
        let datetime_expressions = vec![
            "CAST(DATE_PART('week', CURRENT_DATE()) as INT) AS current_week_num",
            "TRIM(wr.week_start) AS datefrom",
            "TRIM(wr.week_end) AS dateto",
            "DATE_PART('year', order_date) AS year",
            "DATE_PART('month', order_date) AS month"
        ];
        
        println!("=== DATETIME EXPRESSIONS TEST ===");
        for expr in datetime_expressions {
            let result = normalize_expression(expr, "main");
            println!("'{}' -> '{}'", expr, result);
            
            if expr.contains(" AS ") {
                assert!(result.contains(" as \""), "Missing AS clause: {}", result);
            }
            
            if expr.contains("DATE_PART") {
                assert!(result.contains("date_part("), "Function not lowercased: {}", result);
            }
            
            if expr.contains("wr.week_start") {
                assert!(result.contains("\"wr\".\"week_start\""), "Table.column not preserved: {}", result);
            }
        }
    }
    
    #[test]
    fn test_join_conditions_real_patterns() {
        let join_conditions = vec![
            "s.CustomerKey = c.CustomerKey",
            "s.ProductKey = p.ProductKey",
            "wr.week_num == cd.current_week_num"
        ];
        
        println!("=== JOIN CONDITIONS TEST ===");
        for condition in join_conditions {
            let result = normalize_condition_filter(condition);
            println!("'{}' -> '{}'", condition, result);
            
            assert!(result.contains("\""), "Missing quotes: {}", result);
            
            if condition.contains("==") {
                assert!(result.contains("=="), "== should be preserved: {}", result);
            }
        }
    }
    
    #[test]
    fn test_filter_conditions_real_patterns() {
  
        let filter_conditions = vec![
            ("column_1 != 'Site Name'", "\"column_1\" != 'Site Name'"),
            ("order_date > '2021-07-04'", "\"order_date\" > '2021-07-04'"),
            ("billable_value > 100.0", "\"billable_value\" > 100.0"),
            ("c.emailaddress IS NOT NULL", "\"c\".\"emailaddress\" IS NOT NULL")
        ];
        
        println!("=== FILTER CONDITIONS TEST ===");
        for (condition, _expected_pattern) in filter_conditions {
            let result = normalize_condition_filter(condition);
            println!("'{}' -> '{}'", condition, result);
            
            if !condition.contains(".") && condition.contains("column_1") {
                assert!(result.contains("\"column_1\""), "Simple column should be quoted: {}", result);
            }
            if !condition.contains(".") && condition.contains("order_date") {
                assert!(result.contains("\"order_date\""), "Simple column should be quoted: {}", result);
            }
            if !condition.contains(".") && condition.contains("billable_value") {
                assert!(result.contains("\"billable_value\""), "Simple column should be quoted: {}", result);
            }
            if condition.contains("c.emailaddress") {
                assert!(result.contains("\"c\".\"emailaddress\""), "Table.column should be quoted: {}", result);
            }
        }
    }
    
    #[test]
    fn test_complete_query_simulation() {

        println!("=== COMPLETE QUERY SIMULATION ===");
        
        let select_exprs = vec![
            normalize_expression("customer_name", "s"),
            normalize_expression("order_date", "s"),
            normalize_expression("ABS(billable_value) AS abs_billable_value", "s"),
            normalize_expression("ROUND(SQRT(billable_value), 2) AS SQRT_billable_value", "s"),
            normalize_expression("billable_value * 2 AS double_billable_value", "s"),
            normalize_expression("billable_value / 100 AS percentage_billable", "s")
        ];
        
        let agg_exprs = vec![
            normalize_expression("ROUND(AVG(ABS(billable_value)), 2) AS avg_abs_billable", "s"),
            normalize_expression("SUM(billable_value) AS total_billable", "s"),
            normalize_expression("MAX(ABS(billable_value)) AS max_abs_billable", "s"),
            normalize_expression("SUM(billable_value) * 2 AS double_total_billable", "s"),
            normalize_expression("SUM(billable_value) / 100 AS percentage_total_billable", "s")
        ];
        
        let all_exprs = [select_exprs, agg_exprs].concat();
        let combined = all_exprs.join(", ");
        
        println!("Combined SELECT clause:");
        println!("{}", combined);
        
        assert!(!combined.contains("  "), "Found double spaces in combined query");
        assert!(!combined.contains(", ,"), "Found empty expressions in combined query");
        assert!(!combined.contains(" as  "), "Found malformed AS clauses");
        
        let filter = normalize_condition_filter("cusTomer_name == 'Customer IRRVL'");
        println!("Filter clause: {}", filter);
        
        let simulated_sql = format!(
            "SELECT {} FROM table s WHERE {} GROUP BY {} ORDER BY {} LIMIT 10",
            combined,
            filter,
            "\"s\".\"customer_name\", \"s\".\"order_date\"", 
            "\"total_billable\" DESC"
        );
        
        println!("Simulated SQL:");
        println!("{}", simulated_sql);
        
        assert!(!simulated_sql.contains(" AS "), "Found uppercase AS - should be lowercase");
        assert!(simulated_sql.contains(" as \""), "Missing lowercase AS clauses");
    }
}