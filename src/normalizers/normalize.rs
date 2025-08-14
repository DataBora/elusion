use crate::prelude::*;

pub static AS_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\s+AS\s+").expect("Failed to compile AS regex")
});

pub static TABLE_COLUMN_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b")
        .expect("Failed to compile table.column regex")
});

pub static STRING_LITERAL_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"'([^']*)'")
        .expect("Failed to compile table.column regex")
});

pub static FUNCTION_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^([A-Za-z_][A-Za-z0-9_]*)\s*\((.*)\)$")
        .expect("Failed to compile function regex")
});

pub static SIMPLE_COLUMN_PATTERN: Lazy<Regex> = Lazy::new(|| {
    //Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$")
      Regex::new(r"\b[A-Za-z_][A-Za-z0-9_]*\b").expect("Failed to compile simple column regex")
});

pub static OPERATOR_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"([\+\-\*\/])")
        .expect("Failed to compile operator regex")
});

pub static WINDOW_FUNCTION_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^(\w+)\((.*)\)$")
        .expect("Failed to compile window function regex")
});

// PostgreSQL-style casting pattern (::TYPE) - handles both simple columns and table.column
pub static POSTGRES_CAST_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)::(TEXT|VARCHAR|INTEGER|BIGINT|FLOAT|DOUBLE|BOOLEAN|DATE|TIMESTAMP)")
        .expect("Failed to compile PostgreSQL cast regex")
});

pub static AGGREGATE_FUNCTIONS: Lazy<std::collections::HashSet<&'static str>> = Lazy::new(|| {
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

pub static DATETIME_FUNCTIONS: Lazy<std::collections::HashSet<&'static str>> = Lazy::new(|| {
    [
        "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "DATE_BIN", "DATE_FORMAT",
        "DATE_PART", "DATE_TRUNC", "DATEPART", "DATETRUNC", "FROM_UNIXTIME", "MAKE_DATE",
        "NOW", "TO_CHAR", "TO_DATE", "TO_LOCAL_TIME", "TO_TIMESTAMP", "TO_TIMESTAMP_MICROS",
        "TO_TIMESTAMP_MILLIS", "TO_TIMESTAMP_NANOS", "TO_TIMESTAMP_SECONDS", "TO_UNIXTIME", "TODAY"
    ].into_iter().collect()
});

pub static STRING_FUNCTIONS: Lazy<std::collections::HashSet<&'static str>> = Lazy::new(|| {
    [
        "TRIM", "LTRIM", "RTRIM", "UPPER", "LOWER", "LENGTH", "LEN", "CHAR_LENGTH", "CHARACTER_LENGTH",
        "LEFT", "RIGHT", "SUBSTRING", "SUBSTR", "MID", "POSITION", "STRPOS", "CHARINDEX", 
        "LOCATE", "FIND", "INSTR",
        "CONCAT", "CONCAT_WS", "STRING_AGG", "GROUP_CONCAT", "LISTAGG",
        "REPLACE", "TRANSLATE", "REVERSE", "REPEAT", "REPLICATE", "SPACE", "STUFF",
        "LPAD", "RPAD", "PADL", "PADR", "PAD",
        "INITCAP", "PROPER", "TITLE", "CAPITALIZE",
        "SPLIT_PART", "SPLIT", "PARSENAME", "SUBSTRING_INDEX",
        "LIKE", "ILIKE", "SIMILAR", "REGEXP", "REGEXP_LIKE", "REGEXP_REPLACE", "REGEXP_SUBSTR",
        "ASCII", "CHR", "CHAR", "UNICODE", "NCHAR",
        "CAST", "CONVERT", "TRY_CAST", "TRY_CONVERT", "TO_CHAR", "TO_VARCHAR", "TO_TEXT",
        "COALESCE", "NULLIF", "ISNULL", "NVL", "NVL2", "IFNULL",
        "SOUNDEX", "DIFFERENCE"
    ].into_iter().collect()
});

#[allow(dead_code)]
pub static SQL_KEYWORDS: Lazy<std::collections::HashSet<&'static str>> = Lazy::new(|| {
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
       // println!("DEBUG: parse_function_arguments input: '{}'", args);

        if args.trim().is_empty() {
            return Vec::new();
        }
        
        let mut result = Vec::new();
        let mut current_arg = String::new();
        let mut paren_depth = 0;
        let mut in_single_quotes = false;
        let mut in_double_quotes = false;
        let mut escaped = false;
        
        let chars: Vec<char> = args.chars().collect();
        let mut i = 0;
        
        while i < chars.len() {
            let ch = chars[i];
            
           // println!("DEBUG: pos={}, ch='{}', paren_depth={}, in_single_quotes={}, in_double_quotes={}, current_arg='{}'", 
          //          i, ch, paren_depth, in_single_quotes, in_double_quotes, current_arg);
            
            if escaped {
                current_arg.push(ch);
                escaped = false;
                i += 1;
                continue;
            }
            
            match ch {
                '\\' if in_single_quotes || in_double_quotes => {
                    escaped = true;
                    current_arg.push(ch);
                },
                '\'' if !in_double_quotes => {
                    in_single_quotes = !in_single_quotes;
                    current_arg.push(ch);
                },
                '"' if !in_single_quotes => {
                    in_double_quotes = !in_double_quotes;
                    current_arg.push(ch);
                },
                '(' if !in_single_quotes && !in_double_quotes => {
                    paren_depth += 1;
                    current_arg.push(ch);
                   // println!("DEBUG: Opening paren, depth now {}", paren_depth);
                },
                ')' if !in_single_quotes && !in_double_quotes => {
                    paren_depth -= 1;
                    current_arg.push(ch);
                   // println!("DEBUG: Closing paren, depth now {}", paren_depth);
                },
                ',' if !in_single_quotes && !in_double_quotes && paren_depth == 0 => {
                    let trimmed = current_arg.trim();
                    if !trimmed.is_empty() {
                   //     println!("DEBUG: Found top-level argument: '{}'", trimmed);
                        result.push(trimmed.to_string());
                    }
                    current_arg.clear();
                },
                _ => {
                    current_arg.push(ch);
                }
            }
            
            i += 1;
        }
        
        let trimmed = current_arg.trim();
        if !trimmed.is_empty() {
           // println!("DEBUG: Final argument: '{}'", trimmed);
            result.push(trimmed.to_string());
        }
        
       // println!("DEBUG: parse_function_arguments result: {:?}", result);
        result
    }

    /// Converts PostgreSQL-style casting (::TYPE) to standard SQL CAST() function
    pub fn normalize_postgres_casting(expr: &str) -> String {
  //  println!("DEBUG normalize_postgres_casting input: '{}'", expr);
    
    let result = POSTGRES_CAST_PATTERN.replace_all(expr, |caps: &regex::Captures| {
        let column = &caps[1];
        let data_type = &caps[2];
        let replacement = format!("CAST({} AS {})", column, data_type);
      //  println!("DEBUG: Postgres cast replacement: '{}' -> '{}'", caps.get(0).unwrap().as_str(), replacement);
        replacement
    }).to_string();
    
    //println!("DEBUG normalize_postgres_casting output: '{}'", result);
    result
}

    pub fn normalize_expression(expr: &str, table_alias: &str) -> String {
        if expr.to_uppercase().contains(" OVER ") {
            return normalize_window_function(expr);
        }
        
        let expr_with_standard_cast = normalize_postgres_casting(expr);
        
        #[cfg(test)]
        eprintln!("DEBUG: Processing expression '{}', looking for AS patterns", expr_with_standard_cast);
        
        // Use case-insensitive AS pattern instead of the static one
        let as_pattern = regex::Regex::new(r"(?i)\s+AS\s+").expect("Failed to compile AS regex");
        let all_matches: Vec<_> = as_pattern.find_iter(&expr_with_standard_cast).collect();
        
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
                    alias_part.trim_matches('"').trim_matches('\'').replace(' ', "_").to_lowercase());
                    
                #[cfg(test)]
                eprintln!("DEBUG: Final expression result: '{}'", final_result);
                
                return final_result;
            }
        }
        
        #[cfg(test)]
        eprintln!("DEBUG: No top-level AS found, using fallback splitn logic");
        
        // Fallback for simple cases -  case insensitive
        if expr_with_standard_cast.to_uppercase().contains(" AS ") {
            // Find the AS 
            if let Some(as_match) = as_pattern.find(&expr_with_standard_cast) {
                let expr_part = expr_with_standard_cast[..as_match.start()].trim();
                let alias_part = expr_with_standard_cast[as_match.end()..].trim();
                
                #[cfg(test)]
                eprintln!("DEBUG: Fallback found AS, expr_part='{}', alias_part='{}'", expr_part, alias_part);
                
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
                    alias_part.trim_matches('"').trim_matches('\'').to_lowercase()
                );
            }
        }
        
        let result = if is_aggregate_expression(&expr_with_standard_cast) {
            normalize_aggregate_expression(&expr_with_standard_cast, table_alias)
        } else if is_datetime_expression(&expr_with_standard_cast) {
            normalize_datetime_expression(&expr_with_standard_cast, table_alias)
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
    // Add this debug version to your normalize_simple_expression to trace the issue:
    pub fn normalize_simple_expression(expr: &str, table_alias: &str) -> String {
      //  println!("Input: '{}'", expr);
        
        let expr_trimmed = expr.trim();
       // println!("After trim: '{}'", expr_trimmed);
        
        let expr_with_standard_cast = normalize_postgres_casting(expr_trimmed);
       // println!("After postgres casting: '{}'", expr_with_standard_cast);
        
        if expr_with_standard_cast.to_uppercase().starts_with("CASE") {
      //      println!("CASE expression detected, delegating...");
            return normalize_case_expression(&expr_with_standard_cast, table_alias);
        }
        
        if let Some(caps) = FUNCTION_PATTERN.captures(&expr_with_standard_cast) {
            let func_name = &caps[1];
            let args = &caps[2];
            
       //     println!("Function detected - name: '{}', args: '{}'", func_name, args);
            
            // Special handling for POSITION function
            if func_name.to_uppercase() == "POSITION" {
            let upper_args = args.to_uppercase();
            if let Some(in_pos) = upper_args.find(" IN ") {
                let search_expr = args[..in_pos].trim();
                let column_expr = args[in_pos + 4..].trim();
                
                #[cfg(test)]
                eprintln!("DEBUG: POSITION function - search: '{}', column: '{}'", search_expr, column_expr);
                
                // search expression (usually a string literal)
                let normalized_search = if search_expr.starts_with('\'') && search_expr.ends_with('\'') {
                    search_expr.to_string()
                } else {
                    normalize_simple_expression(search_expr, table_alias)
                };
                
                // column expression
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
            
            // Special handling for CAST function
            if func_name.to_uppercase() == "CAST" {
              //  println!("CAST function detected!");
                
                let mut paren_count = 0;
                let mut as_pos = None;
                let args_upper = args.to_uppercase();
                let mut i = 0;
                
                while i < args.len() {
                    let c = args.chars().nth(i).unwrap();
                    match c {
                        '(' => paren_count += 1,
                        ')' => paren_count -= 1,
                        _ => {}
                    }
                    
                    // Only look for " AS " when we're at the top level (paren_count == 0)
                    if paren_count == 0 && i + 4 <= args.len() {
                        if args_upper[i..].starts_with(" AS ") {
                            as_pos = Some(i);
                            break;
                        }
                    }
                    i += 1;
                }
                
                if let Some(as_pos) = as_pos {
                let expression = args[..as_pos].trim();
                let after_as = &args[as_pos + 4..].trim();
                
                // Find the end of the datatype
                let mut datatype_end = after_as.len();
                for (i, c) in after_as.chars().enumerate() {
                    if c.is_whitespace() || c == ',' || c == ')' {
                        datatype_end = i;
                        break;
                    }
                }
                
                let datatype = &after_as[..datatype_end].trim();
                
        //        println!("DEBUG CAST: args='{}', as_pos={}", args, as_pos);
          //      println!("DEBUG CAST: expression='{}', after_as='{}'", expression, after_as);
          //      println!("DEBUG CAST: datatype_end={}, datatype='{}'", datatype_end, datatype);
                
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
                
                let result = format!("{}({} as {})", 
                    func_name.to_lowercase(), 
                    normalized_expr, 
                    datatype.to_lowercase());
                
              //  println!("DEBUG CAST: final result='{}'", result);
                
                return result;
                } else {
                   // println!("No AS found in CAST, falling through to general function handling");
                }
            }
            
            // General function handling
          //  println!("Using general function argument parsing...");
            let arg_parts = parse_function_arguments(args);
           // println!("Parsed arguments: {:?}", arg_parts);
            
            let mut normalized_args = Vec::with_capacity(arg_parts.len());
            
            for (_i, arg) in arg_parts.iter().enumerate() {
                let arg_trimmed = arg.trim();
              //  println!("Processing argument {}: '{}'", i, arg_trimmed);
                
                if arg_trimmed.starts_with('\'') && arg_trimmed.ends_with('\'') {
                //println!("  -> String literal, preserving as-is");
                    normalized_args.push(arg_trimmed.to_string());
                } else if arg_trimmed.starts_with('"') && arg_trimmed.ends_with('"') {
                  //  println!("  -> Quoted identifier, preserving as-is");
                    normalized_args.push(arg_trimmed.to_string());
                } else if arg_trimmed.chars().all(|c| c.is_ascii_digit() || c == '.' || c == '-') {
                 //   println!("  -> Numeric literal, preserving as-is");
                    normalized_args.push(arg_trimmed.to_string());
                } else if matches!(arg_trimmed.to_uppercase().as_str(), 
                                "TEXT" | "INTEGER" | "BIGINT" | "VARCHAR" | "FLOAT" | "DOUBLE" | 
                                "BOOLEAN" | "DATE" | "TIMESTAMP") {
                 //   println!("  -> Data type, lowercasing");
                    normalized_args.push(arg_trimmed.to_lowercase());
                } else if FUNCTION_PATTERN.is_match(arg_trimmed) {
                 //   println!("  -> Nested function, recursing");
                    let nested_result = normalize_simple_expression(arg_trimmed, table_alias);
                  //  println!("  -> Nested result: '{}'", nested_result);
                    normalized_args.push(nested_result);
                } else if TABLE_COLUMN_PATTERN.is_match(arg_trimmed) {
                  //  println!("  -> Table.column pattern");
                    let normalized = TABLE_COLUMN_PATTERN
                        .replace_all(arg_trimmed, "\"$1\".\"$2\"")
                        .to_string()
                        .to_lowercase();
                 //   println!("  -> Normalized to: '{}'", normalized);
                    normalized_args.push(normalized);
                } else if is_simple_column(arg_trimmed) {
                 //   println!("  -> Simple column, adding table prefix");
                    let result = format!("\"{}\".\"{}\"", 
                        table_alias.to_lowercase(), 
                        arg_trimmed.to_lowercase());
                 //   println!("  -> Result: '{}'", result);
                    normalized_args.push(result);
                } else if OPERATOR_PATTERN.is_match(arg_trimmed) {
                   // println!("  -> Operator expression");
                    let result = normalize_expression_with_operators(arg_trimmed, table_alias);
                  //  println!("  -> Result: '{}'", result);
                    normalized_args.push(result);
                } else {
                 //   println!("  -> Default case, lowercasing");
                    let result = arg_trimmed.to_lowercase();
                  //  println!("  -> Result: '{}'", result);
                    normalized_args.push(result);
                }
            }
            
            let final_result = format!("{}({})", func_name.to_lowercase(), normalized_args.join(", "));
         //   println!("Final function result: '{}'", final_result);
            return final_result;
            
        } else if OPERATOR_PATTERN.is_match(&expr_with_standard_cast) {
           // println!("Operator pattern detected");
            return normalize_expression_with_operators(&expr_with_standard_cast, table_alias);
        } else if TABLE_COLUMN_PATTERN.is_match(&expr_with_standard_cast) {
           // println!("Table.column pattern detected");
            let result = TABLE_COLUMN_PATTERN
                .replace_all(&expr_with_standard_cast, "\"$1\".\"$2\"")
                .to_string()
                .to_lowercase();
           // println!("Result: '{}'", result);
            return result;
        } else if is_simple_column(&expr_with_standard_cast) {
          //  println!("Simple column detected");
            let result = format!("\"{}\".\"{}\"", 
                table_alias.to_lowercase(), 
                expr_with_standard_cast.to_lowercase()); //.replace(' ', "_")
          //  println!("Result: '{}'", result);
            return result;
        } else {
          //  println!("Default case, lowercasing");
            let result = expr_with_standard_cast.to_lowercase();
          //  println!("Result: '{}'", result);
            return result;
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
      //  println!("DEBUG: normalize_case_expression input: '{}'", expr);

        let mut result = expr.to_string();
        
        // First, handle table.column patterns
        result = TABLE_COLUMN_PATTERN
            .replace_all(&result, "\"$1\".\"$2\"")
            .to_string();

        let simple_column_in_context = Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*)\b").unwrap();
        
        // list of positions to replace to avoid modifying while iterating
        let mut replacements = Vec::new();
        
        for caps in simple_column_in_context.captures_iter(&result) {
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
                // Keep as-is
                continue;
            } else if is_simple_column(word) {
                let replacement = format!("\"{}\".\"{}\"", table_alias.to_lowercase(), word.to_lowercase());
                replacements.push((match_start, match_end, replacement));
            } else {
                // Don't automatically lowercase - this can corrupt expressions
                let replacement = word.to_lowercase();
                replacements.push((match_start, match_end, replacement));
            }
        }
        
        // Apply replacements in reverse order to maintain positions
        for (start, end, replacement) in replacements.into_iter().rev() {
            result.replace_range(start..end, &replacement);
        }
        
       // println!("DEBUG: normalize_case_expression output: '{}'", result);
        result  // .to_lowercase()
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

        // println!("DEBUG normalize_column_name called with: '{}'", name);

         if name.contains('(') || name.contains(',') {
      //  println!("WARNING: normalize_column_name called on function expression!");
      //  println!("This will mangle the expression!");
    }
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
    //println!("DEBUG format_table_column called with: '{}'", name);
    
   // if name.contains('(') || name.contains(',') {
      //  println!("ERROR: format_table_column called on function! This will break it!");
  //  }
    
    let result = if let Some(pos) = name.find('.') {
        let table = &name[..pos];
        let column = &name[pos + 1..];
        format!("\"{}\".\"{}\"", 
            table.trim().to_lowercase(), 
            column.trim().replace(' ', "_").to_lowercase())  
    } else {
        format!("\"{}\"", 
            name.trim().replace(' ', "_").to_lowercase()) 
    };
    
   // println!("DEBUG format_table_column result: '{}'", result);
    result
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
        
        // FIXED: More precise checks to avoid false positives
        let is_complex = s.contains('(') || 
                        s.contains(')') || 
                        s.contains(',') ||
                        upper_s.contains(" AS ") ||  // Use uppercase version, not lowercase
                        OPERATOR_PATTERN.is_match(s);

        let has_table_prefix = s.contains('.');
        let is_case_expression = upper_s.trim().starts_with("CASE");
        
        let result = matches_pattern && !is_keyword && !is_complex && !has_table_prefix && !is_case_expression;
        
       // if s.contains("LPAD") || s.contains("CAST") {
         //   println!("DEBUG is_simple_column('{}') -> {}", s, result);
          //  println!("  matches_pattern: {}, is_keyword: {}, is_complex: {}, has_table_prefix: {}", 
                    //matches_pattern, is_keyword, is_complex, has_table_prefix);
       // }
        
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

    pub fn resolve_alias_to_original(alias: &str, raw_selected_columns: &[String]) -> String {
        for raw_expr in raw_selected_columns {
            if raw_expr.to_uppercase().contains(" AS ") {
                let as_pattern = regex::Regex::new(r"(?i)\s+AS\s+").unwrap();
                if let Some(as_match) = as_pattern.find(raw_expr) {
                    let expr_part = raw_expr[..as_match.start()].trim();
                    let alias_part = raw_expr[as_match.end()..].trim().trim_matches('"').trim_matches('\'');
                    
                    if alias_part.eq_ignore_ascii_case(alias) {
                        return expr_part.to_string();
                    }
                }
            }
        }
    
        alias.to_string()
    }

    pub fn is_computed_expression(expr: &str) -> bool {
        let expr_upper = expr.to_uppercase();
        
        // Skip function calls
        if expr.contains('(') && expr.contains(')') {
            return true;
        }
        
        // Skip CASE expressions
        if expr_upper.trim().starts_with("CASE") {
            return true;
        }
        
        // Skip expressions with operators
        if OPERATOR_PATTERN.is_match(expr) {
            return true;
        }
        
        // Skip string literals
        if expr.trim().starts_with('\'') && expr.trim().ends_with('\'') {
            return true;
        }
        
        // Skip complex expressions
        if expr.contains(" WHEN ") || expr.contains(" THEN ") || expr.contains(" ELSE ") {
            return true;
        }
        
        false
    }

    pub fn is_complex_computed_expression(expr: &str) -> bool {
        let expr_upper = expr.to_uppercase();
        
        if OPERATOR_PATTERN.is_match(expr) {

            if !is_string_or_datetime_function(expr) && !expr_upper.contains("CASE") {
                return true;
            }
        }
        
        if expr.trim().starts_with('\'') && expr.trim().ends_with('\'') && !expr.contains('(') {
            return true;
        }
        
        if expr.contains(" WHEN ") && expr.contains(" THEN ") && !expr_upper.trim().starts_with("CASE") {
            return true;
        }
        
        false
    }

    pub fn extract_base_column_name(qualified_name: &str) -> String {
        // extract part before AS
        let column_part = if qualified_name.to_uppercase().contains(" AS ") {
            let as_pattern = Regex::new(r"(?i)\s+AS\s+").unwrap();
            if let Some(as_match) = as_pattern.find(qualified_name) {
                qualified_name[..as_match.start()].trim()
            } else {
                qualified_name
            }
        } else {
            qualified_name
        };

        if let Some(dot_pos) = column_part.rfind('.') {
            column_part[dot_pos + 1..].to_lowercase()
        } else {
            column_part.to_lowercase()
        }
    }
    
    pub fn is_string_or_datetime_function(expr: &str) -> bool {
        let expr_upper = expr.to_uppercase();
        if expr_upper.trim().starts_with("CASE") && expr_upper.contains("WHEN") {
            return true;
        }
 
        let base_expr = if expr_upper.contains(" AS ") {
            if let Some(as_match) = AS_PATTERN.find(expr) {
                expr[..as_match.start()].trim()
            } else {
                expr.trim()
            }
        } else {
            expr.trim()
        };
        
      //  println!("DEBUG: after AS split, base_expr = '{}'", base_expr);

        if let Some(caps) = FUNCTION_PATTERN.captures(base_expr) {
            if let Some(func_name) = caps.get(1) {
                let func = func_name.as_str().to_uppercase();
           //     println!("DEBUG: extracted function = '{}'", func);
                
                if STRING_FUNCTIONS.contains(func.as_str()) {
             //       println!("DEBUG: '{}' found in STRING_FUNCTIONS", func);
                    return true;
                }
                if DATETIME_FUNCTIONS.contains(func.as_str()) {
             //       println!("DEBUG: '{}' found in DATETIME_FUNCTIONS", func);
                    return true;
                }
                
            //    println!("DEBUG: '{}' not found in function sets", func);
            }
        } else {
          //  println!("DEBUG: No function pattern match for '{}'", base_expr);
        }
        
        false
    }

     pub fn is_groupable_column(expr: &str) -> bool {
        let trimmed = expr.trim();
        
        if is_computed_expression(trimmed) {
            return false;
        }

         if is_aggregate_expression(trimmed) {
            return false;
        }

        if is_complex_computed_expression(trimmed) {
            return false;
        }

        if is_string_or_datetime_function(trimmed) {
            return false;
        }
    
        is_simple_column(trimmed) || TABLE_COLUMN_PATTERN.is_match(trimmed)
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
        
        println!("Result3 (LIKE): {}", result3);
        
        let result4 = normalize_expression(
            "CASE WHEN naziv_proizvoda LIKE '%SENI%' THEN 'SENI_PRODUCT' ELSE 'OTHER' END AS product_category",
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

    #[test]
    fn test_string_function_detection() {
        assert!(is_string_or_datetime_function("TRIM(veledrogerija) as clean"));
        assert!(is_string_or_datetime_function("UPPER(region) AS upper_region"));
        assert!(is_string_or_datetime_function("CONCAT(godina, '-', mesec) as period"));
        assert!(is_string_or_datetime_function("CASE WHEN neto_vrednost > 1000 THEN 'HIGH' ELSE 'LOW' END"));
        
        // Should return false for regular columns
        assert!(!is_string_or_datetime_function("grupa as product_group"));
        assert!(!is_string_or_datetime_function("customer_name"));
        
        // Should return false for aggregate functions
        assert!(!is_string_or_datetime_function("SUM(neto_vrednost) AS total_value"));
        assert!(!is_string_or_datetime_function("COUNT(*) AS transaction_count"));
    }

}