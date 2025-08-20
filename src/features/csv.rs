use crate::prelude::*;

static INTEGER_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^[+-]?[0-9]+$").expect("Failed to compile integer regex")
});

static FLOAT_DOT_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^[+-]?[0-9]*\.[0-9]+$").expect("Failed to compile float dot regex")
});

static FLOAT_COMMA_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^[+-]?[0-9]+,[0-9]+$").expect("Failed to compile float comma regex")
});

static THOUSAND_SEPARATOR_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^[+-]?[0-9]{1,3}([,.][0-9]{3})*([.,][0-9]{1,2})?$").expect("Failed to compile thousand separator regex")
});

static BOOLEAN_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^(?i)(true|false|yes|no|da|ne|1|0)$").expect("Failed to compile boolean regex")
});

static PERCENTAGE_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^[+-]?[0-9]*[.,]?[0-9]+%$").expect("Failed to compile percentage regex")
});

static CURRENCY_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^[$€£¥₹]\s*[+-]?[0-9,.]+$|^[+-]?[0-9,.]+\s*[$€£¥₹]$").expect("Failed to compile currency regex")
});

static DATE_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^[0-9]{1,4}[-./][0-9]{1,2}[-./][0-9]{1,4}$|^[0-9]{1,2}:[0-9]{2}(:[0-9]{2})?$").expect("Failed to compile date regex")
});

static NULL_VALUES: Lazy<std::collections::HashSet<&'static str>> = Lazy::new(|| {
    ["", "NULL", "null", "N/A", "n/a", "-"].into_iter().collect()
});

async fn detect_delimiter(file_path: &str) -> ElusionResult<u8> {
    
    let file = File::open(file_path)
        .map_err(|e| ElusionError::Custom(format!("Failed to open file for delimiter detection: {}", e)))?;
    
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    
    // Read header line 
    let header = lines.next().transpose()
        .map_err(|e| ElusionError::Custom(format!("Failed to read header line: {}", e)))?
        .ok_or_else(|| ElusionError::Custom("File is empty".to_string()))?;
    
    let mut delimiter_scores = std::collections::HashMap::new();
    let sample_size = 10; // Check first 10 lines
    
    // Test 
    for &delimiter in &[b',', b'\t', b';', b'|'] {
        let header_cols = header.split(delimiter as char).count();
        let mut consistent_lines = 0;
        let mut total_lines = 0;
        
        // Re-read file 
        let file = File::open(file_path).map_err(|e| ElusionError::Custom(format!("Failed to reopen file: {}", e)))?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let _header = lines.next(); // Skip header
        
        for line in lines.take(sample_size) {
            if let Ok(line) = line {
                total_lines += 1;
                let cols = line.split(delimiter as char).count();
                if cols == header_cols && cols > 1 {
                    consistent_lines += 1;
                }
            }
        }
        
        // Score based on consistency and column count
        let consistency_score = if total_lines > 0 { 
            (consistent_lines as f64 / total_lines as f64) * 100.0 
        } else { 
            0.0 
        };
        
        delimiter_scores.insert(delimiter, (consistency_score, header_cols));
    }
    
    // Choose delimiter with highest consistency score, then by column count
    let detected_delimiter = delimiter_scores
        .into_iter()
        .max_by(|(_, (score1, cols1)), (_, (score2, cols2))| {
            score1.partial_cmp(score2)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(cols1.cmp(cols2))
        })
        .map(|(delim, (score, cols))| {
            let delimiter_name = match delim {
                b'\t' => "tab (TSV)",
                b',' => "comma (CSV)", 
                b';' => "semicolon",
                b'|' => "pipe",
                _ => "unknown"
            };
            println!("🔍 Testing {}: {:.1}% consistency with {} columns", delimiter_name, score, cols);
            delim
        })
        .unwrap_or(b',');
        
    let delimiter_name = match detected_delimiter {
        b'\t' => "TSV (tab-separated)",
        b',' => "CSV (comma-separated)",
        b';' => "semicolon-separated", 
        b'|' => "pipe-separated",
        _ => "unknown format"
    };
    
    println!("🎯 Auto-detected file format: {}", delimiter_name);
    
    Ok(detected_delimiter)
}

///Load CSV with smart casting
pub async fn load_csv_with_type_handling(
    file_path: &str, 
    alias: &str
) -> ElusionResult<AliasedDataFrame> {
    let ctx = SessionContext::new();

    if !LocalPath::new(file_path).exists() {
        return Err(ElusionError::WriteError {
            path: file_path.to_string(),
            operation: "read".to_string(),
            reason: "File not found".to_string(),
            suggestion: "💡 Check if the file path is correct".to_string()
        });
    }

    let read_start = std::time::Instant::now();

    let delimiter = detect_delimiter(file_path).await?;
    let delimiter_name = match delimiter {
        b'\t' => "TSV (tab-separated)",
        b',' => "CSV (comma-separated)",
        b';' => "semicolon-separated",
        b'|' => "pipe-separated",
        _ => "custom delimiter"
    };

    let df = ctx.read_csv(
        file_path,
        CsvReadOptions::new()
            .has_header(true)
            .schema_infer_max_records(0) 
            .delimiter(delimiter)
    ).await.map_err(ElusionError::DataFusion)?;

    let read_elapsed = read_start.elapsed();
    let schema = df.schema();
    let column_count = schema.fields().len();
    
    if let Ok(metadata) = std::fs::metadata(file_path) {
        let file_size = metadata.len();
        println!("📏 File size: {} bytes ({:.2} MB)", file_size, file_size as f64 / 1024.0 / 1024.0);
    }

    println!("✅ Successfully loaded {} with {} columns as strings in {:?}", 
        delimiter_name, column_count, read_elapsed);

    let schema = df.schema();
    let sample_data = get_sample_data(&df, &ctx).await?;
    
    // println!("📋 Original columns: {:?}", 
    //     schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());
    let casting_start = std::time::Instant::now();
    let cast_sql = generate_smart_casting_sql(&sample_data, schema.fields(), alias)?;
    
    println!("🎯 Applying content-based smart casting...");
    // println!("🔍 Generated SQL: {}", cast_sql);
    
    let final_df = ctx.sql(&cast_sql).await.map_err(ElusionError::DataFusion)?;
    
    println!("✅ Successfully applied smart casting");
   

    let schema_elapsed = casting_start.elapsed();
    let total_elapsed = read_start.elapsed();
    
    println!("✅ Schema inferred and table created in {:?}", schema_elapsed + total_elapsed);
    println!("🎉 CSV DataFrame loading completed successfully in {:?} for table alias: '{}'", 
        total_elapsed, alias);

    Ok(AliasedDataFrame {
        dataframe: final_df,
        alias: alias.to_string(),
    })
}

async fn get_sample_data(df: &DataFrame, ctx: &SessionContext) -> ElusionResult<HashMap<String, Vec<String>>> {
   
    let batches = df.clone().collect().await.map_err(ElusionError::DataFusion)?;
    let mem_table = MemTable::try_new(df.schema().clone().into(), vec![batches])
        .map_err(|e| ElusionError::Custom(format!("Failed to create memory table: {}", e)))?;
    
    ctx.register_table("temp_sample", Arc::new(mem_table))
        .map_err(ElusionError::DataFusion)?;
    
    let sample_df = ctx.sql("SELECT * FROM temp_sample LIMIT 100").await
        .map_err(ElusionError::DataFusion)?;
    
    let sample_batches = sample_df.collect().await.map_err(ElusionError::DataFusion)?;
    
    let mut column_samples: HashMap<String, Vec<String>> = HashMap::new();
    
    if let Some(batch) = sample_batches.first() {
        let schema = batch.schema();
        
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let column = batch.column(col_idx);
            let mut samples = Vec::new();
            
            for row_idx in 0..column.len().min(100) {
                if !column.is_null(row_idx) {
                    let value = array_value_to_string(column.as_ref(), row_idx);
                    if !value.is_empty() && value != "null" {
                        samples.push(value);
                    }
                }
            }
            
            column_samples.insert(field.name().to_string(), samples);
        }
    }
    
    Ok(column_samples)
}

// Helper function to convert any Arrow array value to string
fn array_value_to_string(array: &dyn arrow::array::Array, index: usize) -> String {
    use arrow::array::*;
    use arrow::datatypes::DataType;
    
    if array.is_null(index) {
        return String::new();
    }
    
    match array.data_type() {
        DataType::Utf8 => {
            let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
            string_array.value(index).to_string()
        },
        DataType::LargeUtf8 => {
            let string_array = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            string_array.value(index).to_string()
        },
        DataType::Int8 => {
            let int_array = array.as_any().downcast_ref::<Int8Array>().unwrap();
            int_array.value(index).to_string()
        },
        DataType::Int16 => {
            let int_array = array.as_any().downcast_ref::<Int16Array>().unwrap();
            int_array.value(index).to_string()
        },
        DataType::Int32 => {
            let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
            int_array.value(index).to_string()
        },
        DataType::Int64 => {
            let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            int_array.value(index).to_string()
        },
        DataType::UInt8 => {
            let int_array = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            int_array.value(index).to_string()
        },
        DataType::UInt16 => {
            let int_array = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            int_array.value(index).to_string()
        },
        DataType::UInt32 => {
            let int_array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            int_array.value(index).to_string()
        },
        DataType::UInt64 => {
            let int_array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            int_array.value(index).to_string()
        },
        DataType::Float32 => {
            let float_array = array.as_any().downcast_ref::<Float32Array>().unwrap();
            float_array.value(index).to_string()
        },
        DataType::Float64 => {
            let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
            float_array.value(index).to_string()
        },
        DataType::Boolean => {
            let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            bool_array.value(index).to_string()
        },
        DataType::Date32 | DataType::Date64 | DataType::Time32(_) | DataType::Time64(_) | 
        DataType::Timestamp(_, _) | DataType::Duration(_) | DataType::Interval(_) => {
            // For date/time types, use the array's display implementation
            format!("{}", any_other_array_value_to_string(array, index).unwrap_or_default())
        },
        _ => {
            // For any other type
            any_other_array_value_to_string(array, index).unwrap_or_else(|_| {
                // Last resort
                format!("{:?}", array.slice(index, 1))
            })
        }
    }
}

fn clean_column_name(col_name: &str) -> String {
    col_name
        .trim()                  
        .to_lowercase()        
        .replace(' ', "_")          
        .replace('\t', "_")     
        .replace('\n', "_")      
        .replace('\r', "_")  
}

fn generate_smart_casting_sql(
    sample_data: &HashMap<String, Vec<String>>, 
    fields: &[Arc<datafusion::arrow::datatypes::Field>],
    _table_alias: &str
) -> ElusionResult<String> {
    let mut select_parts = Vec::new();
    let empty_samples = Vec::new(); 
    
    for field in fields {
        let original_col_name = field.name();
        let clean_col_name = clean_column_name(original_col_name);

        let samples = sample_data.get(original_col_name).unwrap_or(&empty_samples);
        
        let data_type = infer_column_type(samples, original_col_name);
        let cast_expr = create_casting_expression(original_col_name, &clean_col_name, &data_type);
        
        // println!("🔍 Column '{}' → '{}' inferred as {} based on {} samples", 
        //     original_col_name, clean_col_name, data_type, samples.len());
        
        select_parts.push(cast_expr);
    }
    
    Ok(format!("SELECT {} FROM temp_sample", select_parts.join(", ")))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum InferredDataType {
    Integer,
    Float,
    Boolean,
    Date,
    String,
}

impl std::fmt::Display for InferredDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InferredDataType::Integer => write!(f, "INTEGER"),
            InferredDataType::Float => write!(f, "FLOAT"),
            InferredDataType::Boolean => write!(f, "BOOLEAN"),
            InferredDataType::Date => write!(f, "DATE/STRING"),
            InferredDataType::String => write!(f, "STRING"),
        }
    }
}

fn infer_column_type(samples: &[String], _col_name: &str) -> InferredDataType {
    if samples.is_empty() {
        return InferredDataType::String;
    }
    
    let mut type_votes = HashMap::new();
    
    for sample in samples.iter().take(50) { 
        let trimmed = sample.trim();
        
        if NULL_VALUES.contains(trimmed) {
            continue; 
        }
        
        let detected_type = classify_value(trimmed);
        *type_votes.entry(detected_type).or_insert(0) += 1;
    }
    
    let total_non_null = type_votes.values().sum::<i32>();
    
    if total_non_null == 0 {
        return InferredDataType::String;
    }
    
    // Priority order: Integer -> Float -> Boolean -> Date -> String
    if let Some(&int_count) = type_votes.get(&InferredDataType::Integer) {
        if int_count as f32 / total_non_null as f32 > 0.8 {
            return InferredDataType::Integer;
        }
    }
    
    if let Some(&float_count) = type_votes.get(&InferredDataType::Float) {
        let int_count = type_votes.get(&InferredDataType::Integer).unwrap_or(&0);
        if (float_count + int_count) as f32 / total_non_null as f32 > 0.8 {
            return InferredDataType::Float;
        }
    }
    
    if let Some(&bool_count) = type_votes.get(&InferredDataType::Boolean) {
        if bool_count as f32 / total_non_null as f32 > 0.8 {
            return InferredDataType::Boolean;
        }
    }
    
    if let Some(&date_count) = type_votes.get(&InferredDataType::Date) {
        if date_count as f32 / total_non_null as f32 > 0.8 {
            return InferredDataType::Date;
        }
    }
    
    InferredDataType::String
}

fn classify_value(value: &str) -> InferredDataType {

    if INTEGER_PATTERN.is_match(value) {
        return InferredDataType::Integer;
    }
    
    if FLOAT_DOT_PATTERN.is_match(value) {
        if value.matches('.').count() == 1 {
            return InferredDataType::Float;
        } else {
            return InferredDataType::String; 
        }
    }

    if FLOAT_COMMA_PATTERN.is_match(value) {
        if value.matches(',').count() == 1 {
            return InferredDataType::Float;
        } else {
            return InferredDataType::String; 
        }
    }
    
    if THOUSAND_SEPARATOR_PATTERN.is_match(value) {
        if is_valid_thousand_separator_number(value) {
            return InferredDataType::Float;
        } else {
            return InferredDataType::String;
        }
    }
    
    if PERCENTAGE_PATTERN.is_match(value) {
        return InferredDataType::Float;
    }
    
    if CURRENCY_PATTERN.is_match(value) {
        return InferredDataType::Float;
    }
    
    if BOOLEAN_PATTERN.is_match(value) {
        if !INTEGER_PATTERN.is_match(value) {
            return InferredDataType::Boolean;
        }
    }
    
    if DATE_PATTERN.is_match(value) {

        if is_valid_date_or_time(value) {
            return InferredDataType::Date;
        } else {
            return InferredDataType::String;
        }
    }
    
    InferredDataType::String
}

// Simple validation for thousand separator numbers
fn is_valid_thousand_separator_number(value: &str) -> bool {
    // Remove leading +/- sign
    let cleaned = value.trim_start_matches(['+', '-']);
    
   // println!("Validating thousand separator: '{}'", cleaned);
    
    // Check for comma thousands, dot decimal 1,234.56
    if cleaned.contains(',') && cleaned.contains('.') {
        let parts: Vec<&str> = cleaned.split('.').collect();
        if parts.len() == 2 {
            let integer_part = parts[0];
            let decimal_part = parts[1];
            
            // Decimal part should be 1-6 digits
            if decimal_part.len() > 6 {
              //  println!("Invalid: decimal part too long");
                return false;
            }
            
            // Check comma grouping in integer part
            let result = is_valid_comma_grouping(integer_part);
          //  println!("Comma grouping valid: {}", result);
            return result;
        }
    }
    
    // Check for dot thousands, comma decimal 1.234,56  
    if cleaned.contains('.') && cleaned.contains(',') {
        let parts: Vec<&str> = cleaned.split(',').collect();
        if parts.len() == 2 {
            let integer_part = parts[0];
            let decimal_part = parts[1];
            
            // Decimal part should be 1-6 digits
            if decimal_part.len() > 6 {
               //println!("Invalid: decimal part too long");
                return false;
            }
            
            // Check dot grouping in integer 
            let result = is_valid_dot_grouping(integer_part);
           // println!(" Dot grouping valid: {}", result);
            return result;
        }
    }
    
    // Pure comma separators, no decimal
    if cleaned.contains(',') && !cleaned.contains('.') {
        let result = is_valid_comma_grouping(cleaned);
       // println!("Pure comma grouping valid: {}", result);
        return result;
    }
    
    // Pure dot separators -need to distinguish from decimals
    if cleaned.contains('.') && !cleaned.contains(',') {
        let dot_count = cleaned.matches('.').count();
       // println!("Dot count: {}", dot_count);
        if dot_count > 1 {
            // Multiple dots, likely thousand separators
            let result = is_valid_dot_grouping(cleaned);
           // println!("Multiple dot grouping valid: {}", result);
            return result;
        } else {
            //println!("Single dot, should be handled by FLOAT_DOT_PATTERN");
            return false;
        }
    }
    
   // println!("No valid pattern found, returning false");
    false
}

fn is_valid_comma_grouping(value: &str) -> bool {
    let parts: Vec<&str> = value.split(',').collect();
    
    // Must have at least 2 parts for thousand separators
    if parts.len() < 2 {
        return false;
    }
    
    // First part: 1-3 digits
    if parts[0].is_empty() || parts[0].len() > 3 || !parts[0].chars().all(|c| c.is_ascii_digit()) {
        return false;
    }
    
    // Remaining parts: exactly 3 digits each
    for part in &parts[1..] {
        if part.len() != 3 || !part.chars().all(|c| c.is_ascii_digit()) {
            return false;
        }
    }
    
    true
}

fn is_valid_dot_grouping(value: &str) -> bool {
    let parts: Vec<&str> = value.split('.').collect();
    //println!("Checking dot grouping parts: {:?}", parts);
    
    // Must have at least 2 parts for thousand separators  
    if parts.len() < 2 {
       // println!("Invalid: less than 2 parts");
        return false;
    }
    
    // Reject if there are too many groups - 999,999,999 
    if parts.len() > 4 {
      //  println!("Invalid: too many groups ({}), likely not a number", parts.len());
        return false;
    }
    

    // 999.999.999 would have 3 parts of 3 digits each, which is likely an IP address or similar
    if parts.len() >= 3 && parts.iter().all(|p| p.len() == 3) {
        // Check if this could be an IP address (all parts <= 255)
        if parts.iter().all(|p| p.parse::<u32>().map_or(false, |n| n <= 255)) {
           // println!("Invalid: looks like IP address or similar pattern");
            return false;
        }
        
        // Even if not IP, having 3+ groups of exactly 3 digits is suspicious
        if parts.len() >= 3 {
          //  println!("Invalid: too many groups of exactly 3 digits, likely not a number");
            return false;
        }
    }
    
    // First part: 1-3 digits
    if parts[0].is_empty() || parts[0].len() > 3 || !parts[0].chars().all(|c| c.is_ascii_digit()) {
       // println!("Invalid: first part '{}' is empty, too long, or contains non-digits", parts[0]);
        return false;
    }
    
    // Remaining parts: exactly 3 digits each
    for (i, part) in parts[1..].iter().enumerate() {
        if part.len() != 3 || !part.chars().all(|c| c.is_ascii_digit()) {
            println!("Invalid: part {} '{}' is not exactly 3 digits", i + 1, part);
            return false;
        }
    }
    
   // println!("Valid dot grouping");
    true
}

fn is_valid_date_or_time(value: &str) -> bool {
    // Time format validation
    if value.contains(':') {
        let parts: Vec<&str> = value.split(':').collect();
        if parts.len() == 2 || parts.len() == 3 {
            if let (Ok(hour), Ok(minute)) = (parts[0].parse::<u32>(), parts[1].parse::<u32>()) {
                if hour <= 23 && minute <= 59 {
                    if parts.len() == 3 {
                        if let Ok(second) = parts[2].parse::<u32>() {
                            return second <= 59;
                        }
                        return false;
                    }
                    return true;
                }
            }
        }
        return false;
    }
    
    // Date format validation
    let separators = ['-', '/', '.'];
    for sep in separators {
        if value.contains(sep) {
            let parts: Vec<&str> = value.split(sep).collect();
            if parts.len() == 3 {
                if let Ok(nums) = parts.iter().map(|p| p.parse::<u32>()).collect::<Result<Vec<_>, _>>() {
                    // Special heuristics to filter out non-dates
                    if looks_like_version_number(&nums) {
                        //println!("Looks like version number, not a date");
                        return false;
                    }
                    
                    // Basic validation - at least one arrangement should make sense
                    if is_plausible_date(&nums) {
                        return true;
                    }
                }
            }
        }
    }
    
    false
}

// Heuristic to detect version numbers vs dates
fn looks_like_version_number(nums: &[u32]) -> bool {
    if nums.len() != 3 {
        return false;
    }
    
    // Version numbers typically have small numbers
    // If all numbers are <= 20, it's likely a version number
    // Examples: 1.2.3, 2.0.1, 10.15.2
    if nums.iter().all(|&n| n <= 20) {
        return true;
    }
    
    // If the first number is small (<=50) and others are small (<=100), 
    // and none looks like a year, it's probably a version
    if nums[0] <= 50 && nums[1] <= 100 && nums[2] <= 100 {
        // Make sure none of the numbers looks like a year
        if !nums.iter().any(|&n| n >= 1900 && n <= 2100) {
            return true;
        }
    }
    
    false
}

fn is_plausible_date(nums: &[u32]) -> bool {
    if nums.len() != 3 {
        return false;
    }
    
  //  println!("Checking date values: {:?}", nums);
    
    // Try common date arrangements and validate each
    let arrangements = [
        (nums[0], nums[1], nums[2]), // YYYY-MM-DD 
        (nums[2], nums[1], nums[0]), // DD-MM-YYYY
        (nums[1], nums[0], nums[2]), // MM-DD-YYYY (US format with year at end)
        (nums[2], nums[0], nums[1]), // YYYY-DD-MM (uncommon)
    ];
    
    for (year, month, day) in arrangements {
       // println!(" Trying arrangement: year={}, month={}, day={}", year, month, day);
        
        // Check if this could be a valid date
        if is_valid_date_components(year, month, day) {
            //println!("Valid date found");
            return true;
        }
    }
    
    // Also try 2-digit years (convert to 4-digit)
    for (a, b, c) in arrangements {
        // Try a as 2-digit year
        if a < 100 {
            let year_4digit = if a >= 50 { 1900 + a } else { 2000 + a };
            if is_valid_date_components(year_4digit, b, c) {
               // println!("Valid date found with 2-digit year conversion");
                return true;
            }
        }
        
        // Try c as 2-digit year  
        if c < 100 {
            let year_4digit = if c >= 50 { 1900 + c } else { 2000 + c };
            if is_valid_date_components(a, b, year_4digit) {
               // println!("        -> Valid date found with 2-digit year conversion");
                return true;
            }
        }
    }
    
    //println!("No valid date arrangement found");
    false
}

fn is_valid_date_components(year: u32, month: u32, day: u32) -> bool {
    // Year validation
    if year < 1900 || year > 2100 {
        return false;
    }
    
    // Month validation (1-12)
    if month < 1 || month > 12 {
        return false;
    }
    
    // Day validation (1-31, with basic month-specific checks)
    if day < 1 || day > 31 {
        return false;
    }
    
    // Basic month-specific day validation
    match month {
        2 => {
            // February - check for leap year
            let is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
            if day > if is_leap { 29 } else { 28 } {
                return false;
            }
        },
        4 | 6 | 9 | 11 => {
            // April, June, September, November - 30 days
            if day > 30 {
                return false;
            }
        },
        _ => {
            // January, March, May, July, August, October, December - 31 days
            // Already checked day <= 31 above
        }
    }
    
    true
}

fn create_casting_expression(original_col_name: &str, clean_col_name: &str, data_type: &InferredDataType) -> String {
   let quoted_original_col = format!("\"{}\"", original_col_name);
let quoted_clean_col = format!("\"{}\"", clean_col_name);
    
    match data_type {
        InferredDataType::Integer => {
            format!(
                "CASE 
                    WHEN {} IS NULL OR TRIM({}) = '' OR TRIM({}) IN ('NULL', 'null', 'N/A', 'n/a', '-') THEN NULL
                    WHEN {} ~ '^[+-]?[0-9]+$' THEN CAST({} AS BIGINT)
                    ELSE NULL
                END AS {}",
               quoted_original_col, quoted_original_col, quoted_original_col, 
                quoted_original_col, quoted_original_col, quoted_clean_col
            )
        },
        InferredDataType::Float => {
            format!(
                "CASE 
                    WHEN {} IS NULL OR TRIM({}) = '' OR TRIM({}) IN ('NULL', 'null', 'N/A', 'n/a', '-') THEN NULL
                    -- Pure integers
                    WHEN {} ~ '^[+-]?[0-9]+$' THEN CAST({} AS DOUBLE)
                    -- Decimals with dot
                    WHEN {} ~ '^[+-]?[0-9]*\\.[0-9]+$' THEN CAST({} AS DOUBLE)
                    -- European decimals with comma
                    WHEN {} ~ '^[+-]?[0-9]+,[0-9]+$' THEN CAST(REPLACE({}, ',', '.') AS DOUBLE)
                    -- Remove % and convert percentages
                    WHEN {} ~ '^[+-]?[0-9]*[.,]?[0-9]+%$' THEN 
                        CAST(REPLACE(REPLACE({}, '%', ''), ',', '.') AS DOUBLE) / 100
                    -- Remove currency symbols
                    WHEN {} ~ '^[$€£¥₹]\\s*[+-]?[0-9,.]+$' OR {} ~ '^[+-]?[0-9,.]+\\s*[$€£¥₹]$' THEN
                        CAST(REGEXP_REPLACE(REPLACE({}, ',', '.'), '[$€£¥₹\\s]', '', 'g') AS DOUBLE)
                    -- Thousand separators (US format: 1,234.56)
                    WHEN {} ~ '^[+-]?[0-9]{{1,3}}(,[0-9]{{3}})+\\.[0-9]{{1,2}}$' THEN
                        CAST(REPLACE({}, ',', '') AS DOUBLE)
                    -- Thousand separators (EU format: 1.234,56)
                    WHEN {} ~ '^[+-]?[0-9]{{1,3}}(\\.[0-9]{{3}})+,[0-9]{{1,2}}$' THEN
                        CAST(REPLACE(REGEXP_REPLACE({}, '\\.(?=.*,)', '', 'g'), ',', '.') AS DOUBLE)
                    ELSE NULL
                END AS {}",
                quoted_original_col, quoted_original_col, quoted_original_col, 
                quoted_original_col, quoted_original_col, 
                quoted_original_col, quoted_original_col, 
                quoted_original_col, quoted_original_col, 
                quoted_original_col, quoted_original_col, 
                quoted_original_col, quoted_original_col, quoted_original_col, 
                quoted_original_col, quoted_original_col, 
                quoted_original_col, quoted_original_col, 
                quoted_clean_col
            )
        },
        InferredDataType::Boolean => {
            format!(
                "CASE 
                    WHEN {} IS NULL OR TRIM({}) = '' OR TRIM({}) IN ('NULL', 'null', 'N/A', 'n/a', '-') THEN NULL
                    WHEN UPPER(TRIM({})) IN ('TRUE', 'YES', 'DA', '1') THEN TRUE
                    WHEN UPPER(TRIM({})) IN ('FALSE', 'NO', 'NE', '0') THEN FALSE
                    ELSE NULL
                END AS {}",
                quoted_original_col, quoted_original_col, quoted_original_col, 
                quoted_original_col, quoted_original_col, quoted_clean_col
            )
        },
        InferredDataType::Date | InferredDataType::String => {
            // Keep as string
            format!("{} AS {}", quoted_original_col, quoted_clean_col)
        }
    }
}

// ==============  STREAMING CSV ==============

/// Load CSV with smart casting and Streaming
pub async fn load_csv_with_type_handling_streaming(
    file_path: &str, 
    alias: &str,
) -> ElusionResult<AliasedDataFrame> {
    let ctx = SessionContext::new();

    if !LocalPath::new(file_path).exists() {
        return Err(ElusionError::WriteError {
            path: file_path.to_string(),
            operation: "read".to_string(),
            reason: "File not found".to_string(),
            suggestion: "💡 Check if the file path is correct".to_string()
        });
    }

    let delimiter = detect_delimiter(file_path).await?;
    let delimiter_name = match delimiter {
        b'\t' => "TSV (tab-separated)",
        b',' => "CSV (comma-separated)",
        b';' => "semicolon-separated",
        b'|' => "pipe-separated",
        _ => "custom delimiter"
    };

    println!("🚀 Reading CSV file with streaming and schema detection...");
    let read_start = std::time::Instant::now();

    let temp_df = ctx.read_csv(
        file_path,
        CsvReadOptions::new()
            .has_header(true)
            .schema_infer_max_records(0)  
            .delimiter(delimiter)
    ).await.map_err(ElusionError::DataFusion)?;

    let schema = temp_df.schema();
    let column_count = schema.fields().len();
    
    if let Ok(metadata) = std::fs::metadata(file_path) {
        let file_size = metadata.len();
        println!("📏 File size: {} bytes ({:.2} MB)", file_size, file_size as f64 / 1024.0 / 1024.0);
    }

    println!("✅ Successfully detected schema with {} columns (using ALL records)", column_count);

    let sample_data = get_sample_data_proven(&temp_df, &ctx).await?;
    
    let casting_start = std::time::Instant::now();

    let df = create_streaming_csv_dataframe(
        file_path, 
        &sample_data, 
        schema.fields(), 
        &ctx, 
        alias,
        delimiter  
    ).await?;
    
    let total_elapsed = read_start.elapsed();
    println!("✅ Streaming schema applied in {:?}", casting_start.elapsed());
     println!("🎉 Streaming {} DataFrame setup completed in {:?} for table alias: '{}'", 
        delimiter_name, total_elapsed, alias);
    println!("💡 Data will be processed in chunks when .elusion() is called");

    Ok(AliasedDataFrame {
        dataframe: df,
        alias: alias.to_string(),
    })
}

async fn get_sample_data_proven(df: &DataFrame, ctx: &SessionContext) -> ElusionResult<HashMap<String, Vec<String>>> {
    
    let sample_df = df.clone().limit(0, Some(500))?;
    let sample_batches = sample_df.collect().await.map_err(ElusionError::DataFusion)?;
    
    let mem_table = MemTable::try_new(df.schema().clone().into(), vec![sample_batches])
        .map_err(|e| ElusionError::Custom(format!("Failed to create memory table: {}", e)))?;
    
    ctx.register_table("temp_sample_streaming", Arc::new(mem_table))
        .map_err(ElusionError::DataFusion)?;
    
    let sample_df = ctx.sql("SELECT * FROM temp_sample_streaming LIMIT 500").await
        .map_err(ElusionError::DataFusion)?;
    
    let sample_batches = sample_df.collect().await.map_err(ElusionError::DataFusion)?;
    
    let mut column_samples: HashMap<String, Vec<String>> = HashMap::new();
    
    if let Some(batch) = sample_batches.first() {
        let schema = batch.schema();
        
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let column = batch.column(col_idx);
            let mut samples = Vec::new();
            
            for row_idx in 0..column.len().min(500) {
                if !column.is_null(row_idx) {
                    let value = array_value_to_string(column.as_ref(), row_idx);
                    if !value.is_empty() && value != "null" {
                        samples.push(value);
                    }
                }
            }
            
            column_samples.insert(field.name().to_string(), samples);
        }
    }
    
    Ok(column_samples)
}

async fn create_streaming_csv_dataframe(
    file_path: &str,
    sample_data: &HashMap<String, Vec<String>>,
    original_fields: &[Arc<Field>],
    ctx: &SessionContext,
    alias: &str,
    delimiter: u8
) -> ElusionResult<DataFrame> {
    
    let temp_csv_table = format!("{}_csv_source", alias);

    ctx.register_csv(&temp_csv_table, file_path, CsvReadOptions::new()
        .has_header(true)
        .schema_infer_max_records(0) 
        .delimiter(delimiter)
    ).await.map_err(ElusionError::DataFusion)?;
    
    let cast_sql = generate_smart_casting_sql_streaming(sample_data, original_fields, &temp_csv_table)?;
    
    let df = ctx.sql(&cast_sql).await.map_err(ElusionError::DataFusion)?;
    
    println!("✅ Streaming DataFrame created with proven smart casting logic");
    
    Ok(df)
}

fn generate_smart_casting_sql_streaming(
    sample_data: &HashMap<String, Vec<String>>, 
    fields: &[Arc<Field>],
    table_name: &str
) -> ElusionResult<String> {
    let mut select_parts = Vec::new();
    let empty_samples = Vec::new(); 
    
    for field in fields {
        let original_col_name = field.name();
        let clean_col_name = clean_column_name(original_col_name);

        let samples = sample_data.get(original_col_name).unwrap_or(&empty_samples);
        
        let data_type = infer_column_type(samples, original_col_name);
        let cast_expr = create_casting_expression(original_col_name, &clean_col_name, &data_type);
        
        select_parts.push(cast_expr);
    }
    
    Ok(format!("SELECT {} FROM {}", select_parts.join(", "), table_name))
}

pub async fn load_csv_smart(file_path: &str, alias: &str) -> ElusionResult<AliasedDataFrame> {
    load_csv_with_type_handling_streaming(file_path, alias).await
}
//==========================================================================

#[cfg(test)]
fn debug_regex_matches() {
    let test_value = "999.999.999";
    println!("Testing value: '{}'", test_value);
    
    println!("INTEGER_PATTERN.is_match: {}", INTEGER_PATTERN.is_match(test_value));
    println!("FLOAT_DOT_PATTERN.is_match: {}", FLOAT_DOT_PATTERN.is_match(test_value));
    println!("FLOAT_COMMA_PATTERN.is_match: {}", FLOAT_COMMA_PATTERN.is_match(test_value));
    println!("THOUSAND_SEPARATOR_PATTERN.is_match: {}", THOUSAND_SEPARATOR_PATTERN.is_match(test_value));
    println!("PERCENTAGE_PATTERN.is_match: {}", PERCENTAGE_PATTERN.is_match(test_value));
    println!("CURRENCY_PATTERN.is_match: {}", CURRENCY_PATTERN.is_match(test_value));
    println!("BOOLEAN_PATTERN.is_match: {}", BOOLEAN_PATTERN.is_match(test_value));
    println!("DATE_PATTERN.is_match: {}", DATE_PATTERN.is_match(test_value));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    fn create_test_csv(content: &str, filename: &str, temp_dir: &TempDir) -> String {
        let file_path = temp_dir.path().join(filename);
        let mut file = File::create(&file_path).unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file_path.to_str().unwrap().to_string()
    }

    #[test]
    fn test_debug_specific_case() {
        debug_regex_matches();
        let result = classify_value("999.999.999");
        println!("Final classification: {:?}", result);
        assert!(matches!(result, InferredDataType::String));
    }
    
    #[test]
    fn test_value_classification() {
        // Integer
        assert!(matches!(classify_value("123"), InferredDataType::Integer));
        assert!(matches!(classify_value("-456"), InferredDataType::Integer));
        assert!(matches!(classify_value("+789"), InferredDataType::Integer));
        
        // Float
        assert!(matches!(classify_value("12.34"), InferredDataType::Float));
        assert!(matches!(classify_value("12,34"), InferredDataType::Float));
        assert!(matches!(classify_value("1,234.56"), InferredDataType::Float));
        assert!(matches!(classify_value("15%"), InferredDataType::Float));
        assert!(matches!(classify_value("$123.45"), InferredDataType::Float));
        
        // Boolean 
        assert!(matches!(classify_value("true"), InferredDataType::Boolean));
        assert!(matches!(classify_value("FALSE"), InferredDataType::Boolean));
        assert!(matches!(classify_value("da"), InferredDataType::Boolean));
        
        //  not booleans
        assert!(matches!(classify_value("1"), InferredDataType::Integer));
        assert!(matches!(classify_value("0"), InferredDataType::Integer));
        
        // Date ISO format
        assert!(matches!(classify_value("2024-12-31"), InferredDataType::Date));
        assert!(matches!(classify_value("2024-01-01"), InferredDataType::Date));
        assert!(matches!(classify_value("1999-12-31"), InferredDataType::Date));
        
        // Date  European format
        assert!(matches!(classify_value("31.12.2024"), InferredDataType::Date));
        assert!(matches!(classify_value("01.01.2024"), InferredDataType::Date));
        assert!(matches!(classify_value("12.6.2022"), InferredDataType::Date)); 
        assert!(matches!(classify_value("5.6.2022"), InferredDataType::Date)); 
        
        // Date US format
        assert!(matches!(classify_value("12/31/2024"), InferredDataType::Date));
        assert!(matches!(classify_value("01/01/2024"), InferredDataType::Date));
        assert!(matches!(classify_value("6/12/2022"), InferredDataType::Date));
        
        // Time 
        assert!(matches!(classify_value("14:30"), InferredDataType::Date));
        assert!(matches!(classify_value("09:15:30"), InferredDataType::Date));
        assert!(matches!(classify_value("23:59:59"), InferredDataType::Date));
        assert!(matches!(classify_value("00:00"), InferredDataType::Date));
        
        // String 
        assert!(matches!(classify_value("Jul"), InferredDataType::String));
        assert!(matches!(classify_value("Hello World"), InferredDataType::String));
        assert!(matches!(classify_value("Not-a-date"), InferredDataType::String));
    }
    
    #[test]
    fn test_type_inference() {
        let int_samples = vec!["123".to_string(), "456".to_string(), "789".to_string()];
        assert!(matches!(infer_column_type(&int_samples, "test"), InferredDataType::Integer));
        
        let mixed_samples = vec!["123".to_string(), "Jul".to_string(), "456".to_string()];
        assert!(matches!(infer_column_type(&mixed_samples, "test"), InferredDataType::String));
        
        let float_samples = vec!["12.34".to_string(), "56.78".to_string(), "90.12".to_string()];
        assert!(matches!(infer_column_type(&float_samples, "test"), InferredDataType::Float));
    }
    
    #[test]
    fn test_datetime_classification_comprehensive() {
        // ISO 
        assert!(matches!(classify_value("2024-01-15"), InferredDataType::Date));
        assert!(matches!(classify_value("2024-12-31"), InferredDataType::Date));
        assert!(matches!(classify_value("1900-01-01"), InferredDataType::Date));
        assert!(matches!(classify_value("2099-12-31"), InferredDataType::Date));
        
        // European 
        assert!(matches!(classify_value("15.01.2024"), InferredDataType::Date));
        assert!(matches!(classify_value("31.12.2024"), InferredDataType::Date));
        assert!(matches!(classify_value("15/01/2024"), InferredDataType::Date));
        assert!(matches!(classify_value("31/12/2024"), InferredDataType::Date));
        
        // US 
        assert!(matches!(classify_value("01/15/2024"), InferredDataType::Date));
        assert!(matches!(classify_value("12/31/2024"), InferredDataType::Date));
        assert!(matches!(classify_value("01-15-2024"), InferredDataType::Date));
        
        // Short 
        assert!(matches!(classify_value("2024-1-5"), InferredDataType::Date));
       assert!(matches!(classify_value("24-12-31"), InferredDataType::String));
       assert!(matches!(classify_value("5.1.24"), InferredDataType::String));
        
        // Time formats
        assert!(matches!(classify_value("09:30"), InferredDataType::Date));
        assert!(matches!(classify_value("21:45"), InferredDataType::Date));
        assert!(matches!(classify_value("00:00"), InferredDataType::Date));
        assert!(matches!(classify_value("23:59"), InferredDataType::Date));
        assert!(matches!(classify_value("09:30:45"), InferredDataType::Date));
        assert!(matches!(classify_value("21:45:30"), InferredDataType::Date));
        assert!(matches!(classify_value("00:00:00"), InferredDataType::Date));
        assert!(matches!(classify_value("23:59:59"), InferredDataType::Date));
        
        //Edge cases 
        assert!(matches!(classify_value("123-456-789"), InferredDataType::String)); 
        assert!(matches!(classify_value("999.999.999"), InferredDataType::String)); 
        assert!(matches!(classify_value("32.13.2024"), InferredDataType::String));  
        assert!(matches!(classify_value("2024-13-01"), InferredDataType::String)); 
        assert!(matches!(classify_value("2024-01-32"), InferredDataType::String));  
        assert!(matches!(classify_value("25:00"), InferredDataType::String));   
        assert!(matches!(classify_value("12:60"), InferredDataType::String));   
        assert!(matches!(classify_value("12:30:60"), InferredDataType::String));  
        
        //Version numbers 
        assert!(matches!(classify_value("1.2.3"), InferredDataType::String));
        assert!(matches!(classify_value("2.0.1"), InferredDataType::String));
        assert!(matches!(classify_value("10.15.2"), InferredDataType::String));
        
        // Scientific notation
        assert!(matches!(classify_value("1.23e-4"), InferredDataType::String)); 
        assert!(matches!(classify_value("2.5E+10"), InferredDataType::String)); 
    }
    
    #[test]
    fn test_datetime_with_timezone_and_complex_formats() {
        // ISO
        assert!(matches!(classify_value("2024-01-15T10:30:00Z"), InferredDataType::String));
        assert!(matches!(classify_value("2024-01-15T10:30:00+01:00"), InferredDataType::String));
        assert!(matches!(classify_value("2024-01-15T10:30:00-05:00"), InferredDataType::String));
        
        // RFC 
        assert!(matches!(classify_value("Mon, 15 Jan 2024 10:30:00 GMT"), InferredDataType::String));
        assert!(matches!(classify_value("Monday, January 15, 2024"), InferredDataType::String));
        
        // Timestamp 
        assert!(matches!(classify_value("2024-01-15 10:30:00"), InferredDataType::String));
        assert!(matches!(classify_value("15.01.2024 10:30:00"), InferredDataType::String));
        
        // Unix 
        assert!(matches!(classify_value("1705320600"), InferredDataType::Integer));
        assert!(matches!(classify_value("1234567890"), InferredDataType::Integer));
        
        // Month names
        assert!(matches!(classify_value("January"), InferredDataType::String));
        assert!(matches!(classify_value("Feb"), InferredDataType::String));
        assert!(matches!(classify_value("März"), InferredDataType::String)); // German
        assert!(matches!(classify_value("Janvier"), InferredDataType::String)); // French
        assert!(matches!(classify_value("Januar"), InferredDataType::String)); // Serbian 
        assert!(matches!(classify_value("Jul"), InferredDataType::String)); // Serbian 
    }

    #[test]
    fn test_email_classification() {
        // Valid email addresses from your examples
        assert!(matches!(classify_value("jon24@adventure-works.com"), InferredDataType::String));
        assert!(matches!(classify_value("eugene10@adventure-works.com"), InferredDataType::String));
        assert!(matches!(classify_value("ruben35@adventure-works.com"), InferredDataType::String));
        assert!(matches!(classify_value("christy12@adventure-works.com"), InferredDataType::String));
        assert!(matches!(classify_value("elizabeth5@adventure-works.com"), InferredDataType::String));
        assert!(matches!(classify_value("julio1@adventure-works.com"), InferredDataType::String));
        assert!(matches!(classify_value("marco14@adventure-works.com"), InferredDataType::String));
        
        // More valid String formats
        assert!(matches!(classify_value("user@example.com"), InferredDataType::String));
        assert!(matches!(classify_value("test.String@domain.org"), InferredDataType::String));
        assert!(matches!(classify_value("user+tag@example.co.uk"), InferredDataType::String));
        assert!(matches!(classify_value("user_name@example-domain.com"), InferredDataType::String));
        assert!(matches!(classify_value("123@numbers.net"), InferredDataType::String));
        assert!(matches!(classify_value("user%test@example.info"), InferredDataType::String));
        
        // Edge cases 
        assert!(matches!(classify_value("@example.com"), InferredDataType::String));
        assert!(matches!(classify_value("user@"), InferredDataType::String)); 
        assert!(matches!(classify_value("user@.com"), InferredDataType::String)); 
        assert!(matches!(classify_value("user@example"), InferredDataType::String)); 
        assert!(matches!(classify_value("user@example."), InferredDataType::String));
        assert!(matches!(classify_value("user@example.c"), InferredDataType::String)); 
        assert!(matches!(classify_value("user@@example.com"), InferredDataType::String)); 
        assert!(matches!(classify_value("user.@example.com"), InferredDataType::String)); 
        assert!(matches!(classify_value(".user@example.com"), InferredDataType::String)); 
        assert!(matches!(classify_value("user@example..com"), InferredDataType::String)); 
        
        // Should still correctly classify other types
        assert!(matches!(classify_value("123"), InferredDataType::Integer));
        assert!(matches!(classify_value("12.34"), InferredDataType::Float));
        assert!(matches!(classify_value("2024-01-15"), InferredDataType::Date));
        assert!(matches!(classify_value("true"), InferredDataType::Boolean));
        assert!(matches!(classify_value("hello world"), InferredDataType::String));
    }
    
    #[test]
    fn test_datetime_type_inference_majority_vote() {
        let date_samples = vec![
            "2024-01-15".to_string(), 
            "2024-02-20".to_string(), 
            "2024-03-25".to_string()
        ];
        assert!(matches!(infer_column_type(&date_samples, "date_col"), InferredDataType::Date));
        
        // Mixed date/text STRING due to <80% threshold
        let mixed_date_samples = vec![
            "2024-01-15".to_string(), 
            "Invalid".to_string(), 
            "2024-03-25".to_string()
        ];
        assert!(matches!(infer_column_type(&mixed_date_samples, "mixed_col"), InferredDataType::String));
        
        // Time column
        let time_samples = vec![
            "09:30:00".to_string(), 
            "14:45:30".to_string(), 
            "23:59:59".to_string()
        ];
        assert!(matches!(infer_column_type(&time_samples, "time_col"), InferredDataType::Date));
        
        // Serbian month 
        let serbian_months = vec![
            "Januar".to_string(), 
            "Februar".to_string(), 
            "Jul".to_string(),
            "Decembar".to_string()
        ];
        assert!(matches!(infer_column_type(&serbian_months, "mesec"), InferredDataType::String));
    }

    #[tokio::test]
    async fn test_regular_csv_still_works() {
        let temp_dir = TempDir::new().unwrap();
        let csv_content = r#"name,age,salary,active
        John,25,50000.50,true
        Jane,30,75000.00,false
        Bob,35,60000.25,true"#;
                
        let csv_path = create_test_csv(csv_content, "test.csv", &temp_dir);

        let result = load_csv_with_type_handling(&csv_path, "test_csv").await;
        assert!(result.is_ok(), "Regular CSV loading should work: {:?}", result.err());
        
        let df = result.unwrap();
        let schema = df.dataframe.schema();
        
        // Verify we have 4 columns
        assert_eq!(schema.fields().len(), 4);
    }

    #[tokio::test]
    async fn test_csv_with_quoted_fields() {
        let temp_dir = TempDir::new().unwrap();
        let csv_content = r#"name,description,price
        "John Doe","Software Engineer, Senior",75000
        "Jane Smith","Data Scientist, Lead",85000
        "Bob Wilson","Product Manager",70000"#;
        
        let csv_path = create_test_csv(csv_content, "quoted.csv", &temp_dir);
        
        let result = load_csv_with_type_handling(&csv_path, "quoted_csv").await;
        assert!(result.is_ok(), "CSV with quoted fields should work: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_csv_with_mixed_types() {
        let temp_dir = TempDir::new().unwrap();
        let csv_content = r#"id,name,score,date,percentage,currency,active
        1,Alice,95.5,2024-01-15,85%,$50000,true
        2,Bob,87.2,2024-02-20,92%,$55000,false
        3,Carol,91.8,2024-03-10,78%,$48000,true"#;
        
        let csv_path = create_test_csv(csv_content, "mixed_types.csv", &temp_dir);
        
        let result = load_csv_with_type_handling(&csv_path, "mixed_csv").await;
        assert!(result.is_ok(), "CSV with mixed types should work: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_delimiter_detection_comma() {
        let temp_dir = TempDir::new().unwrap();
        let csv_content = r#"col1,col2,col3
        val1,val2,val3
        data1,data2,data3"#;
        
        let csv_path = create_test_csv(csv_content, "comma.csv", &temp_dir);
        
        let detected = detect_delimiter(&csv_path).await.unwrap();
        assert_eq!(detected, b',', "Should detect comma delimiter");
    }

    #[tokio::test]
    async fn test_delimiter_detection_tab() {
        let temp_dir = TempDir::new().unwrap();
        let tsv_content = "col1\tcol2\tcol3\nval1\tval2\tval3\ndata1\tdata2\tdata3";
        
        let tsv_path = create_test_csv(tsv_content, "test.tsv", &temp_dir);
        
        let detected = detect_delimiter(&tsv_path).await.unwrap();
        assert_eq!(detected, b'\t', "Should detect tab delimiter");
    }

    #[tokio::test]
    async fn test_delimiter_detection_semicolon() {
        let temp_dir = TempDir::new().unwrap();
        let csv_content = r#"col1;col2;col3
        val1;val2;val3
        data1;data2;data3"#;
        
        let csv_path = create_test_csv(csv_content, "semicolon.csv", &temp_dir);
        
        let detected = detect_delimiter(&csv_path).await.unwrap();
        assert_eq!(detected, b';', "Should detect semicolon delimiter");
    }

    #[tokio::test]
    async fn test_delimiter_detection_pipe() {
        let temp_dir = TempDir::new().unwrap();
        let csv_content = r#"col1|col2|col3
        val1|val2|val3
        data1|data2|data3"#;
        
        let csv_path = create_test_csv(csv_content, "pipe.csv", &temp_dir);
        
        let detected = detect_delimiter(&csv_path).await.unwrap();
        assert_eq!(detected, b'|', "Should detect pipe delimiter");
    }

    #[tokio::test]
    async fn test_csv_with_empty_values() {
        let temp_dir = TempDir::new().unwrap();
        let csv_content = r#"name,age,salary,notes
        John,25,50000,Has experience
        Jane,,75000,
        Bob,35,,New hire
        Alice,28,60000,N/A"#;
        
        let csv_path = create_test_csv(csv_content, "empty_vals.csv", &temp_dir);
        
        let result = load_csv_with_type_handling(&csv_path, "empty_csv").await;
        assert!(result.is_ok(), "CSV with empty values should work: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_csv_with_special_characters() {
        let temp_dir = TempDir::new().unwrap();
        let csv_content = r#"name,city,country
        José,São Paulo,Brazil
        François,Zürich,Switzerland
        Müller,München,Germany"#;
        
        let csv_path = create_test_csv(csv_content, "special_chars.csv", &temp_dir);
        
        let result = load_csv_with_type_handling(&csv_path, "special_csv").await;
        assert!(result.is_ok(), "CSV with special characters should work: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_large_csv_columns() {
        let temp_dir = TempDir::new().unwrap();
        
        // Create CSV with many columns
        let mut headers = Vec::new();
        let mut values = Vec::new();
        for i in 1..=50 {
            headers.push(format!("col{}", i));
            values.push(format!("val{}", i));
        }
        
        let csv_content = format!("{}\n{}", headers.join(","), values.join(","));
        let csv_path = create_test_csv(&csv_content, "wide.csv", &temp_dir);
        
        let result = load_csv_with_type_handling(&csv_path, "wide_csv").await;
        assert!(result.is_ok(), "CSV with many columns should work: {:?}", result.err());
        
        let df = result.unwrap();
        let schema = df.dataframe.schema();
        assert_eq!(schema.fields().len(), 50, "Should have 50 columns");
    }

    #[tokio::test]
    async fn test_streaming_csv_compatibility() {
        let temp_dir = TempDir::new().unwrap();
        let csv_content = r#"id,name,value
        1,test1,100
        2,test2,200
        3,test3,300"#;
        
        let csv_path = create_test_csv(csv_content, "stream_test.csv", &temp_dir);
        
        // Test streaming version
        let result = load_csv_with_type_handling_streaming(&csv_path, "stream_csv").await;
        assert!(result.is_ok(), "Streaming CSV loading should work: {:?}", result.err());
        
        let df = result.unwrap();
        let schema = df.dataframe.schema();
        assert_eq!(schema.fields().len(), 3, "Should have 3 columns");
    }

    #[tokio::test]
    async fn test_csv_smart_loader() {
        let temp_dir = TempDir::new().unwrap();
        let csv_content = r#"product,price,in_stock
        Widget,19.99,true
        Gadget,29.50,false
        Tool,15.00,true"#;
        
        let csv_path = create_test_csv(csv_content, "smart_test.csv", &temp_dir);
        
        let result = load_csv_smart(&csv_path, "smart_csv").await;
        assert!(result.is_ok(), "Smart CSV loading should work: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_end_to_end_csv_workflow() {
        let temp_dir = TempDir::new().unwrap();
        let csv_content = r#"customer_id,customer_name,order_total,order_date,is_premium
        1001,John Smith,250.75,2024-01-15,true
        1002,Jane Doe,189.99,2024-01-16,false
        1003,Bob Johnson,375.50,2024-01-17,true"#;
        
        let csv_path = create_test_csv(csv_content, "workflow_test.csv", &temp_dir);
        
        let result = load_csv_with_type_handling(&csv_path, "workflow").await;
        assert!(result.is_ok(), "End-to-end workflow should work");
        
        if let Ok(aliased_df) = result {
            assert_eq!(aliased_df.alias, "workflow");
 
            let schema = aliased_df.dataframe.schema();
            assert_eq!(schema.fields().len(), 5);
        }
    }

    #[tokio::test]
    async fn test_tsv_detection_and_loading() {
        let temp_dir = TempDir::new().unwrap();
        let tsv_content = "name\tage\tsalary\tactive\nJohn\t25\t50000.50\ttrue\nJane\t30\t75000.00\tfalse";
        
        let tsv_path = create_test_csv(tsv_content, "test.csv", &temp_dir);
        
        let detected = detect_delimiter(&tsv_path).await.unwrap();
        assert_eq!(detected, b'\t', "Should detect tab delimiter for TSV");
        
        let result = load_csv_with_type_handling(&tsv_path, "tsv_test").await;
        assert!(result.is_ok(), "TSV loading should work with enhanced CSV loader: {:?}", result.err());
        
        let df = result.unwrap();
        let schema = df.dataframe.schema();
        assert_eq!(schema.fields().len(), 4, "TSV should have 4 columns");
    }

    #[tokio::test]
    async fn test_delimiter_consistency_check() {
        let temp_dir = TempDir::new().unwrap();
        
        let mixed_content = r#"col1,col2,col3
        val1,val2,val3
        bad1;bad2;bad3
        good1,good2,good3"#;
        
        let mixed_path = create_test_csv(mixed_content, "mixed.csv", &temp_dir);
        
        let detected = detect_delimiter(&mixed_path).await.unwrap();

        assert_eq!(detected, b',', "Should detect comma as most consistent delimiter");
    }

    #[test]
    fn test_null_value_detection() {
        assert!(NULL_VALUES.contains(""));
        assert!(NULL_VALUES.contains("NULL"));
        assert!(NULL_VALUES.contains("null"));
        assert!(NULL_VALUES.contains("N/A"));
        assert!(NULL_VALUES.contains("n/a"));
        assert!(NULL_VALUES.contains("-"));
        assert!(!NULL_VALUES.contains("0"));
        assert!(!NULL_VALUES.contains("false"));
    }
}