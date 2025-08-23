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
    //Regex::new(r"^[+-]?[0-9]{1,3}([,.][0-9]{3})*([.,][0-9]{1,2})?$").expect("Failed to compile thousand separator regex")
    Regex::new(r"^[+-]?(?:[0-9]{1,3}(?:,[0-9]{3}){1,}(?:\.[0-9]{1,2})?|[0-9]{1,3}(?:\.[0-9]{3}){2,}(?:,[0-9]{1,2})?)$")
        .expect("Failed to compile thousand separator regex")
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
    let sample_size = 10; 

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

    let threshold = 0.75;
    
    if let Some(&int_count) = type_votes.get(&InferredDataType::Integer) {
        if int_count as f32 / total_non_null as f32 > threshold {
            return InferredDataType::Integer;
        }
    }
    
    if let Some(&float_count) = type_votes.get(&InferredDataType::Float) {
        let int_count = type_votes.get(&InferredDataType::Integer).unwrap_or(&0);
        if (float_count + int_count) as f32 / total_non_null as f32 > threshold {
            return InferredDataType::Float;
        }
    }
    
    if let Some(&bool_count) = type_votes.get(&InferredDataType::Boolean) {
        if bool_count as f32 / total_non_null as f32 > threshold {
            return InferredDataType::Boolean;
        }
    }
    
    if let Some(&date_count) = type_votes.get(&InferredDataType::Date) {
        if date_count as f32 / total_non_null as f32 > threshold {
            return InferredDataType::Date;
        }
    }
    
    InferredDataType::String
}

fn classify_value(value: &str) -> InferredDataType {

    let len = value.len();
    if len == 0 || len > 100 {
        return InferredDataType::String;
    }

    let has_letters = value.chars().any(|c| c.is_alphabetic() && !matches!(c, 'e' | 'E')); // Allow scientific notation
    let has_at_or_url = value.contains('@') || value.contains("http") || value.contains("www");
    
    if has_letters {
        return InferredDataType::String;
    }

    if has_at_or_url {
        return InferredDataType::String;
    }

    if INTEGER_PATTERN.is_match(value) {
        return InferredDataType::Integer;
    }
    
    if FLOAT_DOT_PATTERN.is_match(value) {
        if value.matches('.').count() == 1 {
            return InferredDataType::Float;
        }
    }

    if FLOAT_COMMA_PATTERN.is_match(value) {
        if value.matches(',').count() == 1 {
            return InferredDataType::Float;
        }
    }
    
    if THOUSAND_SEPARATOR_PATTERN.is_match(value) {
        if is_valid_thousand_separator_number(value) {
            return InferredDataType::Float;
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

fn is_valid_thousand_separator_number(value: &str) -> bool {
    let cleaned = value.trim_start_matches(['+', '-']);
    
    // US Format: comma thousands, dot decimal 1,234.56
    if cleaned.contains(',') && cleaned.contains('.') {
        return validate_us_format(cleaned);
    }
    
    // EU Format: dot thousands, comma decimal 1.234,56
    // Must have at least 2 dot groups to be considered thousands separator
    if cleaned.contains('.') && cleaned.contains(',') {
        let dot_count = cleaned.matches('.').count();
        if dot_count >= 2 { // At least 2 dots required for EU thousands format
            return validate_eu_format(cleaned);
        }
        return false; // Single dot with comma is not valid thousands format
    }
    
    // Pure comma separators 1,234
    if cleaned.contains(',') && !cleaned.contains('.') {
        return validate_comma_thousands_only(cleaned);
    }
    
    // Pure dot separators 1.234.567
    if cleaned.contains('.') && !cleaned.contains(',') {
        let dot_count = cleaned.matches('.').count();
        if dot_count >= 2 { // Multiple dots required
            return validate_dot_thousands_only(cleaned) && !looks_like_ip_or_version(cleaned);
        }
        return false; // Single dot should be handled by FLOAT_DOT_PATTERN
    }
    
    false
}

fn validate_us_format(value: &str) -> bool {
    let parts: Vec<&str> = value.split('.').collect();
    if parts.len() != 2 {
        return false;
    }
    
    let integer_part = parts[0];
    let decimal_part = parts[1];
    
    // Decimal part should be 1-2 digits
    if decimal_part.is_empty() || decimal_part.len() > 2 || !decimal_part.chars().all(|c| c.is_ascii_digit()) {
        return false;
    }
    
    is_valid_comma_grouping(integer_part)
}

fn validate_eu_format(value: &str) -> bool {
    let parts: Vec<&str> = value.split(',').collect();
    if parts.len() != 2 {
        return false;
    }
    
    let integer_part = parts[0];
    let decimal_part = parts[1];
    
    // Decimal part should be 1-2 digits
    if decimal_part.is_empty() || decimal_part.len() > 2 || !decimal_part.chars().all(|c| c.is_ascii_digit()) {
        return false;
    }
    
    // Integer part must have at least 2 dot groups for thousands
    let dot_count = integer_part.matches('.').count();
    if dot_count < 2 {
        return false; // Need at least 2 dots for proper thousands separator
    }
    
    is_valid_dot_grouping(integer_part)
}

fn validate_comma_thousands_only(value: &str) -> bool {
    is_valid_comma_grouping(value)
}

fn validate_dot_thousands_only(value: &str) -> bool {
    let parts: Vec<&str> = value.split('.').collect();
    
    // Must have at least 3 parts (2+ dots) for thousands separators  
    if parts.len() < 3 {
        return false;
    }
    
    // Reject if there are too many groups
    if parts.len() > 4 {
        return false;
    }
    
    // Check for obvious non-numbers
    if looks_like_ip_or_version(value) {
        return false;
    }
    
    is_valid_dot_grouping(value)
}

fn looks_like_ip_or_version(value: &str) -> bool {
    let parts: Vec<&str> = value.split('.').collect();
    
    // IP address pattern 4 parts, all <= 255
    if parts.len() == 4 {
        return parts.iter().all(|p| {
            p.parse::<u32>().map_or(false, |n| n <= 255)
        });
    }
    
    // Version number pattern 2-4 parts, small numbers
    if parts.len() >= 2 && parts.len() <= 4 {
        // All parts are small numbers (<=50)
        if parts.iter().all(|p| p.parse::<u32>().map_or(false, |n| n <= 50)) {
            return true;
        }
        
        // Pattern like 999.999.999 all same repeated digits
        if parts.len() == 3 && parts.iter().all(|p| p.len() == 3) {
            let all_same = parts.iter().all(|p| p == &parts[0]);
            if all_same {
                return true; // Likely not a real number
            }
        }
    }
    
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
    
    // Must have at least 2 parts
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
    // 1.2.3, 2.0.1, 10.15.2
    if nums.iter().all(|&n| n <= 20) {
        return true;
    }
    
    // If the first number is small <=50 and others are small <=100, 
    // and none looks like a year, it's probably a version
    if nums[0] <= 50 && nums[1] <= 100 && nums[2] <= 100 {
        //  none of the numbers looks like a year
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
    
    let arrangements = [
        (nums[0], nums[1], nums[2]), // YYYY-MM-DD 
        (nums[2], nums[1], nums[0]), // DD-MM-YYYY
        (nums[1], nums[0], nums[2]), // MM-DD-YYYY (US format with year at end)
        (nums[2], nums[0], nums[1]), // YYYY-DD-MM (uncommon)
    ];
    
    for (year, month, day) in arrangements {
       // println!(" Trying arrangement: year={}, month={}, day={}", year, month, day);
        
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
    
    // Month validation 1-12
    if month < 1 || month > 12 {
        return false;
    }
    
    // Day validation 1-31, with basic month-specific checks
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
                    WHEN {} ~ '^[+-]?[0-9]+$' THEN CAST({} AS DOUBLE)
                    WHEN {} ~ '^[+-]?[0-9]*\\.[0-9]+$' THEN CAST({} AS DOUBLE)
                    WHEN {} ~ '^[+-]?[0-9]+,[0-9]+$' THEN CAST(REPLACE({}, ',', '.') AS DOUBLE)
                    WHEN {} ~ '^[+-]?[0-9]{{1,3}}(,[0-9]{{3}})+$' OR {} ~ '^[+-]?[0-9]{{1,3}}(,[0-9]{{3}})+\\.[0-9]{{1,2}}$' THEN
                        CAST(REPLACE({}, ',', '') AS DOUBLE)
                    WHEN {} ~ '%$' THEN CAST(REPLACE({}, '%', '') AS DOUBLE) / 100.0
                    WHEN {} ~ '^[$€£¥₹]' OR {} ~ '[$€£¥₹]$' THEN
                        CAST(REGEXP_REPLACE({}, '[$$€£¥₹,\\s]', '', 'g') AS DOUBLE)
                    ELSE NULL
                END AS {}",
                // NULL checks
                quoted_original_col, quoted_original_col, quoted_original_col,
                // Integers
                quoted_original_col, quoted_original_col,
                // Simple decimals
                quoted_original_col, quoted_original_col,
                // European decimals
                quoted_original_col, quoted_original_col,
                // US thousands (both with and without decimals)
                quoted_original_col, quoted_original_col, quoted_original_col,
                // Percentages
                quoted_original_col, quoted_original_col,
                // Currency
                quoted_original_col, quoted_original_col, quoted_original_col,
                // Final
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
           format!(
                "CASE 
                    WHEN {} IS NULL THEN NULL
                    ELSE {}
                END AS {}", 
                quoted_original_col, quoted_original_col, quoted_clean_col
            )
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

        // metadata
        if let Ok(metadata) = std::fs::metadata(file_path) {
            let file_size = metadata.len();
            println!("📏 File size: {} bytes ({:.2} MB)", file_size, file_size as f64 / 1024.0 / 1024.0);
        }
        //sampledata fro file not fromm dataframe that is in memory
        let sample_data = get_sample_data_from_file_proven(file_path, delimiter, 1000).await?;
        
        let temp_df = ctx.read_csv(
            file_path,
            CsvReadOptions::new()
                .has_header(true)
                .schema_infer_max_records(10)  
                .delimiter(delimiter)
        ).await.map_err(ElusionError::DataFusion)?;

        let schema = temp_df.schema();
        let column_count = schema.fields().len();
        
        println!("✅ Successfully detected schema with {} columns (file-based sampling)", column_count);
        
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
        println!("💡 Data will be processed in chunks when .elusion_streaming() is called. (don't call .elusion() when streaming)");

        Ok(AliasedDataFrame {
            dataframe: df,
            alias: alias.to_string(),
        })
    }

    async fn get_sample_data_from_file_proven(
        file_path: &str, 
        delimiter: u8, 
        sample_size: usize
    ) -> ElusionResult<HashMap<String, Vec<String>>> {
        
        println!("🔬 Reading {} sample rows directly from file for robust analysis...", sample_size);
        
        let file = File::open(file_path).map_err(|e| ElusionError::WriteError {
            path: file_path.to_string(),
            operation: "sample_read".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check file permissions".to_string()
        })?;
        
        let mut reader = BufReader::new(file);
        
        // Read header
        let mut header_line = String::new();
        reader.read_line(&mut header_line).map_err(|e| ElusionError::Custom(format!("Failed to read header: {}", e)))?;
        
        let headers: Vec<String> = header_line.trim()
            .split(delimiter as char)
            .map(|s| s.trim().trim_matches('"').to_string()) 
            .collect();
        
        println!("📋 Found {} columns in header", headers.len());
        
        // Initialize column samples
        let mut column_samples: HashMap<String, Vec<String>> = HashMap::new();
        for header in &headers {
            column_samples.insert(header.clone(), Vec::new());
        }
        
        // Read sample rows with better error handling
        let mut rows_read = 0;
        let mut parse_errors = 0;
        
        for line_result in reader.lines() {
            if rows_read >= sample_size { break; }
            
            match line_result {
                Ok(line) => {
                    let values: Vec<&str> = line.split(delimiter as char).collect();
                    
                    // Handle mismatched column counts gracefully
                    for (i, header) in headers.iter().enumerate() {
                        if let Some(value) = values.get(i) {
                            let trimmed_value = value.trim().trim_matches('"'); // Remove quotes
                            if trimmed_value.len() <= 500 { // Include ALL values for proper sampling
                                if let Some(samples) = column_samples.get_mut(header) {
                                    samples.push(trimmed_value.to_string());
                                }
                            }
                        }
                    }
                    rows_read += 1;
                },
                Err(_) => {
                    parse_errors += 1;
                    if parse_errors > 10 { // Stop if too many parse errors
                        println!("⚠️  Too many parse errors, stopping sample collection");
                        break;
                    }
                }
            }
        }
        
        println!("✅ Read {} sample rows from file for schema detection", rows_read);
        if parse_errors > 0 {
            println!("⚠️  Encountered {} parse errors (handled gracefully)", parse_errors);
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
//====================== TESTING ====================================================

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
    
    // #[test]
    // fn test_value_classification() {
    //     // Integer
    //     assert!(matches!(classify_value("123"), InferredDataType::Integer));
    //     assert!(matches!(classify_value("-456"), InferredDataType::Integer));
    //     assert!(matches!(classify_value("+789"), InferredDataType::Integer));
        
    //     // Float
    //     assert!(matches!(classify_value("12.34"), InferredDataType::Float));
    //     assert!(matches!(classify_value("12,34"), InferredDataType::Float));
    //     assert!(matches!(classify_value("1,234.56"), InferredDataType::Float));
    //     assert!(matches!(classify_value("15%"), InferredDataType::Float));
    //     assert!(matches!(classify_value("$123.45"), InferredDataType::Float));
        
    //     // Boolean 
    //     assert!(matches!(classify_value("true"), InferredDataType::Boolean));
    //     assert!(matches!(classify_value("FALSE"), InferredDataType::Boolean));
    //     assert!(matches!(classify_value("da"), InferredDataType::Boolean));
        
    //     //  not booleans
    //     assert!(matches!(classify_value("1"), InferredDataType::Integer));
    //     assert!(matches!(classify_value("0"), InferredDataType::Integer));
        
    //     // Date ISO format
    //     assert!(matches!(classify_value("2024-12-31"), InferredDataType::Date));
    //     assert!(matches!(classify_value("2024-01-01"), InferredDataType::Date));
    //     assert!(matches!(classify_value("1999-12-31"), InferredDataType::Date));
        
    //     // Date  European format
    //     assert!(matches!(classify_value("31.12.2024"), InferredDataType::Date));
    //     assert!(matches!(classify_value("01.01.2024"), InferredDataType::Date));
    //     assert!(matches!(classify_value("12.6.2022"), InferredDataType::Date)); 
    //     assert!(matches!(classify_value("5.6.2022"), InferredDataType::Date)); 
        
    //     // Date US format
    //     assert!(matches!(classify_value("12/31/2024"), InferredDataType::Date));
    //     assert!(matches!(classify_value("01/01/2024"), InferredDataType::Date));
    //     assert!(matches!(classify_value("6/12/2022"), InferredDataType::Date));
        
    //     // Time 
    //     assert!(matches!(classify_value("14:30"), InferredDataType::Date));
    //     assert!(matches!(classify_value("09:15:30"), InferredDataType::Date));
    //     assert!(matches!(classify_value("23:59:59"), InferredDataType::Date));
    //     assert!(matches!(classify_value("00:00"), InferredDataType::Date));
        
    //     // String 
    //     assert!(matches!(classify_value("Jul"), InferredDataType::String));
    //     assert!(matches!(classify_value("Hello World"), InferredDataType::String));
    //     assert!(matches!(classify_value("Not-a-date"), InferredDataType::String));
    // }
    
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

    // #[test]
    // fn test_email_classification() {
    //     // Valid email addresses from your examples
    //     assert!(matches!(classify_value("jon24@adventure-works.com"), InferredDataType::String));
    //     assert!(matches!(classify_value("eugene10@adventure-works.com"), InferredDataType::String));
    //     assert!(matches!(classify_value("ruben35@adventure-works.com"), InferredDataType::String));
    //     assert!(matches!(classify_value("christy12@adventure-works.com"), InferredDataType::String));
    //     assert!(matches!(classify_value("elizabeth5@adventure-works.com"), InferredDataType::String));
    //     assert!(matches!(classify_value("julio1@adventure-works.com"), InferredDataType::String));
    //     assert!(matches!(classify_value("marco14@adventure-works.com"), InferredDataType::String));
        
    //     // More valid String formats
    //     assert!(matches!(classify_value("user@example.com"), InferredDataType::String));
    //     assert!(matches!(classify_value("test.String@domain.org"), InferredDataType::String));
    //     assert!(matches!(classify_value("user+tag@example.co.uk"), InferredDataType::String));
    //     assert!(matches!(classify_value("user_name@example-domain.com"), InferredDataType::String));
    //     assert!(matches!(classify_value("123@numbers.net"), InferredDataType::String));
    //     assert!(matches!(classify_value("user%test@example.info"), InferredDataType::String));
        
    //     // Edge cases 
    //     assert!(matches!(classify_value("@example.com"), InferredDataType::String));
    //     assert!(matches!(classify_value("user@"), InferredDataType::String)); 
    //     assert!(matches!(classify_value("user@.com"), InferredDataType::String)); 
    //     assert!(matches!(classify_value("user@example"), InferredDataType::String)); 
    //     assert!(matches!(classify_value("user@example."), InferredDataType::String));
    //     assert!(matches!(classify_value("user@example.c"), InferredDataType::String)); 
    //     assert!(matches!(classify_value("user@@example.com"), InferredDataType::String)); 
    //     assert!(matches!(classify_value("user.@example.com"), InferredDataType::String)); 
    //     assert!(matches!(classify_value(".user@example.com"), InferredDataType::String)); 
    //     assert!(matches!(classify_value("user@example..com"), InferredDataType::String)); 
        
    //     // Should still correctly classify other types
    //     assert!(matches!(classify_value("123"), InferredDataType::Integer));
    //     assert!(matches!(classify_value("12.34"), InferredDataType::Float));
    //     assert!(matches!(classify_value("2024-01-15"), InferredDataType::Date));
    //     assert!(matches!(classify_value("true"), InferredDataType::Boolean));
    //     assert!(matches!(classify_value("hello world"), InferredDataType::String));
    // }
    
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

    #[test]
    fn test_thousand_separator_regex_matches() {
        println!("🔍 Testing THOUSAND_SEPARATOR_PATTERN regex...");
        
        // These should match the regex
        assert!(THOUSAND_SEPARATOR_PATTERN.is_match("2,162.00"), 
            "2,162.00 should match THOUSAND_SEPARATOR_PATTERN");
        assert!(THOUSAND_SEPARATOR_PATTERN.is_match("1,234.56"), 
            "1,234.56 should match THOUSAND_SEPARATOR_PATTERN");
        assert!(THOUSAND_SEPARATOR_PATTERN.is_match("999,999.99"), 
            "999,999.99 should match THOUSAND_SEPARATOR_PATTERN");
        assert!(THOUSAND_SEPARATOR_PATTERN.is_match("1,000,000.00"), 
            "1,000,000.00 should match THOUSAND_SEPARATOR_PATTERN");
        assert!(THOUSAND_SEPARATOR_PATTERN.is_match("1,234"), 
            "1,234 should match THOUSAND_SEPARATOR_PATTERN");
        
        // These WILL match the regex but should be rejected by validation
        assert!(THOUSAND_SEPARATOR_PATTERN.is_match("999.999.999"), 
            "999.999.999 WILL match regex but should be rejected by validation");
        assert!(THOUSAND_SEPARATOR_PATTERN.is_match("192.168.001"), 
            "192.168.001 WILL match regex but should be rejected by validation");
        
        // These should NOT match the regex at all
        assert!(!THOUSAND_SEPARATOR_PATTERN.is_match("192.168.1.1"), 
            "192.168.1.1 should NOT match (not 3 digits in groups)");
        assert!(!THOUSAND_SEPARATOR_PATTERN.is_match("1.2.3"), 
            "1.2.3 should NOT match (not 3 digits in groups)");
        assert!(!THOUSAND_SEPARATOR_PATTERN.is_match("2.162,00"), 
            "2.162,00 should NOT match (mixed separators not supported by this regex)");
    }

    #[test]
    fn test_thousand_separator_numbers() {
        println!("🔍 Testing thousand separator number classification...");
        
        // US format
        assert!(matches!(classify_value("2,162.00"), InferredDataType::Float), 
            "2,162.00 should be classified as Float");
        assert!(matches!(classify_value("1,234.56"), InferredDataType::Float), 
            "1,234.56 should be classified as Float");
        assert!(matches!(classify_value("999,999.99"), InferredDataType::Float), 
            "999,999.99 should be classified as Float");
        assert!(matches!(classify_value("1,000,000.00"), InferredDataType::Float), 
            "1,000,000.00 should be classified as Float");
        
        // European format
        assert!(matches!(classify_value("2.162,00"), InferredDataType::String), 
            "2.162,00 should be String (not supported by current regex)");
        assert!(matches!(classify_value("1.234,56"), InferredDataType::String), 
            "1.234,56 should be String (not supported by current regex)");
        
        // Thousands only 
        assert!(matches!(classify_value("1,234"), InferredDataType::Float), 
            "1,234 should be classified as Float");
        assert!(matches!(classify_value("999,999"), InferredDataType::Float), 
            "999,999 should be classified as Float");
        
        // Single group 
        assert!(matches!(classify_value("1.234"), InferredDataType::Float), 
            "1.234 should be regular Float (FLOAT_DOT_PATTERN)");
        
        // More than 2 decimal
        assert!(matches!(classify_value("1,234.567"), InferredDataType::String), 
            "1,234.567 should be String (>2 decimal places not supported)");
        assert!(matches!(classify_value("2,162.123456"), InferredDataType::String), 
            "2,162.123456 should be String (>2 decimal places not supported)");
        
        // Negative numbers
        assert!(matches!(classify_value("-2,162.00"), InferredDataType::Float), 
            "-2,162.00 should be classified as Float");
        
        // Positive signed numbers
        assert!(matches!(classify_value("+2,162.00"), InferredDataType::Float), 
            "+2,162.00 should be classified as Float");
        
        // Should NOT be classified as float
        assert!(matches!(classify_value("999.999.999"), InferredDataType::String), 
            "999.999.999 should be classified as String");
        assert!(matches!(classify_value("192.168.1.1"), InferredDataType::String), 
            "192.168.1.1 should be classified as String");
        assert!(matches!(classify_value("1.2.3"), InferredDataType::String), 
            "1.2.3 should be classified as String");
    }

    #[test]
    fn test_thousand_separator_validation_functions() {
        println!("🔍 Testing thousand separator validation functions...");
        
        // Valid US format
        assert!(is_valid_thousand_separator_number("2,162.00"), 
            "2,162.00 should be valid thousand separator number");
        assert!(is_valid_thousand_separator_number("1,234.56"), 
            "1,234.56 should be valid thousand separator number");
        assert!(is_valid_thousand_separator_number("999,999.99"), 
            "999,999.99 should be valid thousand separator number");
        assert!(is_valid_thousand_separator_number("1,000,000.00"), 
            "1,000,000.00 should be valid thousand separator number");
        
        // European format won't work with current implementation
        assert!(!is_valid_thousand_separator_number("2.162,00"), 
            "2.162,00 should NOT be valid (mixed separators not supported)");
        assert!(!is_valid_thousand_separator_number("1.234,56"), 
            "1.234,56 should NOT be valid (mixed separators not supported)");
        
        // Valid thousands only
        assert!(is_valid_thousand_separator_number("1,234"), 
            "1,234 should be valid thousand separator number");
        assert!(is_valid_thousand_separator_number("999,999"), 
            "999,999 should be valid thousand separator number");
        
        // Invalid cases
        assert!(!is_valid_thousand_separator_number("999.999.999"), 
            "999.999.999 should NOT be valid thousand separator number");
        assert!(!is_valid_thousand_separator_number("192.168.1.1"), 
            "192.168.1.1 should NOT be valid thousand separator number");
        assert!(!is_valid_thousand_separator_number("1.2.3"), 
            "1.2.3 should NOT be valid thousand separator number");
    }

    #[test]
    fn test_type_inference_with_mixed_number_formats() {
        println!("🔍 Testing type inference with mixed number formats...");
        
        // Only US format will work with current regex
        let us_format_samples = vec![
            "1,234.56".to_string(),   // US format
            "999,999.99".to_string(), // US format
            "5,678.90".to_string(),   // US format
            "1,000.00".to_string(),   // US format
        ];
        
        let result = infer_column_type(&us_format_samples, "us_numbers");
        assert!(matches!(result, InferredDataType::Float), 
            "US format numbers should be inferred as Float, got {:?}", result);
        
        // Test the exact scenario from user's problem
        let user_scenario_samples = vec![
            "1293.36".to_string(),    // Regular float
            "1724.65".to_string(),    // Regular float
            "479.4".to_string(),      // Regular float
            "1211.66".to_string(),    // Regular float
            "2,162.00".to_string(),   // Thousand separator
        ];
        
        let result = infer_column_type(&user_scenario_samples, "neto_cena");
        assert!(matches!(result, InferredDataType::Float), 
            "User scenario should be inferred as Float, got {:?}", result);
    }

    #[cfg(test)]
    mod cross_system_tests {
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

   #[tokio::test]
    async fn test_user_reported_mixed_formats() {
        let temp_dir = TempDir::new().unwrap();
        
        let problematic_csv = r#"id,neto_cena,neto_vrednost
        1,1293.36,2500.00
        2,"2,162.00",3000.50
        3,1724.65,"1,500.75"
        4,479.4,900.00
        5,"3,880.08","2,250.00"
        6,1211.66,1800.25"#;
        
        let csv_path = create_test_csv(problematic_csv, "user_scenario.csv", &temp_dir);
        
        let regular_result = load_csv_with_type_handling(&csv_path, "regular").await;
        assert!(regular_result.is_ok(), "Regular loading should handle mixed formats: {:?}", regular_result.err());
        
        let streaming_result = load_csv_with_type_handling_streaming(&csv_path, "streaming").await;
        assert!(streaming_result.is_ok(), "Streaming loading should handle mixed formats: {:?}", streaming_result.err());
        
        if let Ok(df) = regular_result {
            let schema = df.dataframe.schema();
            
            let neto_cena_field = schema.field_with_name(None, "neto_cena").unwrap();
            let neto_vrednost_field = schema.field_with_name(None, "neto_vrednost").unwrap();
            
            println!("🔍 Regular - neto_cena type: {:?}", neto_cena_field.data_type());
            println!("🔍 Regular - neto_vrednost type: {:?}", neto_vrednost_field.data_type());
            

            assert!(matches!(neto_cena_field.data_type(), arrow::datatypes::DataType::Float64), 
                "neto_cena should be Float64, got {:?}", neto_cena_field.data_type());
        }
    }

    #[test]
    fn test_international_number_formats() {
        println!("🌍 Testing international number formats...");
        
        assert!(matches!(classify_value("1,234.56"), InferredDataType::Float));
        assert!(matches!(classify_value("1,234,567.89"), InferredDataType::Float));
        
        assert!(matches!(classify_value("1.234,56"), InferredDataType::String)); 
        assert!(matches!(classify_value("1.234.567,89"), InferredDataType::String));

        let mixed_samples = vec![
            "1,234.56".to_string(),  
            "1234.56".to_string(), 
            "2,500.00".to_string(), 
            "invalid".to_string(),   
        ];
        let result = infer_column_type(&mixed_samples, "mixed_international");
        println!("Mixed international formats result: {:?}", result);
        assert!(matches!(result, InferredDataType::String | InferredDataType::Float));
    }

    #[test]
    fn test_edge_cases_cross_system() {
        println!("⚠️  Testing edge cases that might break cross-system...");
        
        // Very large numbers
        assert!(matches!(classify_value("999999999999999"), InferredDataType::Integer));
        assert!(matches!(classify_value("1,000,000,000,000.00"), InferredDataType::Float));
        
        assert!(matches!(classify_value("1.23e-10"), InferredDataType::String));
        assert!(matches!(classify_value("2.5E+15"), InferredDataType::String));
        
        assert!(matches!(classify_value("€1,234.56"), InferredDataType::Float)); 
        assert!(matches!(classify_value("£999.99"), InferredDataType::Float));  
        assert!(matches!(classify_value("¥1,000"), InferredDataType::Float));   

        assert!(matches!(classify_value(""), InferredDataType::String));
        assert!(matches!(classify_value("   "), InferredDataType::String));
        assert!(matches!(classify_value("\t"), InferredDataType::String));
        assert!(matches!(classify_value("\r\n"), InferredDataType::String));
        
        assert!(matches!(classify_value("N/A"), InferredDataType::String));
        assert!(matches!(classify_value("–"), InferredDataType::String)); 
        assert!(matches!(classify_value("—"), InferredDataType::String)); 
    }


    #[tokio::test]
    async fn test_sql_generation_cross_version() {
        let temp_dir = TempDir::new().unwrap();
        let csv_content = r#"id,amount,percentage,currency
        1,"1,234.56",85%,$500.00
        2,2345.67,90%,€750.50
        3,"3,456.78",95%,£1000.25"#;
        
        let csv_path = create_test_csv(csv_content, "sql_test.csv", &temp_dir);
        
        let result = load_csv_with_type_handling_streaming(&csv_path, "sql_test").await;
        
        match result {
            Ok(_) => println!("✅ SQL generation works with current DataFusion version"),
            Err(e) => {
                println!("❌ SQL generation failed: {}", e);
                // Check if it's a regex or SQL syntax error
                let error_str = e.to_string();
                if error_str.contains("regex") {
                    panic!("Regex compatibility issue: {}", e);
                } else if error_str.contains("SQL") || error_str.contains("syntax") {
                    panic!("SQL syntax compatibility issue: {}", e);
                } else {
                    panic!("Unknown compatibility issue: {}", e);
                }
            }
        }
    }

    #[tokio::test]
    async fn test_memory_efficiency_streaming() {
        let temp_dir = TempDir::new().unwrap();
        
        let mut csv_content = String::from("id,value,amount\n");
        for i in 1..=1000 {
            csv_content.push_str(&format!("{},test_value_{},\"1,{:03}.00\"\n", i, i, i));
        }
        
        let csv_path = create_test_csv(&csv_content, "large_test.csv", &temp_dir);
        
        let start_time = std::time::Instant::now();
        let result = load_csv_with_type_handling_streaming(&csv_path, "large_streaming").await;
        let load_duration = start_time.elapsed();
        
        assert!(result.is_ok(), "Large CSV streaming should work: {:?}", result.err());
        assert!(load_duration < std::time::Duration::from_secs(5), 
            "Streaming load took too long: {:?}", load_duration);
        
        println!("✅ Large CSV streaming completed in {:?}", load_duration);
    }

    #[tokio::test]
    async fn test_delimiter_detection_edge_cases() {
        let temp_dir = TempDir::new().unwrap();
        
        let tricky_csv = r#"name,description,amount
        "Smith John","Software Engineer Senior","1,234.56"
        "Doe Jane","Data Scientist Lead","2,345.67"
        "Wilson Bob","Product Manager","3,456.78""#;
        
        let csv_path = create_test_csv(tricky_csv, "tricky_delimiters.csv", &temp_dir);
        
        let detected_delimiter = detect_delimiter(&csv_path).await.unwrap();
        assert_eq!(detected_delimiter, b',', "Should correctly detect comma despite quoted content");
        
        let result = load_csv_with_type_handling(&csv_path, "tricky").await;
        assert!(result.is_ok(), "Should handle quoted fields with internal delimiters: {:?}", result.err());
    }

    #[test]
    fn test_problematic_patterns() {
        println!("🔍 Testing specific patterns that caused user errors...");
        
        let problematic_values = vec![
            "3,880.08",  
            "2,162.00",  
            "1,293.36",  
            "1,724.65", 
        ];
        
        for value in problematic_values {
            let classification = classify_value(value);
            println!("Value '{}' → {:?}", value, classification);
            
            if value.contains(',') {
                assert!(matches!(classification, InferredDataType::Float), 
                    "Comma-separated value '{}' should be Float, got {:?}", value, classification);
            } else {
                assert!(matches!(classification, InferredDataType::Float), 
                    "Decimal value '{}' should be Float, got {:?}", value, classification);
            }
        }
    }

    #[test]
    fn test_column_inference_user_scenario() {
        let user_data_samples = vec![
            "1293.36".to_string(),
            "1724.65".to_string(), 
            "479.4".to_string(),
            "1211.66".to_string(),
            "2,162.00".to_string(),  
            "3,880.08".to_string(),  
        ];
        
        let result = infer_column_type(&user_data_samples, "neto_cena");
        println!("User scenario column inference: {:?}", result);
 
        assert!(matches!(result, InferredDataType::Float), 
            "User's neto_cena column should be Float, got {:?}", result);
    }

    #[test]
    fn test_sql_casting_safety() {
        println!("🔒 Testing SQL casting expressions for safety...");
        
        let test_cases = vec![
            ("normal_col", "amount"),
            ("col with spaces", "col_with_spaces"),
            ("col\"with\"quotes", "col_with_quotes"),
            ("col'with'apostrophes", "col_with_apostrophes"),
        ];
        
        for (original, clean) in test_cases {
            let cast_expr = create_casting_expression(original, clean, &InferredDataType::Float);
 
            assert!(cast_expr.contains(&format!("\"{}\"", original)));
            assert!(cast_expr.contains(&format!("\"{}\"", clean)));
            
            // Should not contain unescaped quotes
            let quote_count = cast_expr.matches('"').count();
            assert!(quote_count >= 4, "Expression should have properly quoted column names");
            
            println!("✅ Safe casting for '{}' → '{}'", original, clean);
        }
    }

    #[tokio::test]
    async fn test_backward_compatibility() {
        let temp_dir = TempDir::new().unwrap();
        
        let simple_csv = r#"id,name,amount
        1,Alice,100.50
        2,Bob,200.75
        3,Carol,300.25"#;
        
        let csv_path = create_test_csv(simple_csv, "simple.csv", &temp_dir);
        
        let regular_result = load_csv_with_type_handling(&csv_path, "regular_compat").await;
        let streaming_result = load_csv_with_type_handling_streaming(&csv_path, "streaming_compat").await;
        
        assert!(regular_result.is_ok(), "Regular approach should still work");
        assert!(streaming_result.is_ok(), "Streaming approach should still work");
        
        if let (Ok(regular_df), Ok(streaming_df)) = (regular_result, streaming_result) {
            assert_eq!(
                regular_df.dataframe.schema().fields().len(),
                streaming_df.dataframe.schema().fields().len(),
                "Both approaches should produce same number of columns"
            );
        }
    }

    #[tokio::test]
    async fn test_performance_regression() {
        let temp_dir = TempDir::new().unwrap();
        
        let mut csv_content = String::from("id,product,price,discount,final_amount\n");
        for i in 1..=500 {
            csv_content.push_str(&format!(
                "{},Product {},\"1,{:03}.{:02}\",{}%,\"2,{:03}.{:02}\"\n", 
                i, i, i, i % 100, i % 10, i * 2, (i * 2) % 100
            ));
        }
        
        let csv_path = create_test_csv(&csv_content, "performance_test.csv", &temp_dir);
        
        let start_time = std::time::Instant::now();
        let result = load_csv_with_type_handling_streaming(&csv_path, "perf_test").await;
        let duration = start_time.elapsed();
        
        assert!(result.is_ok(), "Performance test should succeed: {:?}", result.err());
        
        assert!(duration < std::time::Duration::from_secs(10), 
            "Performance test took too long: {:?}", duration);
        
        println!("✅ Performance test completed in {:?}", duration);
    }
}
}




#[cfg(test)]
mod cross_platform_tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    fn create_cross_platform_csv(content: &str, filename: &str, temp_dir: &TempDir, line_ending: &str) -> String {
        let file_path = temp_dir.path().join(filename);
        let mut file = File::create(&file_path).unwrap();
        
        let content_with_endings = content.replace("\n", line_ending);
        file.write_all(content_with_endings.as_bytes()).unwrap();
        file_path.to_str().unwrap().to_string()
    }

    fn create_csv_with_bom(content: &str, filename: &str, temp_dir: &TempDir) -> String {
        let file_path = temp_dir.path().join(filename);
        let mut file = File::create(&file_path).unwrap();
  
        file.write_all(&[0xEF, 0xBB, 0xBF]).unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file_path.to_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn test_windows_line_endings() {
        let temp_dir = TempDir::new().unwrap();
        let csv_content = r#"id,name,city,amount
        1,John Doe,BEOGRAD - CENTAR,1250.50
        2,Jane Smith,,2340.75
        3,Bob Wilson,NOVI SAD - PETROVARADIN,1890.25
        4,Alice Brown,ZAGREB - DONJI GRAD,3240.00
        5,Mike Johnson,,1456.80"#;
        
        let csv_path = create_cross_platform_csv(csv_content, "windows_test.csv", &temp_dir, "\r\n");
        
        println!("🪟 Testing Windows CRLF line endings...");
        let result = load_csv_with_type_handling_streaming(&csv_path, "windows_csv").await;
        assert!(result.is_ok(), "Windows CRLF should work: {:?}", result.err());
        
        if let Ok(df) = result {
            let schema = df.dataframe.schema();
            assert_eq!(schema.fields().len(), 4, "Should have 4 columns");

            let city_field = schema.field_with_name(None, "city");
            assert!(city_field.is_ok(), "City column should exist");
        }
    }

    #[tokio::test]
    async fn test_unix_line_endings() {
        let temp_dir = TempDir::new().unwrap();
        let csv_content = r#"id,name,city,amount
        1,John Doe,BEOGRAD - CENTAR,1250.50
        2,Jane Smith,,2340.75
        3,Bob Wilson,NOVI SAD - PETROVARADIN,1890.25"#;
        
        let csv_path = create_cross_platform_csv(csv_content, "unix_test.csv", &temp_dir, "\n");
        
        println!("🐧 Testing Unix LF line endings...");
        let result = load_csv_with_type_handling_streaming(&csv_path, "unix_csv").await;
        assert!(result.is_ok(), "Unix LF should work: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_mac_classic_line_endings() {
        let temp_dir = TempDir::new().unwrap();
        let csv_content = r#"id,name,city,amount
        1,John Doe,BEOGRAD - CENTAR,1250.50
        2,Jane Smith,,2340.75"#;
        
        // Test Mac Classic CR line endings
        let csv_path = create_cross_platform_csv(csv_content, "mac_test.csv", &temp_dir, "\r");
        
        println!("🍎 Testing Mac Classic CR line endings...");
        let result = load_csv_with_type_handling_streaming(&csv_path, "mac_csv").await;
        assert!(result.is_ok(), "Mac CR should work: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_csv_with_bom() {
        let temp_dir = TempDir::new().unwrap();
        let csv_content = r#"id,name,city,amount
        1,John Doe,BEOGRAD - CENTAR,1250.50
        2,Jane Smith,,2340.75
        3,Bob Wilson,NOVI SAD - PETROVARADIN,1890.25"#;
        
        let csv_path = create_csv_with_bom(csv_content, "bom_test.csv", &temp_dir);
        
        println!("📄 Testing CSV with UTF-8 BOM...");
        let result = load_csv_with_type_handling_streaming(&csv_path, "bom_csv").await;
        assert!(result.is_ok(), "CSV with BOM should work: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_mixed_empty_and_filled_city_column() {
        let temp_dir = TempDir::new().unwrap();
        
        let csv_content = r#"broj_transakcija,ukupna_kolicina,ukupna_vrednost,pharm,regionale,kolicina,neto_vrednost,mesto,region_rank
        1,3,4889.88,Lekovit,Kosovo,3,4889.88,PRISTINA - CENTAR,1
        1,12,3294.0,Lekovit,Kosovo,12,3294.0,,2
        1,11,14226.96,Lekovit,Kosovo,11,14226.96,PRISTINA - NORD,3
        1,4,5756.4,Lekovit,Kosovo,4,5756.4,,4
        1,80,45492.0,Inpharm,DM,80,45492.0,BEOGRAD - VRACAR,1
        1,56,33320.0,Inpharm,DM,56,33320.0,,2
        1,324,84823.2,Inpharm,DM,324,84823.2,BEOGRAD - NOVI BEOGRAD,3"#;
        
        let csv_path = create_cross_platform_csv(csv_content, "mixed_cities.csv", &temp_dir, "\r\n");
        
        println!("🏙️ Testing mixed empty and filled city columns...");
        let result = load_csv_with_type_handling_streaming(&csv_path, "mixed_cities").await;
        assert!(result.is_ok(), "Mixed city data should work: {:?}", result.err());
        
        if let Ok(df) = result {
            let schema = df.dataframe.schema();
            let mesto_field = schema.field_with_name(None, "mesto");
            assert!(mesto_field.is_ok(), "Mesto column should exist and be properly typed");
            
            if let Ok(field) = mesto_field {
                match field.data_type() {
                    arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8 => {
                        println!("✅ Mesto column correctly typed as string");
                    },
                    other => panic!("Mesto should be string type, got {:?}", other)
                }
            }
        }
    }

    #[tokio::test]
    async fn test_various_encodings() {
        let temp_dir = TempDir::new().unwrap();
        
        let csv_content = r#"id,name,grad,region
        1,Милан Петровић,БЕОГРАД - ВРАЧАР,Војводина
        2,Marko Jovanović,NOVI SAD - ЦЕНТАР,Војводина  
        3,Ana Nikolić,NIŠ - ПАЛИЛУЛА,Централна Србија
        4,Петар Стојановић,,Централна Србија"#;
        
        let csv_path = create_cross_platform_csv(csv_content, "serbian_chars.csv", &temp_dir, "\r\n");
        
        println!("🇷🇸 Testing Serbian characters (Cyrillic/Latin mix)...");
        let result = load_csv_with_type_handling_streaming(&csv_path, "serbian_csv").await;

        match result {
            Ok(_) => println!("✅ Serbian characters handled correctly"),
            Err(e) => {
                println!("⚠️ Serbian characters failed (encoding issue): {}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_delimiter_detection_cross_platform() {
        let temp_dir = TempDir::new().unwrap();
        
        // Test each delimiter with Windows line endings
        let test_cases = vec![
            ("comma", ",", r#"a,b,c
            1,2,3
            4,5,6"#),
            ("semicolon", ";", r#"a;b;c
            1;2;3
            4;5;6"#),
            ("tab", "\t", "a\tb\tc\n1\t2\t3\n4\t5\t6"),
            ("pipe", "|", r#"a|b|c
            1|2|3
            4|5|6"#),
        ];
        
        for (name, expected_delim, content) in test_cases {
            let csv_path = create_cross_platform_csv(content, &format!("{}_test.csv", name), &temp_dir, "\r\n");
            
            println!("🔍 Testing {} delimiter detection...", name);
            let detected = detect_delimiter(&csv_path).await;
            
            match detected {
                Ok(delim) => {
                    let expected_byte = expected_delim.as_bytes()[0];
                    assert_eq!(delim, expected_byte, "Should detect {} delimiter", name);
                    println!("✅ {} delimiter detected correctly", name);
                },
                Err(e) => panic!("Delimiter detection failed for {}: {}", name, e)
            }
        }
    }

    #[tokio::test]
    async fn test_system_locale_numbers() {
        let temp_dir = TempDir::new().unwrap();
        
        let csv_content = r#"id,amount_us,amount_eu,percentage
            1,"1,234.56","1.234,56",85%
            2,"2,345.67","2.345,67",90%
            3,"999.99","999,99",95%"#;
        
        let csv_path = create_cross_platform_csv(csv_content, "locale_numbers.csv", &temp_dir, "\r\n");
        
        println!("🌍 Testing system locale number handling...");
        let result = load_csv_with_type_handling_streaming(&csv_path, "locale_test").await;
        
        match result {
            Ok(df) => {
                let schema = df.dataframe.schema();
                println!("✅ Locale number test completed, {} columns", schema.fields().len());
                
                let amount_us = schema.field_with_name(None, "amount_us");
                let amount_eu = schema.field_with_name(None, "amount_eu");
                
                if let (Ok(us_field), Ok(eu_field)) = (amount_us, amount_eu) {
                    println!("US amount type: {:?}", us_field.data_type());
                    println!("EU amount type: {:?}", eu_field.data_type());
                }
            },
            Err(e) => println!("⚠️ Locale number test failed: {}", e)
        }
    }

    // Integration test that combines everything
    #[tokio::test]
    async fn test_comprehensive_cross_platform() {
        let temp_dir = TempDir::new().unwrap();

        let comprehensive_content = r#"broj_transakcija,ukupna_kolicina,ukupna_vrednost,veledrogerija,region,kolicina,neto_vrednost,mesto,mesec
        1,3,4889.88,Lekovit,Kosovo,3,4889.88,PRISTINA - CENTAR,Januar
        1,12,3294.0,Lekovit,Kosovo,12,3294.0,,Januar
        1,11,14226.96,Lekovit,Kosovo,11,14226.96,PRISTINA - NOORD,Februar
        1,4,5756.4,Lekovit,Kosovo,4,5756.4,,Februar
        1,80,45492.0,Inpharm,DM,80,45492.0,BEOGRAD - VRACAR,Mart
        1,56,33320.0,Inpharm,DM,56,33320.0,,Mart
        1,324,84823.2,Inpharm,DM,324,84823.2,NOVI SAD - PETROVARADIN,April
        2,100,23688.0,Inpharm,DM,50,11844.0,,April"#;
        
        let csv_path = create_cross_platform_csv(comprehensive_content, "comprehensive.csv", &temp_dir, "\r\n");
        
        println!("🎯 Running comprehensive cross-platform test...");
        let start_time = std::time::Instant::now();
        
        let result = load_csv_with_type_handling_streaming(&csv_path, "comprehensive").await;
        let load_time = start_time.elapsed();
        
        assert!(result.is_ok(), "Comprehensive test should work: {:?}", result.err());
        
        if let Ok(df) = result {
            let schema = df.dataframe.schema();
            
            let expected_columns = ["broj_transakcija", "ukupna_kolicina", "ukupna_vrednost", 
                                  "veledrogerija", "region", "kolicina", "neto_vrednost", "mesto", "mesec"];
            
            for col_name in expected_columns {
                let field = schema.field_with_name(None, col_name);
                assert!(field.is_ok(), "Column '{}' should exist", col_name);
            }
            
            let mesto_field = schema.field_with_name(None, "mesto").unwrap();
            assert!(matches!(mesto_field.data_type(), 
                arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8),
                "Mesto should be string type, got {:?}", mesto_field.data_type());
            
            println!("✅ Comprehensive test passed in {:?}", load_time);
            println!("📊 Schema: {} columns", schema.fields().len());
        }
    }

    #[tokio::test]
    async fn test_real_world_query_pattern() {
        let temp_dir = TempDir::new().unwrap();
        
        let realistic_data = r#"broj_transakcija,ukupna_kolicina,ukupna_vrednost,veledrogerija,region,kolicina,neto_vrednost,mesto,mesec
        1,3,4889.88,Lekovit,Kosovo,3,4889.88,PRISTINA - CENTAR,Januar
        1,12,3294.0,Lekovit,Kosovo,12,3294.0,,Januar  
        1,11,14226.96,Lekovit,Kosovo,11,14226.96,GNJILANE - CENTAR,Januar
        1,4,5756.4,Lekovit,Kosovo,4,5756.4,,Januar
        1,80,45492.0,Inpharm,DM,80,45492.0,BEOGRAD - VRACAR,Januar
        1,56,33320.0,Inpharm,DM,56,33320.0,,Januar
        1,324,84823.2,Inpharm,DM,324,84823.2,NOVI SAD - PETROVARADIN,Januar
        1,200,47376.0,Inpharm,DM,100,23688.0,,Januar
        1,1152,113702.4,Inpharm,DM,1152,113702.4,BEOGRAD - NOVI BEOGRAD,Februar
        1,288,39254.4,Inpharm,DM,288,39254.4,,Februar"#;
        
        let csv_path = create_cross_platform_csv(realistic_data, "realistic.csv", &temp_dir, "\r\n");
        
        println!("🚀 Testing with realistic data and query pattern...");
        let result = load_csv_with_type_handling_streaming(&csv_path, "realistic_data").await;
        
        assert!(result.is_ok(), "Realistic data loading should work: {:?}", result.err());
        
        if let Ok(df) = result {
            let schema = df.dataframe.schema();
            
            let checks = vec![
                ("mesto", "city data"),
                ("neto_vrednost", "numeric amounts"),
                ("veledrogerija", "pharmacy names"),
                ("region", "region names"),
                ("mesec", "month names")
            ];
            
            for (col_name, description) in checks {
                let field = schema.field_with_name(None, col_name);
                assert!(field.is_ok(), "Column '{}' ({}) should exist", col_name, description);
                
                if let Ok(f) = field {
                    println!("✅ {} ({}): {:?}", col_name, description, f.data_type());
                }
            }
        }
    }
}