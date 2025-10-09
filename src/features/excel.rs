use crate::prelude::*;

#[cfg(feature = "excel")]
use crate::array_value_to_json;

fn excel_date_to_naive_date(excel_date: f64) -> Option<NaiveDate> {
    // Excel uses 1900-01-01 as day 1, but has a leap year bug for 1900
    // So we use 1899-12-30 as the epoch
    let excel_epoch = NaiveDate::from_ymd_opt(1899, 12, 30)?;
    let days = excel_date.trunc() as i64;
    excel_epoch.checked_add_signed(Duration::days(days))
}

fn excel_datetime_to_naive_datetime(excel_datetime: f64) -> Option<NaiveDateTime> {
    // Get the date part
    let date = excel_date_to_naive_date(excel_datetime)?;
    
    // Get the time part (fractional part of the number)
    let time_fraction = excel_datetime.fract();
    let total_seconds = (time_fraction * 86400.0).round() as i64;
    let hours = (total_seconds / 3600) % 24;
    let minutes = (total_seconds / 60) % 60;
    let seconds = total_seconds % 60;
    
    let time = NaiveTime::from_hms_opt(hours as u32, minutes as u32, seconds as u32)?;
    Some(NaiveDateTime::new(date, time))
}

// Type detection for a column of values
#[derive(Debug, Clone)]
enum DetectedType {
    Integer,
    Float,
    Boolean,
    Date,
    DateTime,
    String,
}


    fn detect_column_type(values: &[Value]) -> DetectedType {
        let mut all_null = true;
        let mut can_be_int = true;
        let mut can_be_float = true;
        let mut can_be_bool = true;
        let mut can_be_date = true;
        let mut can_be_datetime = true;
        
        for value in values {
            match value {
                Value::Null => continue,
                _ => all_null = false,
            }
            
            match value {
                Value::Null => {},
                Value::Bool(_) => {
                    can_be_int = false;
                    can_be_float = false;
                    can_be_date = false;
                    can_be_datetime = false;
                },
                Value::Number(n) => {
                    can_be_bool = false;
                    can_be_date = false;
                    can_be_datetime = false;
                    if !n.is_i64() && !n.is_u64() {
                        can_be_int = false;
                    }
                },
                Value::String(s) => {
                    // Check if it's a datetime string
                    if !is_datetime_string(s) {
                        can_be_datetime = false;
                    }
                    // Check if it's a date string
                    if !is_date_string(s) {
                        can_be_date = false;
                    }
                    // Check if it's a numeric string
                    if s.parse::<i64>().is_err() {
                        can_be_int = false;
                    }
                    if s.parse::<f64>().is_err() {
                        can_be_float = false;
                    }
                    // Check if it's a boolean string
                    let lower = s.to_lowercase();
                    if lower != "true" && lower != "false" && lower != "1" && lower != "0" {
                        can_be_bool = false;
                    }
                },
                _ => {
                    can_be_int = false;
                    can_be_float = false;
                    can_be_bool = false;
                    can_be_date = false;
                    can_be_datetime = false;
                }
            }
        }
        
        // Determine the most appropriate type
        if all_null || (!can_be_int && !can_be_float && !can_be_bool && !can_be_date && !can_be_datetime) {
            DetectedType::String
        } else if can_be_bool {
            DetectedType::Boolean
        } else if can_be_int {
            DetectedType::Integer
        } else if can_be_float {
            DetectedType::Float
        } else if can_be_datetime {
            DetectedType::DateTime
        } else if can_be_date {
            DetectedType::Date
        } else {
            DetectedType::String
        }
    }

    // Check if a string looks like a date
    fn is_date_string(s: &str) -> bool {
        // Common date patterns
        let patterns = [
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%Y/%m/%d",
            "%d-%m-%Y",
            "%m-%d-%Y",
            "%d.%m.%Y",
            "%Y.%m.%d",
        ];
        
        for pattern in &patterns {
            if chrono::NaiveDate::parse_from_str(s, pattern).is_ok() {
                return true;
            }
        }
        false
    }

    fn is_datetime_string(s: &str) -> bool {

        let patterns = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%d/%m/%Y %H:%M:%S",
            "%m/%d/%Y %H:%M:%S",
            "%Y/%m/%d %H:%M:%S",
            "%d-%m-%Y %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S%.f",
        ];
        
        for pattern in &patterns {
            if chrono::NaiveDateTime::parse_from_str(s, pattern).is_ok() {
                return true;
            }
        }
        false
    }

    // Enhanced schema inference
    fn infer_schema_from_excel_data(
        data: &[HashMap<String, Value>],
        headers: &[String]
    ) -> Arc<Schema> {
        let mut fields = Vec::new();
        
        for header in headers {
            // Collect all values for this column
            let mut column_values = Vec::new();
            for row in data {
                if let Some(value) = row.get(header) {
                    column_values.push(value.clone());
                } else {
                    column_values.push(Value::Null);
                }
            }
            
            // Detect the column type
            let detected_type = detect_column_type(&column_values);
            
            // Map to Arrow DataType
            let data_type = match detected_type {
                DetectedType::Integer => ArrowDataType::Int64,
                DetectedType::Float => ArrowDataType::Float64,
                DetectedType::Boolean => ArrowDataType::Boolean,
                DetectedType::Date => ArrowDataType::Date32,
                DetectedType::DateTime => ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                DetectedType::String => ArrowDataType::Utf8,
            };
            
            fields.push(Field::new(header, data_type, true));
        }
        
        Arc::new(Schema::new(fields))
    }

    // Enhanced record batch builder
    fn build_record_batch_with_types(
        data: &[HashMap<String, Value>],
        schema: Arc<Schema>
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let mut columns: Vec<Arc<dyn Array>> = Vec::new();
        
        for field in schema.fields() {
            let column_name = field.name();
            let mut builder: Box<dyn ArrayBuilder> = match field.data_type() {
                ArrowDataType::Int64 => Box::new(Int64Builder::with_capacity(data.len())),
                ArrowDataType::Float64 => Box::new(Float64Builder::with_capacity(data.len())),
                ArrowDataType::Boolean => Box::new(BooleanBuilder::with_capacity(data.len())),
                ArrowDataType::Date32 => Box::new(Date32Builder::with_capacity(data.len())),
                ArrowDataType::Timestamp(TimeUnit::Millisecond, _) => Box::new(TimestampMillisecondBuilder::with_capacity(data.len())),
                _ => Box::new(StringBuilder::with_capacity(data.len(), data.len() * 20)),
            };
            
            for row in data {
                let value = row.get(column_name).unwrap_or(&Value::Null);
                
                match field.data_type() {
                    ArrowDataType::Int64 => {
                        let builder = builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
                        match value {
                            Value::Number(n) if n.is_i64() => builder.append_value(n.as_i64().unwrap()),
                            Value::Number(n) if n.is_u64() => builder.append_value(n.as_u64().unwrap() as i64),
                            Value::Number(n) if n.is_f64() => builder.append_value(n.as_f64().unwrap() as i64),
                            Value::String(s) => {
                                if let Ok(v) = s.parse::<i64>() {
                                    builder.append_value(v);
                                } else {
                                    builder.append_null();
                                }
                            },
                            _ => builder.append_null(),
                        }
                    },
                    ArrowDataType::Float64 => {
                        let builder = builder.as_any_mut().downcast_mut::<Float64Builder>().unwrap();
                        match value {
                            Value::Number(n) => {
                                if let Some(f) = n.as_f64() {
                                    builder.append_value(f);
                                } else if let Some(i) = n.as_i64() {
                                    builder.append_value(i as f64);
                                } else if let Some(u) = n.as_u64() {
                                    builder.append_value(u as f64);
                                } else {
                                    builder.append_null();
                                }
                            },
                            Value::String(s) => {
                                if let Ok(v) = s.parse::<f64>() {
                                    builder.append_value(v);
                                } else {
                                    builder.append_null();
                                }
                            },
                            _ => builder.append_null(),
                        }
                    },
                    ArrowDataType::Boolean => {
                        let builder = builder.as_any_mut().downcast_mut::<BooleanBuilder>().unwrap();
                        match value {
                            Value::Bool(b) => builder.append_value(*b),
                            Value::String(s) => {
                                let lower = s.to_lowercase();
                                if lower == "true" || lower == "1" {
                                    builder.append_value(true);
                                } else if lower == "false" || lower == "0" {
                                    builder.append_value(false);
                                } else {
                                    builder.append_null();
                                }
                            },
                            _ => builder.append_null(),
                        }
                    },
                    ArrowDataType::Date32 => {
                        let builder = builder.as_any_mut().downcast_mut::<Date32Builder>().unwrap();
                        match value {
                            Value::String(s) => {
                                // Try to parse the date string
                                let patterns = [
                                    "%Y-%m-%d",
                                    "%d/%m/%Y",
                                    "%m/%d/%Y",
                                    "%Y/%m/%d",
                                    "%d-%m-%Y",
                                    "%m-%d-%Y",
                                    "%d.%m.%Y",
                                    "%Y.%m.%d",
                                ];
                                
                                let mut parsed = false;
                                for pattern in &patterns {
                                    if let Ok(date) = chrono::NaiveDate::parse_from_str(s, pattern) {
                                        let days_since_epoch = (date - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
                                        builder.append_value(days_since_epoch);
                                        parsed = true;
                                        break;
                                    }
                                }
                                if !parsed {
                                    builder.append_null();
                                }
                            },
                            Value::Number(n) => {
                                if let Some(f) = n.as_f64() {
                                    if let Some(date) = excel_date_to_naive_date(f) {
                                        let days_since_epoch = (date - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
                                        builder.append_value(days_since_epoch);
                                    } else {
                                        builder.append_null();
                                    }
                                } else {
                                    builder.append_null();
                                }
                            },
                            _ => builder.append_null(),
                        }
                    },
                    ArrowDataType::Timestamp(TimeUnit::Millisecond, _) => {
                        let builder = builder.as_any_mut().downcast_mut::<TimestampMillisecondBuilder>().unwrap();
                        match value {
                            Value::String(s) => {
                                // Try to parse the datetime string
                                let patterns = [
                                    "%Y-%m-%d %H:%M:%S",
                                    "%Y-%m-%d %H:%M",
                                    "%d/%m/%Y %H:%M:%S",
                                    "%m/%d/%Y %H:%M:%S",
                                    "%Y/%m/%d %H:%M:%S",
                                    "%d-%m-%Y %H:%M:%S",
                                    "%Y-%m-%dT%H:%M:%S",
                                    "%Y-%m-%d %H:%M:%S%.f",
                                ];
                                
                                let mut parsed = false;
                                for pattern in &patterns {
                                    if let Ok(datetime) = chrono::NaiveDateTime::parse_from_str(s, pattern) {
                                        builder.append_value(datetime.and_utc().timestamp_millis());
                                        parsed = true;
                                        break;
                                    }
                                }
                                if !parsed {
                                    builder.append_null();
                                }
                            },
                            Value::Number(n) => {
                                if let Some(f) = n.as_f64() {
                                    if let Some(datetime) = excel_datetime_to_naive_datetime(f) {
                                        builder.append_value(datetime.and_utc().timestamp_millis());
                                    } else {
                                        builder.append_null();
                                    }
                                } else {
                                    builder.append_null();
                                }
                            },
                            _ => builder.append_null(),
                        }
                    },
                    _ => {
                        // Default to string
                        let builder = builder.as_any_mut().downcast_mut::<StringBuilder>().unwrap();
                            match value {
                                Value::Null => builder.append_null(),
                                Value::String(s) => builder.append_value(s),
                                v => builder.append_value(&v.to_string()),
                            }
                    }
                }
            }
            
            columns.push(builder.finish());
        }
        
        RecordBatch::try_new(schema, columns).map_err(|e| e.into())
    }

    /// Load an Excel file (XLSX) into a CustomDataFrame
    pub fn load_excel<'a>(
        file_path: &'a str,
        alias: &'a str,
    ) -> BoxFuture<'a, ElusionResult<AliasedDataFrame>> {
        Box::pin(async move {
            println!("🔄 Starting Excel loading process with enhanced type detection...");

            if !LocalPath::new(file_path).exists() {
                return Err(ElusionError::WriteError {
                    path: file_path.to_string(),
                    operation: "read".to_string(),
                    reason: "File not found".to_string(),
                    suggestion: "💡 Check if the file path is correct".to_string(),
                });
            }

            let read_start = std::time::Instant::now();

            if let Ok(metadata) = std::fs::metadata(file_path) {
                let file_size = metadata.len();
                println!("📏 File size: {} bytes ({:.2} MB)", file_size, file_size as f64 / 1024.0 / 1024.0);
            }

            let mut workbook: Xlsx<_> = open_workbook(file_path)
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: "Excel Reading".to_string(),
                    reason: format!("Failed to open Excel file: {}", e),
                    suggestion: "💡 Ensure the file is a valid Excel (XLSX) file and not corrupted".to_string(),
                })?;
            
            let sheet_names = workbook.sheet_names().to_owned();
            if sheet_names.is_empty() {
                return Err(ElusionError::InvalidOperation {
                    operation: "Excel Reading".to_string(),
                    reason: "Excel file does not contain any sheets".to_string(),
                    suggestion: "💡 Ensure the Excel file contains at least one sheet with data".to_string(),
                });
            }
            
            let sheet_name = &sheet_names[0];
            println!("📋 Found {} sheet(s), processing: '{}'", sheet_names.len(), sheet_name);

            let range = workbook.worksheet_range(sheet_name)
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: "Excel Reading".to_string(),
                    reason: format!("Failed to read sheet '{}': {}", sheet_name, e),
                    suggestion: "💡 The sheet may be corrupted or empty".to_string(),
                })?;
            
            if range.is_empty() {
                return Err(ElusionError::InvalidOperation {
                    operation: "Excel Reading".to_string(),
                    reason: format!("Sheet '{}' is empty", sheet_name),
                    suggestion: "💡 Ensure the sheet contains data".to_string(),
                });
            }

            // Process headers (same as before)
            let headers_row = range.rows().next().ok_or_else(|| ElusionError::InvalidOperation {
                operation: "Excel Reading".to_string(),
                reason: "Failed to read headers from Excel file".to_string(),
                suggestion: "💡 Ensure the first row contains column headers".to_string(),
            })?;
            
            let headers: Vec<String> = headers_row.iter()
                .enumerate()
                .map(|(column_index, cell)| {
                    let header = cell.to_string().trim().to_string();
                    if header.is_empty() {
                        format!("Column_{}", column_index)
                    } else {
                        let sanitized = header.replace(' ', "_")
                            .replace(|c: char| !c.is_alphanumeric() && c != '_', "_");
                        
                        if sanitized.chars().next().map_or(true, |c| !c.is_alphabetic()) {
                            format!("col_{}", sanitized)
                        } else {
                            sanitized
                        }
                    }
                })
                .collect();
            
            // Handle duplicate headers
            let final_headers = {
                let mut seen = HashSet::new();
                let mut unique_headers = Vec::with_capacity(headers.len());
                
                for header in headers {
                    let mut unique_header = header.clone();
                    let mut counter = 1;
                    
                    while !seen.insert(unique_header.clone()) {
                        unique_header = format!("{}_{}", header, counter);
                        counter += 1;
                    }
                    
                    unique_headers.push(unique_header);
                }
                
                unique_headers
            };

            println!("🔄 Converting Excel data with type preservation...");
            
            // Process data rows with better type handling
            let mut all_data: Vec<HashMap<String, Value>> = Vec::new();
            
            for row in range.rows().skip(1) {
                let mut row_map = HashMap::new();
                
                for (i, cell) in row.iter().enumerate() {
                    if i >= final_headers.len() {
                        continue;
                    }
                    
                    // Enhanced type conversion
                    let value = match cell {
                        CalamineDataType::Empty => Value::Null,
                        
                        CalamineDataType::String(s) => {
                            // Try to parse as number first
                            if let Ok(i) = s.parse::<i64>() {
                                Value::Number(serde_json::Number::from(i))
                            } else if let Ok(f) = s.parse::<f64>() {
                                serde_json::Number::from_f64(f)
                                    .map(Value::Number)
                                    .unwrap_or_else(|| Value::String(s.clone()))
                            } else {
                                Value::String(s.clone())
                            }
                        },
                        
                        CalamineDataType::Float(f) => {
                            // Check if it's actually an integer
                            if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                                Value::Number(serde_json::Number::from(f.round() as i64))
                            } else {
                                serde_json::Number::from_f64(*f)
                                    .map(Value::Number)
                                    .unwrap_or(Value::Null)
                            }
                        },
                        
                        CalamineDataType::Int(i) => Value::Number((*i).into()),
                        
                        CalamineDataType::Bool(b) => Value::Bool(*b),
                        
                        CalamineDataType::DateTime(dt) => {
                            let fractional = dt.fract();
                            if fractional > 0.0 {
                                excel_datetime_to_naive_datetime(*dt)
                                    .map(|ndt| Value::String(ndt.format("%Y-%m-%d %H:%M:%S").to_string()))
                                    .unwrap_or(Value::Null)
                            } else {
                                excel_date_to_naive_date(*dt)
                                    .map(|nd| Value::String(nd.format("%Y-%m-%d").to_string()))
                                    .unwrap_or(Value::Null)
                            }
                        },
                        
                        CalamineDataType::Duration(d) => {
                            let hours = (d * 24.0) as i64;
                            let minutes = ((d * 24.0 * 60.0) % 60.0) as i64;
                            let seconds = ((d * 24.0 * 60.0 * 60.0) % 60.0) as i64;
                            Value::String(format!("{}h {}m {}s", hours, minutes, seconds))
                        },
                        
                        CalamineDataType::DateTimeIso(dt_iso) => Value::String(dt_iso.clone()),
                        
                        CalamineDataType::DurationIso(d_iso) => Value::String(d_iso.clone()),
                        
                        CalamineDataType::Error(_) => Value::Null,
                    };
                    
                    row_map.insert(final_headers[i].clone(), value);
                }
                
                all_data.push(row_map);
            }
            
            if all_data.is_empty() {
                return Err(ElusionError::InvalidOperation {
                    operation: "Excel Processing".to_string(),
                    reason: "No valid data rows found in Excel file".to_string(),
                    suggestion: "💡 Ensure the Excel file contains data rows after the header row".to_string(),
                });
            }

            let process_elapsed = read_start.elapsed();
            println!("✅ Data conversion completed: {} rows processed in {:?}", all_data.len(), process_elapsed);

            // Use the enhanced schema inference
            println!("🧠 Inferring schema with type detection...");
            let schema_start = std::time::Instant::now();
            let schema = infer_schema_from_excel_data(&all_data, &final_headers);
            let schema_elapsed = schema_start.elapsed();

            // Use the enhanced record batch builder
            println!("🔧 Building record batch with proper types...");
            let table_start = std::time::Instant::now();
            
            let record_batch = build_record_batch_with_types(&all_data, schema.clone())
                .map_err(|e| ElusionError::SchemaError {
                    message: format!("Failed to build RecordBatch: {}", e),
                    schema: Some(schema.to_string()),
                    suggestion: "💡 Check if the Excel data structure is consistent".to_string(),
                })?;
            
            let ctx = SessionContext::new();
            let mem_table = MemTable::try_new(schema.clone(), vec![vec![record_batch]])
                .map_err(|e| ElusionError::SchemaError {
                    message: format!("Failed to create MemTable: {}", e),
                    schema: Some(schema.to_string()),
                    suggestion: "💡 Verify data types and schema compatibility".to_string(),
                })?;
            
            ctx.register_table(alias, Arc::new(mem_table))
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: "Table Registration".to_string(),
                    reason: format!("Failed to register table: {}", e),
                    suggestion: "💡 Try using a different alias name".to_string(),
                })?;
            
            let df = ctx.table(alias).await
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: "Table Creation".to_string(),
                    reason: format!("Failed to create table: {}", e),
                    suggestion: "💡 Verify table creation parameters".to_string(),
                })?;

            let table_elapsed = table_start.elapsed();
            let total_elapsed = read_start.elapsed();
            
            println!("✅ Schema inferred and table created in {:?}", schema_elapsed + table_elapsed);
            println!("🎉 Excel DataFrame loading completed successfully in {:?} for table alias: '{}'", 
                total_elapsed, alias);
            
            // println!("📊 Type detection summary:");
            // for field in schema.fields() {
            //     println!("   - {} ({})", field.name(), field.data_type());
            // }
            
            Ok(AliasedDataFrame {
                dataframe: df,
                alias: alias.to_string(),
            })
        })
    }


    // Implement From<XlsxError> for ElusionError
    #[cfg(feature = "excel")]
    impl From<rust_xlsxwriter::XlsxError> for ElusionError {
        fn from(error: rust_xlsxwriter::XlsxError) -> Self {
            ElusionError::Custom(format!("Excel writing error: {}", error))
        }
    }

    /// Writes the DataFrame to an Excel file with formatting options
    #[cfg(feature = "excel")]
    pub async fn write_to_excel_impl(
        df: &CustomDataFrame,
        path: &str,
        sheet_name: Option<&str>
    ) -> ElusionResult<()> {

        if !path.ends_with(".xlsx") {
            return Err(ElusionError::Custom(
                "❌ Invalid file extension. Excel files must end with '.xlsx'".to_string()
            ));
        }

        if let Some(parent) = LocalPath::new(path).parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).map_err(|e| ElusionError::WriteError {
                    path: parent.display().to_string(),
                    operation: "create_directory".to_string(),
                    reason: e.to_string(),
                    suggestion: "💡 Check if you have permissions to create directories".to_string(),
                })?;
            }
        }
    
        if fs::metadata(path).is_ok() {
            fs::remove_file(path).map_err(|e| 
                ElusionError::WriteError {
                    path: path.to_string(),
                    operation: "overwrite".to_string(),
                    reason: format!("❌ Failed to delete existing file: {}", e),
                    suggestion: "💡 Check file permissions and ensure no other process is using the file".to_string(),
                }
            )?;
        }
    
        let batches = df.df.clone().collect().await.map_err(|e| 
            ElusionError::InvalidOperation {
                operation: "Data Collection".to_string(),
                reason: format!("Failed to collect DataFrame: {}", e),
                suggestion: "💡 Verify DataFrame is not empty and contains valid data".to_string(),
            }
        )?;
    
        if batches.is_empty() {
            return Err(ElusionError::InvalidOperation {
                operation: "Excel Writing".to_string(),
                reason: "No data to write".to_string(),
                suggestion: "💡 Ensure DataFrame contains data before writing".to_string(),
            });
        }
    
        let mut workbook = Workbook::new();
    
        let sheet_name = sheet_name.unwrap_or("Sheet1");
        let worksheet = workbook.add_worksheet().set_name(sheet_name).map_err(|e| ElusionError::WriteError {
            path: path.to_string(),
            operation: "worksheet_create".to_string(),
            reason: format!("Failed to create worksheet: {}", e),
            suggestion: "💡 Invalid sheet name or workbook error".to_string(),
        })?;
    
        let header_format = Format::new()
            .set_bold()
            .set_font_color(0xFFFFFF)
            .set_background_color(0x329A52)
            .set_align(rust_xlsxwriter::FormatAlign::Center);
        
        let date_format = Format::new()
            .set_num_format("yyyy-mm-dd");
        
        let schema = batches[0].schema();
        let column_count = schema.fields().len();
        
        for (col_idx, field) in schema.fields().iter().enumerate() {
            worksheet.write_string_with_format(0, col_idx as u16, field.name(), &header_format)
                .map_err(|e| ElusionError::WriteError {
                    path: path.to_string(),
                    operation: "write_header".to_string(),
                    reason: format!("Failed to write column header '{}': {}", field.name(), e),
                    suggestion: "💡 Check if the column name contains invalid characters".to_string(),
                })?;
                
            let width = (field.name().len() as f64 * 1.2).max(10.0).min(50.0);
            worksheet.set_column_width(col_idx as u16, width)
                .map_err(|e| ElusionError::WriteError {
                    path: path.to_string(),
                    operation: "set_column_width".to_string(),
                    reason: format!("Failed to set column width: {}", e),
                    suggestion: "💡 Failed to set column width".to_string(),
                })?;
        }
        
        // Write data rows
        let mut row_idx = 1; // Start from row 1 (after headers)
        
        for batch in batches.iter() {
            let row_count = batch.num_rows();
            
            for r in 0..row_count {
                for (c, field) in schema.fields().iter().enumerate() {
                    let col = batch.column(c);
                    
                    if col.is_null(r) {
                        // skip null values show as empty
                        continue;
                    }
                    
                    match field.data_type() {
                        ArrowDataType::Int8 | ArrowDataType::Int16 | ArrowDataType::Int32 | ArrowDataType::Int64 => {
                            let value = match field.data_type() {
                                ArrowDataType::Int8 => {
                                    if let Some(array) = col.as_any().downcast_ref::<Int8Array>() {
                                        if array.is_null(r) { 0.0 } else { array.value(r) as f64 }
                                    } else { 0.0 }
                                },
                                ArrowDataType::Int16 => {
                                    if let Some(array) = col.as_any().downcast_ref::<Int16Array>() {
                                        if array.is_null(r) { 0.0 } else { array.value(r) as f64 }
                                    } else { 0.0 }
                                },
                                ArrowDataType::Int32 => {
                                    if let Some(array) = col.as_any().downcast_ref::<Int32Array>() {
                                        if array.is_null(r) { 0.0 } else { array.value(r) as f64 }
                                    } else { 0.0 }
                                },
                                ArrowDataType::Int64 => {
                                    if let Some(array) = col.as_any().downcast_ref::<Int64Array>() {
                                        if array.is_null(r) { 0.0 } else { array.value(r) as f64 }
                                    } else { 0.0 }
                                },
                                _ => 0.0 
                            };
                            
                            worksheet.write_number(row_idx, c as u16, value)
                                .map_err(|e| ElusionError::WriteError {
                                    path: path.to_string(),
                                    operation: format!("write_number_r{}_c{}", row_idx, c),
                                    reason: format!("Failed to write number: {}", e),
                                    suggestion: "💡 Failed to write number value".to_string(),
                                })?;
                        },
                        ArrowDataType::UInt8 | ArrowDataType::UInt16 | ArrowDataType::UInt32 | ArrowDataType::UInt64 => {
                            let value = match field.data_type() {
                                ArrowDataType::UInt8 => {
                                    if let Some(array) = col.as_any().downcast_ref::<UInt8Array>() {
                                        if array.is_null(r) { 0.0 } else { array.value(r) as f64 }
                                    } else { 0.0 }
                                },
                                ArrowDataType::UInt16 => {
                                    if let Some(array) = col.as_any().downcast_ref::<UInt16Array>() {
                                        if array.is_null(r) { 0.0 } else { array.value(r) as f64 }
                                    } else { 0.0 }
                                },
                                ArrowDataType::UInt32 => {
                                    if let Some(array) = col.as_any().downcast_ref::<UInt32Array>() {
                                        if array.is_null(r) { 0.0 } else { array.value(r) as f64 }
                                    } else { 0.0 }
                                },
                                ArrowDataType::UInt64 => {
                                    if let Some(array) = col.as_any().downcast_ref::<UInt64Array>() {
                                        if array.is_null(r) { 0.0 } else { array.value(r) as f64 }
                                    } else { 0.0 }
                                },
                                _ => 0.0 // Shouldn't reach here
                            };
                            
                            worksheet.write_number(row_idx, c as u16, value)
                                .map_err(|e| ElusionError::WriteError {
                                    path: path.to_string(),
                                    operation: format!("write_number_r{}_c{}", row_idx, c),
                                    reason: format!("Failed to write number: {}", e),
                                    suggestion: "💡 Failed to write number value".to_string(),
                                })?;
                        },
                        ArrowDataType::Float32 | ArrowDataType::Float64 => {
                            let value = match array_value_to_json(col, r)? {
                                serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0),
                                _ => 0.0,
                            };
                            worksheet.write_number(row_idx, c as u16, value)
                                .map_err(|e| ElusionError::WriteError {
                                    path: path.to_string(),
                                    operation: format!("write_number_r{}_c{}", row_idx, c),
                                    reason: format!("Failed to write number: {}", e),
                                    suggestion: "💡 Failed to write number value".to_string(),
                                })?;
                        },
                        ArrowDataType::Boolean => {
                            let value = match array_value_to_json(col, r)? {
                                serde_json::Value::Bool(b) => b,
                                _ => false,
                            };
                            worksheet.write_boolean(row_idx, c as u16, value)
                                .map_err(|e| ElusionError::WriteError {
                                    path: path.to_string(),
                                    operation: format!("write_boolean_r{}_c{}", row_idx, c),
                                    reason: format!("Failed to write boolean: {}", e),
                                    suggestion: "💡 Failed to write boolean value".to_string(),
                                })?;
                        },
                        ArrowDataType::Date32 | ArrowDataType::Date64 => {
                            let date_str = match array_value_to_json(col, r)? {
                                serde_json::Value::String(s) => s,
                                _ => String::new(),
                            };
                            
                            // Format: YYYY-MM-DD
                            let date_parts: Vec<&str> = date_str.split('-').collect();
                            if date_parts.len() == 3 {
                                if let (Ok(year), Ok(month), Ok(day)) = (
                                    date_parts[0].parse::<u16>(),
                                    date_parts[1].parse::<u8>(),
                                    date_parts[2].parse::<u8>(),
                                ) {
                                    let excel_date = ExcelDateTime::from_ymd(year, month, day)
                                        .map_err(|e| ElusionError::WriteError {
                                            path: path.to_string(),
                                            operation: format!("create_date_r{}_c{}", row_idx, c),
                                            reason: format!("Invalid date: {}", e),
                                            suggestion: "💡 Failed to create Excel date".to_string(),
                                        })?;
                                        
                                    worksheet.write_datetime_with_format(row_idx, c as u16, &excel_date, &date_format)
                                        .map_err(|e| ElusionError::WriteError {
                                            path: path.to_string(),
                                            operation: format!("write_date_r{}_c{}", row_idx, c),
                                            reason: format!("Failed to write date: {}", e),
                                            suggestion: "💡 Failed to write date value".to_string(),
                                        })?;
                                } else {
                                    // Fallback to string if parsing fails
                                    worksheet.write_string(row_idx, c as u16, &date_str)
                                        .map_err(|e| ElusionError::WriteError {
                                            path: path.to_string(),
                                            operation: format!("write_date_str_r{}_c{}", row_idx, c),
                                            reason: format!("Failed to write date string: {}", e),
                                            suggestion: "💡 Failed to write date as string".to_string(),
                                        })?;
                                }
                            } else {
                                // Not a YYYY-MM-DD format, write as string
                                worksheet.write_string(row_idx, c as u16, &date_str)
                                    .map_err(|e| ElusionError::WriteError {
                                        path: path.to_string(),
                                        operation: format!("write_date_str_r{}_c{}", row_idx, c),
                                        reason: format!("Failed to write date string: {}", e),
                                        suggestion: "💡 Failed to write date as string".to_string(),
                                    })?;
                            }
                        },
                        _ => {
                            let value = match array_value_to_json(col, r)? {
                                serde_json::Value::String(s) => s,
                                other => other.to_string(),
                            };
                            worksheet.write_string(row_idx, c as u16, &value)
                                .map_err(|e| ElusionError::WriteError {
                                    path: path.to_string(),
                                    operation: format!("write_string_r{}_c{}", row_idx, c),
                                    reason: format!("Failed to write string: {}", e),
                                    suggestion: "💡 Failed to write string value".to_string(),
                                })?;
                        }
                    }
                }
                row_idx += 1;
            }
        }
        
        worksheet.autofilter(0, 0, row_idx - 1, (column_count - 1) as u16)
            .map_err(|e| ElusionError::WriteError {
                path: path.to_string(),
                operation: "add_autofilter".to_string(),
                reason: format!("Failed to add autofilter: {}", e),
                suggestion: "💡 Failed to add autofilter to worksheet".to_string(),
            })?;
 
        workbook.save(path).map_err(|e| ElusionError::WriteError {
            path: path.to_string(),
            operation: "save_workbook".to_string(),
            reason: format!("Failed to save workbook: {}", e),
            suggestion: "💡 Failed to save Excel file. Check if the file is open in another application.".to_string(),
        })?;
        
        println!("✅ Data successfully written to Excel file '{}'", path);
        println!("✅ Wrote {} rows and {} columns", row_idx - 1, column_count);
        
        Ok(())
    }
