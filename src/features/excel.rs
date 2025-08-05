// ======== EXCEL


use crate::prelude::*;
use crate::infer_schema_from_json;
use crate::build_record_batch;
#[cfg(feature = "excel")]
use crate::array_value_to_json;

// Function to convert Excel date (days since 1900-01-01) to NaiveDate
fn excel_date_to_naive_date(excel_date: f64) -> Option<NaiveDate> {
    // Excel dates start at January 0, 1900, which is actually December 31, 1899
    // There's also a leap year bug in Excel that treats 1900 as a leap year
    let excel_epoch = NaiveDate::from_ymd_opt(1899, 12, 30)?;
    
    // Convert to days, ignoring any fractional part (time component)
    let days = excel_date.trunc() as i64;
    
    // Add days to epoch
    excel_epoch.checked_add_signed(Duration::days(days))
}

#[derive(Debug, Clone)]
pub struct ExcelWriteOptions {
    // pub autofilter: bool,     
    // pub freeze_header: bool,   
    // pub table_style: bool,    
    // pub sheet_protection: bool, 
}

impl Default for ExcelWriteOptions {
    fn default() -> Self {
        Self {
            // autofilter: true,
            // freeze_header: true,
            // table_style: true,
            // sheet_protection: false,
        }
    }
}

// Implement From<XlsxError> for ElusionError
#[cfg(feature = "excel")]
impl From<rust_xlsxwriter::XlsxError> for ElusionError {
    fn from(error: rust_xlsxwriter::XlsxError) -> Self {
        ElusionError::Custom(format!("Excel writing error: {}", error))
    }
}


 // ========== EXCEL
    /// Load an Excel file (XLSX) into a CustomDataFrame
    pub fn load_excel<'a>(
        file_path: &'a str,
        alias: &'a str,
    ) -> BoxFuture<'a, ElusionResult<AliasedDataFrame>> {
        Box::pin(async move {

            if !LocalPath::new(file_path).exists() {
                return Err(ElusionError::WriteError {
                    path: file_path.to_string(),
                    operation: "read".to_string(),
                    reason: "File not found".to_string(),
                    suggestion: "💡 Check if the file path is correct".to_string(),
                });
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

            let headers_row = range.rows().next().ok_or_else(|| ElusionError::InvalidOperation {
                operation: "Excel Reading".to_string(),
                reason: "Failed to read headers from Excel file".to_string(),
                suggestion: "💡 Ensure the first row contains column headers".to_string(),
            })?;
            
            // Convert headers to strings, sanitizing as needed
            let headers: Vec<String> = headers_row.iter()
                .enumerate()
                .map(|(column_index, cell)|  {
                    let header = cell.to_string().trim().to_string();
                    if header.is_empty() {
                        format!("Column_{}", column_index)
                    } else {
                        // Replace spaces and special characters with underscores
                        let sanitized = header.replace(' ', "_")
                            .replace(|c: char| !c.is_alphanumeric() && c != '_', "_");
                        
                        // Ensure header starts with a letter
                        if sanitized.chars().next().map_or(true, |c| !c.is_alphabetic()) {
                            format!("col_{}", sanitized)
                        } else {
                            sanitized
                        }
                    }
                })
                .enumerate()
                .map(|(column_index, header)| {
                    if header.is_empty() {
                        format!("Column_{}", column_index)
                    } else {
                        header
                    }
                })
                .collect();
            
            // Check for duplicates in headers
            let mut seen = HashSet::new();
            let mut has_duplicates = false;
            for header in &headers {
                if !seen.insert(header.clone()) {
                    has_duplicates = true;
                    break;
                }
            }
            
            // If duplicates found, make headers unique
            let final_headers = if has_duplicates {
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
            } else {
                headers
            };
            
            // Process the data rows
            let mut all_data: Vec<HashMap<String, Value>> = Vec::new();
            
            // Skip the header row
            for row in range.rows().skip(1) {
                let mut row_map = HashMap::new();
                
                for (i, cell) in row.iter().enumerate() {
                    if i >= final_headers.len() {
                        continue; // Skip cells without headers
                    }
                    
                    // Convert cell value to serde_json::Value based on its type
                    let value = match cell {
                        CalamineDataType::Empty => Value::Null,
                        
                        CalamineDataType::String(s) => Value::String(s.clone()),
                        
                        CalamineDataType::Float(f) => {
                            if f.fract() == 0.0 {
                                // It's an integer
                                Value::Number(serde_json::Number::from(f.round() as i64))
                            } else {
                                // It's a floating point
                                serde_json::Number::from_f64(*f)
                                    .map(Value::Number)
                                    .unwrap_or(Value::Null)
                            }
                        },
                        
                        CalamineDataType::Int(i) => Value::Number((*i).into()),
                        
                        CalamineDataType::Bool(b) => Value::Bool(*b),
                        
                        CalamineDataType::DateTime(dt) => {
                          
                            if let Some(naive_date) = excel_date_to_naive_date(*dt) {
                                Value::String(naive_date.format("%Y-%m-%d").to_string())
                            } else {
                                // Fallback to original representation if conversion fails
                                Value::String(format!("DateTime({})", dt))
                            }
                        },
                        
                        CalamineDataType::Duration(d) => {
                            let hours = (d * 24.0) as i64;
                            let minutes = ((d * 24.0 * 60.0) % 60.0) as i64;
                            let seconds = ((d * 24.0 * 60.0 * 60.0) % 60.0) as i64;
                            Value::String(format!("{}h {}m {}s", hours, minutes, seconds))
                        },
                        
                        CalamineDataType::DateTimeIso(dt_iso) => {
                            // ISO 8601 formatted date/time string
                            Value::String(dt_iso.clone())
                        },
                        
                        CalamineDataType::DurationIso(d_iso) => {
                            // ISO 8601 formatted duration string
                            Value::String(d_iso.clone())
                        },
                        
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
            
            let schema = infer_schema_from_json(&all_data);
            
            let record_batch = build_record_batch(&all_data, schema.clone())
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
            
            Ok(AliasedDataFrame {
                dataframe: df,
                alias: alias.to_string(),
            })
        })
    }


     /// Writes the DataFrame to an Excel file with formatting options
    #[cfg(feature = "excel")]
    pub async fn write_to_excel_impl(
        df: &CustomDataFrame,
        path: &str,
        sheet_name: Option<&str>
    ) -> ElusionResult<()> {

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

