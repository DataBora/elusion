use crate::prelude::*;

// Helper function to convert Arrow array values to serde_json::Value
pub fn array_value_to_json(array: &Arc<dyn Array>, index: usize) -> ElusionResult<serde_json::Value> {
    if array.is_null(index) {
        return Ok(serde_json::Value::Null);
    }
    // matching on array data type and convert 
    match array.data_type() {
        ArrowDataType::Null => Ok(serde_json::Value::Null),
        ArrowDataType::Boolean => {
            let array = array.as_any().downcast_ref::<BooleanArray>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert Boolean array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            Ok(serde_json::Value::Bool(array.value(index)))
        },
        ArrowDataType::Int8 | ArrowDataType::Int16 | ArrowDataType::Int32 => {
            let array = array.as_any().downcast_ref::<Int32Array>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert Int32 array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            Ok(serde_json::Value::Number(serde_json::Number::from(array.value(index))))
        },
        ArrowDataType::Int64 => {
            let array = array.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert Int64 array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            // Convert i64 to serde_json::Number
            let n = array.value(index);
            serde_json::Number::from_f64(n as f64)
                .map(serde_json::Value::Number)
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: format!("Cannot represent i64 value {} as JSON number", n),
                    suggestion: "💡 Consider using string representation for large integers".to_string(),
                })
        },
        ArrowDataType::UInt8 | ArrowDataType::UInt16 | ArrowDataType::UInt32 => {
            let array = array.as_any().downcast_ref::<UInt32Array>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert UInt32 array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            Ok(serde_json::Value::Number(serde_json::Number::from(array.value(index))))
        },
        ArrowDataType::UInt64 => {
            let array = array.as_any().downcast_ref::<UInt64Array>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert UInt64 array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            // Convert u64 to serde_json::Number
            let n = array.value(index);
            serde_json::Number::from_f64(n as f64)
                .map(serde_json::Value::Number)
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: format!("Cannot represent u64 value {} as JSON number", n),
                    suggestion: "💡 Consider using string representation for large integers".to_string(),
                })
        },
        ArrowDataType::Float32 => {
            let array = array.as_any().downcast_ref::<Float32Array>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert Float32 array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            let val = array.value(index) as f64;
            serde_json::Number::from_f64(val)
                .map(serde_json::Value::Number)
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: format!("Cannot represent f32 value {} as JSON number", val),
                    suggestion: "💡 Consider handling special float values differently".to_string(),
                })
        },
        ArrowDataType::Float64 => {
            let array = array.as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert Float64 array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            let val = array.value(index);
            let result = serde_json::Number::from_f64(val)
                .map(serde_json::Value::Number)
                .unwrap_or_else(|| {
                    // Handle special float values like NaN, Infinity
                    if val.is_nan() {
                        serde_json::Value::String("NaN".to_string())
                    } else if val.is_infinite() {
                        if val.is_sign_positive() {
                            serde_json::Value::String("Infinity".to_string())
                        } else {
                            serde_json::Value::String("-Infinity".to_string())
                        }
                    } else {
                        serde_json::Value::Null
                    }
                });
            
            Ok(result)
        },
        ArrowDataType::Utf8 => {
            let array = array.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert String array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            Ok(serde_json::Value::String(array.value(index).to_string()))
        },
        ArrowDataType::LargeUtf8 => {
            let array = array.as_any().downcast_ref::<LargeStringArray>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert LargeString array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            Ok(serde_json::Value::String(array.value(index).to_string()))
        },
        ArrowDataType::Binary => {
            let array = array.as_any().downcast_ref::<BinaryArray>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert Binary array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            // Encode binary data as base64 string
            let bytes = array.value(index);
            let b64 = base64::engine::general_purpose::STANDARD.encode(bytes); 
            Ok(serde_json::Value::String(b64))
        },
        ArrowDataType::LargeBinary => {
            let array = array.as_any().downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert LargeBinary array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            // Encode binary data as base64 string
            let bytes = array.value(index);
            let b64 = base64::engine::general_purpose::STANDARD.encode(bytes); 
            Ok(serde_json::Value::String(b64))
        },
        ArrowDataType::Date32 => {
            let array = array.as_any().downcast_ref::<Date32Array>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert Date32 array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            // Convert to ISO date string
            let days = array.value(index);
            let naive_date = NaiveDate::from_num_days_from_ce_opt(days + 719163)
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Date Conversion".to_string(),
                    reason: format!("Invalid date value: {}", days),
                    suggestion: "💡 Check if date values are within valid range".to_string(),
                })?;
            Ok(serde_json::Value::String(naive_date.format("%Y-%m-%d").to_string()))
        },
        ArrowDataType::Date64 => {
            let array = array.as_any().downcast_ref::<Date64Array>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert Date64 array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            // Convert milliseconds since epoch to datetime string
            let ms = array.value(index);
            let secs = ms / 1000;
            let nsecs = ((ms % 1000) * 1_000_000) as u32;
            let naive_datetime = DateTime::from_timestamp(secs, nsecs)
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Date Conversion".to_string(),
                    reason: format!("Invalid timestamp value: {}", ms),
                    suggestion: "💡 Check if timestamp values are within valid range".to_string(),
                })?;
            Ok(serde_json::Value::String(naive_datetime.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()))
        },
        ArrowDataType::Timestamp(time_unit, _) => {
            // Handle timestamp based on time unit
            match time_unit {
                TimeUnit::Second => {
                    let array = array.as_any().downcast_ref::<TimestampSecondArray>()
                        .ok_or_else(|| ElusionError::InvalidOperation {
                            operation: "Type Conversion".to_string(),
                            reason: "Failed to convert TimestampSecond array".to_string(),
                            suggestion: "💡 This is an internal error, please report it".to_string(),
                        })?;
                    let seconds = array.value(index);
                    let dt = DateTime::from_timestamp(seconds, 0)
                        .ok_or_else(|| ElusionError::InvalidOperation {
                            operation: "Timestamp Conversion".to_string(),
                            reason: format!("Invalid timestamp value: {}", seconds),
                            suggestion: "💡 Check if timestamp values are within valid range".to_string(),
                        })?;
                    Ok(serde_json::Value::String(dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()))
                },
                TimeUnit::Millisecond => {
                    let array = array.as_any().downcast_ref::<TimestampMillisecondArray>()
                        .ok_or_else(|| ElusionError::InvalidOperation {
                            operation: "Type Conversion".to_string(),
                            reason: "Failed to convert TimestampMillisecond array".to_string(),
                            suggestion: "💡 This is an internal error, please report it".to_string(),
                        })?;
                    let ms = array.value(index);
                    let secs = ms / 1000;
                    let nsecs = ((ms % 1000) * 1_000_000) as u32;
                    let dt = DateTime::from_timestamp(secs, nsecs)
                        .ok_or_else(|| ElusionError::InvalidOperation {
                            operation: "Timestamp Conversion".to_string(),
                            reason: format!("Invalid timestamp value: {}", ms),
                            suggestion: "💡 Check if timestamp values are within valid range".to_string(),
                        })?;
                    Ok(serde_json::Value::String(dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()))
                },
                TimeUnit::Microsecond => {
                    let array = array.as_any().downcast_ref::<TimestampMicrosecondArray>()
                        .ok_or_else(|| ElusionError::InvalidOperation {
                            operation: "Type Conversion".to_string(),
                            reason: "Failed to convert TimestampMicrosecond array".to_string(),
                            suggestion: "💡 This is an internal error, please report it".to_string(),
                        })?;
                    let us = array.value(index);
                    let secs = us / 1_000_000;
                    let nsecs = ((us % 1_000_000) * 1_000) as u32;
                    let dt = DateTime::from_timestamp(secs, nsecs)
                        .ok_or_else(|| ElusionError::InvalidOperation {
                            operation: "Timestamp Conversion".to_string(),
                            reason: format!("Invalid timestamp value: {}", us),
                            suggestion: "💡 Check if timestamp values are within valid range".to_string(),
                        })?;
                    Ok(serde_json::Value::String(dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()))
                },
                TimeUnit::Nanosecond => {
                    let array = array.as_any().downcast_ref::<TimestampNanosecondArray>()
                        .ok_or_else(|| ElusionError::InvalidOperation {
                            operation: "Type Conversion".to_string(),
                            reason: "Failed to convert TimestampNanosecond array".to_string(),
                            suggestion: "💡 This is an internal error, please report it".to_string(),
                        })?;
                    let ns = array.value(index);
                    let secs = ns / 1_000_000_000;
                    let nsecs = (ns % 1_000_000_000) as u32;
                    let dt = DateTime::from_timestamp(secs, nsecs)
                        .ok_or_else(|| ElusionError::InvalidOperation {
                            operation: "Timestamp Conversion".to_string(),
                            reason: format!("Invalid timestamp value: {}", ns),
                            suggestion: "💡 Check if timestamp values are within valid range".to_string(),
                        })?;
                    Ok(serde_json::Value::String(dt.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string()))
                },
            }
        },
        ArrowDataType::List(_) => {
            let list_array = array.as_any().downcast_ref::<ListArray>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert List array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            
            let values = list_array.value(index);
            let mut json_values = Vec::new();
            
            for i in 0..values.len() {
                let json_value = array_value_to_json(&values, i)?;
                json_values.push(json_value);
            }
            
            Ok(serde_json::Value::Array(json_values))
        },
        _ => {
            Ok(serde_json::Value::String(format!("Unsupported type: {:?}", array.data_type())))
        }
    }
}