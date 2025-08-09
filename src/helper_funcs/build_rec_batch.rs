use crate::prelude::*;

pub fn build_record_batch(
    rows: &[HashMap<String, Value>],
    schema: Arc<Schema>
) -> ArrowResult<RecordBatch> {
    let mut builders: Vec<Box<dyn ArrayBuilder>> = Vec::new();

    // Simplified builder creation - only use Int64, UInt64, Float64, or StringBuilder
    for field in schema.fields() {
        let builder: Box<dyn ArrayBuilder> = match field.data_type() {
            ArrowDataType::Int64 => Box::new(Int64Builder::new()),
            ArrowDataType::UInt64 => Box::new(UInt64Builder::new()),
            ArrowDataType::Float64 => Box::new(Float64Builder::new()),
            _ => Box::new(StringBuilder::new()), // Everything else becomes string
        };
        builders.push(builder);
    }

    for row in rows {
        for (i, field) in schema.fields().iter().enumerate() {
            let key = field.name();
            let value = row.get(key);

            match field.data_type() {
                ArrowDataType::Int64 => {
                    let builder = builders[i]
                        .as_any_mut()
                        .downcast_mut::<Int64Builder>()
                        .expect("Expected Int64Builder");

                    match value {
                        Some(Value::Number(n)) => {
                            // Handle all possible number scenarios
                            if let Some(i) = n.as_i64() {
                                builder.append_value(i);
                            } else {
                                builder.append_null();
                            }
                        },
                        // Everything non-number becomes null
                        _ => builder.append_null(),
                    }
                },
                ArrowDataType::UInt64 => {
                    let builder = builders[i]
                        .as_any_mut()
                        .downcast_mut::<UInt64Builder>()
                        .expect("Expected UInt64Builder");

                    match value {
                        Some(Value::Number(n)) => {
                            // Only accept valid unsigned integers
                            if let Some(u) = n.as_u64() {
                                builder.append_value(u);
                            } else {
                                builder.append_null();
                            }
                        },
                        _ => builder.append_null(),
                    }
                },
                ArrowDataType::Float64 => {
                    let builder = builders[i]
                        .as_any_mut()
                        .downcast_mut::<Float64Builder>()
                        .expect("Expected Float64Builder");

                    match value {
                        Some(Value::Number(n)) => {
                            // Handle all possible float scenarios
                            if let Some(f) = n.as_f64() {
                                builder.append_value(f);
                            } else {
                                builder.append_null();
                            }
                        },
                        _ => builder.append_null(),
                    }
                },
                _ => {
                    // Default string handling - handles ALL other cases
                    let builder = builders[i]
                        .as_any_mut()
                        .downcast_mut::<StringBuilder>()
                        .expect("Expected StringBuilder");

                    match value {
                        Some(v) => {
                            // Comprehensive string conversion for ANY JSON value
                            let string_val = match v {
                                Value::Null => "null".to_string(),
                                Value::Bool(b) => b.to_string(),
                                Value::Number(n) => {
                                    if n.is_f64() {
                                        // Handle special float values
                                        if let Some(f) = n.as_f64() {
                                            if f.is_nan() {
                                                "NaN".to_string()
                                            } else if f.is_infinite() {
                                                if f.is_sign_positive() {
                                                    "Infinity".to_string()
                                                } else {
                                                    "-Infinity".to_string()
                                                }
                                            } else {
                                                f.to_string()
                                            }
                                        } else {
                                            n.to_string()
                                        }
                                    } else {
                                        n.to_string()
                                    }
                                },
                                Value::String(s) => {
                                    // Handle potentially invalid UTF-8 or special characters
                                    s.chars()
                                        .map(|c| if c.is_control() { 
                                            format!("\\u{:04x}", c as u32) 
                                        } else { 
                                            c.to_string() 
                                        })
                                        .collect()
                                },
                                Value::Array(arr) => {
                                    // Safely handle nested arrays
                                    serde_json::to_string(arr)
                                        .unwrap_or_else(|_| "[]".to_string())
                                },
                                Value::Object(obj) => {
                                    // Safely handle nested objects
                                    serde_json::to_string(obj)
                                        .unwrap_or_else(|_| "{}".to_string())
                                },
                            };
                            // Ensure the string is valid UTF-8
                            builder.append_value(&string_val);
                        },
                        None => builder.append_null(),
                    }
                },
            }
        }
    }

    let arrays: Vec<ArrayRef> = builders.into_iter().map(|mut b| b.finish()).collect();
    RecordBatch::try_new(schema.clone(), arrays)
}