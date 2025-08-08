use crate::prelude::*;

/// Function to infer schema from rows
pub fn infer_schema_from_json(rows: &[HashMap<String, Value>]) -> SchemaRef {
    let mut fields_map: HashMap<String, ArrowDataType> = HashMap::new();
    let mut keys_set: HashSet<String> = HashSet::new();

    for row in rows {
        for (k, v) in row {
            keys_set.insert(k.clone());
            let inferred_type = infer_arrow_type(v);
            // If the key already exists, ensure the type is compatible 
            fields_map
                .entry(k.clone())
                .and_modify(|existing_type| {
                    *existing_type = promote_types(existing_type.clone(), inferred_type.clone());
                })
                .or_insert(inferred_type);
        }
    }

    let fields: Vec<Field> = keys_set.into_iter().map(|k| {
        let data_type = fields_map.get(&k).unwrap_or(&ArrowDataType::Utf8).clone();
        Field::new(&k, data_type, true)
    }).collect();

    Arc::new(Schema::new(fields))
}

fn infer_arrow_type(value: &Value) -> ArrowDataType {
    match value {
        Value::Null => ArrowDataType::Utf8,  // Always default null to Utf8
        Value::Bool(_) => ArrowDataType::Utf8,  // Changed to Utf8 for consistency
        Value::Number(n) => {
            if n.is_i64() {
                ArrowDataType::Int64
            } else if n.is_u64() {
                ArrowDataType::UInt64
            } else if let Some(f) = n.as_f64() {
                if f.is_finite() {
                    ArrowDataType::Float64
                } else {
                    ArrowDataType::Utf8  // Handle Infinity and NaN as strings
                }
            } else {
                ArrowDataType::Utf8  // Default for any other numeric types
            }
        },
        Value::String(_) => ArrowDataType::Utf8,
        Value::Array(_) => ArrowDataType::Utf8,  // Always serialize arrays to strings
        Value::Object(_) => ArrowDataType::Utf8,  // Always serialize objects to strings
    }
}
//helper function to promote types
fn promote_types(a: ArrowDataType, b: ArrowDataType) -> ArrowDataType {
    match (a, b) {
        // If either type is Utf8, result is Utf8
        (Utf8, _) | (_, Utf8) => Utf8,
        
        // Only keep numeric types if they're the same
        (Int64, Int64) => Int64,
        (UInt64, UInt64) => UInt64,
        (Float64, Float64) => Float64,
        
        // Any other combination defaults to Utf8
        _ => Utf8,
    }
}
