// use tokio::fs::File;
// use tokio::io::{AsyncBufReadExt, BufReader};
// use serde_json::Value;
// use arrow::datatypes::{Schema, Field, DataType};
// use arrow::record_batch::RecordBatch;
// use std::sync::Arc;
// use std::error::Error as StdError;
// use tokio::io::AsyncSeekExt;
// use tokio::io::AsyncReadExt;

// use arrow::array::{StringArray, Int64Array, BooleanArray}; 

// pub async fn infer_json_schema(file: &mut File) -> Result<Schema, Box<dyn StdError>> {
//     // Move to the start of the file to infer schema
//     file.seek(std::io::SeekFrom::Start(0)).await?;

//     let mut sample = String::new();
//     let mut reader = BufReader::new(file);
//     reader.read_to_string(&mut sample).await?;

//     // Parse the sample as a JSON array
//     let json_values: Vec<Value> = serde_json::from_str(&sample)?;

//     // Handle empty file or empty array scenario
//     if json_values.is_empty() {
//         // Return a schema with a default field
//         return Ok(Schema::new(vec![
//             Arc::new(Field::new("empty", DataType::Utf8, true)), // Default field for empty data
//         ]));
//     }

//     // Infer schema from the first object
//     let fields = if let Some(Value::Object(obj)) = json_values.first() {
//         obj.iter().map(|(key, value)| {
//             let data_type = match value {
//                 Value::String(_) => DataType::Utf8,
//                 Value::Number(n) => {
//                     if n.is_i64() {
//                         DataType::Int64
//                     } else if n.is_f64() {
//                         DataType::Float64
//                     } else {
//                         DataType::Float64 // Default to f64 for other numbers
//                     }
//                 },
//                 Value::Bool(_) => DataType::Boolean,
//                 Value::Null => DataType::Utf8, // Default to nullable string for null values
//                 Value::Array(_) => DataType::Utf8, // Convert arrays to string
//                 Value::Object(_) => DataType::Utf8, // Convert nested objects to string
//             };
//             Arc::new(Field::new(key, data_type, true)) // Wrap the field in Arc
//         }).collect::<Vec<Arc<Field>>>() // Collect as Vec<Arc<Field>>
//     } else {
//         vec![] // This case should never occur because we've already checked for empty arrays
//     };

//     Ok(Schema::new(fields)) // Pass Vec<Arc<Field>> to Schema::new
// }

// pub async fn read_json(path: &str) -> Result<Vec<RecordBatch>, Box<dyn StdError>> {
//     let mut file = File::open(path).await?;

//     let schema = Arc::new(infer_json_schema(&mut file).await?);

//     let reader = BufReader::new(file);
//     let mut lines = reader.lines(); // Stream over each line

//     let mut batches = Vec::new();
    
//     // Create temporary vectors to hold Arrow arrays for each column
//     let mut string_column: Vec<String> = Vec::new();
//     let mut int64_column: Vec<i64> = Vec::new();
//     let mut bool_column: Vec<bool> = Vec::new();
    
//     // Loop over each line of the JSON file
//     while let Some(line) = lines.next_line().await? {
//         if let Ok(json_value) = serde_json::from_str::<Value>(&line) {
//             // Reset column vectors for the next batch
//             string_column.clear();
//             int64_column.clear();
//             bool_column.clear();
            
//             // For each field in the schema, extract the corresponding value from the JSON object
//             for field in schema.fields() {
//                 let field_name = field.name();
//                 let data_type = field.data_type();
                
//                 // Match based on the data type and extract values accordingly
//                 match data_type {
//                     DataType::Utf8 => {
//                         if let Some(value) = json_value.get(field_name).and_then(|v| v.as_str()) {
//                             string_column.push(value.to_string());
//                         } else {
//                             string_column.push("".to_string()); // Default value for missing field
//                         }
//                     },
//                     DataType::Int64 => {
//                         if let Some(value) = json_value.get(field_name).and_then(|v| v.as_i64()) {
//                             int64_column.push(value);
//                         } else {
//                             int64_column.push(0); // Default value for missing field
//                         }
//                     },
//                     DataType::Boolean => {
//                         if let Some(value) = json_value.get(field_name).and_then(|v| v.as_bool()) {
//                             bool_column.push(value);
//                         } else {
//                             bool_column.push(false); // Default value for missing field
//                         }
//                     },
//                     _ => {
//                         // Handle other types as needed
//                         // For now, we'll default to a string representation
//                         string_column.push("".to_string());
//                     }
//                 }
//             }

//             // Create Arrow arrays from the columns
//             let string_array = StringArray::from(string_column.clone());
//             let int64_array = Int64Array::from(int64_column.clone());
//             let bool_array = BooleanArray::from(bool_column.clone());

//             // Create a RecordBatch with the arrays
//             let record_batch = RecordBatch::try_new(
//                 schema.clone(),
//                 vec![
//                     Arc::new(string_array),
//                     Arc::new(int64_array),
//                     Arc::new(bool_array),
//                 ],
//             )?;

//             // Add the batch to the list
//             batches.push(record_batch);
//         }
//     }

//     Ok(batches)
// }