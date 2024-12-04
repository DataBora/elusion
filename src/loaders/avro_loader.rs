// use std::sync::Arc;
// use std::error::Error as StdError;
// use apache_avro::Reader as AvroReader;
// use arrow::datatypes::{Schema, DataType, Field};
// use arrow::array::{ArrayRef, StringBuilder, Int64Builder, Float64Builder};
// use arrow::record_batch::RecordBatch;
// use std::fs::File;

// #[derive(Debug)]
// struct ConverterError(String);

// impl std::fmt::Display for ConverterError {
//     fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
//         write!(f, "{}", self.0)
//     }
// }

// impl StdError for ConverterError {}

// fn avro_schema_to_arrow_schema(avro_schema: &apache_avro::Schema) -> Schema {
//     let fields = match avro_schema {
//         apache_avro::Schema::Record(record) => record.fields.iter().map(|field|{
//             let data_type = match field.schema {
//                 apache_avro::Schema::String => DataType::Utf8,
//                 apache_avro::Schema::Long => DataType::Int64,
//                 apache_avro::Schema::Int => DataType::Int32,
//                 apache_avro::Schema::Double => DataType::Float64,
//                 apache_avro::Schema::Float => DataType::Float32,
//                 apache_avro::Schema::Boolean => DataType::Boolean,
               
//                 _ => DataType::Utf8, // Default to string for unsupported types
//             };
//             Field::new(&field.name, data_type, true)
//         }).collect(),
//         _ => vec![], // Handle other schema types if needed
//     };
//     Schema::new(fields)
// }

// fn avro_value_to_array(
//     values: Vec<apache_avro::types::Value>,
//     field_type: &DataType
// ) -> Result<ArrayRef, Box<dyn StdError>> {
//     match field_type {
//         DataType::Utf8 => {
//             let mut builder = StringBuilder::new();
//             for value in values {
//                 match value {
//                     apache_avro::types::Value::String(s) => builder.append_value(s),
//                     _ => builder.append_null(),
//                 }
//             }
//             Ok(Arc::new(builder.finish()))
//         },
//         DataType::Int64 => {
//             let mut builder = Int64Builder::new();
//             for value in values {
//                 match value {
//                     apache_avro::types::Value::Long(n) => builder.append_value(n),
//                     _ => builder.append_null(),
//                 }
//             }
//             Ok(Arc::new(builder.finish()))
//         },
//         DataType::Float64 => {
//             let mut builder = Float64Builder::new();
//             for value in values {
//                 match value {
//                     apache_avro::types::Value::Double(n) => builder.append_value(n),
//                     _ => builder.append_null(),
//                 }
//             }
//             Ok(Arc::new(builder.finish()))
//         },
//         // Add more type conversions as needed
//         _ => Err(Box::new(ConverterError("Unsupported data type".into())))
//     }
// }

// fn read_avro(path: &str) -> Result<Vec<RecordBatch>, Box<dyn StdError>> {
//     let file = File::open(path)?;
//     let reader = AvroReader::new(file)?;
//     let avro_schema = reader.writer_schema();
//     let arrow_schema = Arc::new(avro_schema_to_arrow_schema(&avro_schema));
    
//     // Collect all records
//     let mut records: Vec<apache_avro::types::Value> = Vec::new();
//     for value in reader {
//         records.push(value?);
//     }

//     // Group records into batches
//     let batch_size = 1024;
//     let mut batches = Vec::new();
    
//     for chunk in records.chunks(batch_size) {
//         let mut arrays: Vec<ArrayRef> = Vec::new();
        
//         // Process each field
//         for field in arrow_schema.fields() {
//             let mut field_values = Vec::new();
            
//             // Extract values for this field from the records
//             for record in chunk {
//                 if let apache_avro::types::Value::Record(record_fields) = record {
//                     if let Some(value) = record_fields.iter().find(|(name, _)| name == field.name()) {
//                         field_values.push(value.1.clone());
//                     }
//                 }
//             }
            
//             // Convert values to Arrow array
//             let array = avro_value_to_array(field_values, field.data_type())?;
//             arrays.push(array);
//         }
        
//         let batch = RecordBatch::try_new(arrow_schema.clone(), arrays)?;
//         batches.push(batch);
//     }

//     Ok(batches)
// }