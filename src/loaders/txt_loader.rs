// use std::fs::File;
// use std::sync::Arc;
// use arrow::datatypes::{Schema, Field, DataType};
// use arrow_csv::reader::ReaderBuilder;
// use arrow::record_batch::RecordBatch;
// use arrow::error::Result;

// fn read_txt(path: &str, delimiter: u8, batch_size: usize) -> Result<Vec<RecordBatch>> {
//     // Define schema manually or infer it as needed (just like we did for CSV)
//     let fields = vec![
//         Field::new("column1", DataType::Utf8, false),
//         Field::new("column2", DataType::Utf8, false),
//         Field::new("column3", DataType::Float64, false),  // Example with a float column
//     ];

//     let schema = Arc::new(Schema::new(fields));

//     // Open the text file
//     let file = File::open(path)?;
//     let builder = ReaderBuilder::new(schema)
//         .with_batch_size(batch_size)
//         .with_delimiter(delimiter); // Specify delimiter for TXT file (space, tab, etc.)

//     // Build the reader for the TXT file
//     let mut reader = builder.build(file)?;

//     // Collect all record batches
//     let mut batches = Vec::new();
//     while let Some(batch) = reader.next() {
//         batches.push(batch?);
//     }

//     Ok(batches)
// }