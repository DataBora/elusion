// use futures::TryStreamExt; 
// use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
// use tokio::fs::File;
// use arrow::record_batch::RecordBatch;

// pub async fn read_parquet_async(path: &str) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
//     // Open the file asynchronously
//     let file = File::open(path).await?;
    
   
//     let builder = ParquetRecordBatchStreamBuilder::new(file)
//         .await
//         .unwrap()
//         .with_batch_size(1024);

//     // Get the file metadata from the builder
//     let file_metadata = builder.metadata().file_metadata();
    
//     // Define a projection mask to select specific columns (for example, columns 1, 2, and 6)
//     let mask = ProjectionMask::roots(file_metadata.schema_descr(), [1, 2, 6]);

//     // Apply the projection and build the stream
//     let stream = builder.with_projection(mask).build().unwrap();

//     // Collect results asynchronously into a Vec
//     let results = stream.try_collect::<Vec<_>>().await.unwrap();
    
//     // Return the collected batches
//     Ok(results)
// }