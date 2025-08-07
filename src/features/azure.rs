use crate::prelude::*;
#[cfg(feature = "azure")]
use crate::lowercase_column_names;
#[cfg(feature = "azure")]
 use crate::infer_schema_from_json;
#[cfg(feature = "azure")]
use crate::build_record_batch;
#[cfg(feature = "azure")]
use crate::array_value_to_json;
#[cfg(feature = "azure")]
use tokio::time::{timeout, Duration};

//=============== AZURE
// Enum for writing options, probably will not use it as its bad for users to write enums
#[derive(Debug, Clone, Copy, PartialEq)]
#[allow(dead_code)]
pub enum AzureWriteMode {
    Overwrite,
    Append,
    ErrorIfExists,
}

// Azure ULR validator helper function
#[cfg(feature = "azure")]
fn validate_azure_url(url: &str) -> ElusionResult<()> {
    if !url.starts_with("https://") {
        return Err(ElusionError::Custom("Bad url format. Expected format: https://{account}.{endpoint}.core.windows.net/{container}/{blob}".to_string()));
    }

    if !url.contains(".blob.core.windows.net/") && !url.contains(".dfs.core.windows.net/") {
        return Err(ElusionError::Custom(
            "URL must contain either '.blob.core.windows.net/' or '.dfs.core.windows.net/'".to_string()
        ));
    }

    Ok(())
}


// Optimized JSON processing function using streaming parser
#[cfg(feature = "azure")]
fn process_json_content(content: &[u8]) -> ElusionResult<Vec<HashMap<String, Value>>> {
    let reader = BufReader::new(content);
    let stream = Deserializer::from_reader(reader).into_iter::<Value>();
    
    let mut results = Vec::new();
    let mut stream = stream.peekable();

    match stream.peek() {
        Some(Ok(Value::Array(_))) => {
            for value in stream {
                match value {
                    Ok(Value::Array(array)) => {
                        for item in array {
                            if let Value::Object(map) = item {
                                let mut base_map = map.clone();
                                
                                if let Some(Value::Array(fields)) = base_map.remove("fields") {
                                    for field in fields {
                                        let mut row = base_map.clone();
                                        if let Value::Object(field_obj) = field {
                                            for (key, val) in field_obj {
                                                row.insert(format!("field_{}", key), val);
                                            }
                                        }
                                        results.push(row.into_iter().collect());
                                    }
                                } else {
                                    results.push(base_map.into_iter().collect());
                                }
                            }
                        }
                    }
                    Ok(_) => continue,
                    Err(e) => return Err(ElusionError::Custom(format!("JSON parsing error: {}", e))),
                }
            }
        }
        Some(Ok(Value::Object(_))) => {
            for value in stream {
                if let Ok(Value::Object(map)) = value {
                    let mut base_map = map.clone();
                    if let Some(Value::Array(fields)) = base_map.remove("fields") {
                        for field in fields {
                            let mut row = base_map.clone();
                            if let Value::Object(field_obj) = field {
                                for (key, val) in field_obj {
                                    row.insert(format!("field_{}", key), val);
                                }
                            }
                            results.push(row.into_iter().collect());
                        }
                    } else {
                        results.push(base_map.into_iter().collect());
                    }
                }
            }
        }
        Some(Ok(Value::Null)) | 
        Some(Ok(Value::Bool(_))) |
        Some(Ok(Value::Number(_))) |
        Some(Ok(Value::String(_))) => {
            return Err(ElusionError::Custom("JSON content must be an array or object".to_string()));
        }
        Some(Err(e)) => return Err(ElusionError::Custom(format!("JSON parsing error: {}", e))),
        None => return Err(ElusionError::Custom("Empty JSON content".to_string())),
    }

    if results.is_empty() {
        return Err(ElusionError::Custom("No valid JSON data found".to_string()));
    }
    
    Ok(results)
}

#[cfg(feature = "azure")]
async fn process_csv_content(_name: &str, content: Vec<u8>) -> ElusionResult<Vec<HashMap<String, Value>>> {
    // Create a CSV reader directly from the bytes
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .trim(All)
        .from_reader(content.as_slice());

    // Get headers
    let headers: Vec<String> = reader
        .headers()
        .map_err(|e| ElusionError::Custom(format!("Failed to read CSV headers: {}", e)))?
        .iter()
        .map(|h| h.trim().to_string())
        .collect();

    let estimated_rows = content.len() / (headers.len() * 20);
    let mut results = Vec::with_capacity(estimated_rows);

    for record in reader.records() {
        match record {
            Ok(record) => {
                let mut map = HashMap::with_capacity(headers.len());
                for (header, field) in headers.iter().zip(record.iter()) {
                    let value = if field.is_empty() {
                        Value::Null
                    } else if let Ok(num) = field.parse::<i64>() {
                        Value::Number(num.into())
                    } else if let Ok(num) = field.parse::<f64>() {
                        match serde_json::Number::from_f64(num) {
                            Some(n) => Value::Number(n),
                            None => Value::String(field.to_string())
                        }
                    } else if field.eq_ignore_ascii_case("true") {
                        Value::Bool(true)
                    } else if field.eq_ignore_ascii_case("false") {
                        Value::Bool(false)
                    } else {
                        Value::String(field.to_string())
                    };

                    map.insert(header.clone(), value);
                }
                results.push(map);
            }
            Err(e) => {
                println!("*** Warning ***: Error reading CSV record: {}", e);
                continue;
            }
        }
    }

    Ok(results)
}


    #[cfg(feature = "azure")]
    fn setup_azure_client_impl(url: &str, sas_token: &str) -> ElusionResult<(ContainerClient, String)> {
        // Validate URL format and parse components
        let url_parts: Vec<&str> = url.split('/').collect();
        if url_parts.len() < 5 {
            return Err(ElusionError::Custom(
                "Invalid URL format. Expected format: https://{account}.{endpoint}.core.windows.net/{container}/{blob}".to_string()
            ));
        }

        let (account, endpoint_type) = url_parts[2]
            .split('.')
            .next()
            .map(|acc| {
                if url.contains(".dfs.") {
                    (acc, "dfs")
                } else {
                    (acc, "blob")
                }
            })
            .ok_or_else(|| ElusionError::Custom("Invalid URL format: cannot extract account name".to_string()))?;

        // Validate container and blob name
        let container = url_parts[3].to_string();
        if container.is_empty() {
            return Err(ElusionError::Custom("Container name cannot be empty".to_string()));
        }

        let blob_name = url_parts[4..].join("/");
        if blob_name.is_empty() {
            return Err(ElusionError::Custom("Blob name cannot be empty".to_string()));
        }

        // Validate SAS token expiry
        if let Some(expiry_param) = sas_token.split('&').find(|&param| param.starts_with("se=")) {
            let expiry = expiry_param.trim_start_matches("se=");
            // Parse the expiry timestamp (typically in format like "2024-01-29T00:00:00Z")
            if let Ok(expiry_time) = chrono::DateTime::parse_from_rfc3339(expiry) {
                let now = chrono::Utc::now();
                if expiry_time < now {
                    return Err(ElusionError::Custom("SAS token has expired".to_string()));
                }
            }
        }

        // Create storage credentials
        let credentials = StorageCredentials::sas_token(sas_token.to_string())
            .map_err(|e| ElusionError::Custom(format!("Invalid SAS token: {}", e)))?;

        // Create client based on endpoint type
        let client = if endpoint_type == "dfs" {
            let cloud_location = CloudLocation::Public {
                account: account.to_string(),
            };
            ClientBuilder::with_location(cloud_location, credentials)
                .blob_service_client()
                .container_client(container)
        } else {
            ClientBuilder::new(account.to_string(), credentials)
                .blob_service_client()
                .container_client(container)
        };

        Ok((client, blob_name))
    }

    /// Function to write PARQUET to Azure BLOB Storage with overwrite and append modes
    #[cfg(feature = "azure")]
    pub async fn write_parquet_to_azure_with_sas_impl(
        df: &CustomDataFrame,
        mode: &str,
        url: &str,
        sas_token: &str,
    ) -> ElusionResult<()> {
        validate_azure_url(url)?;
        
        let (client, blob_name) = setup_azure_client_impl(url, sas_token)?;
        let blob_client = client.blob_client(&blob_name);

        match mode {
            "overwrite" => {
                // Existing overwrite logic
                let batches: Vec<RecordBatch> = df.df.clone().collect().await
                    .map_err(|e| ElusionError::Custom(format!("Failed to collect DataFrame: {}", e)))?;

                let props = WriterProperties::builder()
                    .set_writer_version(WriterVersion::PARQUET_2_0)
                    .set_compression(Compression::SNAPPY)
                    .set_created_by("Elusion".to_string())
                    .build();

                let mut buffer = Vec::new();
                {
                    let schema = df.df.schema();
                    let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone().into(), Some(props))
                        .map_err(|e| ElusionError::Custom(format!("Failed to create Parquet writer: {}", e)))?;

                    for batch in batches {
                        writer.write(&batch)
                            .map_err(|e| ElusionError::Custom(format!("Failed to write batch to Parquet: {}", e)))?;
                    }
                    writer.close()
                        .map_err(|e| ElusionError::Custom(format!("Failed to close Parquet writer: {}", e)))?;
                }

                upload_to_azure_impl(&blob_client, buffer).await?;
                println!("✅ Successfully overwrote parquet data to Azure blob: {}", url);
            }
            "append" => {
                let ctx = SessionContext::new();
                
                // Try to download existing blob
                match blob_client.get().into_stream().try_collect::<Vec<_>>().await {
                    Ok(chunks) => {
                        // Combine all chunks into a single buffer
                        let mut existing_data = Vec::new();
                        for chunk in chunks {
                            let data = chunk.data.collect().await
                                .map_err(|e| ElusionError::Custom(format!("Failed to collect chunk data: {}", e)))?;
                            existing_data.extend(data);
                        }
                        
                        // Create temporary file to store existing data
                        let temp_file = Builder::new()
                            .prefix("azure_parquet_")
                            .suffix(".parquet")  
                            .tempfile()
                            .map_err(|e| ElusionError::Custom(format!("Failed to create temp file: {}", e)))?;
                        
                        std::fs::write(&temp_file, existing_data)
                            .map_err(|e| ElusionError::Custom(format!("Failed to write to temp file: {}", e)))?;
            
                        let existing_df = ctx.read_parquet(
                            temp_file.path().to_str().unwrap(),
                            ParquetReadOptions::default()
                        ).await.map_err(|e| ElusionError::Custom(
                            format!("Failed to read existing parquet: {}", e)
                        ))?;

                        // Register existing data
                        ctx.register_table(
                            "existing_data",
                            Arc::new(MemTable::try_new(
                                existing_df.schema().clone().into(),
                                vec![existing_df.clone().collect().await.map_err(|e| 
                                    ElusionError::Custom(format!("Failed to collect existing data: {}", e)))?]
                            ).map_err(|e| ElusionError::Custom(
                                format!("Failed to create memory table: {}", e)
                            ))?)
                        ).map_err(|e| ElusionError::Custom(
                            format!("Failed to register existing data: {}", e)
                        ))?;

                        // Register new data
                        ctx.register_table(
                            "new_data",
                            Arc::new(MemTable::try_new(
                                df.df.schema().clone().into(),
                                vec![df.df.clone().collect().await.map_err(|e|
                                    ElusionError::Custom(format!("Failed to collect new data: {}", e)))?]
                            ).map_err(|e| ElusionError::Custom(
                                format!("Failed to create memory table: {}", e)
                            ))?)
                        ).map_err(|e| ElusionError::Custom(
                            format!("Failed to register new data: {}", e)
                        ))?;

                        // Build column list with proper quoting
                        let column_list = existing_df.schema()
                            .fields()
                            .iter()
                            .map(|f| format!("\"{}\"", f.name()))
                            .collect::<Vec<_>>()
                            .join(", ");

                        // Combine data using UNION ALL
                        let sql = format!(
                            "SELECT {} FROM existing_data UNION ALL SELECT {} FROM new_data",
                            column_list, column_list
                        );

                        let combined_df = ctx.sql(&sql).await
                            .map_err(|e| ElusionError::Custom(
                                format!("Failed to combine data: {}", e)
                            ))?;

                        // Convert combined DataFrame to RecordBatches
                        let batches = combined_df.clone().collect().await
                            .map_err(|e| ElusionError::Custom(format!("Failed to collect combined data: {}", e)))?;

                        // Write combined data
                        let props = WriterProperties::builder()
                            .set_writer_version(WriterVersion::PARQUET_2_0)
                            .set_compression(Compression::SNAPPY)
                            .set_created_by("Elusion".to_string())
                            .build();

                        let mut buffer = Vec::new();
                        {
                            let schema = combined_df.schema();
                            let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone().into(), Some(props))
                                .map_err(|e| ElusionError::Custom(format!("Failed to create Parquet writer: {}", e)))?;

                            for batch in batches {
                                writer.write(&batch)
                                    .map_err(|e| ElusionError::Custom(format!("Failed to write batch to Parquet: {}", e)))?;
                            }
                            writer.close()
                                .map_err(|e| ElusionError::Custom(format!("Failed to close Parquet writer: {}", e)))?;
                        }

                        // Upload combined data
                        upload_to_azure_impl(&blob_client, buffer).await?;
                        println!("✅ Successfully appended parquet data to Azure blob: {}", url);
                    }
                    Err(_) => {
                        // If blob doesn't exist, create it with initial data
                        let batches: Vec<RecordBatch> = df.clone().df.collect().await
                            .map_err(|e| ElusionError::Custom(format!("Failed to collect DataFrame: {}", e)))?;

                        let props = WriterProperties::builder()
                            .set_writer_version(WriterVersion::PARQUET_2_0)
                            .set_compression(Compression::SNAPPY)
                            .set_created_by("Elusion".to_string())
                            .build();

                        let mut buffer = Vec::new();
                        {
                            let schema = df.df.schema();
                            let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone().into(), Some(props))
                                .map_err(|e| ElusionError::Custom(format!("Failed to create Parquet writer: {}", e)))?;

                            for batch in batches {
                                writer.write(&batch)
                                    .map_err(|e| ElusionError::Custom(format!("Failed to write batch to Parquet: {}", e)))?;
                            }
                            writer.close()
                                .map_err(|e| ElusionError::Custom(format!("Failed to close Parquet writer: {}", e)))?;
                        }

                        upload_to_azure_impl(&blob_client, buffer).await?;
                        println!("✅ Successfully created initial parquet data in Azure blob: {}", url);
                    }
                }
            }
            _ => return Err(ElusionError::InvalidOperation {
                operation: mode.to_string(),
                reason: "Invalid write mode".to_string(),
                suggestion: "💡 Use 'overwrite' or 'append'".to_string()
            })
        }
    
        Ok(())
    }

    // Helper method for uploading data to Azure
    #[cfg(feature = "azure")]
    async fn upload_to_azure_impl(blob_client: &BlobClient, buffer: Vec<u8>) -> ElusionResult<()> {
        let content = Bytes::from(buffer);
        let content_length = content.len();

        if content_length > 1_073_741_824 {  // 1GB threshold
            let block_id = STANDARD.encode(format!("{:0>20}", 1));

            blob_client
                .put_block(block_id.clone(), content)
                .await
                .map_err(|e| ElusionError::Custom(format!("Failed to upload block to Azure: {}", e)))?;

            let block_list = BlockList {
                blocks: vec![BlobBlockType::Uncommitted(block_id.into_bytes().into())],
            };

            blob_client
                .put_block_list(block_list)
                .content_type("application/parquet")
                .await
                .map_err(|e| ElusionError::Custom(format!("Failed to commit block list: {}", e)))?;
        } else {
            blob_client
                .put_block_blob(content)
                .content_type("application/parquet")
                .await
                .map_err(|e| ElusionError::Custom(format!("Failed to upload blob to Azure: {}", e)))?;
        }

        Ok(())
    }

    /// Function to write JSON to Azure BLOB Storage 
    #[cfg(feature = "azure")]
    pub async fn write_json_to_azure_with_sas_impl(
        df: &CustomDataFrame,
        url: &str,
        sas_token: &str,
        pretty: bool,
    ) -> ElusionResult<()> {
        validate_azure_url(url)?;
        
        let (client, blob_name) = setup_azure_client_impl(url, sas_token)?;
        let blob_client = client.blob_client(&blob_name);
    
        let batches = df.df.clone().collect().await.map_err(|e| 
            ElusionError::InvalidOperation {
                operation: "Data Collection".to_string(),
                reason: format!("Failed to collect DataFrame: {}", e),
                suggestion: "💡 Verify DataFrame is not empty and contains valid data".to_string(),
            }
        )?;
    
        if batches.is_empty() {
            return Err(ElusionError::InvalidOperation {
                operation: "JSON Writing".to_string(),
                reason: "No data to write".to_string(),
                suggestion: "💡 Ensure DataFrame contains data before writing".to_string(),
            });
        }
    
        let mut buffer = Vec::new();
        let mut rows_written = 0;
        {
            let mut writer = BufWriter::new(&mut buffer);
            
            writeln!(writer, "[").map_err(|e| ElusionError::WriteError {
                path: url.to_string(),
                operation: "begin_json".to_string(),
                reason: e.to_string(),
                suggestion: "💡 Check memory allocation".to_string(),
            })?;
        
            let mut first_row = true;
            
            for batch in batches.iter() {
                let row_count = batch.num_rows();
                let column_count = batch.num_columns();
                
                // skip empty batches
                if row_count == 0 || column_count == 0 {
                    continue;
                }
        
                let column_names: Vec<String> = batch.schema().fields().iter()
                    .map(|f| f.name().to_string())
                    .collect();
        
                for row_idx in 0..row_count {
                    if !first_row {
                        writeln!(writer, ",").map_err(|e| ElusionError::WriteError {
                            path: url.to_string(),
                            operation: "write_separator".to_string(),
                            reason: e.to_string(),
                            suggestion: "💡 Check memory allocation".to_string(),
                        })?;
                    }
                    first_row = false;
                    rows_written += 1;
                    // createing  JSON object for the row
                    let mut row_obj = serde_json::Map::new();
                    
                    for col_idx in 0..column_count {
                        let col_name = &column_names[col_idx];
                        let array = batch.column(col_idx);
                        
                        // arrow array value to serde_json::Value
                        let json_value = array_value_to_json(array, row_idx)?;
                        row_obj.insert(col_name.to_string(), json_value);
                    }
                    //  row to JSON
                    let json_value = serde_json::Value::Object(row_obj);
                    
                    if pretty {
                        serde_json::to_writer_pretty(&mut writer, &json_value)
                            .map_err(|e| ElusionError::WriteError {
                                path: url.to_string(),
                                operation: format!("write_row_{}", rows_written),
                                reason: format!("JSON serialization error: {}", e),
                                suggestion: "💡 Check if row contains valid JSON data".to_string(),
                            })?;
                    } else {
                        serde_json::to_writer(&mut writer, &json_value)
                            .map_err(|e| ElusionError::WriteError {
                                path: url.to_string(),
                                operation: format!("write_row_{}", rows_written),
                                reason: format!("JSON serialization error: {}", e),
                                suggestion: "💡 Check if row contains valid JSON data".to_string(),
                            })?;
                    }
                }
            }
 
            writeln!(writer, "\n]").map_err(|e| ElusionError::WriteError {
                path: url.to_string(),
                operation: "end_json".to_string(),
                reason: e.to_string(),
                suggestion: "💡 Check memory allocation".to_string(),
            })?;
        
            writer.flush().map_err(|e| ElusionError::WriteError {
                path: url.to_string(),
                operation: "flush".to_string(),
                reason: e.to_string(),
                suggestion: "💡 Failed to flush data to buffer".to_string(),
            })?;
        } 
    
        // buffer to Bytes for Azure upload
        let content = Bytes::from(buffer);
        
        upload_json_to_azure_impl(&blob_client, content).await?;
        
        println!("✅ Successfully wrote JSON data to Azure blob: {}", url);
        
        if rows_written == 0 {
            println!("*** Warning ***: No rows were written to the blob. Check if this is expected.");
        } else {
            println!("✅ Wrote {} rows to JSON blob", rows_written);
        }
        
        Ok(())
    }

    // Helper method for uploading JSON data to Azure
    #[cfg(feature = "azure")]
    async fn upload_json_to_azure_impl(blob_client: &BlobClient, content: Bytes) -> ElusionResult<()> {
        let content_length = content.len();
    
        if content_length > 1_073_741_824 {  // 1GB threshold
            let block_id = STANDARD.encode(format!("{:0>20}", 1));
    
            blob_client
                .put_block(block_id.clone(), content)
                .await
                .map_err(|e| ElusionError::Custom(format!("Failed to upload block to Azure: {}", e)))?;
    
            let block_list = BlockList {
                blocks: vec![BlobBlockType::Uncommitted(block_id.into_bytes().into())],
            };
    
            blob_client
                .put_block_list(block_list)
                .content_type("application/json")
                .await
                .map_err(|e| ElusionError::Custom(format!("Failed to commit block list: {}", e)))?;
        } else {
            blob_client
                .put_block_blob(content)
                .content_type("application/json")
                .await
                .map_err(|e| ElusionError::Custom(format!("Failed to upload blob to Azure: {}", e)))?;
        }
    
        Ok(())
    }

     /// Aazure function that connects to Azure blob storage
    #[cfg(feature = "azure")]
    pub async fn from_azure_with_sas_token_impl(
        url: &str,
        sas_token: &str,
        filter_keyword: Option<&str>, 
        alias: &str,
    ) -> ElusionResult<CustomDataFrame> {
        
        // const MAX_MEMORY_BYTES: usize = 8 * 1024 * 1024 * 1024; 

        validate_azure_url(url)?;
        
        println!("Starting from_azure_with_sas_token with url={}, alias={}", url, alias);
        // Extract account and container from URL
        let url_parts: Vec<&str> = url.split('/').collect();
        let (account, endpoint_type) = url_parts[2]
            .split('.')
            .next()
            .map(|acc| {
                if url.contains(".dfs.") {
                    (acc, "dfs")
                } else {
                    (acc, "blob")
                }
            })
            .ok_or_else(|| ElusionError::Custom("Invalid URL format".to_string()))?;


        let container = url_parts.get(3)//last()
            .ok_or_else(|| ElusionError::Custom("Invalid URL format".to_string()))?
            .to_string();

        println!("✅ Extracted container='{}'", container);


        let credentials = StorageCredentials::sas_token(sas_token.to_string())
            .map_err(|e| ElusionError::Custom(format!("Invalid SAS token: {}", e)))?;

        println!("✅ Created StorageCredentials");

        let client = if endpoint_type == "dfs" {
            // For ADLS Gen2, create client with cloud location
            let cloud_location = CloudLocation::Public {
                account: account.to_string(),
            };
            ClientBuilder::with_location(cloud_location, credentials)
                .blob_service_client()
                .container_client(container)
        } else {
            ClientBuilder::new(account.to_string(), credentials)
                .blob_service_client()
                .container_client(container)
        };

        println!("✅ Created Azure client, starting to list blobs...");

      

        println!("🔍 Created blob listing stream");

        let list_blobs_future = async {
        let mut blobs: Vec<String> = Vec::new(); 
        let mut total_size: usize = 0;
        let mut stream = client.list_blobs().into_stream();
        
        while let Some(response) = stream.next().await {
            println!("🔍 Got response from blob listing");
            let response = response.map_err(|e| {
                println!("❌ Error listing blobs: {}", e);
                ElusionError::Custom(format!("Failed to list blobs: {}", e))
            })?;
            
            
            for blob in response.blobs.blobs() {
                println!("🔍 Checking blob: {}", blob.name);
                if (blob.name.ends_with(".json") || blob.name.ends_with(".csv")) && 
                filter_keyword.map_or(true, |keyword| blob.name.contains(keyword)) {
                    println!("✅ Adding blob '{}' to download list", blob.name);
                    total_size += blob.properties.content_length as usize;
                    blobs.push(blob.name.clone());
                } else {
                    println!("⏭️ Skipping blob '{}' (doesn't match criteria)", blob.name);
                }
            }
        }
        
        Ok::<(Vec<String>, usize), ElusionError>((blobs, total_size))
    };
    
    //  30-second timeout
    let (blobs, total_size) = match timeout(Duration::from_secs(30), list_blobs_future).await {
        Ok(result) => result?,
        Err(_) => {
            return Err(ElusionError::Custom(
                "Timeout: Blob listing took longer than 30 seconds. Check your network connection and SAS token permissions.".to_string()
            ));
        }
    };

        println!("✅ Finished listing blobs. Found {} matching blobs", blobs.len());

        // // Check total data size against memory limit
        // if total_size > MAX_MEMORY_BYTES {
        //     return Err(ElusionError::Custom(format!(
        //         "Total data size ({} bytes) exceeds maximum allowed memory of {} bytes. 
        //         Please use a machine with more RAM or implement streaming processing.",
        //         total_size, 
        //         MAX_MEMORY_BYTES
        //     )));
        // }

        println!("Total number of blobs to process: {}", blobs.len());
        println!("Total size of blobs: {} bytes", total_size);

        let mut all_data = Vec::new(); 

        let concurrency_limit = num_cpus::get() * 16; 
        let client_ref = &client;
        let results = stream::iter(blobs.iter())
            .map(|blob_name| async move {
                let blob_client = client_ref.blob_client(blob_name);
                let content = blob_client
                    .get_content()
                    .await
                    .map_err(|e| ElusionError::Custom(format!("Failed to get blob content: {}", e)))?;

                println!("Got content for blob: {} ({} bytes)", blob_name, content.len());
                
                if blob_name.ends_with(".json") {
                    process_json_content(&content)
                } else {
                    process_csv_content(blob_name, content).await
                }
            })
            .buffer_unordered(concurrency_limit);

        pin_mut!(results);
        while let Some(result) = results.next().await {
            let mut blob_data = result?;
            all_data.append(&mut blob_data);
        }

        println!("Total records after reading all blobs: {}", all_data.len());

        if all_data.is_empty() {
            return Err(ElusionError::Custom(format!(
                "No valid JSON files found{} (size > 2KB)",
                filter_keyword.map_or("".to_string(), |k| format!(" containing keyword: {}", k))
            )));
        }

        let schema = infer_schema_from_json(&all_data);
        let batch = build_record_batch(&all_data, schema.clone())
            .map_err(|e| ElusionError::Custom(format!("Failed to build RecordBatch: {}", e)))?;

        let ctx = SessionContext::new();
        let mem_table = MemTable::try_new(schema, vec![vec![batch]])
            .map_err(|e| ElusionError::Custom(format!("Failed to create MemTable: {}", e)))?;

        ctx.register_table(alias, Arc::new(mem_table))
            .map_err(|e| ElusionError::Custom(format!("Failed to register table: {}", e)))?;

        let df = ctx.table(alias)
            .await
            .map_err(|e| ElusionError::Custom(format!("Failed to create DataFrame: {}", e)))?;

        let df = lowercase_column_names(df).await?;

        println!("✅ Successfully created and registered in-memory table with alias '{}'", alias);
        // info!("Returning CustomDataFrame for alias '{}'", alias);
        Ok(CustomDataFrame {
            df,
            table_alias: alias.to_string(),
            from_table: alias.to_string(),
            selected_columns: Vec::new(),
            alias_map: Vec::new(),
            aggregations: Vec::new(),
            group_by_columns: Vec::new(),
            where_conditions: Vec::new(),
            having_conditions: Vec::new(),
            order_by_columns: Vec::new(),
            limit_count: None,
            joins: Vec::new(),
            window_functions: Vec::new(),
            ctes: Vec::new(),
            subquery_source: None,
            set_operations: Vec::new(),
            query: String::new(),
            aggregated_df: None,
            union_tables: None,
            original_expressions: Vec::new(),
        })
    }

    #[cfg(feature = "azure")]
pub async fn test_azure_connection_impl(
    url: &str,
    sas_token: &str,
) -> ElusionResult<()> {
    use tokio::time::{timeout, Duration};
    
    println!("🔍 Testing Azure connection...");
    
    validate_azure_url(url)?;
    
    let url_parts: Vec<&str> = url.split('/').collect();
    let (account, endpoint_type) = url_parts[2]
        .split('.')
        .next()
        .map(|acc| {
            if url.contains(".dfs.") {
                (acc, "dfs")
            } else {
                (acc, "blob")
            }
        })
        .ok_or_else(|| ElusionError::Custom("Invalid URL format".to_string()))?;

    let container = url_parts.get(3)
        .ok_or_else(|| ElusionError::Custom("Invalid URL format".to_string()))?
        .to_string();

    println!("✅ Account: {}, Container: {}, Type: {}", account, container, endpoint_type);

    let credentials = StorageCredentials::sas_token(sas_token.to_string())
        .map_err(|e| ElusionError::Custom(format!("Invalid SAS token: {}", e)))?;

    let client = if endpoint_type == "dfs" {
        let cloud_location = CloudLocation::Public {
            account: account.to_string(),
        };
        ClientBuilder::with_location(cloud_location, credentials)
            .blob_service_client()
            .container_client(container)
    } else {
        ClientBuilder::new(account.to_string(), credentials)
            .blob_service_client()
            .container_client(container)
    };

    println!("✅ Created client, testing basic connectivity...");

    // Test with a shorter timeout and simpler operation
    let test_future = async {
        // Try to get container properties first
        match client.get_properties().await {
            Ok(props) => {
                println!("✅ Container properties retrieved successfully");
                println!("   Last modified: {:?}", props.container.last_modified);
                Ok(())
            }
            Err(e) => {
                println!("❌ Failed to get container properties: {}", e);
                Err(ElusionError::Custom(format!("Container access failed: {}", e)))
            }
        }
    };

    match timeout(Duration::from_secs(10), test_future).await {
        Ok(result) => result?,
        Err(_) => {
            return Err(ElusionError::Custom("Timeout: Container access took longer than 10 seconds".to_string()));
        }
    }

    println!("✅ Azure connection test passed!");
    Ok(())
}