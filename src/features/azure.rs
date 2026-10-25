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
#[cfg(feature = "azure")]
use azure_storage_blob::{BlobClient, BlobContainerClient};
#[cfg(feature = "azure")]
use azure_storage_blob::models::BlobClientUploadOptions;

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

    #[cfg(feature = "azure")]
    fn process_json_content(content: &[u8]) -> ElusionResult<Vec<HashMap<String, Value>>> {
        // Skip empty or very small files 
        if content.len() < 3 {
            println!("⚠️ JSON file too small ({} bytes), skipping", content.len());
            return Ok(Vec::new()); 
        }
    
        let content_str = String::from_utf8_lossy(content);
        if content_str.trim().is_empty() {
            println!("⚠️ JSON file is empty or whitespace only, skipping");
            return Ok(Vec::new());
        }

        let reader = BufReader::new(content);
        let stream = Deserializer::from_reader(reader).into_iter::<Value>();
        
        let mut results = Vec::new();
        let mut stream = stream.peekable();
        //checking if json is valid 
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
                        Err(e) => {
                            println!("⚠️ Warning: JSON parsing error in file: {}", e);
                        
                            continue;
                        }
                    }
                }
            }
            Some(Ok(Value::Object(_))) => {
                for value in stream {
                    match value {
                        Ok(Value::Object(map)) => {
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
                        Err(e) => {
                            println!("⚠️ Warning: JSON parsing error in file: {}", e);
                        
                            continue;
                        }
                        _ => continue,
                    }
                }
            }
            Some(Ok(Value::Null)) | 
            Some(Ok(Value::Bool(_))) |
            Some(Ok(Value::Number(_))) |
            Some(Ok(Value::String(_))) => {
                println!("⚠️ Warning: JSON content is not an array or object, skipping");
                return Ok(Vec::new()); 
            }
            Some(Err(e)) => {
                println!("⚠️ Warning: JSON parsing error: {}, skipping file", e);
                return Ok(Vec::new()); 
            }
            None => {
                println!("⚠️ Warning: Empty JSON content, skipping file");
                return Ok(Vec::new()); 
            }
        }

        if results.is_empty() {
            println!("⚠️ Warning: No valid JSON data found in file, but continuing...");
        }
        
        Ok(results) // Always return Ok() to continue processing other files
    }

    #[cfg(feature = "azure")]
    async fn process_csv_content(_name: &str, content: Vec<u8>) -> ElusionResult<Vec<HashMap<String, Value>>> {
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .trim(All)
            .from_reader(content.as_slice());

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

    // Splits https://{account}.{endpoint}.core.windows.net/{container}/{blob} into (account, container, blob)
    #[cfg(feature = "azure")]
    fn parse_azure_url_impl(url: &str) -> ElusionResult<(String, String, String)> {
        let url_parts: Vec<&str> = url.split('/').collect();
        if url_parts.len() < 4 {
            return Err(ElusionError::Custom(
                "Invalid URL format. Expected format: https://{account}.{endpoint}.core.windows.net/{container}/{blob}".to_string()
            ));
        }

        let account = url_parts[2]
            .split('.')
            .next()
            .filter(|acc| !acc.is_empty())
            .ok_or_else(|| ElusionError::Custom("Invalid URL format: cannot extract account name".to_string()))?;

        let container = url_parts[3];
        if container.is_empty() {
            return Err(ElusionError::Custom("Container name cannot be empty".to_string()));
        }

        let blob_name = url_parts.get(4..).map(|parts| parts.join("/")).unwrap_or_default();

        Ok((account.to_string(), container.to_string(), blob_name))
    }

    // Builds https://{account}.blob.core.windows.net/{container}[/{blob}]?{sas}
    // Both blob and dfs URLs are served through the blob endpoint
    #[cfg(feature = "azure")]
    fn build_azure_sas_url_impl(account: &str, container: &str, blob_name: Option<&str>, sas_token: &str) -> ElusionResult<url::Url> {
        let mut sas_url = url::Url::parse(&format!("https://{}.blob.core.windows.net", account))
            .map_err(|e| ElusionError::Custom(format!("Invalid Azure account name '{}': {}", account, e)))?;
        {
            let mut segments = sas_url.path_segments_mut()
                .map_err(|_| ElusionError::Custom("Invalid Azure endpoint URL".to_string()))?;
            segments.push(container);
            if let Some(blob_name) = blob_name {
                segments.extend(blob_name.split('/'));
            }
        }
        sas_url.set_query(Some(sas_token.trim_start_matches('?')));
        Ok(sas_url)
    }

    #[cfg(feature = "azure")]
    fn azure_container_client_impl(account: &str, container: &str, sas_token: &str) -> ElusionResult<BlobContainerClient> {
        let container_url = build_azure_sas_url_impl(account, container, None, sas_token)?;
        BlobContainerClient::new(container_url, None, None)
            .map_err(|e| ElusionError::Custom(format!("Failed to create Azure container client: {}", e)))
    }

    #[cfg(feature = "azure")]
    fn azure_blob_client_impl(account: &str, container: &str, blob_name: &str, sas_token: &str) -> ElusionResult<BlobClient> {
        let blob_url = build_azure_sas_url_impl(account, container, Some(blob_name), sas_token)?;
        BlobClient::new(blob_url, None, None)
            .map_err(|e| ElusionError::Custom(format!("Failed to create Azure blob client: {}", e)))
    }

    // Downloads the whole blob; the SDK splits large blobs into parallel range requests
    #[cfg(feature = "azure")]
    async fn download_from_azure_impl(blob_client: &BlobClient) -> Result<Vec<u8>, String> {
        let response = blob_client.download(None).await.map_err(|e| e.to_string())?;
        let content = response.body.collect().await.map_err(|e| e.to_string())?;
        Ok(content.to_vec())
    }

    #[cfg(feature = "azure")]
    fn setup_azure_client_impl(url: &str, sas_token: &str) -> ElusionResult<BlobClient> {
        let (account, container, blob_name) = parse_azure_url_impl(url)?;
        if blob_name.is_empty() {
            return Err(ElusionError::Custom("Blob name cannot be empty".to_string()));
        }
        //  SAS token expiry
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

        azure_blob_client_impl(&account, &container, &blob_name, sas_token)
    }

    /// Function to write PARQUET to Azure BLOB Storage with overwrite and append modes
    #[cfg(feature = "azure")]
    pub async fn write_parquet_to_azure_with_sas_impl(
        df: &CustomDataFrame,
        mode: &str,
        url: &str,
        sas_token: &str,
    ) -> ElusionResult<()> {

        if !url.ends_with(".parquet") {
            return Err(ElusionError::Custom(
                "❌ Invalid file extension. Parquet files must end with '.parquet'".to_string()
            ));
        }

        validate_azure_url(url)?;

        let blob_client = setup_azure_client_impl(url, sas_token)?;

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

                upload_to_azure_impl(&blob_client, Bytes::from(buffer), "application/parquet").await?;
                println!("✅ Successfully overwrote parquet data to Azure blob: {}", url);
            }
            "append" => {
                let ctx = SessionContext::new();
                
                // Try to download existing blob
                match download_from_azure_impl(&blob_client).await {
                    Ok(existing_data) => {
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

                        let column_list = existing_df.schema()
                            .fields()
                            .iter()
                            .map(|f| format!("\"{}\"", f.name()))
                            .collect::<Vec<_>>()
                            .join(", ");

                        let sql = format!(
                            "SELECT {} FROM existing_data UNION ALL SELECT {} FROM new_data",
                            column_list, column_list
                        );

                        let combined_df = ctx.sql(&sql).await
                            .map_err(|e| ElusionError::Custom(
                                format!("Failed to combine data: {}", e)
                            ))?;

                        let batches = combined_df.clone().collect().await
                            .map_err(|e| ElusionError::Custom(format!("Failed to collect combined data: {}", e)))?;

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

                        upload_to_azure_impl(&blob_client, Bytes::from(buffer), "application/parquet").await?;
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

                        upload_to_azure_impl(&blob_client, Bytes::from(buffer), "application/parquet").await?;
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

    // Helper method for uploading data to Azure (overwrites existing blob)
    // The SDK stages large content as parallel blocks and commits them automatically
    #[cfg(feature = "azure")]
    async fn upload_to_azure_impl(blob_client: &BlobClient, content: Bytes, content_type: &str) -> ElusionResult<()> {
        let options = BlobClientUploadOptions {
            blob_content_type: Some(content_type.to_string()),
            ..Default::default()
        };

        blob_client
            .upload(content.into(), Some(options))
            .await
            .map_err(|e| ElusionError::Custom(format!("Failed to upload blob to Azure: {}", e)))?;

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

        if !url.ends_with(".json") {
            return Err(ElusionError::Custom(
                "❌ Invalid file extension. JSON files must end with '.json'".to_string()
            ));
        }

        validate_azure_url(url)?;

        let blob_client = setup_azure_client_impl(url, sas_token)?;

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
        
        upload_to_azure_impl(&blob_client, content, "application/json").await?;
        
        println!("✅ Successfully wrote JSON data to Azure blob: {}", url);
        
        if rows_written == 0 {
            println!("*** Warning ***: No rows were written to the blob. Check if this is expected.");
        } else {
            println!("✅ Wrote {} rows to JSON blob", rows_written);
        }
        
        Ok(())
    }


    /// Azure function that connects to Azure blob storage with smart direct access
    #[cfg(feature = "azure")]
    pub async fn from_azure_with_sas_token_impl(
        url: &str,
        sas_token: &str,
        filter_keyword: Option<&str>, 
        alias: &str,
    ) -> ElusionResult<CustomDataFrame> {
        // check for csv and json
        let is_direct_path = filter_keyword
            .map(|path| path.ends_with(".json") || path.ends_with(".csv"))
            .unwrap_or(false);
        
        if is_direct_path {
            // DIRECT BLOB ACCESS MODE 
            let blob_path = filter_keyword.unwrap(); 
            println!("🎯 Direct blob access mode for: {}", blob_path);
            
            let (account, container, _) = parse_azure_url_impl(url)?;
            let blob_client = azure_blob_client_impl(&account, &container, blob_path, sas_token)?;

            let content = download_from_azure_impl(&blob_client)
                .await
                .map_err(|e| ElusionError::Custom(format!("Failed to get blob '{}': {}", blob_path, e)))?;

            println!("✅ Retrieved blob content ({} bytes)", content.len());
            
            let mut all_data = Vec::new();
            
            if blob_path.ends_with(".json") {
                all_data = process_json_content(&content)?;
            } else if blob_path.ends_with(".csv") {
                all_data = process_csv_content(blob_path, content).await?;
            }

            if all_data.is_empty() {
                return Err(ElusionError::Custom(
                    "No valid data found in blob".to_string()
                ));
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

            println!("✅ Successfully loaded {} records from specific blob", all_data.len());
            
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
                needs_normalization: false,
                raw_selected_columns: Vec::new(),
                raw_group_by_columns: Vec::new(),
                raw_where_conditions: Vec::new(),
                raw_having_conditions: Vec::new(),
                raw_join_conditions: Vec::new(),
                raw_aggregations: Vec::new(),
                uses_group_by_all: false
            })
            
        } else {
            //  BLOB LISTING MODE 
            validate_azure_url(url)?;
            
            println!("🔍 Blob listing mode{}", 
                if let Some(filter) = filter_keyword {
                    format!(" with filter: {}", filter)
                } else {
                    String::new()
                }
            );
            
            println!("Starting from_azure_with_sas_token with url={}, alias={}", url, alias);

            let (account, container, _) = parse_azure_url_impl(url)?;

            println!("✅ Extracted container='{}'", container);

            let client = azure_container_client_impl(&account, &container, sas_token)?;

            println!("✅ Created Azure client, starting to list blobs...");

            let list_blobs_future = async {
            let mut blobs: Vec<String> = Vec::new();
            let mut total_size: usize = 0;
            let mut pager = client.list_blobs(None)
                .map_err(|e| ElusionError::Custom(format!("Failed to list blobs: {}", e)))?;

            println!("🔍 Created blob listing stream");

            while let Some(blob) = pager.try_next().await.map_err(|e| {
                println!("❌ Error listing blobs: {}", e);
                ElusionError::Custom(format!("Failed to list blobs: {}", e))
            })? {
                let Some(name) = blob.name else { continue };
                println!("🔍 Checking blob: {}", name);
                if (name.ends_with(".json") || name.ends_with(".csv")) &&
                filter_keyword.map_or(true, |keyword| name.contains(keyword)) {
                    println!("✅ Adding blob '{}' to download list", name);
                    total_size += blob.properties
                        .and_then(|p| p.content_length)
                        .unwrap_or(0) as usize;
                    blobs.push(name);
                } else {
                    println!("⏭️ Skipping blob '{}' (doesn't match criteria)", name);
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

            println!("Total number of blobs to process: {}", blobs.len());
            println!("Total size of blobs: {} bytes", total_size);

            let mut all_data = Vec::new(); 

            let concurrency_limit = num_cpus::get() * 16;
            let results = stream::iter(blobs.into_iter())
                .map(|blob_name| {
                    let blob_client = azure_blob_client_impl(&account, &container, &blob_name, sas_token);
                    async move {
                        let content = download_from_azure_impl(&blob_client?)
                            .await
                            .map_err(|e| ElusionError::Custom(format!("Failed to get blob content: {}", e)))?;

                        println!("Got content for blob: {} ({} bytes)", blob_name, content.len());

                        if blob_name.ends_with(".json") {
                            process_json_content(&content)
                        } else {
                            process_csv_content(&blob_name, content).await
                        }
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
                needs_normalization: false,
                raw_selected_columns: Vec::new(),
                raw_group_by_columns: Vec::new(),
                raw_where_conditions: Vec::new(),
                raw_having_conditions: Vec::new(),
                raw_join_conditions: Vec::new(),
                raw_aggregations: Vec::new(),
                uses_group_by_all: false
            })
        }
    }

