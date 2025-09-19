#[cfg(feature = "fabric")]
use crate::prelude::*;
#[cfg(feature = "fabric")]
use crate::ElusionError;
#[cfg(feature = "fabric")]
use crate::ElusionResult;
#[cfg(feature = "fabric")]
use crate::CustomDataFrame;
#[cfg(feature = "fabric")]
use crate::array_value_to_json;
#[cfg(feature = "fabric")]
use reqwest;
#[cfg(feature = "fabric")]
use std::io::{BufWriter, Write};
#[cfg(feature = "fabric")]
use arrow::record_batch::RecordBatch;
#[cfg(feature = "fabric")]
use parquet::arrow::ArrowWriter;
#[cfg(feature = "fabric")]
use parquet::file::properties::{WriterProperties, WriterVersion};
#[cfg(feature = "fabric")]
use parquet::basic::Compression;

#[cfg(feature = "fabric")]
#[derive(Debug, Clone)]
pub struct ParsedOneLakePath {
    pub workspace_id: String,
    pub lakehouse_id: Option<String>,
    pub warehouse_id: Option<String>,
    pub base_path: String,
    pub is_lakehouse: bool,
}

#[cfg(feature = "fabric")]
#[derive(Debug, Clone)]
pub struct OneLakeConfig {
    pub workspace_id: String,
    pub lakehouse_id: Option<String>,
    pub warehouse_id: Option<String>,
}

#[cfg(feature = "fabric")]
pub struct OneLakeClient {
    config: OneLakeConfig,
    access_token: Option<String>,
    fabric_token: Option<String>,
}

#[cfg(feature = "fabric")]
impl OneLakeClient {
    pub fn new(config: OneLakeConfig) -> Self {
        Self {
            config,
            access_token: None,
            fabric_token: None,
        }
    }

    // Authentication using Azure CLI
    pub async fn authenticate(&mut self) -> ElusionResult<()> {
        println!("🔍 Authenticating with Azure CLI for Fabric - OneLake access...");
        
        // Try direct Python approach first
        match self.execute_az_via_python(&["--version"]).await {
            Ok(version_output) => {
                if version_output.status.success() {
                    println!("✅ Azure CLI via Python works");
                    
                    // Check if logged in and get tokens
                    if self.get_tokens_via_python().await.is_ok() {
                        println!("✅ Successfully authenticated with Fabric - OneLake!");
                        return Ok(());
                    }
                }
            },
            Err(_) => {}
        }

        // Fallback to standard Azure CLI paths
        let az_paths = self.get_azure_cli_paths();
        
        for az_path in az_paths.iter() {
            if let Ok(output) = std::process::Command::new(az_path)
                .args(["--version"])
                .env("PYTHONIOENCODING", "utf-8")
                .env("PYTHONUTF8", "1")
                .output() 
            {
                if output.status.success() && self.get_tokens_via_cli(az_path).await.is_ok() {
                    println!("✅ Successfully authenticated with Fabric - OneLake!");
                    return Ok(());
                }
            }
        }

        Err(ElusionError::Custom(
            "Fabric - OneLake authentication failed. Please run 'az login' and ensure you have access to Microsoft Fabric.".to_string()
        ))
    }

    async fn get_tokens_via_python(&mut self) -> ElusionResult<()> {
        // Check if logged in
        match self.execute_az_via_python(&["account", "show"]).await {
            Ok(account_output) => {
                if account_output.status.success() {
                    // Get Azure Storage token (required for OneLake data access)
                    if let Ok(token_output) = self.execute_az_via_python(&["account", "get-access-token", "--resource", "https://storage.azure.com/", "--output", "json"]).await {
                        if token_output.status.success() {
                            let token_json = String::from_utf8_lossy(&token_output.stdout);
                            if let Ok(token_data) = serde_json::from_str::<serde_json::Value>(&token_json) {
                                if let Some(access_token) = token_data["accessToken"].as_str() {
                                    self.access_token = Some(access_token.to_string());
                                }
                            }
                        }
                    }

                    // Get Fabric API token (for workspace/lakehouse discovery if needed)
                    if let Ok(fabric_output) = self.execute_az_via_python(&["account", "get-access-token", "--resource", "https://api.fabric.microsoft.com/", "--output", "json"]).await {
                        if fabric_output.status.success() {
                            let fabric_json = String::from_utf8_lossy(&fabric_output.stdout);
                            if let Ok(fabric_data) = serde_json::from_str::<serde_json::Value>(&fabric_json) {
                                if let Some(fabric_token) = fabric_data["accessToken"].as_str() {
                                    self.fabric_token = Some(fabric_token.to_string());
                                }
                            }
                        }
                    }

                    // Only require storage token for basic file operations
                    if self.access_token.is_some() {
                        return Ok(());
                    }
                }
            },
            Err(_) => {}
        }
        Err(ElusionError::Custom("Failed to get tokens via Python".to_string()))
    }

    async fn get_tokens_via_cli(&mut self, az_path: &str) -> ElusionResult<()> {
        // Check login and get tokens
        if let Ok(account_output) = std::process::Command::new(az_path)
            .args(["account", "show"])
            .env("PYTHONIOENCODING", "utf-8")
            .env("PYTHONUTF8", "1")
            .output() 
        {
            if account_output.status.success() {
                // Get Azure Storage token (required for OneLake data access)
                if let Ok(storage_token_output) = std::process::Command::new(az_path)
                    .args(["account", "get-access-token", "--resource", "https://storage.azure.com/", "--output", "json"])
                    .env("PYTHONIOENCODING", "utf-8")
                    .env("PYTHONUTF8", "1")
                    .output()
                {
                    if storage_token_output.status.success() {
                        let token_json = String::from_utf8_lossy(&storage_token_output.stdout);
                        if let Ok(token_data) = serde_json::from_str::<serde_json::Value>(&token_json) {
                            if let Some(access_token) = token_data["accessToken"].as_str() {
                                self.access_token = Some(access_token.to_string());
                            }
                        }
                    }
                }

                // Get Fabric token (optional for basic file operations)
                if let Ok(fabric_token_output) = std::process::Command::new(az_path)
                    .args(["account", "get-access-token", "--resource", "https://api.fabric.microsoft.com/", "--output", "json"])
                    .env("PYTHONIOENCODING", "utf-8")
                    .env("PYTHONUTF8", "1")
                    .output()
                {
                    if fabric_token_output.status.success() {
                        let fabric_json = String::from_utf8_lossy(&fabric_token_output.stdout);
                        if let Ok(fabric_data) = serde_json::from_str::<serde_json::Value>(&fabric_json) {
                            if let Some(fabric_token) = fabric_data["accessToken"].as_str() {
                                self.fabric_token = Some(fabric_token.to_string());
                            }
                        }
                    }
                }

                // Only require storage token for basic file operations
                if self.access_token.is_some() {
                    return Ok(());
                }
            }
        }
        Err(ElusionError::Custom("Failed to get tokens via CLI".to_string()))
    }

    async fn execute_az_via_python(&self, args: &[&str]) -> ElusionResult<std::process::Output> {
        let python_path = if cfg!(target_os = "windows") {
            r#"C:\Program Files\Microsoft SDKs\Azure\CLI2\python.exe"#
        } else {
            "python3"
        };
        
        if !std::path::Path::new(python_path).exists() {
            return Err(ElusionError::Custom("Azure CLI Python not found".to_string()));
        }
        
        let mut full_args = vec!["-X", "utf8", "-m", "azure.cli"];
        full_args.extend(args);
        
        std::process::Command::new(python_path)
            .args(&full_args)
            .env("PYTHONIOENCODING", "utf-8")
            .env("PYTHONUTF8", "1")
            .output()
            .map_err(|e| ElusionError::Custom(format!("Failed to execute Azure CLI via Python: {}", e)))
    }

    fn get_azure_cli_paths(&self) -> Vec<&'static str> {
        if cfg!(target_os = "windows") {
            vec![
                r#"C:\Program Files\Microsoft SDKs\Azure\CLI2\python.exe"#,
                "az.cmd",
                "az.exe",
                "C:\\Program Files\\Microsoft SDKs\\Azure\\CLI2\\wbin\\az.cmd",
                "C:\\Program Files (x86)\\Microsoft SDKs\\Azure\\CLI2\\wbin\\az.cmd",
            ]
        } else if cfg!(target_os = "macos") {
            vec![
                "az",
                "/usr/local/bin/az",
                "/opt/homebrew/bin/az",
            ]
        } else {
            vec![
                "az",
                "/usr/local/bin/az",
                "/usr/bin/az",
                "/home/$USER/.local/bin/az",
            ]
        }
    }

    fn build_onelake_read_url(&self, file_path: &str) -> ElusionResult<String> {
        let clean_path = file_path.trim_start_matches('/');
        
        if let Some(lakehouse_id) = &self.config.lakehouse_id {
            Ok(format!(
                "https://onelake.dfs.fabric.microsoft.com/{}/{}/Files/{}", 
                self.config.workspace_id,
                lakehouse_id,
                clean_path
            ))
        } else if let Some(warehouse_id) = &self.config.warehouse_id {
            Ok(format!(
                "https://onelake.dfs.fabric.microsoft.com/{}/{}/Files/{}", 
                self.config.workspace_id,
                warehouse_id,
                clean_path
            ))
        } else {
            Err(ElusionError::Custom("Either lakehouse_id or warehouse_id must be specified".to_string()))
        }
    }

    fn build_onelake_write_url(&self, file_path: &str) -> ElusionResult<String> {
        let clean_path = file_path.trim_start_matches('/');
        
        if let Some(lakehouse_id) = &self.config.lakehouse_id {
            let is_guid = lakehouse_id.len() == 36 && 
                         lakehouse_id.chars().filter(|&c| c == '-').count() == 4;
            
            if is_guid {

                Ok(format!(
                    "https://onelake.dfs.fabric.microsoft.com/{}/{}/Files/{}", 
                    self.config.workspace_id,
                    lakehouse_id,
                    clean_path
                ))
            } else {
                Ok(format!(
                    "https://onelake.dfs.fabric.microsoft.com/{}/{}.Lakehouse/Files/{}", 
                    self.config.workspace_id,
                    lakehouse_id,
                    clean_path
                ))
            }
        } else if let Some(warehouse_id) = &self.config.warehouse_id {
            let is_guid = warehouse_id.len() == 36 && 
                         warehouse_id.chars().filter(|&c| c == '-').count() == 4;
            
            if is_guid {
                Ok(format!(
                    "https://onelake.dfs.fabric.microsoft.com/{}/{}/Files/{}", 
                    self.config.workspace_id,
                    warehouse_id,
                    clean_path
                ))
            } else {
                Ok(format!(
                    "https://onelake.dfs.fabric.microsoft.com/{}/{}.Warehouse/Files/{}", 
                    self.config.workspace_id,
                    warehouse_id,
                    clean_path
                ))
            }
        } else {
            Err(ElusionError::Custom("Either lakehouse_id or warehouse_id must be specified".to_string()))
        }
    }

    // Download file from OneLake
    pub async fn download_file(&mut self, file_path: &str) -> ElusionResult<Vec<u8>> {
        self.authenticate().await?;
        let token = self.access_token.as_ref()
            .ok_or_else(|| ElusionError::Custom("Not authenticated".to_string()))?;

        let onelake_url = self.build_onelake_read_url(file_path)?;

        println!("📥 Downloading file from OneLake: {}", file_path);

        let response = reqwest::Client::new()
            .get(&onelake_url)
            .bearer_auth(token)
            .header("x-ms-version", "2020-06-12")
            .send()
            .await
            .map_err(|e| ElusionError::Custom(format!("Failed to download file: {}", e)))?;

        if response.status().is_success() {
            let content = response.bytes().await
                .map_err(|e| ElusionError::Custom(format!("Failed to read file content: {}", e)))?;
            
            println!("✅ Successfully downloaded {} bytes", content.len());
            Ok(content.to_vec())
        } else {
            Err(ElusionError::Custom(format!("Failed to download file '{}': HTTP {}", file_path, response.status())))
        }
    }

    // Upload file to OneLake
    pub async fn upload_file(&mut self, file_path: &str, content: Vec<u8>) -> ElusionResult<()> {
        self.authenticate().await?;
        let token = self.access_token.as_ref()
            .ok_or_else(|| ElusionError::Custom("Not authenticated".to_string()))?;

        let onelake_url = self.build_onelake_write_url(file_path)?;  // Use write URL
        
        // For ADLS Gen2, we need to use the query parameter for the resource type
        let create_url = format!("{}?resource=file", onelake_url);

        println!("📤 Uploading file to OneLake: {}", file_path);

        // First, create the file
        let create_response = reqwest::Client::new()
            .put(&create_url)
            .bearer_auth(&token)
            .header("x-ms-version", "2020-06-12")
            .header("Content-Length", "0")
            .send()
            .await
            .map_err(|e| ElusionError::Custom(format!("Failed to create file: {}", e)))?;

        if !create_response.status().is_success() {
            let status = create_response.status();
            let error_text = create_response.text().await.unwrap_or_else(|_| "No error details".to_string());
            return Err(ElusionError::Custom(format!(
                "Failed to create file '{}': HTTP {} - {}", 
                file_path, 
                status, 
                error_text
            )));
        }

        // Then, append the data
        let append_url = format!("{}?action=append&position=0", onelake_url);
        
        let append_response = reqwest::Client::new()
            .patch(&append_url)
            .bearer_auth(&token)
            .header("x-ms-version", "2020-06-12")
            .header("Content-Length", content.len().to_string())
            .body(content.clone())
            .send()
            .await
            .map_err(|e| ElusionError::Custom(format!("Failed to append data: {}", e)))?;

        if !append_response.status().is_success() {
            let status = append_response.status();
            let error_text = append_response.text().await.unwrap_or_else(|_| "No error details".to_string());
            return Err(ElusionError::Custom(format!(
                "Failed to append data to '{}': HTTP {} - {}", 
                file_path, 
                status, 
                error_text
            )));
        }

        // Finally, flush the file
        let flush_url = format!("{}?action=flush&position={}", onelake_url, content.len());
        
        let flush_response = reqwest::Client::new()
            .patch(&flush_url)
            .bearer_auth(&token)
            .header("x-ms-version", "2020-06-12")
            .header("Content-Length", "0")
            .send()
            .await
            .map_err(|e| ElusionError::Custom(format!("Failed to flush file: {}", e)))?;

        if flush_response.status().is_success() {
            println!("✅ Successfully uploaded file to OneLake");
            Ok(())
        } else {
            let status = flush_response.status();
            let error_text = flush_response.text().await.unwrap_or_else(|_| "No error details".to_string());
            Err(ElusionError::Custom(format!(
                "Failed to flush file '{}': HTTP {} - {}", 
                file_path, 
                status, 
                error_text
            )))
        }
    }

    // Create client with auto-detected credentials
    pub async fn new_with_cli_auth(
        workspace_id: String,
        lakehouse_id: Option<String>,
        warehouse_id: Option<String>,
    ) -> ElusionResult<Self> {

        let config = OneLakeConfig {
            workspace_id,
            lakehouse_id,
            warehouse_id,
        };

        Ok(OneLakeClient::new(config))
    }

    // Parse ABFSS path to extract workspace_id, lakehouse/warehouse_id, and path
    fn parse_abfss_path(abfss_path: &str) -> ElusionResult<ParsedOneLakePath> {
        if !abfss_path.starts_with("abfss://") {
            return Err(ElusionError::Custom(
                "Invalid ABFSS path format. Expected: abfss://workspace_id@onelake.dfs.fabric.microsoft.com/lakehouse_id/Files/...".to_string()
            ));
        }

        if !abfss_path.contains("@onelake.dfs.fabric.microsoft.com") {
            return Err(ElusionError::Custom(
                "Invalid OneLake ABFSS path. Must contain '@onelake.dfs.fabric.microsoft.com'".to_string()
            ));
        }

        let path_without_prefix = abfss_path.trim_start_matches("abfss://");
        let parts: Vec<&str> = path_without_prefix.split('@').collect();
        if parts.len() != 2 {
            return Err(ElusionError::Custom(
                "Invalid ABFSS path format. Cannot extract workspace ID".to_string()
            ));
        }

        let workspace_id = parts[0].to_string();
        let remaining_parts: Vec<&str> = parts[1].split('/').collect();
        
        if remaining_parts.len() < 3 {
            return Err(ElusionError::Custom(
                "Invalid ABFSS path format. Missing lakehouse/warehouse ID or Files path".to_string()
            ));
        }

        let lakehouse_warehouse_part = remaining_parts[1];
        let base_path = remaining_parts[2..].join("/");

        // Check if it's a GUID (36 characters with hyphens)
        let is_guid = lakehouse_warehouse_part.len() == 36 && 
                     lakehouse_warehouse_part.chars().filter(|&c| c == '-').count() == 4;

        let (lakehouse_id, warehouse_id, is_lakehouse) = if lakehouse_warehouse_part.ends_with(".Lakehouse") {
            let id = lakehouse_warehouse_part.trim_end_matches(".Lakehouse").to_string();
            (Some(id), None, true)
        } else if lakehouse_warehouse_part.ends_with(".Warehouse") {
            let id = lakehouse_warehouse_part.trim_end_matches(".Warehouse").to_string();
            (None, Some(id), false)
        } else if is_guid {
            // If it's a GUID without suffix, assume it's a lakehouse
            (Some(lakehouse_warehouse_part.to_string()), None, true)
        } else {
            // Otherwise, it might be a friendly name - still assume lakehouse
            (Some(lakehouse_warehouse_part.to_string()), None, true)
        };

        Ok(ParsedOneLakePath {
            workspace_id,
            lakehouse_id,
            warehouse_id,
            base_path,
            is_lakehouse,
        })
    }
}

// ABFSS Path Implementation - Load from OneLake
#[cfg(feature = "fabric")]
pub async fn load_from_fabric_abfss_impl(
    abfss_path: &str,
    file_path: &str,
    alias: &str,
) -> ElusionResult<CustomDataFrame> {
    // Parse the ABFSS path
    let parsed = OneLakeClient::parse_abfss_path(abfss_path)?;
    
    println!("Parsed Fabric - OneLake path:");
    println!("  Workspace ID: {}", parsed.workspace_id);
    if parsed.is_lakehouse {
        println!("  Lakehouse ID: {}", parsed.lakehouse_id.as_ref().unwrap());
    } else {
        println!("  Warehouse ID: {}", parsed.warehouse_id.as_ref().unwrap());
    }
    println!("  Base Path: {}", parsed.base_path);
    println!("  File: {}", file_path);

    // Create client with auto-detected credentials
    let mut client = OneLakeClient::new_with_cli_auth(
        parsed.workspace_id,
        parsed.lakehouse_id,
        parsed.warehouse_id,
    ).await?;

    // Build full file path
    let full_file_path = if parsed.base_path == "Files" || parsed.base_path.is_empty() {
        file_path.to_string()
    } else {
        format!("{}/{}", parsed.base_path.trim_start_matches("Files/"), file_path)
    };

    // Download content from OneLake
    let content = client.download_file(&full_file_path).await?;

    // Write to temporary file
    let temp_dir = std::env::temp_dir();
    let file_extension = file_path
        .split('.')
        .last()
        .unwrap_or("tmp")
        .to_lowercase();
    
    let temp_file = temp_dir.join(format!(
        "onelake_{}_{}.{}", 
        alias,
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
        file_extension
    ));
    
    std::fs::write(&temp_file, content)
        .map_err(|e| ElusionError::Custom(format!("Failed to write temporary file: {}", e)))?;

    // Use the existing load function from CustomDataFrame
    let aliased_df = CustomDataFrame::load(
        temp_file.to_str().unwrap(),
        alias
    ).await?;

    let _ = std::fs::remove_file(temp_file);

    Ok(CustomDataFrame {
        df: aliased_df.dataframe,
        table_alias: aliased_df.alias.clone(),
        from_table: aliased_df.alias.clone(),
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
        uses_group_by_all: false,
    })
}

// Write Parquet to OneLake using ABFSS path
#[cfg(feature = "fabric")]
pub async fn write_parquet_to_fabric_abfss_impl(
    df: &CustomDataFrame,
    abfss_path: &str,
    file_path: &str,
) -> ElusionResult<()> {
    if !file_path.ends_with(".parquet") {
        return Err(ElusionError::Custom(
            "Invalid file extension. Parquet files must end with '.parquet'".to_string()
        ));
    }

    // Parse the ABFSS path
    let parsed = OneLakeClient::parse_abfss_path(abfss_path)?;

    // Create client with auto-detected credentials
    let mut client = OneLakeClient::new_with_cli_auth(
        parsed.workspace_id,
        parsed.lakehouse_id,
        parsed.warehouse_id,
    ).await?;

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

    // Build full file path
    let full_file_path = if parsed.base_path == "Files" || parsed.base_path.is_empty() {
        file_path.to_string()
    } else {
        format!("{}/{}", parsed.base_path.trim_start_matches("Files/"), file_path)
    };

    client.upload_file(&full_file_path, buffer).await?;
    println!("Successfully wrote Parquet data to OneLake: {}", file_path);

    Ok(())
}

// Write JSON to OneLake using ABFSS path
#[cfg(feature = "fabric")]
pub async fn write_json_to_fabric_abfss_impl(
    df: &CustomDataFrame,
    abfss_path: &str,
    file_path: &str,
    pretty: bool,
) -> ElusionResult<()> {
    if !file_path.ends_with(".json") {
        return Err(ElusionError::Custom(
            "Invalid file extension. JSON files must end with '.json'".to_string()
        ));
    }

    // Parse the ABFSS path
    let parsed = OneLakeClient::parse_abfss_path(abfss_path)?;

    // Create client with auto-detected credentials
    let mut client = OneLakeClient::new_with_cli_auth(
        parsed.workspace_id,
        parsed.lakehouse_id,
        parsed.warehouse_id,
    ).await?;

    let batches = df.df.clone().collect().await.map_err(|e| 
        ElusionError::InvalidOperation {
            operation: "Data Collection".to_string(),
            reason: format!("Failed to collect DataFrame: {}", e),
            suggestion: "Verify DataFrame is not empty and contains valid data".to_string(),
        }
    )?;

    if batches.is_empty() {
        return Err(ElusionError::InvalidOperation {
            operation: "JSON Writing".to_string(),
            reason: "No data to write".to_string(),
            suggestion: "Ensure DataFrame contains data before writing".to_string(),
        });
    }

    let mut buffer = Vec::new();
    let mut rows_written = 0;
    {
        let mut writer = BufWriter::new(&mut buffer);
        
        writeln!(writer, "[").map_err(|e| ElusionError::WriteError {
            path: file_path.to_string(),
            operation: "begin_json".to_string(),
            reason: e.to_string(),
            suggestion: "Check memory allocation".to_string(),
        })?;
    
        let mut first_row = true;
        
        for batch in batches.iter() {
            let row_count = batch.num_rows();
            let column_count = batch.num_columns();
            
            if row_count == 0 || column_count == 0 {
                continue;
            }
    
            let column_names: Vec<String> = batch.schema().fields().iter()
                .map(|f| f.name().to_string())
                .collect();
    
            for row_idx in 0..row_count {
                if !first_row {
                    writeln!(writer, ",").map_err(|e| ElusionError::WriteError {
                        path: file_path.to_string(),
                        operation: "write_separator".to_string(),
                        reason: e.to_string(),
                        suggestion: "Check memory allocation".to_string(),
                    })?;
                }
                first_row = false;
                rows_written += 1;
                
                let mut row_obj = serde_json::Map::new();
                
                for col_idx in 0..column_count {
                    let col_name = &column_names[col_idx];
                    let array = batch.column(col_idx);
                    
                    let json_value = array_value_to_json(array, row_idx)?;
                    row_obj.insert(col_name.to_string(), json_value);
                }
                
                let json_value = serde_json::Value::Object(row_obj);
                
                if pretty {
                    serde_json::to_writer_pretty(&mut writer, &json_value)
                        .map_err(|e| ElusionError::WriteError {
                            path: file_path.to_string(),
                            operation: format!("write_row_{}", rows_written),
                            reason: format!("JSON serialization error: {}", e),
                            suggestion: "Check if row contains valid JSON data".to_string(),
                        })?;
                } else {
                    serde_json::to_writer(&mut writer, &json_value)
                        .map_err(|e| ElusionError::WriteError {
                            path: file_path.to_string(),
                            operation: format!("write_row_{}", rows_written),
                            reason: format!("JSON serialization error: {}", e),
                            suggestion: "Check if row contains valid JSON data".to_string(),
                        })?;
                }
            }
        }

        writeln!(writer, "\n]").map_err(|e| ElusionError::WriteError {
            path: file_path.to_string(),
            operation: "end_json".to_string(),
            reason: e.to_string(),
            suggestion: "Check memory allocation".to_string(),
        })?;
    
        writer.flush().map_err(|e| ElusionError::WriteError {
            path: file_path.to_string(),
            operation: "flush".to_string(),
            reason: e.to_string(),
            suggestion: "Failed to flush data to buffer".to_string(),
        })?;
    } 

    // Build full file path
    let full_file_path = if parsed.base_path == "Files" || parsed.base_path.is_empty() {
        file_path.to_string()
    } else {
        format!("{}/{}", parsed.base_path.trim_start_matches("Files/"), file_path)
    };

    client.upload_file(&full_file_path, buffer).await?;
    
    println!("Successfully wrote JSON data to Fabric - OneLake: {}", file_path);
    
    if rows_written == 0 {
        println!("Warning: No rows were written to the file. Check if this is expected.");
    } else {
        println!("Wrote {} rows to JSON file", rows_written);
    }
    
    Ok(())
}