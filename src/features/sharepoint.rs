
#[cfg(feature = "sharepoint")]
use crate::ElusionError;
#[cfg(feature = "sharepoint")]
use crate::ElusionResult;
#[cfg(feature = "sharepoint")]
use crate::CustomDataFrame;
#[cfg(feature = "sharepoint")]
use crate::lowercase_column_names;

#[cfg(feature = "sharepoint")]
#[derive(Debug, Clone)]
pub struct SharePointConfig {
    pub site_url: String,
}

#[cfg(feature = "sharepoint")]
impl SharePointConfig {
    pub fn new(site_url: String) -> Self {
        Self { site_url }
    }
}

#[cfg(feature = "sharepoint")]
pub struct SharePointClient {
    config: SharePointConfig,
    access_token: Option<String>,
    site_id: Option<String>,
}

#[cfg(feature = "sharepoint")]
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SharePointFileInfo {
    pub name: String,
    pub size: Option<u64>,
    pub created_date_time: Option<String>,
    pub last_modified_date_time: Option<String>,
    pub web_url: Option<String>,
    pub download_url: Option<String>,
}

#[cfg(feature = "sharepoint")]
impl SharePointClient {
    pub fn new(config: SharePointConfig) -> Self {
        Self {
            config,
            access_token: None,
            site_id: None,
        }
    }

    async fn execute_az_via_python(&self, args: &[&str]) -> ElusionResult<std::process::Output> {
        let python_path = r#"C:\Program Files\Microsoft SDKs\Azure\CLI2\python.exe"#;
        
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

    /// Authenticate using Azure CLI 
    async fn authenticate(&mut self) -> ElusionResult<()> {
        println!("🔍 Authenticating with Azure CLI (Unicode-safe method)...");
        
        // Try direct Python approach first (works with Unicode usernames)
        match self.execute_az_via_python(&["--version"]).await {
            Ok(version_output) => {
                if version_output.status.success() {
                    println!("✅ Azure CLI via Python works");
                    
                    // Check if logged in
                    match self.execute_az_via_python(&["account", "show"]).await {
                        Ok(account_output) => {
                            if account_output.status.success() {
                                println!("✅ Already logged in to Azure");
                                
                                // Get access token
                                match self.execute_az_via_python(&[
                                    "account", 
                                    "get-access-token", 
                                    "--resource", 
                                    "https://graph.microsoft.com/", 
                                    "--output", 
                                    "json"
                                ]).await {
                                    Ok(token_output) => {
                                        if token_output.status.success() {
                                            let token_json = String::from_utf8_lossy(&token_output.stdout);
                                            if let Ok(token_data) = serde_json::from_str::<serde_json::Value>(&token_json) {
                                                if let Some(access_token) = token_data["accessToken"].as_str() {
                                                    self.access_token = Some(access_token.to_string());
                                                    println!("✅ Successfully authenticated with Azure CLI");
                                                    return Ok(());
                                                }
                                            }
                                        }
                                    },
                                    Err(e) => {
                                        println!("⚠️ Python token command failed: {}", e);
                                    }
                                }
                            } else {
                                println!("⚠️ Not logged in to Azure CLI. Please run: az login");
                                return Err(ElusionError::Custom(
                                    "Please login to Azure CLI first: az login".to_string()
                                ));
                            }
                        },
                        Err(e) => {
                            println!("⚠️ Account check failed: {}", e);
                        }
                    }
                }
            },
            Err(e) => {
                println!("⚠️ Python method not available: {}", e);
            }
        }
        
        // Fallback to checking all Azure CLI paths
        println!("🔄 Checking alternative Azure CLI paths...");
        
        let az_paths = Self::get_azure_cli_paths();
        
        for az_path in az_paths.iter() {
            if az_path.contains("python.exe") {
                continue; // Already tried Python
            }
            
            if let Ok(output) = std::process::Command::new(az_path)
                .args(["--version"])
                .env("PYTHONIOENCODING", "utf-8")
                .env("PYTHONUTF8", "1")
                .output() 
            {
                if output.status.success() {
                    println!("✅ Found working Azure CLI at: {}", az_path);
                    
                    // Check login status
                    if let Ok(account_output) = std::process::Command::new(az_path)
                        .args(["account", "show"])
                        .env("PYTHONIOENCODING", "utf-8")
                        .env("PYTHONUTF8", "1")
                        .output() 
                    {
                        if account_output.status.success() {
                            // Get token
                            if let Ok(token_output) = std::process::Command::new(az_path)
                                .args([
                                    "account", 
                                    "get-access-token", 
                                    "--resource", 
                                    "https://graph.microsoft.com/", 
                                    "--output", 
                                    "json"
                                ])
                                .env("PYTHONIOENCODING", "utf-8")
                                .env("PYTHONUTF8", "1")
                                .output()
                            {
                                if token_output.status.success() {
                                    let token_json = String::from_utf8_lossy(&token_output.stdout);
                                    if let Ok(token_data) = serde_json::from_str::<serde_json::Value>(&token_json) {
                                        if let Some(access_token) = token_data["accessToken"].as_str() {
                                            self.access_token = Some(access_token.to_string());
                                            println!("✅ Successfully authenticated!");
                                            return Ok(());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        Err(ElusionError::Custom(
            "Azure CLI authentication failed.\n\
            Please:\n\
            1. Install Azure CLI: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli\n\
            2. Run: az login\n\
            3. Verify: az account show".to_string()
        ))
    }

    /// Get Azure CLI paths for all platforms
    fn get_azure_cli_paths() -> Vec<&'static str> {
        if cfg!(target_os = "windows") {
            vec![
                r#"C:\Program Files\Microsoft SDKs\Azure\CLI2\python.exe"#,
                "az.cmd",
                "az.exe", 
                "az",
                "C:\\Program Files\\Microsoft SDKs\\Azure\\CLI2\\wbin\\az.cmd",
                "C:\\Program Files (x86)\\Microsoft SDKs\\Azure\\CLI2\\wbin\\az.cmd",
                "C:\\ProgramData\\chocolatey\\bin\\az.cmd",
                "C:\\Users\\%USERNAME%\\scoop\\shims\\az.cmd",
            ]
        } else if cfg!(target_os = "macos") {
            vec![
                "az",
                "/usr/local/bin/az",
                "/opt/homebrew/bin/az",
                "/usr/bin/az",
                "/opt/local/bin/az",
            ]
        } else {
            vec![
                "az",
                "/usr/local/bin/az",
                "/usr/bin/az",
                "/bin/az",
                "~/.local/bin/az",
                "/snap/bin/azure-cli",
            ]
        }
    }

    /// Extract tenant info from SharePoint URL
    fn extract_tenant_info(site_url: &str) -> ElusionResult<(String, String, String)> {
        let url = url::Url::parse(site_url)
            .map_err(|e| ElusionError::Custom(format!("Invalid site URL: {}", e)))?;
        
        let hostname = url.host_str()
            .ok_or_else(|| ElusionError::Custom("Invalid hostname in URL".to_string()))?;
        
        let site_path = url.path().trim_start_matches('/').trim_end_matches('/');
        
        let tenant_name = if hostname.contains(".sharepoint.com") {
            hostname
                .split(".sharepoint.com")
                .next()
                .unwrap_or("unknown")
                .split('.')
                .next()
                .unwrap_or("unknown")
                .split('-')
                .next()
                .unwrap_or("unknown")
                .to_string()
        } else {
            hostname
                .split('.')
                .next()
                .unwrap_or("unknown")
                .to_string()
        };
        
        Ok((hostname.to_string(), tenant_name, site_path.to_string()))
    }

    /// Get site ID from the SharePoint URL
    async fn get_site_id(&mut self) -> ElusionResult<String> {
        if let Some(site_id) = &self.site_id {
            return Ok(site_id.clone());
        }

        let token = self.access_token.as_ref()
            .ok_or_else(|| ElusionError::Custom("Not authenticated".to_string()))?;

        let (original_hostname, tenant_name, site_path) = Self::extract_tenant_info(&self.config.site_url)?;
        
        let hostname_variants = vec![
            original_hostname.clone(),
            format!("{}.sharepoint.com", tenant_name),
            if original_hostname.contains(".sharepoint.com") {
                let base = original_hostname.split(".sharepoint.com").next().unwrap_or(&tenant_name);
                let clean_base = base.split('.').next().unwrap_or(&tenant_name);
                format!("{}.sharepoint.com", clean_base)
            } else {
                format!("{}.sharepoint.com", tenant_name)
            },
        ];
        
        let mut unique_hostnames = Vec::new();
        for hostname in hostname_variants {
            if !unique_hostnames.contains(&hostname) {
                unique_hostnames.push(hostname);
            }
        }
        
        println!("🔍 Trying {} hostname variations for tenant '{}'", unique_hostnames.len(), tenant_name);
        
        for (i, host) in unique_hostnames.iter().enumerate() {
            let graph_url = format!("https://graph.microsoft.com/v1.0/sites/{}:/{}", host, site_path);
            
            println!("  {}: {}", i + 1, host);
            
            if let Ok(response) = reqwest::Client::new()
                .get(&graph_url)
                .bearer_auth(token)
                .send()
                .await {
                if response.status().is_success() {
                    if let Ok(site_info) = response.json::<serde_json::Value>().await {
                        if let Some(site_id) = site_info["id"].as_str() {
                            self.site_id = Some(site_id.to_string());
                            println!("✅ Found site with hostname: {}", host);
                            return Ok(site_id.to_string());
                        }
                    }
                }
            }
        }
        
        Err(ElusionError::Custom(format!(
            "Could not find site ID. Please verify your SharePoint site URL: {}", 
            self.config.site_url
        )))
    }

    /// Download a file from SharePoint
    async fn download_file(&mut self, file_path: &str) -> ElusionResult<Vec<u8>> {
        self.authenticate().await?;
        let site_id = self.get_site_id().await?;
        let token = self.access_token.as_ref().unwrap();
        
        println!("📥 Downloading file: {}", file_path);
        
        let file_paths = vec![
            file_path.to_string(),
            file_path.trim_start_matches("Shared Documents/").to_string(),
            file_path.trim_start_matches("Documents/").to_string(),
            if file_path.starts_with("Shared Documents/") {
                file_path.replace("Shared Documents/", "Documents/")
            } else if file_path.starts_with("Documents/") {
                file_path.replace("Documents/", "Shared Documents/")
            } else {
                format!("Shared Documents/{}", file_path)
            },
        ];
        
        let mut unique_paths = Vec::new();
        for path in file_paths {
            if !unique_paths.contains(&path) {
                unique_paths.push(path);
            }
        }
        
        for (i, path) in unique_paths.iter().enumerate() {
            let file_url = format!("https://graph.microsoft.com/v1.0/sites/{}/drive/root:/{}:/content", site_id, path);
            
            println!("  Trying path {}: {}", i + 1, path);
            
            if let Ok(response) = reqwest::Client::new()
                .get(&file_url)
                .bearer_auth(token)
                .send()
                .await {
                if response.status().is_success() {
                    if let Ok(content) = response.bytes().await {
                        println!("✅ Successfully downloaded {} bytes", content.len());
                        return Ok(content.to_vec());
                    }
                }
            }
        }
        
        Err(ElusionError::Custom(format!(
            "Could not download file '{}'. Please verify the file exists and you have access.", 
            file_path
        )))
    }

    /// List files in a SharePoint folder
    pub async fn list_folder_contents(&mut self, folder_path: &str) -> ElusionResult<Vec<SharePointFileInfo>> {
        self.authenticate().await?;
        let site_id = self.get_site_id().await?;
        let token = self.access_token.as_ref().unwrap();
        
        println!("📁 Listing contents of folder: {}", folder_path);
        
        let folder_paths = vec![
            folder_path.to_string(),
            folder_path.trim_start_matches("Shared Documents/").to_string(),
            folder_path.trim_start_matches("Documents/").to_string(),
            if folder_path.starts_with("Shared Documents/") {
                folder_path.replace("Shared Documents/", "Documents/")
            } else if folder_path.starts_with("Documents/") {
                folder_path.replace("Documents/", "Shared Documents/")
            } else {
                format!("Shared Documents/{}", folder_path)
            },
        ];
        
        let mut unique_paths = Vec::new();
        for path in folder_paths {
            if !unique_paths.contains(&path) {
                unique_paths.push(path);
            }
        }
        
        for (i, path) in unique_paths.iter().enumerate() {
            let folder_url = format!(
                "https://graph.microsoft.com/v1.0/sites/{}/drive/root:/{}:/children", 
                site_id, 
                path.trim_start_matches('/').trim_end_matches('/')
            );
            
            println!("  Trying folder path {}: {}", i + 1, path);
            
            if let Ok(response) = reqwest::Client::new()
                .get(&folder_url)
                .bearer_auth(token)
                .send()
                .await {
                
                if response.status().is_success() {
                    if let Ok(folder_data) = response.json::<serde_json::Value>().await {
                        if let Some(items) = folder_data["value"].as_array() {
                            let mut files = Vec::new();
                            
                            for item in items {
                                if item["file"].is_object() {
                                    let file_info = SharePointFileInfo {
                                        name: item["name"].as_str().unwrap_or("unknown").to_string(),
                                        size: item["size"].as_u64(),
                                        created_date_time: item["createdDateTime"].as_str().map(|s| s.to_string()),
                                        last_modified_date_time: item["lastModifiedDateTime"].as_str().map(|s| s.to_string()),
                                        web_url: item["webUrl"].as_str().map(|s| s.to_string()),
                                        download_url: item["@microsoft.graph.downloadUrl"].as_str().map(|s| s.to_string()),
                                    };
                                    files.push(file_info);
                                }
                            }
                            
                            println!("✅ Found {} files in folder", files.len());
                            return Ok(files);
                        }
                    }
                }
            }
        }
        
        Err(ElusionError::Custom(format!(
            "Could not list folder contents: {}", 
            folder_path
        )))
    }
}

#[cfg(feature = "sharepoint")]
pub async fn load_from_sharepoint_impl(
    site_url: &str,
    file_path: &str,
    alias: &str,
) -> ElusionResult<CustomDataFrame> {
    let config = SharePointConfig::new(site_url.to_string());
    let mut client = SharePointClient::new(config);
    let content = client.download_file(file_path).await?;
    
    let file_extension = file_path
        .split('.')
        .last()
        .unwrap_or("")
        .to_lowercase();
    
    println!("🔍 Detected file type: {} for {}", file_extension, file_path);
    
    let temp_dir = std::env::temp_dir();
    let temp_file = temp_dir.join(format!("sharepoint_{}.{}", 
        chrono::Utc::now().timestamp(), 
        file_extension
    ));
    
    std::fs::write(&temp_file, content)?;
    
    let aliased_df = match file_extension.as_str() {
        "xlsx" | "xls" => {
            println!("📊 Loading as Excel file...");
            crate::features::excel::load_excel(temp_file.to_str().unwrap(), alias).await?
        },
        "csv" => {
            println!("📋 Loading as CSV file...");
            CustomDataFrame::load_csv(temp_file.to_str().unwrap(), alias).await?
        },
        "json" => {
            println!("🔧 Loading as JSON file...");
            CustomDataFrame::load_json(temp_file.to_str().unwrap(), alias).await?
        },
        "parquet" => {
            println!("🗄️ Loading as Parquet file...");
            CustomDataFrame::load_parquet(temp_file.to_str().unwrap(), alias).await?
        },
        _ => {
            let _ = std::fs::remove_file(&temp_file);
            return Err(ElusionError::InvalidOperation {
                operation: "SharePoint File Loading".to_string(),
                reason: format!("Unsupported file extension: '{}'", file_extension),
                suggestion: "💡 Supported formats: xlsx, xls, csv, json, parquet".to_string(),
            });
        }
    };
    
    let _ = std::fs::remove_file(temp_file);
    
    let normalized_df = lowercase_column_names(aliased_df.dataframe).await?;
    
    println!("✅ Successfully loaded {} as '{}'", file_path, alias);
    
    Ok(CustomDataFrame {
        df: normalized_df,
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

/// Load Excel file from SharePoint - handles everything automatically
#[cfg(feature = "sharepoint")]
async fn load_excel_from_sharepoint(
    site_url: &str,
    file_path: &str,
) -> ElusionResult<CustomDataFrame> {
    let config = SharePointConfig::new(site_url.to_string());
    let mut client = SharePointClient::new(config);
    let content = client.download_file(file_path).await?;
    
    let temp_dir = std::env::temp_dir();
    let temp_file = temp_dir.join(format!("sharepoint_{}.xlsx", chrono::Utc::now().timestamp()));
    std::fs::write(&temp_file, content)?;
    
    let aliased_df = crate::features::excel::load_excel(temp_file.to_str().unwrap(), "sharepoint_data").await?;
    let _ = std::fs::remove_file(temp_file);
    
    let normalized_df = lowercase_column_names(aliased_df.dataframe).await?;
    Ok(CustomDataFrame {
        df: normalized_df,
        table_alias: "sharepoint_excel".to_string(),
        from_table: "sharepoint_excel".to_string(),
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

/// Load CSV file from SharePoint - handles everything automatically  
#[cfg(feature = "sharepoint")]
async fn load_csv_from_sharepoint(
    site_url: &str,
    file_path: &str,
) -> ElusionResult<CustomDataFrame> {
    let config = SharePointConfig::new(site_url.to_string());
    let mut client = SharePointClient::new(config);
    let content = client.download_file(file_path).await?;
    
    let temp_dir = std::env::temp_dir();
    let temp_file = temp_dir.join(format!("sharepoint_{}.csv", chrono::Utc::now().timestamp()));
    std::fs::write(&temp_file, content)?;
    
    let aliased_df = CustomDataFrame::load_csv(temp_file.to_str().unwrap(), "sharepoint_data").await?;
    let _ = std::fs::remove_file(temp_file);

    let normalized_df = lowercase_column_names(aliased_df.dataframe).await?;
    
    Ok(CustomDataFrame {
        df: normalized_df,
        table_alias: "sharepoint_csv".to_string(),
        from_table: "sharepoint_csv".to_string(),
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

/// Load JSON file from SharePoint - handles everything automatically
#[cfg(feature = "sharepoint")]
async fn load_json_from_sharepoint(
    site_url: &str,
    file_path: &str,
) -> ElusionResult<CustomDataFrame> {
    let config = SharePointConfig::new(site_url.to_string());
    let mut client = SharePointClient::new(config);
    let content = client.download_file(file_path).await?;
    
    let temp_dir = std::env::temp_dir();
    let temp_file = temp_dir.join(format!("sharepoint_{}.json", chrono::Utc::now().timestamp()));
    std::fs::write(&temp_file, content)?;
    
    let aliased_df = CustomDataFrame::load_json(temp_file.to_str().unwrap(), "sharepoint_data").await?;
    let _ = std::fs::remove_file(temp_file);

    let normalized_df = lowercase_column_names(aliased_df.dataframe).await?;
    
    Ok(CustomDataFrame {
        df: normalized_df,
        table_alias: "sharepoint_json".to_string(),
        from_table: "sharepoint_json".to_string(),
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

/// Load Parquet file from SharePoint - handles everything automatically
#[cfg(feature = "sharepoint")]
async fn load_parquet_from_sharepoint(
    site_url: &str,
    file_path: &str,
) -> ElusionResult<CustomDataFrame> {
    let config = SharePointConfig::new(site_url.to_string());
    let mut client = SharePointClient::new(config);
    let content = client.download_file(file_path).await?;
    
    let temp_dir = std::env::temp_dir();
    let temp_file = temp_dir.join(format!("sharepoint_{}.parquet", chrono::Utc::now().timestamp()));
    std::fs::write(&temp_file, content)?;
    
    let aliased_df = CustomDataFrame::load_parquet(temp_file.to_str().unwrap(), "sharepoint_data").await?;
    let _ = std::fs::remove_file(temp_file);

    let normalized_df = lowercase_column_names(aliased_df.dataframe).await?;
    
    Ok(CustomDataFrame {
        df: normalized_df,
        table_alias: "sharepoint_parquet".to_string(),
        from_table: "sharepoint_parquet".to_string(),
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

    /// Load all files from a SharePoint folder and union them if they have compatible schemas
    /// Supports CSV, Excel, JSON, and Parquet files
    #[cfg(feature = "sharepoint")]
    pub async fn load_folder_from_sharepoint_impl(
        site_url: &str,
        folder_path: &str,
        file_extensions: Option<Vec<&str>>, // Filter by extensions, e.g., vec!["xlsx", "csv"]
        result_alias: &str,
    ) -> ElusionResult<CustomDataFrame> {
        let config = SharePointConfig::new(
            site_url.to_string(),
        );
        
        let mut client = SharePointClient::new(config);
        
        // Get list of files in the folder
        let files = client.list_folder_contents(folder_path).await?;
        
        let mut dataframes = Vec::new();
        
        for file_info in files {
            // Skip if file extensions filter is specified and file doesn't match
            if let Some(ref extensions) = file_extensions {
                let file_ext = file_info.name
                    .split('.')
                    .last()
                    .unwrap_or("")
                    .to_lowercase();
                
                if !extensions.iter().any(|ext| ext.to_lowercase() == file_ext) {
                    continue;
                }
            }
            
            // Download and process file based on extension
            let file_path = format!("{}/{}", folder_path.trim_end_matches('/'), file_info.name);
            
            match file_info.name.split('.').last().unwrap_or("").to_lowercase().as_str() {
                "csv" => {
                    match load_csv_from_sharepoint(site_url, &file_path).await {
                        Ok(df) => {
                            println!("✅ Loaded CSV: {}", file_info.name);
                            dataframes.push(df);
                        },
                        Err(e) => {
                            eprintln!("⚠️ Failed to load CSV file {}: {}", file_info.name, e);
                            continue;
                        }
                    }
                },
                "xlsx" | "xls" => {
                    match load_excel_from_sharepoint(site_url, &file_path).await {
                        Ok(df) => {
                            println!("✅ Loaded Excel: {}", file_info.name);
                            dataframes.push(df);
                        },
                        Err(e) => {
                            eprintln!("⚠️ Failed to load Excel file {}: {}", file_info.name, e);
                            continue;
                        }
                    }
                },
                "json" => {
                    match load_json_from_sharepoint(site_url, &file_path).await {
                        Ok(df) => {
                            println!("✅ Loaded JSON: {}", file_info.name);
                            dataframes.push(df);
                        },
                        Err(e) => {
                            eprintln!("⚠️ Failed to load JSON file {}: {}", file_info.name, e);
                            continue;
                        }
                    }
                },
                "parquet" => {
                    match load_parquet_from_sharepoint(site_url, &file_path).await {
                        Ok(df) => {
                            println!("✅ Loaded Parquet: {}", file_info.name);
                            dataframes.push(df);
                        },
                        Err(e) => {
                            eprintln!("⚠️ Failed to load Parquet file {}: {}", file_info.name, e);
                            continue;
                        }
                    }
                },
                _ => {
                    println!("⏭️ Skipping unsupported file type: {}", file_info.name);
                }
            }
        }
        
        if dataframes.is_empty() {
            return Err(ElusionError::InvalidOperation {
                operation: "SharePoint Folder Loading".to_string(),
                reason: "No supported files found or all files failed to load".to_string(),
                suggestion: "💡 Check folder path and ensure it contains CSV, Excel, JSON, or Parquet files".to_string(),
            });
        }
        
        // If only one file, return it directly
        if dataframes.len() == 1 {
            println!("📄 Single file loaded, returning as-is");
            return dataframes.into_iter().next().unwrap().elusion(result_alias).await;
        }
        
        // Check schema compatibility by column names AND types
        println!("🔍 Checking schema compatibility for {} files (names + types)...", dataframes.len());
        
        let first_schema = dataframes[0].df.schema();
        let mut compatible_schemas = true;
        let mut schema_issues = Vec::new();
        
        // Print first file schema for reference
        println!("📋 File 1 schema:");
        for (i, field) in first_schema.fields().iter().enumerate() {
            println!("   Column {}: '{}' ({})", i + 1, field.name(), field.data_type());
        }
        
        for (file_idx, df) in dataframes.iter().enumerate().skip(1) {
            let current_schema = df.df.schema();
            
            println!("📋 File {} schema:", file_idx + 1);
            for (i, field) in current_schema.fields().iter().enumerate() {
                println!("   Column {}: '{}' ({})", i + 1, field.name(), field.data_type());
            }
            
            // Check if column count matches
            if first_schema.fields().len() != current_schema.fields().len() {
                compatible_schemas = false;
                schema_issues.push(format!("File {} has {} columns, but first file has {}", 
                    file_idx + 1, current_schema.fields().len(), first_schema.fields().len()));
                continue;
            }
            
            // Check if column names and types match (case insensitive names)
            for (col_idx, first_field) in first_schema.fields().iter().enumerate() {
                if let Some(current_field) = current_schema.fields().get(col_idx) {
                    // Check column name (case insensitive)
                    if first_field.name().to_lowercase() != current_field.name().to_lowercase() {
                        compatible_schemas = false;
                        schema_issues.push(format!("File {} column {} name is '{}', but first file has '{}'", 
                            file_idx + 1, col_idx + 1, current_field.name(), first_field.name()));
                    }
                    
                    // Check column type
                    if first_field.data_type() != current_field.data_type() {
                        compatible_schemas = false;
                        schema_issues.push(format!("File {} column {} ('{}') type is {:?}, but first file has {:?}", 
                            file_idx + 1, col_idx + 1, current_field.name(), 
                            current_field.data_type(), first_field.data_type()));
                    }
                }
            }
        }
        
        if !compatible_schemas {
            println!("⚠️ Schema compatibility issues found:");
            for issue in &schema_issues {
                println!("   {}", issue);
            }
            
            // Since all columns are UTF8, just reorder them by name to match first file
            println!("🔧 Reordering columns by name to match first file...");
            
            // Get the column order from the first file
            let first_file_columns: Vec<String> = first_schema.fields()
                .iter()
                .map(|field| field.name().clone())
                .collect();
            
            println!("📋 Target column order: {:?}", first_file_columns);
            
            let mut reordered_dataframes = Vec::new();
            
            for (i, df) in dataframes.clone().into_iter().enumerate() {
                // Select columns in the same order as first file
                let column_refs: Vec<&str> = first_file_columns.iter().map(|s| s.as_str()).collect();
                let reordered_df = df.select_vec(column_refs);
                
                // Create temporary alias
                let temp_alias = format!("reordered_file_{}", i + 1);
                match reordered_df.elusion(&temp_alias).await {
                    Ok(standardized_df) => {
                        println!("✅ Reordered file {} columns", i + 1);
                        reordered_dataframes.push(standardized_df);
                    },
                    Err(e) => {
                        eprintln!("⚠️ Failed to reorder file {} columns: {}", i + 1, e);
                        continue;
                    }
                }
            }
            
            if reordered_dataframes.is_empty() {
                println!("📄 Column reordering failed, returning first file only");
                return dataframes.into_iter().next().unwrap().elusion(result_alias).await;
            }
            
            dataframes = reordered_dataframes;
            println!("✅ All files reordered to match first file column order");
        } else {
            println!("✅ All schemas are compatible!");
        }
        
        // Union the compatible dataframes
        println!("🔗 Unioning {} files with compatible schemas...", dataframes.len());
        
        let total_files = dataframes.len();
        let mut result = dataframes.clone().into_iter().next().unwrap();
        
        // Union with remaining dataframes using union_all to keep all data
        for (i, df) in dataframes.into_iter().enumerate().skip(1) {
            result = result.union_all(df).await
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: "SharePoint Folder Union All".to_string(),
                    reason: format!("Failed to union file {}: {}", i + 1, e),
                    suggestion: "💡 Check that all files have compatible schemas".to_string(),
                })?;
            
            println!("✅ Unioned file {}/{}", i + 1, total_files - 1);
        }
        
        println!("🎉 Successfully combined {} files using UNION ALL", total_files);
        
        // Final elusion with the desired alias
        result.elusion(result_alias).await
    }
    /// Load all files from SharePoint folder and add filename as a column
    /// Same as load_folder_from_sharepoint but adds a "filename" column to track source files
    #[cfg(feature = "sharepoint")]
    pub async fn load_folder_from_sharepoint_with_filename_column_impl(
        site_url: &str,
        folder_path: &str,
        file_extensions: Option<Vec<&str>>,
        result_alias: &str,
    ) -> ElusionResult<CustomDataFrame> {
        let config = SharePointConfig::new(
            site_url.to_string(),
        );
        
        let mut client = SharePointClient::new(config);
        
        // Get list of files in the folder
        let files = client.list_folder_contents(folder_path).await?;
        
        let mut dataframes = Vec::new();
        
        for file_info in files {
            // Skip if file extensions filter is specified and file doesn't match
            if let Some(ref extensions) = file_extensions {
                let file_ext = file_info.name
                    .split('.')
                    .last()
                    .unwrap_or("")
                    .to_lowercase();
                
                if !extensions.iter().any(|ext| ext.to_lowercase() == file_ext) {
                    continue;
                }
            }
            
            // Download and process file based on extension
            let file_path = format!("{}/{}", folder_path.trim_end_matches('/'), file_info.name);
            
            let mut loaded_df = None;
            
            match file_info.name.split('.').last().unwrap_or("").to_lowercase().as_str() {
                "csv" => {
                    match load_csv_from_sharepoint(site_url, &file_path).await {
                        Ok(df) => {
                            println!("✅ Loaded CSV: {}", file_info.name);
                            loaded_df = Some(df);
                        },
                        Err(e) => {
                            eprintln!("⚠️ Failed to load CSV file {}: {}", file_info.name, e);
                            continue;
                        }
                    }
                },
                "xlsx" | "xls" => {
                    match load_excel_from_sharepoint(site_url, &file_path).await {
                        Ok(df) => {
                            println!("✅ Loaded Excel: {}", file_info.name);
                            loaded_df = Some(df);
                        },
                        Err(e) => {
                            eprintln!("⚠️ Failed to load Excel file {}: {}", file_info.name, e);
                            continue;
                        }
                    }
                },
                "json" => {
                    match load_json_from_sharepoint(site_url, &file_path).await {
                        Ok(df) => {
                            println!("✅ Loaded JSON: {}", file_info.name);
                            loaded_df = Some(df);
                        },
                        Err(e) => {
                            eprintln!("⚠️ Failed to load JSON file {}: {}", file_info.name, e);
                            continue;
                        }
                    }
                },
                "parquet" => {
                    match load_parquet_from_sharepoint(site_url, &file_path).await {
                        Ok(df) => {
                            println!("✅ Loaded Parquet: {}", file_info.name);
                            loaded_df = Some(df);
                        },
                        Err(e) => {
                            eprintln!("⚠️ Failed to load Parquet file {}: {}", file_info.name, e);
                            continue;
                        }
                    }
                },
                _ => {
                    println!("⏭️ Skipping unsupported file type: {}", file_info.name);
                }
            }
            
            // Add filename column to the loaded dataframe
            if let Some(mut df) = loaded_df {
                // Add filename as a new column using select with literal value
                df = df.select_vec(vec![
                    &format!("'{}' AS filename_added", file_info.name), 
                    "*"
                ]);
                
                // Execute the selection to create the dataframe with filename column
                let temp_alias = format!("file_with_filename_{}", dataframes.len());
                match df.elusion(&temp_alias).await {
                    Ok(filename_df) => {
                        println!("✅ Added filename column to {}", file_info.name);
                        dataframes.push(filename_df);
                    },
                    Err(e) => {
                        eprintln!("⚠️ Failed to add filename to {}: {}", file_info.name, e);
                        continue;
                    }
                }
            }
        }
        
        if dataframes.is_empty() {
            return Err(ElusionError::InvalidOperation {
                operation: "SharePoint Folder Loading with Filename".to_string(),
                reason: "No supported files found or all files failed to load".to_string(),
                suggestion: "💡 Check folder path and ensure it contains supported files".to_string(),
            });
        }
        
        // If only one file, return it directly
        if dataframes.len() == 1 {
            println!("📄 Single file loaded with filename column");
            return dataframes.into_iter().next().unwrap().elusion(result_alias).await;
        }
        
        // Check schema compatibility (all files should now have filename as first column)
        println!("🔍 Checking schema compatibility for {} files with filename columns...", dataframes.len());
        
        let first_schema = dataframes[0].df.schema();
        let mut compatible_schemas = true;
        let mut schema_issues = Vec::new();
        
        for (file_idx, df) in dataframes.iter().enumerate().skip(1) {
            let current_schema = df.df.schema();
            
            // Check if column count matches
            if first_schema.fields().len() != current_schema.fields().len() {
                compatible_schemas = false;
                schema_issues.push(format!("File {} has {} columns, but first file has {}", 
                    file_idx + 1, current_schema.fields().len(), first_schema.fields().len()));
                continue;
            }
            
            // Check if column names match (should be identical now with filename column)
            for (col_idx, first_field) in first_schema.fields().iter().enumerate() {
                if let Some(current_field) = current_schema.fields().get(col_idx) {
                    if first_field.name().to_lowercase() != current_field.name().to_lowercase() {
                        compatible_schemas = false;
                        schema_issues.push(format!("File {} column {} name is '{}', but first file has '{}'", 
                            file_idx + 1, col_idx + 1, current_field.name(), first_field.name()));
                    }
                }
            }
        }
        
        if !compatible_schemas {
            println!("⚠️ Schema compatibility issues found:");
            for issue in &schema_issues {
                println!("   {}", issue);
            }
            
            // Reorder columns by name to match first file (same as original function)
            println!("🔧 Reordering columns by name to match first file...");
            
            let first_file_columns: Vec<String> = first_schema.fields()
                .iter()
                .map(|field| field.name().clone())
                .collect();
            
            println!("📋 Target column order: {:?}", first_file_columns);
            
            let mut reordered_dataframes = Vec::new();
            
            for (i, df) in dataframes.clone().into_iter().enumerate() {
                let column_refs: Vec<&str> = first_file_columns.iter().map(|s| s.as_str()).collect();
                let reordered_df = df.select_vec(column_refs);
                
                let temp_alias = format!("reordered_file_{}", i + 1);
                match reordered_df.elusion(&temp_alias).await {
                    Ok(standardized_df) => {
                        println!("✅ Reordered file {} columns", i + 1);
                        reordered_dataframes.push(standardized_df);
                    },
                    Err(e) => {
                        eprintln!("⚠️ Failed to reorder file {} columns: {}", i + 1, e);
                        continue;
                    }
                }
            }
            
            if reordered_dataframes.is_empty() {
                println!("📄 Column reordering failed, returning first file only");
                return dataframes.into_iter().next().unwrap().elusion(result_alias).await;
            }
            
            dataframes = reordered_dataframes;
            println!("✅ All files reordered to match first file column order");
        } else {
            println!("✅ All schemas are compatible!");
        }
        
        // Union the compatible dataframes
        println!("🔗 Unioning {} files with filename tracking...", dataframes.len());
        
        let total_files = dataframes.len();
        let mut result = dataframes.clone().into_iter().next().unwrap();
        
        // Union with remaining dataframes using union_all to keep all data
        for (i, df) in dataframes.into_iter().enumerate().skip(1) {
            result = result.union_all(df).await
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: "SharePoint Folder Union with Filename".to_string(),
                    reason: format!("Failed to union file {}: {}", i + 1, e),
                    suggestion: "💡 Check that all files have compatible schemas".to_string(),
                })?;
            
            println!("✅ Unioned file {}/{}", i + 1, total_files - 1);
        }
        
        println!("🎉 Successfully combined {} files with filename tracking", total_files);
        
        // Final elusion with the desired alias
        result.elusion(result_alias).await
    }