pub mod prelude;
// =========== DataFusion
use regex::Regex;
use datafusion::prelude::*;
use datafusion::error::DataFusionError;
use futures::future::BoxFuture;
use datafusion::datasource::MemTable;
use std::sync::Arc;
use arrow::datatypes::{Field, DataType as ArrowDataType, Schema, SchemaRef};
use chrono::NaiveDate;
use arrow::array::{StringBuilder, ArrayRef,  ArrayBuilder, Float64Builder, Int64Builder, UInt64Builder};
 
use arrow::record_batch::RecordBatch;
use ArrowDataType::*;
use arrow::csv::writer::WriterBuilder;

// ========= CSV defects
use std::fs::{self, File, OpenOptions};
use std::io::{Write, BufWriter};

//============ WRITERS
use datafusion::prelude::SessionContext;
use datafusion::dataframe::{DataFrame,DataFrameWriteOptions};

// ========= JSON   
use serde_json::{json, Value};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use arrow::error::Result as ArrowResult;    
use datafusion::arrow::datatypes::TimeUnit;
//---json writer
use arrow::array::{ListArray,TimestampMicrosecondArray,TimestampMillisecondArray,TimestampSecondArray,LargeBinaryArray,BinaryArray,LargeStringArray,Float32Array,UInt64Array,UInt32Array,BooleanArray};

// ========== DELTA
use std::result::Result;
use std::path::{Path as LocalPath, PathBuf};
use deltalake::operations::DeltaOps;
use deltalake::writer::{RecordBatchWriter, WriteMode, DeltaWriter};
use deltalake::{open_table, DeltaTableBuilder, DeltaTableError, ObjectStore, Path as DeltaPath};
use deltalake::protocol::SaveMode;
use deltalake::kernel::{DataType as DeltaType, Metadata, Protocol, StructType};
use deltalake::kernel::StructField;
use futures::StreamExt;
use deltalake::storage::object_store::local::LocalFileSystem;
// use object_store::path::Path as ObjectStorePath;

// =========== ERRROR
use std::fmt::{self, Debug};
use std::error::Error;

// ======== PIVOT
use arrow::compute;
use arrow::array::StringArray;

// ======== EXCEL
#[cfg(feature = "excel")]
use rust_xlsxwriter::{Format, Workbook, ExcelDateTime};
#[cfg(feature = "excel")]
use arrow::array::{Int8Array, Int16Array,UInt8Array, UInt16Array};

use calamine::DataType as CalamineDataType;
use calamine::{Reader, Xlsx, open_workbook};

//=========== SHARE POINT
#[cfg(feature = "sharepoint")]
use reqwest;
#[cfg(feature = "sharepoint")]
use url;

#[cfg(feature = "sharepoint")]
#[derive(Debug, Clone)]
pub struct SharePointConfig {
    pub tenant_id: String,
    pub client_id: String,
    pub site_url: String,
}

#[cfg(feature = "sharepoint")]
impl SharePointConfig {
    pub fn new(tenant_id: String, client_id: String, site_url: String) -> Self {
        Self {
            tenant_id,
            client_id,
            site_url,
        }
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

    /// Cross-platform Azure CLI path detection
    fn get_azure_cli_paths() -> Vec<&'static str> {
        if cfg!(target_os = "windows") {
            vec![
                "az.cmd",
                "az",
                "C:\\Program Files\\Microsoft SDKs\\Azure\\CLI2\\wbin\\az.cmd",
                "C:\\Program Files (x86)\\Microsoft SDKs\\Azure\\CLI2\\wbin\\az.cmd",
                "C:\\Program Files\\Microsoft SDKs\\Azure\\CLI2\\wbin\\az.exe",
                "C:\\Program Files (x86)\\Microsoft SDKs\\Azure\\CLI2\\wbin\\az.exe",
            ]
        } else if cfg!(target_os = "macos") {
            vec![
                "az",
                "/usr/local/bin/az",
                "/opt/homebrew/bin/az",
                "/usr/bin/az",
            ]
        } else {
            // Linux and other Unix-like systems
            vec![
                "az",
                "/usr/local/bin/az",
                "/usr/bin/az",
                "/opt/az/bin/az",
                "~/.local/bin/az",
            ]
        }
    }

    /// Smart authentication - tries multiple methods
    async fn authenticate(&mut self) -> ElusionResult<()> {
        let az_paths = Self::get_azure_cli_paths();
        
        println!("🔍 Searching for Azure CLI on {} platform...", 
            if cfg!(target_os = "windows") { "Windows" }
            else if cfg!(target_os = "macos") { "macOS" } 
            else { "Linux" }
        );
        
        for (i, az_path) in az_paths.iter().enumerate() {
            if let Ok(output) = std::process::Command::new(az_path).args(["--version"]).output() {
                if output.status.success() {
                    println!("✅ Found Azure CLI at: {}", az_path);
                    
                    // Check if logged in
                    if let Ok(account_output) = std::process::Command::new(az_path).args(["account", "show"]).output() {
                        if account_output.status.success() {
                            // Get token
                            if let Ok(token_output) = std::process::Command::new(az_path)
                                .args(["account", "get-access-token", "--resource", "https://graph.microsoft.com/", "--output", "json"])
                                .output() {
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
                            }
                        } else {
                            println!("⚠️ Azure CLI found but not logged in. Please run: az login");
                        }
                    }
                }
            } else if i == 0 {
                // Only log for the first attempt to avoid spam
                println!("🔍 Azure CLI not found in PATH, trying other locations...");
            }
        }
        
        Err(ElusionError::Custom(format!(
            "Azure CLI not found or not authenticated. Please:\n\
            1. Install Azure CLI: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli\n\
            2. Run: az login\n\
            3. Verify: az account show\n\n\
            Platform: {}", 
            if cfg!(target_os = "windows") { "Windows" }
            else if cfg!(target_os = "macos") { "macOS" } 
            else { "Linux" }
        )))
    }

    /// Extract tenant name from SharePoint URL for generic hostname variations
    fn extract_tenant_info(site_url: &str) -> ElusionResult<(String, String, String)> {
        let url = url::Url::parse(site_url)
            .map_err(|e| ElusionError::Custom(format!("Invalid site URL: {}", e)))?;
        
        let hostname = url.host_str()
            .ok_or_else(|| ElusionError::Custom("Invalid hostname in URL".to_string()))?;
        
        let site_path = url.path().trim_start_matches('/').trim_end_matches('/');
        
        // Extract tenant name from hostname
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
            // Fallback: use everything before first dot
            hostname
                .split('.')
                .next()
                .unwrap_or("unknown")
                .to_string()
        };
        
        Ok((hostname.to_string(), tenant_name, site_path.to_string()))
    }

    /// Get site ID with smart URL handling for any tenant
    async fn get_site_id(&mut self) -> ElusionResult<String> {
        if let Some(site_id) = &self.site_id {
            return Ok(site_id.clone());
        }

        let token = self.access_token.as_ref()
            .ok_or_else(|| ElusionError::Custom("Not authenticated".to_string()))?;

        let (original_hostname, tenant_name, site_path) = Self::extract_tenant_info(&self.config.site_url)?;
        
        // Generate hostname variations for different SharePoint configurations
        let hostname_variants = vec![
            // Original hostname
            original_hostname.clone(),
            
            // Standard SharePoint Online format
            format!("{}.sharepoint.com", tenant_name),
            
            // Remove country/region codes (e.g., .rs, .de, .uk)
            if original_hostname.contains(".sharepoint.com") {
                let base = original_hostname.split(".sharepoint.com").next().unwrap_or(&tenant_name);
                let clean_base = base.split('.').next().unwrap_or(&tenant_name);
                format!("{}.sharepoint.com", clean_base)
            } else {
                format!("{}.sharepoint.com", tenant_name)
            },
            
            // Handle admin or special suffixes
            if original_hostname.contains("-") {
                let clean_name = original_hostname.split('-').next().unwrap_or(&tenant_name);
                let clean_tenant = clean_name.split('.').next().unwrap_or(&tenant_name);
                format!("{}.sharepoint.com", clean_tenant)
            } else {
                format!("{}.sharepoint.com", tenant_name)
            },
            
            // Government cloud variations 
            format!("{}.sharepoint.us", tenant_name),
            format!("{}.sharepoint-mil.us", tenant_name),
        ];
        
        // Remove duplicates
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
                } else {
                    if let Ok(error_text) = response.text().await {
                        if error_text.contains("invalidRequest") {
                            println!("    ❌ Invalid hostname: {}", host);
                        } else {
                            println!("    ⚠️ Error: {}", error_text.chars().take(100).collect::<String>());
                        }
                    }
                }
            }
        }
        
        Err(ElusionError::Custom(format!(
            "Could not find site ID for any hostname variation of '{}'. \
            Please verify your SharePoint site URL is correct.", 
            tenant_name
        )))
    }

    /// Smart file download - tries multiple approaches
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
        
        // Remove duplicates
        let mut unique_paths = Vec::new();
        for path in file_paths {
            if !unique_paths.contains(&path) {
                unique_paths.push(path);
            }
        }
        
        // direct Graph API download
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
                } else {
                    println!("    ❌ HTTP {}", response.status());
                }
            }
        }
        
        // search-based approach as fallback
        println!("🔍 Trying search-based approach...");
        let filename = file_path.split('/').last().unwrap_or(file_path);
        let search_url = format!("https://graph.microsoft.com/v1.0/sites/{}/drive/root/search(q='{}')", 
            site_id, filename
        );
        
        if let Ok(response) = reqwest::Client::new()
            .get(&search_url)
            .bearer_auth(token)
            .send()
            .await {
            if response.status().is_success() {
                if let Ok(search_data) = response.json::<serde_json::Value>().await {
                    if let Some(items) = search_data["value"].as_array() {
                        println!("🔍 Found {} matching files", items.len());
                        
                        for item in items {
                            if let Some(item_name) = item["name"].as_str() {
                                if item_name == filename {
                                    if let Some(download_url) = item["@microsoft.graph.downloadUrl"].as_str() {
                                        if let Ok(file_response) = reqwest::Client::new().get(download_url).send().await {
                                            if file_response.status().is_success() {
                                                if let Ok(content) = file_response.bytes().await {
                                                    println!("✅ Downloaded via search: {} bytes", content.len());
                                                    return Ok(content.to_vec());
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        
        
        Err(ElusionError::Custom(format!(
            "Could not download file '{}'. Please verify:\n\
            1. File exists at the specified path\n\
            2. You have permission to access the file\n\
            3. File path is correct (try with/without 'Shared Documents/' prefix)\n\
            Platform: {}", 
            file_path,
            if cfg!(target_os = "windows") { "Windows" }
            else if cfg!(target_os = "macos") { "macOS" } 
            else { "Linux" }
        )))
    }

    /// List all files in a SharePoint folder
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
        
        // Remove duplicates
        let mut unique_paths = Vec::new();
        for path in folder_paths {
            if !unique_paths.contains(&path) {
                unique_paths.push(path);
            }
        }
        
        // Try each folder path variation
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
                                // Only include files, not folders
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
                            
                            println!("✅ Found {} files in folder: {}", files.len(), path);
                            return Ok(files);
                        }
                    }
                } else {
                    // FIX: Get status before consuming response
                    let status = response.status();
                    if let Ok(error_text) = response.text().await {
                        println!("    ❌ HTTP {}: {}", status, 
                            error_text.chars().take(100).collect::<String>());
                    }
                }
            }
        }
        
        // Fallback: try to find folder by search
        println!("🔍 Trying search-based folder discovery...");
        let folder_name = folder_path.split('/').last().unwrap_or(folder_path);
        let search_url = format!(
            "https://graph.microsoft.com/v1.0/sites/{}/drive/root/search(q='{}')", 
            site_id, 
            folder_name
        );
        
        if let Ok(response) = reqwest::Client::new()
            .get(&search_url)
            .bearer_auth(token)
            .send()
            .await {
            
            if response.status().is_success() {
                if let Ok(search_data) = response.json::<serde_json::Value>().await {
                    if let Some(items) = search_data["value"].as_array() {
                        // Look for folders matching our search
                        for item in items {
                            if item["folder"].is_object() {
                                if let Some(item_name) = item["name"].as_str() {
                                    if item_name == folder_name {
                                        // Found the folder, now get its contents
                                        if let Some(item_id) = item["id"].as_str() {
                                            let children_url = format!(
                                                "https://graph.microsoft.com/v1.0/sites/{}/drive/items/{}/children", 
                                                site_id, 
                                                item_id
                                            );
                                            
                                            if let Ok(children_response) = reqwest::Client::new()
                                                .get(&children_url)
                                                .bearer_auth(token)
                                                .send()
                                                .await {
                                                
                                                if children_response.status().is_success() {
                                                    if let Ok(children_data) = children_response.json::<serde_json::Value>().await {
                                                        if let Some(child_items) = children_data["value"].as_array() {
                                                            let mut files = Vec::new();
                                                            
                                                            for child_item in child_items {
                                                                // Only include files, not folders
                                                                if child_item["file"].is_object() {
                                                                    let file_info = SharePointFileInfo {
                                                                        name: child_item["name"].as_str().unwrap_or("unknown").to_string(),
                                                                        size: child_item["size"].as_u64(),
                                                                        created_date_time: child_item["createdDateTime"].as_str().map(|s| s.to_string()),
                                                                        last_modified_date_time: child_item["lastModifiedDateTime"].as_str().map(|s| s.to_string()),
                                                                        web_url: child_item["webUrl"].as_str().map(|s| s.to_string()),
                                                                        download_url: child_item["@microsoft.graph.downloadUrl"].as_str().map(|s| s.to_string()),
                                                                    };
                                                                    files.push(file_info);
                                                                }
                                                            }
                                                            
                                                            println!("✅ Found {} files via search in folder: {}", files.len(), item_name);
                                                            return Ok(files);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        Err(ElusionError::Custom(format!(
            "Could not list contents of folder '{}'. Please verify:\n\
            1. Folder exists at the specified path\n\
            2. You have permission to access the folder\n\
            3. Folder path is correct (try with/without 'Shared Documents/' prefix)\n\
            Platform: {}", 
            folder_path,
            if cfg!(target_os = "windows") { "Windows" }
            else if cfg!(target_os = "macos") { "macOS" } 
            else { "Linux" }
        )))
    }
}


//=======================

// Function to convert Excel date (days since 1900-01-01) to NaiveDate
fn excel_date_to_naive_date(excel_date: f64) -> Option<NaiveDate> {
    // Excel dates start at January 0, 1900, which is actually December 31, 1899
    // There's also a leap year bug in Excel that treats 1900 as a leap year
    let excel_epoch = NaiveDate::from_ymd_opt(1899, 12, 30)?;
    
    // Convert to days, ignoring any fractional part (time component)
    let days = excel_date.trunc() as i64;
    
    // Add days to epoch
    excel_epoch.checked_add_signed(Duration::days(days))
}

#[derive(Debug, Clone)]
pub struct ExcelWriteOptions {
    pub autofilter: bool,     
    pub freeze_header: bool,   
    pub table_style: bool,    
    pub sheet_protection: bool, 
}

impl Default for ExcelWriteOptions {
    fn default() -> Self {
        Self {
            autofilter: true,
            freeze_header: true,
            table_style: true,
            sheet_protection: false,
        }
    }
}

// Implement From<XlsxError> for ElusionError
#[cfg(feature = "excel")]
impl From<rust_xlsxwriter::XlsxError> for ElusionError {
    fn from(error: rust_xlsxwriter::XlsxError) -> Self {
        ElusionError::Custom(format!("Excel writing error: {}", error))
    }
}

// ======== PLOTTING
#[cfg(feature = "dashboard")]
use plotly::{Plot, Scatter, Bar, Histogram, BoxPlot, Pie};
#[cfg(feature = "dashboard")]
use plotly::common::{Mode, Line, Marker, Orientation};
#[cfg(feature = "dashboard")]
use plotly::layout::{Axis, Layout};
#[cfg(feature = "dashboard")]
use plotly::color::Rgb;
#[cfg(feature = "dashboard")]
use plotly::layout::update_menu::{Button,UpdateMenu,UpdateMenuDirection};
#[cfg(feature = "dashboard")]
use plotly::layout::{DragMode, RangeSlider};

use arrow::array::{Array, Float64Array,Int64Array,Int32Array,TimestampNanosecondArray, Date64Array,Date32Array};
#[cfg(feature = "dashboard")]
use std::cmp::Ordering;
#[cfg(not(feature = "dashboard"))]
pub struct Plot;

// ======== STATISTICS
use datafusion::common::ScalarValue;

// ========== AZURE
#[cfg(feature = "azure")]
use azure_storage_blobs::prelude::*;
#[cfg(feature = "azure")]
use azure_storage::StorageCredentials;
#[cfg(feature = "azure")]
use azure_storage::CloudLocation;
#[cfg(feature = "azure")]
use futures::stream;
use std::io::BufReader;
#[cfg(feature = "azure")]
use futures::pin_mut;
#[cfg(feature = "azure")]
use csv::ReaderBuilder;
#[cfg(feature = "azure")]
use csv::Trim::All;
use serde_json::Deserializer;
// ==== pisanje
#[cfg(feature = "azure")]
use azure_storage_blobs::blob::{BlockList, BlobBlockType};
#[cfg(any(feature = "api", feature = "azure"))]
use bytes::Bytes;
#[cfg(feature = "azure")]
use datafusion::parquet::basic::Compression;
#[cfg(feature = "azure")]
use datafusion::parquet::file::properties::{WriterProperties, WriterVersion};
#[cfg(feature = "azure")]
use datafusion::parquet::arrow::ArrowWriter;
#[cfg(feature = "azure")]
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
#[cfg(feature = "azure")]
use futures::TryStreamExt;
#[cfg(feature = "azure")]
use tempfile::Builder;

//  ==================== Pipeline Scheduler
use std::future::Future;
use tokio_cron_scheduler::{JobScheduler, Job};

// ======== From API
#[cfg(any(feature = "api", feature = "azure"))]
use reqwest::Client;

//========== VIEWS
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use chrono::{DateTime, Utc};
use std::sync::Mutex;
use lazy_static::lazy_static;

// =========== DATE TABLE BUILDER
use arrow::array::Int32Builder;
use arrow::array::BooleanBuilder;
use chrono::{Datelike, Weekday, Duration, NaiveDateTime, NaiveTime};

//================ POSTGRES
#[cfg(feature = "postgres")]
use tokio_postgres::{Client as PostgresClient,NoTls, Error as PgError, Row as PostgresRow};
#[cfg(feature = "postgres")]
use tokio_postgres::types::{Type, ToSql};
#[cfg(feature = "postgres")]
use tokio::sync::Mutex as PostgresMutex;
#[cfg(feature = "postgres")]
use rust_decimal::Decimal;
#[cfg(feature = "postgres")]
use rust_decimal::prelude::ToPrimitive;

//================ MYSQL
#[cfg(feature = "mysql")]
use mysql_async::{Pool as MySqlPool, OptsBuilder as MySqlOptsBuilder, Conn as MySqlConn, Row as MySqlRow, Value as MySqlValue, Error as MySqlError};
#[cfg(feature = "mysql")]
use mysql_async::prelude::*;

/// MySQL connection configuration options
#[cfg(feature = "mysql")]
#[derive(Debug, Clone)]
pub struct MySqlConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub pool_size: Option<usize>,
}

#[cfg(feature = "mysql")]
impl Default for MySqlConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 3306,
            user: "root".to_string(),
            password: String::new(),
            database: "mysql".to_string(),
            pool_size: Some(5),
        }
    }
}

#[cfg(feature = "mysql")]
impl MySqlConfig {
    /// Create a new MySqlConfig with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a connection string from the configuration
    pub fn connection_string(&self) -> String {
        format!(
            "mysql://{}:{}@{}:{}/{}",
            self.user,
            self.password,
            self.host,
            self.port,
            self.database
        )
    }
}

#[cfg(not(feature = "mysql"))]
#[derive(Debug, Clone)]
pub struct MySqlConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub pool_size: Option<usize>,
}

#[cfg(not(feature = "mysql"))]
impl Default for MySqlConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 3306,
            user: "root".to_string(),
            password: String::new(),
            database: "mysql".to_string(),
            pool_size: Some(5),
        }
    }
}

#[cfg(not(feature = "mysql"))]
impl MySqlConfig {
    /// Create a new MySqlConfig with default values (stub)
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a connection string from the configuration (stub)
    pub fn connection_string(&self) -> String {
        String::new()
    }
}

/// MySQL connection manager that supports connection pooling
#[cfg(feature = "mysql")]
pub struct MySqlConnection {
    pool: MySqlPool,
}

#[cfg(not(feature = "mysql"))]
pub struct MySqlConnection {
}

#[cfg(feature = "mysql")]
impl MySqlConnection {
    /// Create a new MySQL connection manager
    pub async fn new(config: MySqlConfig) -> Result<Self, MySqlError> {
        let opts = MySqlOptsBuilder::default()
            .ip_or_hostname(config.host)
            .tcp_port(config.port)
            .user(Some(config.user))
            .pass(Some(config.password))
            .db_name(Some(config.database));

        let pool = MySqlPool::new(opts);
        
        Ok(Self { pool })
    }

    /// Get a client from the pool
    async fn get_conn(&self) -> Result<MySqlConn, MySqlError> {
        self.pool.get_conn().await
    }

    /// Execute a query that returns rows
    pub async fn query(&self, query: &str) -> Result<Vec<MySqlRow>, MySqlError> {
        let mut conn = self.get_conn().await?;
        let result = conn.query(query).await?;
        Ok(result)
    }

    /// Check if the connection is valid
    pub async fn ping(&self) -> Result<(), MySqlError> {
        let mut conn = self.get_conn().await?;
        
        conn.ping().await
    }

    /// Disconnect and close the pool
    pub async fn disconnect(self) -> Result<(), MySqlError> {
        self.pool.disconnect().await
    }
}

#[cfg(not(feature = "mysql"))]
impl MySqlConnection {
    /// Create a new MySQL connection manager (stub)
    pub async fn new(_config: MySqlConfig) -> Result<Self, ElusionError> {
        Err(ElusionError::Custom("*** Warning ***: MySQL feature not enabled. Add feature = [\"mysql\"] under [dependencies]".to_string()))
    }

    /// Execute a query that returns rows (stub)
    pub async fn query<P>(&self, _query: &str, _params: P) -> Result<Vec<()>, ElusionError>
    where
        P: Send,
    {
        Err(ElusionError::Custom("*** Warning ***: MySQL feature not enabled. Add feature = [\"mysql\"] under [dependencies]".to_string()))
    }

    /// Check if the connection is valid (stub)
    pub async fn ping(&self) -> Result<(), ElusionError> {
        Err(ElusionError::Custom("*** Warning ***: MySQL feature not enabled. Add feature = [\"mysql\"] under [dependencies]".to_string()))
    }
}

#[cfg(feature = "mysql")]
impl From<mysql_async::Error> for ElusionError {
    fn from(err: mysql_async::Error) -> Self {
        ElusionError::Custom(format!("MySQL error: {}", err))
    }
}


/// PostgreSQL connection configuration options
#[cfg(feature = "postgres")]
#[derive(Debug, Clone)]
pub struct PostgresConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub pool_size: Option<usize>,
}
#[cfg(feature = "postgres")]
impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5433,
            user: "postgres".to_string(),
            password: String::new(),
            database: "postgres".to_string(),
            pool_size: Some(5),
        }
    }
}
#[cfg(feature = "postgres")]
impl PostgresConfig {
    /// Create a new PostgresConfig with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a connection string from the configuration
    pub fn connection_string(&self) -> String {
        let mut params = Vec::new();
        
        params.push(format!("host={}", self.host));
        params.push(format!("port={}", self.port));
        params.push(format!("user={}", self.user));
        params.push(format!("dbname={}", self.database));
        
        if !self.password.is_empty() {
            params.push(format!("password={}", self.password));
        }
        
        // Add sslmode=prefer for better compatibility
        params.push("sslmode=prefer".to_string());
        
        params.join(" ")
    }
}

#[cfg(not(feature = "postgres"))]
#[derive(Debug, Clone)]
pub struct PostgresConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub pool_size: Option<usize>,
}

#[cfg(not(feature = "postgres"))]
impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5432,
            user: "postgres".to_string(),
            password: String::new(),
            database: "postgres".to_string(),
            pool_size: Some(5),
        }
    }
}

#[cfg(not(feature = "postgres"))]
impl PostgresConfig {
    /// Create a new PostgresConfig with default values (stub)
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a connection string from the configuration (stub)
    pub fn connection_string(&self) -> String {
        String::new()
    }
}

/// PostgreSQL connection manager that supports connection pooling
#[cfg(feature = "postgres")]
pub struct PostgresConnection {
    config: PostgresConfig,
    client_pool: Arc<PostgresMutex<Vec<PostgresClient>>>,
}

#[cfg(not(feature = "postgres"))]
pub struct PostgresConnection {
}

#[cfg(feature = "postgres")]
impl PostgresConnection {
    /// Create a new PostgreSQL connection manager
    pub async fn new(config: PostgresConfig) -> Result<Self, PgError> {
        let pool_size = config.pool_size.unwrap_or(5);
        let mut clients = Vec::with_capacity(pool_size);

        // Create initial connection
        let conn_str = config.connection_string();
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
        
        // Spawn the connection task
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });
        
        clients.push(client);
        
        // Create pool of connections
        for _ in 1..pool_size {
            let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
            
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("Connection error: {}", e);
                }
            });
            
            clients.push(client);
        }

        Ok(Self {
            config,
            client_pool: Arc::new(PostgresMutex::new(clients)),
        })
    }

    /// Get a client from the pool
    async fn get_client(&self) -> Result<PostgresClient, PgError> {
        let mut pool = self.client_pool.lock().await;
        
        if let Some(client) = pool.pop() {
            Ok(client)
        } else {
            // If pool is empty, create a new connection
            let conn_str = self.config.connection_string();
            let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
            
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("Connection error: {}", e);
                }
            });
            
            Ok(client)
        }
    }

    /// Return a client to the pool
    async fn return_client(&self, client: PostgresClient) {
        let mut pool = self.client_pool.lock().await;
        
        if pool.len() < self.config.pool_size.unwrap_or(5) {
            pool.push(client);
        }
        // If pool is at capacity, client will be dropped
    }

    /// Execute a query that returns rows
    pub async fn query(&self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<PostgresRow>, PgError> {
        let client = self.get_client().await?;
        
        let result = client.query(query, params).await;
        
        self.return_client(client).await;
        
        result
    }

    /// Check if the connection is valid
    pub async fn ping(&self) -> Result<(), PgError> {
        let client = self.get_client().await?;
        
        let result = client.execute("SELECT 1", &[]).await;
        
        self.return_client(client).await;
        
        result.map(|_| ())
    }

}

#[cfg(not(feature = "postgres"))]
impl PostgresConnection {
    /// Create a new PostgreSQL connection manager (stub)
    pub async fn new(_config: PostgresConfig) -> Result<Self, ElusionError> {
        Err(ElusionError::Custom("*** Warning ***: Postgres feature not enabled. Add feature under [dependencies]".to_string()))
    }

    /// Execute a query that returns rows (stub)
    pub async fn query(&self, _query: &str, _params: &[&str]) -> Result<Vec<()>, ElusionError> {
        Err(ElusionError::Custom("*** Warning ***: Postgres feature not enabled. Add feature under [dependencies]".to_string()))
    }

    /// Check if the connection is valid (stub)
    pub async fn ping(&self) -> Result<(), ElusionError> {
        Err(ElusionError::Custom("*** Warning ***: Postgres feature not enabled. Add feature under [dependencies]".to_string()))
    }
}

#[cfg(feature = "postgres")]
impl From<tokio_postgres::error::Error> for ElusionError {
    fn from(err: tokio_postgres::error::Error) -> Self {
        ElusionError::Custom(format!("PostgreSQL error: {}", err))
    }
}

pub enum DateFormat {
    IsoDate,            // YYYY-MM-DD
    IsoDateTime,        // YYYY-MM-DD HH:MM:SS
    UsDate,             // MM/DD/YYYY
    EuropeanDate,       // DD.MM.YYYY
    EuropeanDateDash,   // DD-MM-YYYY
    BritishDate,        // DD/MM/YYYY
    HumanReadable,      // 1 Jan 2025
    HumanReadableTime,  // 1 Jan 2025 00:00
    SlashYMD,           // YYYY/MM/DD
    DotYMD,             // YYYY.MM.DD
    CompactDate,        // YYYYMMDD
    YearMonth,          // YYYY-MM
    MonthYear,          // MM-YYYY
    MonthNameYear,      // January 2025
    Custom(String)      // Custom format string
}

impl DateFormat {
    fn format_str(&self) -> &str {
        match self {
            DateFormat::IsoDate => "%Y-%m-%d",
            DateFormat::IsoDateTime => "%Y-%m-%d %H:%M:%S",
            DateFormat::UsDate => "%m/%d/%Y",
            DateFormat::EuropeanDate => "%d.%m.%Y",
            DateFormat::EuropeanDateDash => "%d-%m-%Y",
            DateFormat::BritishDate => "%d/%m/%Y",
            DateFormat::HumanReadable => "%e %b %Y",
            DateFormat::HumanReadableTime => "%e %b %Y %H:%M",
            DateFormat::SlashYMD => "%Y/%m/%d",
            DateFormat::DotYMD => "%Y.%m.%d",
            DateFormat::CompactDate => "%Y%m%d",
            DateFormat::YearMonth => "%Y-%m",
            DateFormat::MonthYear => "%m-%Y",
            DateFormat::MonthNameYear => "%B %Y",
            DateFormat::Custom(fmt) => fmt,
        }
    }
}
pub struct MaterializedView {
    // Name of the materialized view
    pub(crate) name: String,
    // The SQL query that defines this view
    definition: String,
    // The actual data stored as batches
    data: Vec<RecordBatch>,
    // Time when this view was created/refreshed
    refresh_time: DateTime<Utc>,
    // Optional time-to-live in seconds
    ttl: Option<u64>,
}

impl MaterializedView {
    fn is_valid(&self) -> bool {
        if let Some(ttl) = self.ttl {
            let now = Utc::now();
            let age = now.signed_duration_since(self.refresh_time).num_seconds();
            return age < ttl as i64;
        }
        true
    }

    fn display_info(&self) -> String {
        format!(
            "View '{}' - Created: {}, TTL: {} seconds",
            self.name,
            self.refresh_time.format("%Y-%m-%d %H:%M:%S"),
            self.ttl.map_or("None".to_string(), |ttl| ttl.to_string())
        )
    }
}

pub struct QueryCache {
    cached_queries: HashMap<u64, (Vec<RecordBatch>, DateTime<Utc>)>,
    max_cache_size: usize,
    ttl_seconds: Option<u64>,
}

impl QueryCache {
    pub fn new(max_cache_size: usize, ttl_seconds: Option<u64>) -> Self {
        Self {
            cached_queries: HashMap::new(),
            max_cache_size,
            ttl_seconds,
        }
    }

    pub fn cache_query(&mut self, query: &str, result: Vec<RecordBatch>) {
        if self.cached_queries.len() >= self.max_cache_size {
            // Simple LRU eviction - remove the oldest entry
            if let Some(oldest) = self.cached_queries
                .iter()
                .min_by_key(|(_, (_, time))| time) {
                let key = *oldest.0;
                self.cached_queries.remove(&key);
            }
        }

        let mut hasher = DefaultHasher::new();
        query.hash(&mut hasher);
        let query_hash = hasher.finish();
        self.cached_queries.insert(query_hash, (result, Utc::now()));
    }

    pub fn get_cached_result(&mut self, query: &str) -> Option<Vec<RecordBatch>> {
        let mut hasher = DefaultHasher::new();
        query.hash(&mut hasher);
        let query_hash = hasher.finish();

        if let Some((result, timestamp)) = self.cached_queries.get(&query_hash) {
            // Check TTL if set
            if let Some(ttl) = self.ttl_seconds {
                let now = Utc::now();
                let age = now.signed_duration_since(*timestamp).num_seconds();
                if age > ttl as i64 {
                    // Cache entry expired, remove it
                    self.cached_queries.remove(&query_hash);
                    return None;
                }
            }
            return Some(result.clone());
        }
        None
    }

    pub fn clear(&mut self) {
        self.cached_queries.clear();
    }

    pub fn invalidate(&mut self, table_names: &[String]) {
        // if any tables are modified, clear entire cache
        if !table_names.is_empty() {
            println!("Invalidating cache due to changes in tables: {:?}", table_names);
            self.clear();
        }
    }
}

pub struct MaterializedViewManager {
    views: HashMap<String, MaterializedView>,
    max_views: usize,
}

impl MaterializedViewManager {
    pub fn new(max_views: usize) -> Self {
        Self {
            views: HashMap::new(),
            max_views,
        }
    }

    pub async fn create_view(
        &mut self,
        ctx: &SessionContext,
        name: &str,
        query: &str,
        ttl: Option<u64>,
    ) -> ElusionResult<()> {
        // Check if we've hit the max number of views
        if self.views.len() >= self.max_views && !self.views.contains_key(name) {
            return Err(ElusionError::Custom(
                format!("Maximum number of materialized views ({}) reached", self.max_views)
            ));
        }

        // Execute the query
        let df = ctx.sql(query).await.map_err(|e| ElusionError::Custom(
            format!("Failed to execute query for materialized view: {}", e)
        ))?;

        let batches = df.collect().await.map_err(|e| ElusionError::Custom(
            format!("Failed to collect results for materialized view: {}", e)
        ))?;

        // Create or update the materialized view
        let view = MaterializedView {
            name: name.to_string(),
            definition: query.to_string(),
            data: batches,
            refresh_time: Utc::now(),
            ttl,
        };

        self.views.insert(name.to_string(), view);
        Ok(())
    }

    pub async fn refresh_view(
        &mut self,
        ctx: &SessionContext,
        name: &str,
    ) -> ElusionResult<()> {
        if let Some(view) = self.views.get(name) {
            let query = view.definition.clone();
            let ttl = view.ttl;
            return self.create_view(ctx, name, &query, ttl).await;
        }
        Err(ElusionError::Custom(format!("View '{}' not found", name)))
    }

    pub async fn get_view_as_dataframe(
        &self,
        ctx: &SessionContext,
        name: &str,
    ) -> ElusionResult<DataFrame> {
        if let Some(view) = self.views.get(name) {
            if !view.is_valid() {
                return Err(ElusionError::Custom(
                    format!("View '{}' has expired", name)
                ));
            }

            let schema = match view.data.first() {
                Some(batch) => batch.schema(),
                None => return Err(ElusionError::Custom(
                    format!("View '{}' contains no data", name)
                )),
            };

            let mem_table = MemTable::try_new(schema.clone(), vec![view.data.clone()])
                .map_err(|e| ElusionError::Custom(
                    format!("Failed to create memory table from view: {}", e)
                ))?;

            let table_name = format!("view_{}", name);
            ctx.register_table(&table_name, Arc::new(mem_table))
                .map_err(|e| ElusionError::Custom(
                    format!("Failed to register table from view: {}", e)
                ))?;

            let df = ctx.table(&table_name).await
                .map_err(|e| ElusionError::Custom(
                    format!("Failed to create DataFrame from view: {}", e)
                ))?;

            Ok(df)
        } else {
            Err(ElusionError::Custom(format!("View '{}' not found", name)))
        }
    }

    pub fn drop_view(&mut self, name: &str) -> ElusionResult<()> {
        if self.views.remove(name).is_some() {
            println!("View '{}' droped.", name);
            Ok(())
        } else {
            Err(ElusionError::Custom(format!("View '{}' not found", name)))
        }
    }

    pub fn list_views(&self) -> Vec<(String, DateTime<Utc>, Option<u64>)> {
        let mut result = Vec::new();

        if self.views.is_empty() {
            return result;
        }

        for (view_name, view) in &self.views {
            println!("{}", view.display_info());
            result.push((view_name.clone(), view.refresh_time, view.ttl));
        }
        result
    }


    pub fn get_view_metadata(&self, name: &str) -> Option<(String, DateTime<Utc>, Option<u64>)> {
        self.views.get(name).map(|view| (
            view.definition.clone(),
            view.refresh_time,
            view.ttl
        ))
    }
}

// Global state for caching and materialized views
lazy_static! {
    static ref QUERY_CACHE: Mutex<QueryCache> = Mutex::new(QueryCache::new(100, Some(3600))); // 1 hour TTL
    static ref MATERIALIZED_VIEW_MANAGER: Mutex<MaterializedViewManager> = Mutex::new(MaterializedViewManager::new(50));
}

//=============== AZURE

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

// Enum for writing options
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AzureWriteMode {
    Overwrite,
    Append,
    ErrorIfExists,
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

// Helper function to convert Arrow array values to serde_json::Value
fn array_value_to_json(array: &Arc<dyn Array>, index: usize) -> ElusionResult<serde_json::Value> {
    if array.is_null(index) {
        return Ok(serde_json::Value::Null);
    }
    // matching on array data type and convert 
    match array.data_type() {
        ArrowDataType::Null => Ok(serde_json::Value::Null),
        ArrowDataType::Boolean => {
            let array = array.as_any().downcast_ref::<BooleanArray>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert Boolean array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            Ok(serde_json::Value::Bool(array.value(index)))
        },
        ArrowDataType::Int8 | ArrowDataType::Int16 | ArrowDataType::Int32 => {
            let array = array.as_any().downcast_ref::<Int32Array>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert Int32 array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            Ok(serde_json::Value::Number(serde_json::Number::from(array.value(index))))
        },
        ArrowDataType::Int64 => {
            let array = array.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert Int64 array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            // Convert i64 to serde_json::Number
            let n = array.value(index);
            serde_json::Number::from_f64(n as f64)
                .map(serde_json::Value::Number)
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: format!("Cannot represent i64 value {} as JSON number", n),
                    suggestion: "💡 Consider using string representation for large integers".to_string(),
                })
        },
        ArrowDataType::UInt8 | ArrowDataType::UInt16 | ArrowDataType::UInt32 => {
            let array = array.as_any().downcast_ref::<UInt32Array>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert UInt32 array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            Ok(serde_json::Value::Number(serde_json::Number::from(array.value(index))))
        },
        ArrowDataType::UInt64 => {
            let array = array.as_any().downcast_ref::<UInt64Array>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert UInt64 array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            // Convert u64 to serde_json::Number
            let n = array.value(index);
            serde_json::Number::from_f64(n as f64)
                .map(serde_json::Value::Number)
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: format!("Cannot represent u64 value {} as JSON number", n),
                    suggestion: "💡 Consider using string representation for large integers".to_string(),
                })
        },
        ArrowDataType::Float32 => {
            let array = array.as_any().downcast_ref::<Float32Array>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert Float32 array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            let val = array.value(index) as f64;
            serde_json::Number::from_f64(val)
                .map(serde_json::Value::Number)
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: format!("Cannot represent f32 value {} as JSON number", val),
                    suggestion: "💡 Consider handling special float values differently".to_string(),
                })
        },
        ArrowDataType::Float64 => {
            let array = array.as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert Float64 array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            let val = array.value(index);
            let result = serde_json::Number::from_f64(val)
                .map(serde_json::Value::Number)
                .unwrap_or_else(|| {
                    // Handle special float values like NaN, Infinity
                    if val.is_nan() {
                        serde_json::Value::String("NaN".to_string())
                    } else if val.is_infinite() {
                        if val.is_sign_positive() {
                            serde_json::Value::String("Infinity".to_string())
                        } else {
                            serde_json::Value::String("-Infinity".to_string())
                        }
                    } else {
                        serde_json::Value::Null
                    }
                });
            
            Ok(result)
        },
        ArrowDataType::Utf8 => {
            let array = array.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert String array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            Ok(serde_json::Value::String(array.value(index).to_string()))
        },
        ArrowDataType::LargeUtf8 => {
            let array = array.as_any().downcast_ref::<LargeStringArray>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert LargeString array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            Ok(serde_json::Value::String(array.value(index).to_string()))
        },
        ArrowDataType::Binary => {
            let array = array.as_any().downcast_ref::<BinaryArray>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert Binary array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            // Encode binary data as base64 string
            let bytes = array.value(index);
            let b64 = base64::engine::general_purpose::STANDARD.encode(bytes); 
            Ok(serde_json::Value::String(b64))
        },
        ArrowDataType::LargeBinary => {
            let array = array.as_any().downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert LargeBinary array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            // Encode binary data as base64 string
            let bytes = array.value(index);
            let b64 = base64::engine::general_purpose::STANDARD.encode(bytes); 
            Ok(serde_json::Value::String(b64))
        },
        ArrowDataType::Date32 => {
            let array = array.as_any().downcast_ref::<Date32Array>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert Date32 array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            // Convert to ISO date string
            let days = array.value(index);
            let naive_date = NaiveDate::from_num_days_from_ce_opt(days + 719163)
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Date Conversion".to_string(),
                    reason: format!("Invalid date value: {}", days),
                    suggestion: "💡 Check if date values are within valid range".to_string(),
                })?;
            Ok(serde_json::Value::String(naive_date.format("%Y-%m-%d").to_string()))
        },
        ArrowDataType::Date64 => {
            let array = array.as_any().downcast_ref::<Date64Array>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert Date64 array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            // Convert milliseconds since epoch to datetime string
            let ms = array.value(index);
            let secs = ms / 1000;
            let nsecs = ((ms % 1000) * 1_000_000) as u32;
            let naive_datetime = DateTime::from_timestamp(secs, nsecs)
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Date Conversion".to_string(),
                    reason: format!("Invalid timestamp value: {}", ms),
                    suggestion: "💡 Check if timestamp values are within valid range".to_string(),
                })?;
            Ok(serde_json::Value::String(naive_datetime.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()))
        },
        ArrowDataType::Timestamp(time_unit, _) => {
            // Handle timestamp based on time unit
            match time_unit {
                TimeUnit::Second => {
                    let array = array.as_any().downcast_ref::<TimestampSecondArray>()
                        .ok_or_else(|| ElusionError::InvalidOperation {
                            operation: "Type Conversion".to_string(),
                            reason: "Failed to convert TimestampSecond array".to_string(),
                            suggestion: "💡 This is an internal error, please report it".to_string(),
                        })?;
                    let seconds = array.value(index);
                    let dt = DateTime::from_timestamp(seconds, 0)
                        .ok_or_else(|| ElusionError::InvalidOperation {
                            operation: "Timestamp Conversion".to_string(),
                            reason: format!("Invalid timestamp value: {}", seconds),
                            suggestion: "💡 Check if timestamp values are within valid range".to_string(),
                        })?;
                    Ok(serde_json::Value::String(dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()))
                },
                TimeUnit::Millisecond => {
                    let array = array.as_any().downcast_ref::<TimestampMillisecondArray>()
                        .ok_or_else(|| ElusionError::InvalidOperation {
                            operation: "Type Conversion".to_string(),
                            reason: "Failed to convert TimestampMillisecond array".to_string(),
                            suggestion: "💡 This is an internal error, please report it".to_string(),
                        })?;
                    let ms = array.value(index);
                    let secs = ms / 1000;
                    let nsecs = ((ms % 1000) * 1_000_000) as u32;
                    let dt = DateTime::from_timestamp(secs, nsecs)
                        .ok_or_else(|| ElusionError::InvalidOperation {
                            operation: "Timestamp Conversion".to_string(),
                            reason: format!("Invalid timestamp value: {}", ms),
                            suggestion: "💡 Check if timestamp values are within valid range".to_string(),
                        })?;
                    Ok(serde_json::Value::String(dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()))
                },
                TimeUnit::Microsecond => {
                    let array = array.as_any().downcast_ref::<TimestampMicrosecondArray>()
                        .ok_or_else(|| ElusionError::InvalidOperation {
                            operation: "Type Conversion".to_string(),
                            reason: "Failed to convert TimestampMicrosecond array".to_string(),
                            suggestion: "💡 This is an internal error, please report it".to_string(),
                        })?;
                    let us = array.value(index);
                    let secs = us / 1_000_000;
                    let nsecs = ((us % 1_000_000) * 1_000) as u32;
                    let dt = DateTime::from_timestamp(secs, nsecs)
                        .ok_or_else(|| ElusionError::InvalidOperation {
                            operation: "Timestamp Conversion".to_string(),
                            reason: format!("Invalid timestamp value: {}", us),
                            suggestion: "💡 Check if timestamp values are within valid range".to_string(),
                        })?;
                    Ok(serde_json::Value::String(dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()))
                },
                TimeUnit::Nanosecond => {
                    let array = array.as_any().downcast_ref::<TimestampNanosecondArray>()
                        .ok_or_else(|| ElusionError::InvalidOperation {
                            operation: "Type Conversion".to_string(),
                            reason: "Failed to convert TimestampNanosecond array".to_string(),
                            suggestion: "💡 This is an internal error, please report it".to_string(),
                        })?;
                    let ns = array.value(index);
                    let secs = ns / 1_000_000_000;
                    let nsecs = (ns % 1_000_000_000) as u32;
                    let dt = DateTime::from_timestamp(secs, nsecs)
                        .ok_or_else(|| ElusionError::InvalidOperation {
                            operation: "Timestamp Conversion".to_string(),
                            reason: format!("Invalid timestamp value: {}", ns),
                            suggestion: "💡 Check if timestamp values are within valid range".to_string(),
                        })?;
                    Ok(serde_json::Value::String(dt.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string()))
                },
            }
        },
        ArrowDataType::List(_) => {
            let list_array = array.as_any().downcast_ref::<ListArray>()
                .ok_or_else(|| ElusionError::InvalidOperation {
                    operation: "Type Conversion".to_string(),
                    reason: "Failed to convert List array".to_string(),
                    suggestion: "💡 This is an internal error, please report it".to_string(),
                })?;
            
            let values = list_array.value(index);
            let mut json_values = Vec::new();
            
            for i in 0..values.len() {
                let json_value = array_value_to_json(&values, i)?;
                json_values.push(json_value);
            }
            
            Ok(serde_json::Value::Array(json_values))
        },
        _ => {
            Ok(serde_json::Value::String(format!("Unsupported type: {:?}", array.data_type())))
        }
    }
}

// ===== struct to manage ODBC DB connections


#[derive(Debug, PartialEq, Clone)]
pub enum DatabaseType {
    MySQL,
    PostgreSQL,
    MongoDB,
    SQLServer,
    Unknown
}

//======= Ploting Helper functions
#[cfg(feature = "dashboard")]
fn convert_to_f64_vec(array: &dyn Array) -> ElusionResult<Vec<f64>> {
    match array.data_type() {
        ArrowDataType::Float64 => {
            let float_array = array.as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| ElusionError::Custom("Failed to downcast to Float64Array".to_string()))?;
            Ok(float_array.values().to_vec())
        },
        ArrowDataType::Int64 => {
            let int_array = array.as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| ElusionError::Custom("Failed to downcast to Int64Array".to_string()))?;
            Ok(int_array.values().iter().map(|&x| x as f64).collect())
        },
        ArrowDataType::Date32 => {
            let date_array = array.as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| ElusionError::Custom("Failed to downcast to Date32Array".to_string()))?;
            Ok(convert_date32_to_timestamps(date_array))
        },
        ArrowDataType::Utf8 => {
            let string_array = array.as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| ElusionError::Custom("Failed to downcast to StringArray".to_string()))?;
            let mut values = Vec::with_capacity(array.len());
            for i in 0..array.len() {
                let value = string_array.value(i).parse::<f64>().unwrap_or(0.0);
                values.push(value);
            }
            Ok(values)
        },
        other_type => {
            Err(ElusionError::Custom(format!("Unsupported data type for plotting: {:?}", other_type)))
        }
    }
}

#[cfg(feature = "dashboard")]
fn convert_to_string_vec(array: &dyn Array) -> ElusionResult<Vec<String>> {
    match array.data_type() {
        ArrowDataType::Utf8 => {
            let string_array = array.as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| ElusionError::Custom("Failed to downcast to StringArray".to_string()))?;
            
            let mut values = Vec::with_capacity(array.len());
            for i in 0..array.len() {
                values.push(string_array.value(i).to_string());
            }
            Ok(values)
        },
        other_type => {
            Err(ElusionError::Custom(format!("Expected string type but got: {:?}", other_type)))
        }
    }
}

#[cfg(feature = "dashboard")]
fn convert_date32_to_timestamps(array: &Date32Array) -> Vec<f64> {
    array.values()
        .iter()
        .map(|&days| {
            // Convert days since epoch to timestamp
            let date = NaiveDate::from_num_days_from_ce_opt(days + 719163)
                .unwrap_or(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
            let datetime = date.and_hms_opt(0, 0, 0).unwrap();
            datetime.and_utc().timestamp() as f64 * 1000.0 // Convert to milliseconds for plotly
        })
        .collect()
}

// Helper function to sort date-value pairs
#[cfg(feature = "dashboard")]
fn sort_by_date(x_values: &[f64], y_values: &[f64]) -> (Vec<f64>, Vec<f64>) {
    let mut pairs: Vec<(f64, f64)> = x_values.iter()
        .cloned()
        .zip(y_values.iter().cloned())
        .collect();
    
    // Sort by date (x values)
    pairs.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
    
    // Unzip back into separate vectors
    pairs.into_iter().unzip()
}

//helper funciton for converting dates for dashboard
#[cfg(feature = "dashboard")]
fn parse_date_string(date_str: &str) -> Option<chrono::NaiveDateTime> {
    // Try different date formats
    let formats = [
        // Standard formats
        "%Y-%m-%d",           // 2024-03-12
        "%d.%m.%Y",           // 1.2.2024
        "%d/%m/%Y",           // 1/2/2024
        "%Y/%m/%d",           // 2024/03/12
        
        // Formats with month names
        "%d %b %Y",           // 1 Jan 2024
        "%d %B %Y",           // 1 January 2024
        "%b %d %Y",           // Jan 1 2024
        "%B %d %Y",           // January 1 2024
        
        // Formats with time
        "%Y-%m-%d %H:%M:%S",  // 2024-03-12 15:30:00
        "%d.%m.%Y %H:%M:%S",  // 1.2.2024 15:30:00
        
        // Additional regional formats
        "%m/%d/%Y",           // US format: 3/12/2024
        "%Y.%m.%d",           // 2024.03.12
    ];

    for format in formats {
        if let Ok(date) = chrono::NaiveDateTime::parse_from_str(date_str, format) {
            return Some(date);
        } else if let Ok(date) = chrono::NaiveDate::parse_from_str(date_str, format) {
            return Some(date.and_hms_opt(0, 0, 0).unwrap_or_default());
        }
    }

    None
}

// ======== Custom error type
#[derive(Debug)]
pub enum ElusionError {
    MissingColumn {
        column: String,
        available_columns: Vec<String>,
    },
    InvalidDataType {
        column: String,
        expected: String,
        found: String,
    },
    DuplicateColumn {
        column: String,
        locations: Vec<String>,
    },
    InvalidOperation {
        operation: String,
        reason: String,
        suggestion: String,
    },
    SchemaError {
        message: String,
        schema: Option<String>,
        suggestion: String,
    },
    JoinError {
        message: String,
        left_table: String,
        right_table: String,
        suggestion: String,
    },
    GroupByError {
        message: String,
        invalid_columns: Vec<String>,
        suggestion: String,
    },
    WriteError {
        path: String,
        operation: String,
        reason: String,
        suggestion: String,
    },
    PartitionError {
        message: String,
        partition_columns: Vec<String>,
        suggestion: String,
    },
    AggregationError {
        message: String,
        function: String,
        column: String,
        suggestion: String,
    },
    OrderByError {
        message: String,
        columns: Vec<String>,
        suggestion: String,
    },
    WindowFunctionError {
        message: String,
        function: String,
        details: String,
        suggestion: String,
    },
    LimitError {
        message: String,
        value: u64,
        suggestion: String,
    },
    SetOperationError {
        operation: String,
        reason: String,
        suggestion: String,
    },
    DataFusion(DataFusionError),
    Io(std::io::Error),
    Custom(String),
}

impl fmt::Display for ElusionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ElusionError::MissingColumn { column, available_columns } => {
                let suggestion = suggest_similar_column(column, available_columns);
                write!(
                    f,
                    "🔍 Column Not Found: '{}'\n\
                     📋 Available columns are: {}\n\
                     💡 Did you mean '{}'?\n\
                     🔧 Check for typos or use .display_schema() to see all available columns.",
                    column,
                    available_columns.join(", "),
                    suggestion
                )
            },
            ElusionError::InvalidDataType { column, expected, found } => write!(
                f,
                "📊 Type Mismatch in column '{}'\n\
                 ❌ Found: {}\n\
                 ✅ Expected: {}\n\
                 💡 Try: .with_column(\"{}\", cast(\"{}\", {}));",
                column, found, expected, column, column, expected
            ),
            ElusionError::DuplicateColumn { column, locations } => write!(
                f,
                "🔄 Duplicate Column: '{}'\n\
                 📍 Found in: {}\n\
                 💡 Try using table aliases or renaming columns:\n\
                 .select([\"table1.{} as table1_{}\", \"table2.{} as table2_{}\"])",
                column,
                locations.join(", "),
                column, column, column, column
            ),
            ElusionError::InvalidOperation { operation, reason, suggestion } => write!(
                f,
                "⚠️ Invalid Operation: {}\n\
                 ❌ Problem: {}\n\
                 💡 Suggestion: {}",
                operation, reason, suggestion
            ),
            ElusionError::SchemaError { message, schema, suggestion } => {
                let schema_info = schema.as_ref().map_or(
                    String::new(),
                    |s| format!("\n📋 Current Schema:\n{}", s)
                );
                write!(
                    f,
                    "🏗️ Schema Error: {}{}\n\
                     💡 Suggestion: {}",
                    message, schema_info, suggestion
                )
            },
            ElusionError::JoinError { message, left_table, right_table, suggestion } => write!(
                f,
                "🤝 Join Error:\n\
                 ❌ {}\n\
                 📌 Left Table: {}\n\
                 📌 Right Table: {}\n\
                 💡 Suggestion: {}",
                message, left_table, right_table, suggestion
            ),
            ElusionError::GroupByError { message, invalid_columns, suggestion } => write!(
                f,
                "📊 Group By Error: {}\n\
                 ❌ Invalid columns: {}\n\
                 💡 Suggestion: {}",
                message,
                invalid_columns.join(", "),
                suggestion
            ),
            ElusionError::WriteError { path, operation, reason, suggestion } => write!(
                f,
                "💾 Write Error during {} operation\n\
                 📍 Path: {}\n\
                 ❌ Problem: {}\n\
                 💡 Suggestion: {}",
                operation, path, reason, suggestion
            ),
            ElusionError::DataFusion(err) => write!(
                f,
                "⚡ DataFusion Error: {}\n\
                 💡 Don't worry! Here's what you can try:\n\
                 1. Check your column names and types\n\
                 2. Verify your SQL syntax\n\
                 3. Use .display_schema() to see available columns\n\
                 4. Try breaking down complex operations into smaller steps",
                err
            ),
            ElusionError::Io(err) => write!(
                f,
                "📁 I/O Error: {}\n\
                 💡 Quick fixes to try:\n\
                 1. Check if the file/directory exists\n\
                 2. Verify your permissions\n\
                 3. Ensure the path is correct\n\
                 4. Close any programs using the file",
                err
            ),
            ElusionError::PartitionError { message, partition_columns, suggestion } => write!(
                f,
                "📦 Partition Error: {}\n\
                 ❌ Affected partition columns: {}\n\
                 💡 Suggestion: {}",
                message,
                partition_columns.join(", "),
                suggestion
            ),
            ElusionError::AggregationError { message, function, column, suggestion } => write!(
                f,
                "📊 Aggregation Error in function '{}'\n\
                 ❌ Problem with column '{}': {}\n\
                 💡 Suggestion: {}",
                function, column, message, suggestion
            ),
            ElusionError::OrderByError { message, columns, suggestion } => write!(
                f,
                "🔄 Order By Error: {}\n\
                 ❌ Problem with columns: {}\n\
                 💡 Suggestion: {}",
                message,
                columns.join(", "),
                suggestion
            ),
            ElusionError::WindowFunctionError { message, function, details, suggestion } => write!(
                f,
                "🪟 Window Function Error in '{}'\n\
                 ❌ Problem: {}\n\
                 📝 Details: {}\n\
                 💡 Suggestion: {}",
                function, message, details, suggestion
            ),
            ElusionError::LimitError { message, value, suggestion } => write!(
                f,
                "🔢 Limit Error: {}\n\
                 ❌ Invalid limit value: {}\n\
                 💡 Suggestion: {}",
                message, value, suggestion
            ),
            ElusionError::SetOperationError { operation, reason, suggestion } => write!(
                f,
                "🔄 Set Operation Error in '{}'\n\
                 ❌ Problem: {}\n\
                 💡 Suggestion: {}",
                operation, reason, suggestion
            ),
            ElusionError::Custom(err) => write!(f, "💫 {}", err),
        }
    }
}

impl From<DataFusionError> for ElusionError {
    fn from(err: DataFusionError) -> Self {
        match &err {
            DataFusionError::SchemaError(schema_err, _context) => {
                let error_msg = schema_err.to_string();
                
                if error_msg.contains("Column") && error_msg.contains("not found") {
                    if let Some(col_name) = extract_column_name_from_error(&error_msg) {
                        return ElusionError::MissingColumn {
                            column: col_name,
                            available_columns: extract_available_columns_from_error(&error_msg),
                        };
                    }
                }
                
                if error_msg.contains("Cannot cast") {
                    if let Some((col, expected, found)) = extract_type_info_from_error(&error_msg) {
                        return ElusionError::InvalidDataType {
                            column: col,
                            expected,
                            found,
                        };
                    }
                }

                if error_msg.contains("Schema") {
                    return ElusionError::SchemaError {
                        message: error_msg,
                        schema: None,
                        suggestion: "💡 Check column names and data types in your schema".to_string(),
                    };
                }

                ElusionError::DataFusion(err)
            },
            DataFusionError::Plan(plan_err) => {
                let error_msg = plan_err.to_string();
                
                if error_msg.contains("Duplicate column") {
                    if let Some((col, locs)) = extract_duplicate_column_info(&error_msg) {
                        return ElusionError::DuplicateColumn {
                            column: col,
                            locations: locs,
                        };
                    }
                }

                if error_msg.contains("JOIN") {
                    return ElusionError::JoinError {
                        message: error_msg.clone(),
                        left_table: "unknown".to_string(),
                        right_table: "unknown".to_string(),
                        suggestion: "💡 Check join conditions and table names".to_string(),
                    };
                }

                ElusionError::DataFusion(err)
            },
            DataFusionError::Execution(exec_err) => {
                let error_msg = exec_err.to_string();

                if error_msg.contains("aggregate") || error_msg.contains("SUM") || 
                error_msg.contains("AVG") || error_msg.contains("COUNT") {
                 if let Some((func, col)) = extract_aggregation_error(&error_msg) {
                     return ElusionError::AggregationError {
                         message: error_msg.clone(),
                         function: func,
                         column: col,
                         suggestion: "💡 Verify aggregation function syntax and column data types".to_string(),
                     };
                 }
             }
                if error_msg.contains("GROUP BY") {
                    return ElusionError::GroupByError {
                        message: error_msg.clone(),
                        invalid_columns: Vec::new(),
                        suggestion: "💡 Ensure all non-aggregated columns are included in GROUP BY".to_string(),
                    };
                }

                if error_msg.contains("PARTITION BY") {
                    return ElusionError::PartitionError {
                        message: error_msg.clone(),
                        partition_columns: Vec::new(),
                        suggestion: "💡 Check partition column names and data types".to_string(),
                    };
                }

                if error_msg.contains("ORDER BY") {
                    return ElusionError::OrderByError {
                        message: error_msg.clone(),
                        columns: Vec::new(),
                        suggestion: "💡 Verify column names and sort directions".to_string(),
                    };
                }

                if error_msg.contains("OVER") || error_msg.contains("window") {
                    if let Some((func, details)) = extract_window_function_error(&error_msg) {
                        return ElusionError::WindowFunctionError {
                            message: error_msg.clone(),
                            function: func,
                            details,
                            suggestion: "💡 Check window function syntax and parameters".to_string(),
                        };
                    }
                }

                if error_msg.contains("LIMIT") {
                    return ElusionError::LimitError {
                        message: error_msg.clone(),
                        value: 0,
                        suggestion: "💡 Ensure limit value is a positive integer".to_string(),
                    };
                }

                if error_msg.contains("UNION") || error_msg.contains("INTERSECT") || error_msg.contains("EXCEPT") {
                    return ElusionError::SetOperationError {
                        operation: "Set Operation".to_string(),
                        reason: error_msg.clone(),
                        suggestion: "💡 Ensure both sides of the operation have compatible schemas".to_string(),
                    };
                }

                ElusionError::DataFusion(err)
            },
            DataFusionError::NotImplemented(msg) => {
                ElusionError::InvalidOperation {
                    operation: "Operation not supported".to_string(),
                    reason: msg.clone(),
                    suggestion: "💡 Try using an alternative approach or check documentation for supported features".to_string(),
                }
            },
            DataFusionError::Internal(msg) => {
                ElusionError::Custom(format!("Internal error: {}. Please report this issue.", msg))
            },
            _ => ElusionError::DataFusion(err)
        }
    }
}

fn extract_window_function_error(err: &str) -> Option<(String, String)> {
    let re = Regex::new(r"Window function '([^']+)' error: (.+)").ok()?;
    let caps = re.captures(err)?;
    Some((
        caps.get(1)?.as_str().to_string(),
        caps.get(2)?.as_str().to_string(),
    ))
}

fn extract_aggregation_error(err: &str) -> Option<(String, String)> {
    let re = Regex::new(r"Aggregate function '([^']+)' error on column '([^']+)'").ok()?;
    let caps = re.captures(err)?;
    Some((
        caps.get(1)?.as_str().to_string(),
        caps.get(2)?.as_str().to_string(),
    ))
}
// Helper functions for error parsing
fn extract_column_name_from_error(err: &str) -> Option<String> {
    let re = Regex::new(r"Column '([^']+)'").ok()?;
    re.captures(err)?.get(1).map(|m| m.as_str().to_string())
}

fn extract_available_columns_from_error(err: &str) -> Vec<String> {
    if let Some(re) = Regex::new(r"Available fields are: \[(.*?)\]").ok() {
        if let Some(caps) = re.captures(err) {
            if let Some(fields) = caps.get(1) {
                return fields.as_str()
                    .split(',')
                    .map(|s| s.trim().trim_matches('\'').to_string())
                    .collect();
            }
        }
    }
    Vec::new()
}

fn extract_type_info_from_error(err: &str) -> Option<(String, String, String)> {
    let re = Regex::new(r"Cannot cast column '([^']+)' from ([^ ]+) to ([^ ]+)").ok()?;
    let caps = re.captures(err)?;
    Some((
        caps.get(1)?.as_str().to_string(),
        caps.get(3)?.as_str().to_string(),
        caps.get(2)?.as_str().to_string(),
    ))
}

fn extract_duplicate_column_info(err: &str) -> Option<(String, Vec<String>)> {
    let re = Regex::new(r"Duplicate column '([^']+)' in schema: \[(.*?)\]").ok()?;
    let caps = re.captures(err)?;
    Some((
        caps.get(1)?.as_str().to_string(),
        caps.get(2)?
            .as_str()
            .split(',')
            .map(|s| s.trim().to_string())
            .collect()
    ))
}

// Helper function to suggest similar column names using basic string similarity
fn suggest_similar_column(target: &str, available: &[String]) -> String {
    available
        .iter()
        .map(|col| (col, string_similarity(target, col)))
        .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap())
        .map(|(col, _)| col.clone())
        .unwrap_or_else(|| "".to_string())
}

// Simple string similarity function (you might want to use a proper crate like 'strsim' in production)
fn string_similarity(s1: &str, s2: &str) -> f64 {
    let s1_lower = s1.to_lowercase();
    let s2_lower = s2.to_lowercase();
    
    // Check for exact prefix match
    if s1_lower.starts_with(&s2_lower) || s2_lower.starts_with(&s1_lower) {
        return 0.9;
    }
    
    // Check for common substring
    let common_len = s1_lower.chars()
        .zip(s2_lower.chars())
        .take_while(|(c1, c2)| c1 == c2)
        .count() as f64;
    
    if common_len > 0.0 {
        return common_len / s1_lower.len().max(s2_lower.len()) as f64;
    }
    
    // Fall back to character frequency similarity
    let max_len = s1_lower.len().max(s2_lower.len()) as f64;
    let common_chars = s1_lower.chars()
        .filter(|c| s2_lower.contains(*c))
        .count() as f64;
    
    common_chars / max_len
}

impl Error for ElusionError {}

impl From<std::io::Error> for ElusionError {
    fn from(err: std::io::Error) -> Self {
        ElusionError::Io(err)
    }
}

pub type ElusionResult<T> = Result<T, ElusionError>;

#[derive(Clone, Debug)]
pub struct Join {
    dataframe: CustomDataFrame,
    condition: String,
    join_type: String,
}

#[derive(Clone, Debug)]
pub struct CustomDataFrame {
    df: DataFrame,
    table_alias: String,
    from_table: String,
    selected_columns: Vec<String>,
    pub alias_map: Vec<(String, String)>,
    aggregations: Vec<String>,
    group_by_columns: Vec<String>,
    where_conditions: Vec<String>,
    having_conditions: Vec<String>,
    order_by_columns: Vec<(String, bool)>, 
    limit_count: Option<u64>,
    joins: Vec<Join>,
    window_functions: Vec<String>,
    ctes: Vec<String>,
    pub subquery_source: Option<String>,
    set_operations: Vec<String>,
    pub query: String,
    pub aggregated_df: Option<DataFrame>,
    union_tables: Option<Vec<(String, DataFrame, String)>>, 
    original_expressions: Vec<String>,
}

// =================== JSON heler functions

#[derive(Deserialize, Serialize, Debug)]
struct GenericJson {
    #[serde(flatten)]
    fields: HashMap<String, Value>,
}
/// Function to infer schema from rows
fn infer_schema_from_json(rows: &[HashMap<String, Value>]) -> SchemaRef {
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
//helper function for building record batch
fn build_record_batch(
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

// ================= Statistics
#[derive(Debug, Default)]
pub struct ColumnStats {
    pub columns: Vec<ColumnStatistics>,
}

#[derive(Debug)]
pub struct ColumnStatistics {
    pub name: String,
    pub total_count: i64,
    pub non_null_count: i64,
    pub mean: Option<f64>,
    pub min_value: ScalarValue,
    pub max_value: ScalarValue,
    pub std_dev: Option<f64>,
}


#[derive(Debug)]
pub struct NullAnalysis {
    pub counts: Vec<NullCount>,
}

#[derive(Debug)]
pub struct NullCount {
    pub column_name: String,
    pub total_rows: i64,
    pub null_count: i64,
    pub null_percentage: f64,
}
//======================= CSV WRITING OPTION ============================//

#[derive(Debug, Clone)]
pub struct CsvWriteOptions {
    pub delimiter: u8,
    pub escape: u8,
    pub quote: u8,
    pub double_quote: bool,
    // pub date_format: Option<String>,
    // pub time_format: Option<String>,
    // pub timestamp_format: Option<String>,
    // pub timestamp_tz_format: Option<String>,
    pub null_value: String,
}

impl Default for CsvWriteOptions {
    fn default() -> Self {
        Self {
            delimiter: b',',
            escape: b'\\',
            quote: b'"',
            double_quote: true,
            // date_format: None,// "%Y-%m-%d".to_string(),
            // time_format: None, // "%H-%M-%S".to_string(),
            // timestamp_format: None, //"%Y-%m-%d %H:%M:%S".to_string(),
            // timestamp_tz_format: None, // "%Y-%m-%dT%H:%M:%S%z".to_string(),
            null_value: "NULL".to_string(),
        }
    }
}

impl CsvWriteOptions {
    pub fn validate(&self) -> Result<(), ElusionError> {

        // Validate delimiter
        if !self.delimiter.is_ascii() {
            return Err(ElusionError::InvalidOperation {
                operation: "CSV Write".to_string(),
                reason: format!("Delimiter '{}' is not a valid ASCII character", 
                    self.delimiter as char),
                suggestion: "💡 Use an ASCII character for delimiter".to_string()
            });
        }
        
        // Validate escape character
        if !self.escape.is_ascii() {
            return Err(ElusionError::Custom(format!(
                "Escape character '{}' is not a valid ASCII character.",
                self.escape as char
            )));
        }

        // Validate quote character
        if !self.quote.is_ascii() {
            return Err(ElusionError::Custom(format!(
                "Quote character '{}' is not a valid ASCII character.",
                self.quote as char
            )));
        }
        
        // Validate null_value
        if self.null_value.trim().is_empty() {
            return Err(ElusionError::Custom("Null value representation cannot be empty.".to_string()));
        }
        
        // Ensure null_value does not contain delimiter or quote characters
        let delimiter_char = self.delimiter as char;
        let quote_char = self.quote as char;
        
        if self.null_value.contains(delimiter_char) {
            return Err(ElusionError::Custom(format!(
                "Null value '{}' cannot contain the delimiter '{}'.",
                self.null_value, delimiter_char
            )));
        }
        
        if self.null_value.contains(quote_char) {
            return Err(ElusionError::Custom(format!(
                "Null value '{}' cannot contain the quote character '{}'.",
                self.null_value, quote_char
            )));
        }
        
        Ok(())
    }
}
// ================== NORMALIZERS

async fn lowercase_column_names(df: DataFrame) -> ElusionResult<DataFrame> {
    let schema = df.schema();
   
    // Create a SELECT statement that renames all columns to lowercase
    let columns: Vec<String> = schema.fields()
        .iter()
        .map(|f| format!("\"{}\" as \"{}\"", f.name(),  f.name().trim().replace(" ", "_").to_lowercase()))
        .collect();

    let ctx = SessionContext::new();
    
    // Register original DataFrame with proper schema conversion
    let batches = df.clone().collect().await?;
    let mem_table = MemTable::try_new(schema.clone().into(), vec![batches])?;
    ctx.register_table("temp_table", Arc::new(mem_table))?;
    
    // Create new DataFrame with lowercase columns
    let sql = format!("SELECT {} FROM temp_table", columns.join(", "));
    ctx.sql(&sql).await.map_err(|e| ElusionError::Custom(format!("Failed to lowercase column names: {}", e)))
}

/// Normalizes an alias by trimming whitespace and converting it to lowercase.
fn normalize_alias_write(alias: &str) -> String {
    alias.trim().to_lowercase()
}

/// Normalizes column name by trimming whitespace and properly quoting table aliases and column names.
fn normalize_column_name(name: &str) -> String {
    // Case-insensitive check for " AS " with more flexible whitespace handling
    let name_upper = name.to_uppercase();
    if name_upper.contains(" AS ") {
       
        // let pattern = regex::Regex::new(r"(?i)\s+AS\s+") //r"(?i)^(.*?)\s+AS\s+(.+?)$"
        // .unwrap();

        let pattern = match regex::Regex::new(r"(?i)\s+AS\s+") {
            Ok(re) => re,
            Err(e) => {
                // Log error and return a safe default
                eprintln!("Column parsing error in SELECT() function: {}", e);
                return name.to_string();
            }
        };

        let parts: Vec<&str> = pattern.split(name).collect();
        
        if parts.len() >= 2 {
            let column = parts[0].trim();
            let alias = parts[1].trim();
            
            if let Some(pos) = column.find('.') {
                let table = &column[..pos];
                let col = &column[pos + 1..];
                format!("\"{}\".\"{}\" AS \"{}\"",
                    table.trim().to_lowercase(),
                    col.trim().to_lowercase(),
                    alias.to_lowercase())
            } else {
                format!("\"{}\" AS \"{}\"",
                    column.trim().to_lowercase(),
                    alias.to_lowercase())
            }
        } else {

            if let Some(pos) = name.find('.') {
                let table = &name[..pos];
                let column = &name[pos + 1..];
                format!("\"{}\".\"{}\"", 
                    table.trim().to_lowercase(), 
                    column.trim().replace(" ", "_").to_lowercase())
            } else {
                format!("\"{}\"", 
                    name.trim().replace(" ", "_").to_lowercase())
            }
        }
    } else {

        if let Some(pos) = name.find('.') {
            let table = &name[..pos];
            let column = &name[pos + 1..];
            format!("\"{}\".\"{}\"", 
                table.trim().to_lowercase(), 
                column.trim().replace(" ", "_").to_lowercase())
        } else {
            format!("\"{}\"", 
                name.trim().replace(" ", "_").to_lowercase())
        }
    }
}
/// Normalizes an alias by trimming whitespace and converting it to lowercase.
fn normalize_alias(alias: &str) -> String {
    // alias.trim().to_lowercase()
    format!("\"{}\"", alias.trim().to_lowercase())
}

/// Normalizes a condition string by properly quoting table aliases and column names.
fn normalize_condition(condition: &str) -> String {
    // let re = Regex::new(r"(\b\w+)\.(\w+\b)").unwrap();
    let re = Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b").unwrap();
    
    re.replace_all(condition.trim(), "\"$1\".\"$2\"").to_string().to_lowercase()
}

fn normalize_condition_filter(condition: &str) -> String {
    // let re = Regex::new(r"(\b\w+)\.(\w+\b)").unwrap();
    let re = Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b").unwrap();
    
    // re.replace_all(condition.trim(), "\"$1\".\"$2\"").to_string()
    re.replace_all(condition.trim(), |caps: &regex::Captures| {
        let table = &caps[1];
        let column = &caps[2];
        format!("\"{}\".\"{}\"", table, column.to_lowercase())
    }).to_string()
}

/// Normalizes an expression by properly quoting table aliases and column names.
/// Example:
/// - "SUM(s.OrderQuantity) AS total_quantity" becomes "SUM(\"s\".\"OrderQuantity\") AS total_quantity"
/// Normalizes an expression by properly quoting table aliases and column names.
fn normalize_expression(expr: &str, table_alias: &str) -> String {
    let parts: Vec<&str> = expr.splitn(2, " AS ").collect();
    
    if parts.len() == 2 {
        let expr_part = parts[0].trim();
        let alias_part = parts[1].trim();
        
        let normalized_expr = if is_aggregate_expression(expr_part) {
            normalize_aggregate_expression(expr_part, table_alias)
        } else if is_datetime_expression(expr_part) {
            normalize_datetime_expression(expr_part)
        } else {
            normalize_simple_expression(expr_part, table_alias)
        };

        format!("{} AS \"{}\"", 
            normalized_expr.to_lowercase(), 
            alias_part.replace(" ", "_").to_lowercase())
    } else {
        if is_aggregate_expression(expr) {
            normalize_aggregate_expression(expr, table_alias).to_lowercase()
        } else if is_datetime_expression(expr) {
            normalize_datetime_expression(expr).to_lowercase()
        } else {
            normalize_simple_expression(expr, table_alias).to_lowercase()
        }
    }
}

fn normalize_aggregate_expression(expr: &str, table_alias: &str) -> String {
    let re = Regex::new(r"^([A-Za-z_][A-Za-z0-9_]*)\s*\((.*)\)$").unwrap();
    if let Some(caps) = re.captures(expr.trim()) {
        let func_name = &caps[1];
        let args = &caps[2];
        let normalized_args = args.split(',')
            .map(|arg| normalize_simple_expression(arg.trim(), table_alias))
            .collect::<Vec<_>>()
            .join(", ");
        format!("{}({})", func_name.to_lowercase(), normalized_args.to_lowercase())
    } else {
        expr.to_lowercase()
    }
}

fn normalize_simple_expression(expr: &str, table_alias: &str) -> String {
    let col_re = Regex::new(r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\.(?P<column>[A-Za-z_][A-Za-z0-9_]*)").unwrap();
    let func_re = Regex::new(r"^([A-Za-z_][A-Za-z0-9_]*)\s*\((.*)\)$").unwrap();
    let operator_re = Regex::new(r"([\+\-\*\/])").unwrap();
 
    if let Some(caps) = func_re.captures(expr) {
        let func_name = &caps[1];
        let args = &caps[2];
        
        let normalized_args = args.split(',')
            .map(|arg| normalize_simple_expression(arg.trim(), table_alias))
            .collect::<Vec<_>>()
            .join(", ");
            
        format!("{}({})", func_name.to_lowercase(), normalized_args.to_lowercase())
    } else if operator_re.is_match(expr) {
        let mut result = String::new();
        let mut parts = operator_re.split(expr).peekable();
        
        while let Some(part) = parts.next() {
            let trimmed = part.trim();
            if col_re.is_match(trimmed) {
                result.push_str(&trimmed.to_lowercase());
            } else if is_simple_column(trimmed) {
                result.push_str(&format!("\"{}\".\"{}\"", 
                    table_alias.to_lowercase(), 
                    trimmed.to_lowercase()));
            } else {
                result.push_str(&trimmed.to_lowercase());
            }
            
            if parts.peek().is_some() {
                if let Some(op) = expr.chars().skip_while(|c| !"+-*/%".contains(*c)).next() {
                    result.push_str(&format!(" {} ", op));
                }
            }
        }
        result
    } else if col_re.is_match(expr) {
        col_re.replace_all(expr, "\"$1\".\"$2\"")
            .to_string()
            .to_lowercase()
    } else if is_simple_column(expr) {
        format!("\"{}\".\"{}\"", 
            table_alias.to_lowercase(), 
            expr.trim().replace(" ", "_").to_lowercase())
    } else {
        expr.to_lowercase()
    }
}

/// Helper function to determine if a string is an expression.
fn is_expression(s: &str) -> bool {
    // Check for presence of arithmetic operators or function-like patterns
    let operators = ['+', '-', '*', '/', '%' ,'(', ')', ',', '.'];
    let has_operator = s.chars().any(|c| operators.contains(&c));
    let has_function = Regex::new(r"\b[A-Za-z_][A-Za-z0-9_]*\s*\(").unwrap().is_match(s);
    has_operator || has_function
}

/// Returns true if the string contains only alphanumeric characters and underscores.
fn is_simple_column(s: &str) -> bool {
    let re = Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$").unwrap();
    re.is_match(s)
}

/// Helper function to determine if an expression is an aggregate.
fn is_aggregate_expression(expr: &str) -> bool {
    let aggregate_functions = [
    //agg funcs
    "SUM", "AVG", "MAX", "MIN", "MEAN", "MEDIAN","COUNT", "LAST_VALUE", "FIRST_VALUE",  
    "GROUPING", "STRING_AGG", "ARRAY_AGG","VAR", "VAR_POP", "VAR_POPULATION", "VAR_SAMP", "VAR_SAMPLE",  
    "BIT_AND", "BIT_OR", "BIT_XOR", "BOOL_AND", "BOOL_OR",
    //scalar funcs unnecessary here but willl use it else where
    "ABS", "FLOOR", "CEIL", "SQRT", "ISNAN", "ISZERO",  "PI", "POW", "POWER", "RADIANS", "RANDOM", "ROUND",  
   "FACTORIAL", "ACOS", "ACOSH", "ASIN", "ASINH",  "COS", "COSH", "COT", "DEGREES", "EXP","SIN", "SINH", "TAN", "TANH", "TRUNC", "CBRT", "ATAN", "ATAN2", "ATANH", "GCD", "LCM", "LN",  "LOG", "LOG10", "LOG2", "NANVL", "SIGNUM"
   ];
   
   aggregate_functions.iter().any(|&func| expr.to_uppercase().starts_with(func))
 
}

fn is_datetime_expression(expr: &str) -> bool {
    // List of all datetime functions to check for
    let datetime_functions = [
        "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "DATE_BIN", "DATE_FORMAT",
        "DATE_PART", "DATE_TRUNC", "DATEPART", "DATETRUNC", "FROM_UNIXTIME", "MAKE_DATE",
        "NOW", "TO_CHAR", "TO_DATE", "TO_LOCAL_TIME", "TO_TIMESTAMP", "TO_TIMESTAMP_MICROS",
        "TO_TIMESTAMP_MILLIS", "TO_TIMESTAMP_NANOS", "TO_TIMESTAMP_SECONDS", "TO_UNIXTIME", "TODAY"
    ];

    datetime_functions.iter().any(|&func| expr.to_uppercase().starts_with(func))
}

/// Normalizes datetime expressions by quoting column names with double quotes.
fn normalize_datetime_expression(expr: &str) -> String {
    let re = Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*)\.(?P<column>[A-Za-z_][A-Za-z0-9_]*)\b").unwrap();

    let expr_with_columns = re.replace_all(expr, |caps: &regex::Captures| {
        format!("\"{}\"", caps["column"].to_lowercase())
    }).to_string();

    expr_with_columns.to_lowercase()
}

/// window functions normalization
fn normalize_window_function(expression: &str) -> String {
    let parts: Vec<&str> = expression.splitn(2, " OVER ").collect();
    if parts.len() != 2 {
        return expression.to_lowercase();
    }

    let function_part = parts[0].trim();
    let over_part = parts[1].trim();

    let func_regex = Regex::new(r"^(\w+)\((.*)\)$").unwrap();

    let (normalized_function, maybe_args) = if let Some(caps) = func_regex.captures(function_part) {
        let func_name = &caps[1];
        let arg_list_str = &caps[2];

        let raw_args: Vec<&str> = arg_list_str.split(',').map(|s| s.trim()).collect();
        
        let normalized_args: Vec<String> = raw_args
            .iter()
            .map(|arg| normalize_function_arg(arg))
            .collect();

        (func_name.to_lowercase(), Some(normalized_args))
    } else {
        (function_part.to_lowercase(), None)
    };

    let rebuilt_function = if let Some(args) = maybe_args {
        format!("{}({})", normalized_function, args.join(", ").to_lowercase())
    } else {
        normalized_function
    };

    let re_cols = Regex::new(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b").unwrap();
    let normalized_over = re_cols.replace_all(over_part, "\"$1\".\"$2\"")
        .to_string()
        .to_lowercase();

    format!("{} OVER {}", rebuilt_function, normalized_over)
}

/// Helper: Normalize one argument if it looks like a table.column reference.
fn normalize_function_arg(arg: &str) -> String {
    let re_table_col = Regex::new(r"^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)$").unwrap();

    if let Some(caps) = re_table_col.captures(arg) {
        let table = &caps[1];
        let col = &caps[2];
        format!("\"{}\".\"{}\"", 
            table.to_lowercase(), 
            col.to_lowercase())
    } else {
        arg.to_lowercase()
    }
}
// ================= DELTA
/// Attempt to glean the Arrow schema of a DataFusion `DataFrame` by collecting
/// a **small sample** (up to 1 row). If there's **no data**, returns an empty schema
/// or an error
async fn glean_arrow_schema(df: &DataFrame) -> ElusionResult<SchemaRef> {

    let limited_df = df.clone().limit(0, Some(1))
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Schema Inference".to_string(),
            reason: format!("Failed to limit DataFrame: {}", e),
            suggestion: "💡 Check if the DataFrame is valid".to_string()
        })?;
    
        let batches = limited_df.collect().await
        .map_err(|e| ElusionError::SchemaError {
            message: format!("Failed to collect sample batch: {}", e),
            schema: None,
            suggestion: "💡 Verify DataFrame contains valid data".to_string()
        })?;

    if let Some(first_batch) = batches.get(0) {
        Ok(first_batch.schema())
    } else {
        let empty_fields: Vec<Field> = vec![];
        let empty_schema = Schema::new(empty_fields);
        Ok(Arc::new(empty_schema))
    }
}

// Helper function to convert Arrow DataType to Delta DataType
fn arrow_to_delta_type(arrow_type: &ArrowDataType) -> DeltaType {

    match arrow_type {
        ArrowDataType::Boolean => DeltaType::BOOLEAN,
        ArrowDataType::Int8 => DeltaType::BYTE,
        ArrowDataType::Int16 => DeltaType::SHORT,
        ArrowDataType::Int32 => DeltaType::INTEGER,
        ArrowDataType::Int64 => DeltaType::LONG,
        ArrowDataType::Float32 => DeltaType::FLOAT,
        ArrowDataType::Float64 => DeltaType::DOUBLE,
        ArrowDataType::Utf8 => DeltaType::STRING,
        ArrowDataType::Date32 => DeltaType::DATE,
        ArrowDataType::Date64 => DeltaType::DATE,
        ArrowDataType::Timestamp(TimeUnit::Second, _) => DeltaType::TIMESTAMP,
        ArrowDataType::Timestamp(TimeUnit::Millisecond, _) => DeltaType::TIMESTAMP,
        ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => DeltaType::TIMESTAMP,
        ArrowDataType::Timestamp(TimeUnit::Nanosecond, _) => DeltaType::TIMESTAMP,
        ArrowDataType::Binary => DeltaType::BINARY,
        _ => DeltaType::STRING, // Default to String for unsupported types
    }
}

/// Helper struct to manage path conversions between different path types
#[derive(Clone)]
struct DeltaPathManager {
    base_path: PathBuf,
}

impl DeltaPathManager {
    /// Create a new DeltaPathManager from a string path
    pub fn new<P: AsRef<LocalPath>>(path: P) -> Self {
        let normalized = path
            .as_ref()
            .to_string_lossy()
            .replace('\\', "/")
            .trim_end_matches('/')
            .to_string();
        
        Self {
            base_path: PathBuf::from(normalized),
        }
    }

    /// Get the base path as a string with forward slashes
    pub fn base_path_str(&self) -> String {
        self.base_path.to_string_lossy().replace('\\', "/")
    }

    /// Get the delta log path
    pub fn delta_log_path(&self) -> DeltaPath {
        let base = self.base_path_str();
        DeltaPath::from(format!("{base}/_delta_log"))
    }

    /// Convert to ObjectStorePath
    // pub fn to_object_store_path(&self) -> ObjectStorePath {
    //     ObjectStorePath::from(self.base_path_str())
    // }

    /// Get path for table operations
    pub fn table_path(&self) -> String {
        self.base_path_str()
    }
    /// Get the drive prefix (e.g., "C:/", "D:/") from the base path
    pub fn drive_prefix(&self) -> String {
        let base_path = self.base_path_str();
        if let Some(colon_pos) = base_path.find(':') {
            base_path[..colon_pos + 2].to_string() // Include drive letter, colon, and slash
        } else {
            "/".to_string() // Fallback for non-Windows paths
        }
    }

    /// Normalize a file URI with the correct drive letter
    pub fn normalize_uri(&self, uri: &str) -> String {
        let drive_prefix = self.drive_prefix();
        
        // removing any existing drive letter prefix pattern and leading slashes
        let path = uri.trim_start_matches(|c| c != '/' && c != '\\')
            .trim_start_matches(['/', '\\']);
        
        // correct drive prefix and normalize separators
        format!("{}{}", drive_prefix, path).replace('\\', "/")
    }

    pub fn is_delta_table(&self) -> bool {
        let delta_log = self.base_path.join("_delta_log");
        let delta_log_exists = delta_log.is_dir();
        
        if delta_log_exists {
            // Additional check: is .json files in _delta_log
            if let Ok(entries) = fs::read_dir(&delta_log) {
                for entry in entries {
                    if let Ok(entry) = entry {
                        if let Some(ext) = entry.path().extension() {
                            if ext == "json" {
                                return true;
                            }
                        }
                    }
                }
            }
        }
        false
    }
}

/// Helper function to append a Protocol action to the Delta log
async fn append_protocol_action(
    store: &Arc<dyn ObjectStore>,
    delta_log_path: &DeltaPath,
    protocol_action: Value,
) -> Result<(), DeltaTableError> {

    let latest_version = get_latest_version(store, delta_log_path).await?;
    let next_version = latest_version + 1;
    let protocol_file = format!("{:020}.json", next_version);

    let child_path = delta_log_path.child(&*protocol_file);
    
    let protocol_file_path = DeltaPath::from(child_path);

    let action_str = serde_json::to_string(&protocol_action)
        .map_err(|e| DeltaTableError::Generic(format!("Failed to serialize Protocol action: {e}")))?;

    store
        .put(&protocol_file_path, action_str.into_bytes().into())
        .await
        .map_err(|e| DeltaTableError::Generic(format!("Failed to write Protocol action to Delta log: {e}")))?;

    Ok(())
}

/// Helper function to get the latest version number in the Delta log
async fn get_latest_version(
    store: &Arc<dyn ObjectStore>,
    delta_log_path: &DeltaPath,
) -> Result<i64, DeltaTableError> {
    let mut versions = Vec::new();

    let mut stream = store.list(Some(delta_log_path));

    while let Some(res) = stream.next().await {
        let metadata = res.map_err(|e| DeltaTableError::Generic(format!("Failed to list Delta log files: {e}")))?;
        // Get the location string from ObjectMeta
        let path_str = metadata.location.as_ref();

        if let Some(file_name) = path_str.split('/').last() {
            println!("Detected log file: {}", file_name); 
            if let Some(version_str) = file_name.strip_suffix(".json") {
                if let Ok(version) = version_str.parse::<i64>() {
                    println!("Parsed version: {}", version);
                    versions.push(version);
                }
            }
        }
    }

    let latest = versions.into_iter().max().unwrap_or(-1);
    println!("Latest version detected: {}", latest); 
    Ok(latest)
}

/// This is the lower-level writer function that actually does the work
async fn write_to_delta_impl(
    df: &DataFrame,
    path: &str,
    partition_columns: Option<Vec<String>>,
    overwrite: bool,
    write_mode: WriteMode,
) -> Result<(), DeltaTableError> {
    let path_manager = DeltaPathManager::new(path);

    // get the Arrow schema
    let arrow_schema_ref = glean_arrow_schema(df)
        .await
        .map_err(|e| DeltaTableError::Generic(format!("Could not glean Arrow schema: {e}")))?;

    // Convert Arrow schema to Delta schema fields
    let delta_fields: Vec<StructField> = arrow_schema_ref
        .fields()
        .iter()
        .map(|field| {
            let nullable = field.is_nullable();
            let name = field.name().clone();
            let data_type = arrow_to_delta_type(field.data_type());
            StructField::new(name, data_type, nullable)
        })
        .collect();

    //  basic configuration
    let mut config: HashMap<String, Option<String>> = HashMap::new();
    config.insert("delta.minWriterVersion".to_string(), Some("7".to_string()));
    config.insert("delta.minReaderVersion".to_string(), Some("3".to_string()));

    if overwrite {
        // Removing the existing directory if it exists
        if let Err(e) = fs::remove_dir_all(&path_manager.base_path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(DeltaTableError::Generic(format!(
                    "Failed to remove existing directory at '{}': {e}",
                    path
                )));
            }
        }

        //directory structure
        fs::create_dir_all(&path_manager.base_path)
            .map_err(|e| DeltaTableError::Generic(format!("Failed to create directory structure: {e}")))?;

        //  metadata with empty HashMap
        let metadata = Metadata::try_new(
            StructType::new(delta_fields.clone()),
            partition_columns.clone().unwrap_or_default(),
            HashMap::new()
        )?;

        // configuration in the metadata action
        let metadata_action = json!({
            "metaData": {
                "id": metadata.id,
                "name": metadata.name,
                "description": metadata.description,
                "format": {
                    "provider": "parquet",
                    "options": {}
                },
                "schemaString": metadata.schema_string,
                "partitionColumns": metadata.partition_columns,
                "configuration": {
                    "delta.minReaderVersion": "3",
                    "delta.minWriterVersion": "7"
                },
                "created_time": metadata.created_time
            }
        });

        // store and protocol
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let delta_log_path = path_manager.delta_log_path();
        let protocol = Protocol::new(3, 7);

        let protocol_action = json!({
            "protocol": {
                "minReaderVersion": protocol.min_reader_version,
                "minWriterVersion": protocol.min_writer_version,
                "readerFeatures": [],
                "writerFeatures": []
            }
        });
        append_protocol_action(&store, &delta_log_path, protocol_action).await?;
        append_protocol_action(&store, &delta_log_path, metadata_action).await?;

        // table initialzzation
        let _ = DeltaOps::try_from_uri(&path_manager.table_path())
            .await
            .map_err(|e| DeltaTableError::Generic(format!("Failed to init DeltaOps: {e}")))?
            .create()
            .with_columns(delta_fields.clone())
            .with_partition_columns(partition_columns.clone().unwrap_or_default())
            .with_save_mode(SaveMode::Overwrite)
            .with_configuration(config.clone())
            .await?;
    } else {
        // For append mode, check if table exists
        if !DeltaTableBuilder::from_uri(&path_manager.table_path()).build().is_ok() {
            // Create directory structure
            fs::create_dir_all(&path_manager.base_path)
                .map_err(|e| DeltaTableError::Generic(format!("Failed to create directory structure: {e}")))?;

            // metadata with empty HashMap
            let metadata = Metadata::try_new(
                StructType::new(delta_fields.clone()),
                partition_columns.clone().unwrap_or_default(),
                HashMap::new()
            )?;

            // configuration in the metadata action
            let metadata_action = json!({
                "metaData": {
                    "id": metadata.id,
                    "name": metadata.name,
                    "description": metadata.description,
                    "format": {
                        "provider": "parquet",
                        "options": {}
                    },
                    "schemaString": metadata.schema_string,
                    "partitionColumns": metadata.partition_columns,
                    "configuration": {
                        "delta.minReaderVersion": "3",
                        "delta.minWriterVersion": "7"
                    },
                    "created_time": metadata.created_time
                }
            });

            //  store and protocol
            let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
            let delta_log_path = path_manager.delta_log_path();
            let protocol = Protocol::new(3, 7);

            let protocol_action = json!({
                "protocol": {
                    "minReaderVersion": protocol.min_reader_version,
                    "minWriterVersion": protocol.min_writer_version,
                    "readerFeatures": [],
                    "writerFeatures": []
                }
            });

            append_protocol_action(&store, &delta_log_path, protocol_action).await?;
            append_protocol_action(&store, &delta_log_path, metadata_action).await?;

            //  table initialization
            let _ = DeltaOps::try_from_uri(&path_manager.table_path())
                .await
                .map_err(|e| DeltaTableError::Generic(format!("Failed to init DeltaOps: {e}")))?
                .create()
                .with_columns(delta_fields.clone())
                .with_partition_columns(partition_columns.clone().unwrap_or_default())
                .with_save_mode(SaveMode::Append)
                .with_configuration(config.clone())
                .await?;
        }
    }

    // Load table after initialization
    let mut table = DeltaTableBuilder::from_uri(&path_manager.table_path())
        .build()
        .map_err(|e| DeltaTableError::Generic(format!("Failed to build Delta table: {e}")))?;

    // Ensure table is loaded
    table.load()
        .await
        .map_err(|e| DeltaTableError::Generic(format!("Failed to load table: {e}")))?;

    // Write data
    let batches = df
        .clone()
        .collect()
        .await
        .map_err(|e| DeltaTableError::Generic(format!("DataFusion collect error: {e}")))?;

    let mut writer_config = HashMap::new();
    writer_config.insert("delta.protocol.minWriterVersion".to_string(), "7".to_string());
    writer_config.insert("delta.protocol.minReaderVersion".to_string(), "3".to_string());

    let mut writer = RecordBatchWriter::try_new(
        &path_manager.table_path(),
        arrow_schema_ref,
        partition_columns,
        Some(writer_config)
    )?;

    for batch in batches {
        writer.write_with_mode(batch, write_mode).await?;
    }

    let version = writer
        .flush_and_commit(&mut table)
        .await
        .map_err(|e| DeltaTableError::Generic(format!("Failed to flush and commit: {e}")))?;

    println!("Wrote data to Delta table at version: {version}");
    Ok(())
}


// Auxiliary struct to hold aliased DataFrame
pub struct AliasedDataFrame {
    dataframe: DataFrame,
    alias: String,
}

impl CustomDataFrame {
    /// Creates an empty DataFrame with a minimal schema and a single row
    /// This can be used as a base for date tables or other data generation
    pub async fn empty() -> ElusionResult<Self> {
        // Create a new session context
        let ctx = SessionContext::new();

        let sql = "SELECT 1 as dummy";
        
        // Execute the SQL to create the single-row DataFrame
        let df = ctx.sql(sql).await
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Single Row Creation".to_string(),
                reason: format!("Failed to create single-row DataFrame: {}", e),
                suggestion: "💡 Verify SQL execution capabilities in context.".to_string()
            })?;
        
        // Return a new CustomDataFrame with the single-row DataFrame
        Ok(CustomDataFrame {
            df,
            table_alias: "dummy_table".to_string(),
            from_table: "dummy_table".to_string(),
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

    /// A CustomDataFrame containing a date table with one row per day in the range
    pub async fn create_date_range_table(
        start_date: &str,
        end_date: &str,
        alias: &str
    ) -> ElusionResult<Self> {

        let start = NaiveDate::parse_from_str(start_date, "%Y-%m-%d")
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Date Parsing".to_string(),
                reason: format!("Failed to parse start_date '{}': {}", start_date, e),
                suggestion: "💡 Ensure date format is YYYY-MM-DD".to_string(),
            })?;
            
        let end = NaiveDate::parse_from_str(end_date, "%Y-%m-%d")
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Date Parsing".to_string(),
                reason: format!("Failed to parse end_date '{}': {}", end_date, e),
                suggestion: "💡 Ensure date format is YYYY-MM-DD".to_string(),
            })?;
            
        if end < start {
            return Err(ElusionError::InvalidOperation {
                operation: "Date Range Validation".to_string(),
                reason: format!("End date '{}' is before start date '{}'", end_date, start_date),
                suggestion: "💡 Ensure end_date is after or equal to start_date".to_string(),
            });
        }
        
        let duration = end.signed_duration_since(start);
        let days = duration.num_days() as usize + 1; // Include the end date

        let mut date_array = StringBuilder::new();
        let mut year_array = Int32Builder::new();
        let mut month_array = Int32Builder::new();
        let mut day_array = Int32Builder::new();
        let mut quarter_array = Int32Builder::new();
        let mut week_num_array = Int32Builder::new();
        let mut day_of_week_array = Int32Builder::new();
        let mut day_of_week_name_array = StringBuilder::new(); 
        let mut day_of_year_array = Int32Builder::new();
        let mut week_start_array = StringBuilder::new();
        let mut month_start_array = StringBuilder::new();
        let mut quarter_start_array = StringBuilder::new();
        let mut year_start_array = StringBuilder::new();
        let mut is_weekend_array = BooleanBuilder::new();
        
        // Generate data for each day in the range
        for day_offset in 0..days {
            let current_date = start + chrono::Duration::days(day_offset as i64);
            
            // Date string (YYYY-MM-DD)
            date_array.append_value(current_date.format("%Y-%m-%d").to_string());
            
            // Year
            year_array.append_value(current_date.year());
            
            // Month
            month_array.append_value(current_date.month() as i32);
            
            // Day of month
            day_array.append_value(current_date.day() as i32);
            
            // Quarter
            let quarter = ((current_date.month() - 1) / 3 + 1) as i32;
            quarter_array.append_value(quarter);
            
            // Week number
            let week_num = current_date.iso_week().week() as i32;
            week_num_array.append_value(week_num);
            
            // Day of week (0 = Sunday, 6 = Saturday)
            let day_of_week = current_date.weekday().number_from_sunday() - 1;
            day_of_week_array.append_value(day_of_week as i32);
            
            // Day of week name (Monday, Tuesday, etc.)
            let day_name = match current_date.weekday() {
                Weekday::Mon => "Monday",
                Weekday::Tue => "Tuesday",
                Weekday::Wed => "Wednesday",
                Weekday::Thu => "Thursday",
                Weekday::Fri => "Friday",
                Weekday::Sat => "Saturday",
                Weekday::Sun => "Sunday",
            };
            day_of_week_name_array.append_value(day_name);
            
            // Day of year
            let day_of_year = current_date.ordinal() as i32;
            day_of_year_array.append_value(day_of_year);
            
            // Week start (first day of the week) is sunday
            let week_start = current_date - chrono::Duration::days(current_date.weekday().number_from_sunday() as i64 - 1);
            week_start_array.append_value(week_start.format("%Y-%m-%d").to_string());
            
            // Month start
            let month_start = chrono::NaiveDate::from_ymd_opt(current_date.year(), current_date.month(), 1)
                .unwrap_or(current_date);
            month_start_array.append_value(month_start.format("%Y-%m-%d").to_string());
            
            // Quarter start
            let quarter_start = chrono::NaiveDate::from_ymd_opt(
                current_date.year(), 
                ((quarter - 1) * 3 + 1) as u32, 
                1
            ).unwrap_or(current_date);
            quarter_start_array.append_value(quarter_start.format("%Y-%m-%d").to_string());
            
            // Year start
            let year_start = chrono::NaiveDate::from_ymd_opt(current_date.year(), 1, 1)
                .unwrap_or(current_date);
            year_start_array.append_value(year_start.format("%Y-%m-%d").to_string());
            
            // Weekend flag (Saturday = 6, Sunday = 0)
            // Changed to correctly flag Saturday and Sunday as weekend
            is_weekend_array.append_value(current_date.weekday() == chrono::Weekday::Sat || 
                                          current_date.weekday() == chrono::Weekday::Sun);
        }
        
        // Create a comprehensive schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("date", ArrowDataType::Utf8, false),
            Field::new("year", ArrowDataType::Int32, false),
            Field::new("month", ArrowDataType::Int32, false),
            Field::new("day", ArrowDataType::Int32, false),
            Field::new("quarter", ArrowDataType::Int32, false),
            Field::new("week_num", ArrowDataType::Int32, false),
            Field::new("day_of_week", ArrowDataType::Int32, false),
            Field::new("day_of_week_name", ArrowDataType::Utf8, false), 
            Field::new("day_of_year", ArrowDataType::Int32, false),
            Field::new("week_start", ArrowDataType::Utf8, false),
            Field::new("month_start", ArrowDataType::Utf8, false),
            Field::new("quarter_start", ArrowDataType::Utf8, false),
            Field::new("year_start", ArrowDataType::Utf8, false),
            Field::new("is_weekend", ArrowDataType::Boolean, false),
        ]));
        
        // Create the record batch with all columns
        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(date_array.finish()),
                Arc::new(year_array.finish()),
                Arc::new(month_array.finish()),
                Arc::new(day_array.finish()),
                Arc::new(quarter_array.finish()),
                Arc::new(week_num_array.finish()),
                Arc::new(day_of_week_array.finish()),
                Arc::new(day_of_week_name_array.finish()), 
                Arc::new(day_of_year_array.finish()),
                Arc::new(week_start_array.finish()),
                Arc::new(month_start_array.finish()),
                Arc::new(quarter_start_array.finish()),
                Arc::new(year_start_array.finish()),
                Arc::new(is_weekend_array.finish()),
            ]
        ).map_err(|e| ElusionError::Custom(
            format!("Failed to create record batch: {}", e)
        ))?;
        
        // Create a session context for SQL execution
        let ctx = SessionContext::new();
        
        // Create a memory table with the date range data
        let mem_table = MemTable::try_new(schema.clone().into(), vec![vec![record_batch]])
            .map_err(|e| ElusionError::SchemaError {
                message: format!("Failed to create date table: {}", e),
                schema: Some(schema.to_string()),
                suggestion: "💡 This is likely an internal error".to_string(),
            })?;
        
        // Register the date table
        ctx.register_table(alias, Arc::new(mem_table))
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Table Registration".to_string(),
                reason: format!("Failed to register date table: {}", e),
                suggestion: "💡 Try a different alias name".to_string(),
            })?;
        
        // Create DataFrame
        let df = ctx.table(alias).await
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "DataFrame Creation".to_string(),
                reason: format!("Failed to create date table DataFrame: {}", e),
                suggestion: "💡 Verify table registration succeeded".to_string(),
            })?;
        
        // Return new CustomDataFrame
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

    /// Create a date range table with multiple date formats and period ranges
    pub async fn create_formatted_date_range_table(
        start_date: &str,
        end_date: &str,
        alias: &str,
        format_name: String,
        format: DateFormat,
        include_period_ranges: bool,
        week_start_day: Weekday
    ) -> ElusionResult<Self> {
    
        let start = NaiveDate::parse_from_str(start_date, "%Y-%m-%d")
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Date Parsing".to_string(),
                reason: format!("Failed to parse start_date '{}': {}", start_date, e),
                suggestion: "💡 Ensure date format is YYYY-MM-DD".to_string(),
            })?;
            
        let end = NaiveDate::parse_from_str(end_date, "%Y-%m-%d")
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Date Parsing".to_string(),
                reason: format!("Failed to parse end_date '{}': {}", end_date, e),
                suggestion: "💡 Ensure date format is YYYY-MM-DD".to_string(),
            })?;
            
        if end < start {
            return Err(ElusionError::InvalidOperation {
                operation: "Date Range Validation".to_string(),
                reason: format!("End date '{}' is before start date '{}'", end_date, start_date),
                suggestion: "💡 Ensure end_date is after or equal to start_date".to_string(),
            });
        }
        
        let days = end.signed_duration_since(start).num_days() as usize + 1;
        
        // Check if format includes time components
        let format_str = format.format_str();
        let has_time_components = format_str.contains("%H") || 
                                 format_str.contains("%M") || 
                                 format_str.contains("%S") ||
                                 format_str.contains("%I") ||
                                 format_str.contains("%p") ||
                                 format_str.contains("%P");
        
        // create builder for the date column
        let mut date_builder = StringBuilder::new();
        
        let mut year_builder = Int32Builder::new();
        let mut month_builder = Int32Builder::new();
        let mut day_builder = Int32Builder::new();
        let mut quarter_builder = Int32Builder::new();
        let mut week_num_builder = Int32Builder::new();
        let mut day_of_week_builder = Int32Builder::new();
        let mut day_of_week_name_builder = StringBuilder::new();
        let mut day_of_year_builder = Int32Builder::new();
        let mut is_weekend_builder = BooleanBuilder::new();
        
        let mut week_start_builder = StringBuilder::new();
        let mut week_end_builder = StringBuilder::new();
        let mut month_start_builder = StringBuilder::new();
        let mut month_end_builder = StringBuilder::new();
        let mut quarter_start_builder = StringBuilder::new();
        let mut quarter_end_builder = StringBuilder::new();
        let mut year_start_builder = StringBuilder::new();
        let mut year_end_builder = StringBuilder::new();
    
        for day_offset in 0..days {
            let current_date = start + chrono::Duration::days(day_offset as i64);
            
            // Format date with the specified format, handling time components if present
            let formatted_date = if has_time_components {
                // Convert date to datetime at midnight for formats with time components
                let datetime = NaiveDateTime::new(
                    current_date, 
                    NaiveTime::from_hms_opt(0, 0, 0).unwrap()
                );
                datetime.format(format_str).to_string()
            } else {
                current_date.format(format_str).to_string()
            };
            
            date_builder.append_value(formatted_date);
            
            // year
            year_builder.append_value(current_date.year());
            
            // month
            month_builder.append_value(current_date.month() as i32);
            
            // day of month
            day_builder.append_value(current_date.day() as i32);
            
            // quarter
            let quarter = ((current_date.month() - 1) / 3 + 1) as i32;
            quarter_builder.append_value(quarter);
            
            // week number
            let week_num = current_date.iso_week().week() as i32;
            week_num_builder.append_value(week_num);
            
            // day of week (0 = Monday, 6 = Sunday if week_start_day is Monday)
            // or (0 = Sunday, 6 = Saturday if week_start_day is Sunday)
            let adjusted_weekday = match week_start_day {
                Weekday::Mon => (current_date.weekday().num_days_from_monday()) as i32,
                Weekday::Sun => (current_date.weekday().num_days_from_sunday()) as i32,
                _ => ((current_date.weekday().num_days_from_monday() + 
                     7 - week_start_day.num_days_from_monday()) % 7) as i32,
            };
            day_of_week_builder.append_value(adjusted_weekday);
            
            let day_name = match current_date.weekday() {
                Weekday::Mon => "Monday",
                Weekday::Tue => "Tuesday",
                Weekday::Wed => "Wednesday",
                Weekday::Thu => "Thursday",
                Weekday::Fri => "Friday",
                Weekday::Sat => "Saturday",
                Weekday::Sun => "Sunday",
            };
            day_of_week_name_builder.append_value(day_name);
            
            // day of year
            day_of_year_builder.append_value(current_date.ordinal() as i32);
            
            // weekend flag (Saturday and Sunday)
            is_weekend_builder.append_value(current_date.weekday() == Weekday::Sat || 
                                           current_date.weekday() == Weekday::Sun);
            
            if include_period_ranges {
                // week start and end
                let days_since_week_start = current_date.weekday().num_days_from_monday() as i64;
                let adjusted_days = match week_start_day {
                    Weekday::Mon => days_since_week_start,
                    // if week starts on Sunday and today is Sunday, then it's 0 days from week start
                    Weekday::Sun => if current_date.weekday() == Weekday::Sun { 0 } 
                                            else { (days_since_week_start + 1) % 7 },
                    // for other start days, calculating the offset
                    _ => (days_since_week_start + 7 - 
                         week_start_day.num_days_from_monday() as i64) % 7,
                };
                
                let week_start = current_date - Duration::days(adjusted_days);
                let week_end = week_start + Duration::days(6); // Week end is 6 days after start
                
                // month start (first day of month)
                let month_start = NaiveDate::from_ymd_opt(
                    current_date.year(), current_date.month(), 1).unwrap();
                
                // month end (last day of month)
                let month_end = if current_date.month() == 12 {
                    NaiveDate::from_ymd_opt(current_date.year() + 1, 1, 1).unwrap() - Duration::days(1)
                } else {
                    NaiveDate::from_ymd_opt(current_date.year(), current_date.month() + 1, 1).unwrap() - Duration::days(1)
                };
                
                // quarter start
                let quarter_start_month = ((quarter - 1) * 3 + 1) as u32;
                let quarter_start = chrono::NaiveDate::from_ymd_opt(
                    current_date.year(), quarter_start_month, 1).unwrap();
                
                // quarter end
                let quarter_end_month = quarter_start_month + 2;
                let quarter_end_year = if quarter_end_month > 12 {
                    current_date.year() + 1
                } else {
                    current_date.year()
                };
                let quarter_end_month_adj = if quarter_end_month > 12 {
                    quarter_end_month - 12
                } else {
                    quarter_end_month
                };
                
                let quarter_end = if quarter_end_month_adj == 12 {
                    chrono::NaiveDate::from_ymd_opt(quarter_end_year + 1, 1, 1).unwrap() - chrono::Duration::days(1)
                } else {
                    chrono::NaiveDate::from_ymd_opt(quarter_end_year, quarter_end_month_adj + 1, 1).unwrap() - chrono::Duration::days(1)
                };
                
                // year start and end
                let year_start = NaiveDate::from_ymd_opt(current_date.year(), 1, 1).unwrap();
                let year_end = NaiveDate::from_ymd_opt(current_date.year(), 12, 31).unwrap();
                
                // Format period dates, handling time components if present
                let format_period_date = |date: NaiveDate| -> String {
                    if has_time_components {
                        // Add midnight time for formats with time components
                        let datetime = NaiveDateTime::new(
                            date, 
                            NaiveTime::from_hms_opt(0, 0, 0).unwrap()
                        );
                        datetime.format(format_str).to_string()
                    } else {
                        date.format(format_str).to_string()
                    }
                };
                
                // Apply formatting to all period dates
                week_start_builder.append_value(format_period_date(week_start));
                week_end_builder.append_value(format_period_date(week_end));
                month_start_builder.append_value(format_period_date(month_start));
                month_end_builder.append_value(format_period_date(month_end));
                quarter_start_builder.append_value(format_period_date(quarter_start));
                quarter_end_builder.append_value(format_period_date(quarter_end));
                year_start_builder.append_value(format_period_date(year_start));
                year_end_builder.append_value(format_period_date(year_end));
            }
        }
        
        let mut fields = Vec::new();
        let mut columns = Vec::new();
        
        // date format column
        fields.push(Field::new(&format_name, ArrowDataType::Utf8, false));
        columns.push(Arc::new(date_builder.finish()) as Arc<dyn Array>);
        
        // date parts columns
        fields.push(Field::new("year", ArrowDataType::Int32, false));
        columns.push(Arc::new(year_builder.finish()) as Arc<dyn Array>);
        
        fields.push(Field::new("month", ArrowDataType::Int32, false));
        columns.push(Arc::new(month_builder.finish()) as Arc<dyn Array>);
        
        fields.push(Field::new("day", ArrowDataType::Int32, false));
        columns.push(Arc::new(day_builder.finish()) as Arc<dyn Array>);
        
        fields.push(Field::new("quarter", ArrowDataType::Int32, false));
        columns.push(Arc::new(quarter_builder.finish()) as Arc<dyn Array>);
        
        fields.push(Field::new("week_num", ArrowDataType::Int32, false));
        columns.push(Arc::new(week_num_builder.finish()) as Arc<dyn Array>);
        
        fields.push(Field::new("day_of_week", ArrowDataType::Int32, false));
        columns.push(Arc::new(day_of_week_builder.finish()) as Arc<dyn Array>);
        
        fields.push(Field::new("day_of_week_name", ArrowDataType::Utf8, false));
        columns.push(Arc::new(day_of_week_name_builder.finish()) as Arc<dyn Array>);
        
        fields.push(Field::new("day_of_year", ArrowDataType::Int32, false));
        columns.push(Arc::new(day_of_year_builder.finish()) as Arc<dyn Array>);
        
        fields.push(Field::new("is_weekend", ArrowDataType::Boolean, false));
        columns.push(Arc::new(is_weekend_builder.finish()) as Arc<dyn Array>);
        
        // period range columns
        if include_period_ranges {
            fields.push(Field::new("week_start", ArrowDataType::Utf8, false));
            columns.push(Arc::new(week_start_builder.finish()) as Arc<dyn Array>);
            
            fields.push(Field::new("week_end", ArrowDataType::Utf8, false));
            columns.push(Arc::new(week_end_builder.finish()) as Arc<dyn Array>);
            
            fields.push(Field::new("month_start", ArrowDataType::Utf8, false));
            columns.push(Arc::new(month_start_builder.finish()) as Arc<dyn Array>);
            
            fields.push(Field::new("month_end", ArrowDataType::Utf8, false));
            columns.push(Arc::new(month_end_builder.finish()) as Arc<dyn Array>);
            
            fields.push(Field::new("quarter_start", ArrowDataType::Utf8, false));
            columns.push(Arc::new(quarter_start_builder.finish()) as Arc<dyn Array>);
            
            fields.push(Field::new("quarter_end", ArrowDataType::Utf8, false));
            columns.push(Arc::new(quarter_end_builder.finish()) as Arc<dyn Array>);
            
            fields.push(Field::new("year_start", ArrowDataType::Utf8, false));
            columns.push(Arc::new(year_start_builder.finish()) as Arc<dyn Array>);
            
            fields.push(Field::new("year_end", ArrowDataType::Utf8, false));
            columns.push(Arc::new(year_end_builder.finish()) as Arc<dyn Array>);
        }
        
        let schema = Arc::new(Schema::new(fields));
        
        let record_batch = RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| ElusionError::Custom(
                format!("Failed to create record batch: {}", e)
            ))?;
        
        let ctx = SessionContext::new();
        
        let mem_table = MemTable::try_new(schema.clone().into(), vec![vec![record_batch]])
            .map_err(|e| ElusionError::SchemaError {
                message: format!("Failed to create date table: {}", e),
                schema: Some(schema.to_string()),
                suggestion: "💡 This is likely an internal error".to_string(),
            })?;
        
        ctx.register_table(alias, Arc::new(mem_table))
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Table Registration".to_string(),
                reason: format!("Failed to register date table: {}", e),
                suggestion: "💡 Try a different alias name".to_string(),
            })?;
        
        let df = ctx.table(alias).await
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "DataFrame Creation".to_string(),
                reason: format!("Failed to create date table DataFrame: {}", e),
                suggestion: "💡 Verify table registration succeeded".to_string(),
            })?;
        
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
    
    
      /// Create a materialized view from the current DataFrame state
      pub async fn create_view(
          &self,
          view_name: &str,
          ttl_seconds: Option<u64>,
      ) -> ElusionResult<()> {
          // Get the SQL that would be executed for this DataFrame
          let sql = self.construct_sql();
          
          // Create a new SessionContext for this operation
          let ctx = SessionContext::new();
          
          // Register necessary tables
          Self::register_df_as_table(&ctx, &self.table_alias, &self.df).await?;
          
          for join in &self.joins {
              Self::register_df_as_table(&ctx, &join.dataframe.table_alias, &join.dataframe.df).await?;
          }
          
          // Create the materialized view
          let mut manager = MATERIALIZED_VIEW_MANAGER.lock().unwrap();
          manager.create_view(&ctx, view_name, &sql, ttl_seconds).await
      }
      
      /// Get a DataFrame from a materialized view
      pub async fn from_view(view_name: &str) -> ElusionResult<Self> {
          let ctx = SessionContext::new();
          let manager = MATERIALIZED_VIEW_MANAGER.lock().unwrap();
          
          let df = manager.get_view_as_dataframe(&ctx, view_name).await?;
          
          Ok(CustomDataFrame {
              df,
              table_alias: view_name.to_string(),
              from_table: view_name.to_string(),
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
      
      /// Refresh a materialized view
      pub async fn refresh_view(view_name: &str) -> ElusionResult<()> {
          let ctx = SessionContext::new();
          let mut manager = MATERIALIZED_VIEW_MANAGER.lock().unwrap();
          manager.refresh_view(&ctx, view_name).await
      }
      
      /// Drop a materialized view
      pub async fn drop_view(view_name: &str) -> ElusionResult<()> {
          let mut manager = MATERIALIZED_VIEW_MANAGER.lock().unwrap();
          manager.drop_view(view_name)
      }
      
      /// List all materialized views
      pub async fn list_views() -> Vec<(String, DateTime<Utc>, Option<u64>)> {
        let manager = MATERIALIZED_VIEW_MANAGER.lock().unwrap();
        let views = manager.list_views();
        
        if views.is_empty() {
            println!("There are no materialized views created.");
        }
        
        views
    }
      
      /// Execute query with caching
      pub async fn elusion_with_cache(&self, alias: &str) -> ElusionResult<Self> {
          let sql = self.construct_sql();
          
          // Try to get from cache first
          let mut cache = QUERY_CACHE.lock().unwrap();
          if let Some(cached_result) = cache.get_cached_result(&sql) {
              println!("✅ Using cached result for query");
              
              // Create a DataFrame from the cached result
              let ctx = SessionContext::new();
              let schema = cached_result[0].schema();
              
              let mem_table = MemTable::try_new(schema.clone(), vec![cached_result])
                  .map_err(|e| ElusionError::Custom(format!("Failed to create memory table from cache: {}", e)))?;
              
              ctx.register_table(alias, Arc::new(mem_table))
                  .map_err(|e| ElusionError::Custom(format!("Failed to register table from cache: {}", e)))?;
              
              let df = ctx.table(alias).await
                  .map_err(|e| ElusionError::Custom(format!("Failed to create DataFrame from cache: {}", e)))?;
              
              return Ok(CustomDataFrame {
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
                  query: sql,
                  aggregated_df: None,
                  union_tables: None,
                  original_expressions: self.original_expressions.clone(),
              });
          }
          
          // Not in cache, execute the query
          let result = self.elusion(alias).await?;
          
          // Cache the result
          let batches = result.df.clone().collect().await
              .map_err(|e| ElusionError::Custom(format!("Failed to collect batches: {}", e)))?;
          
          cache.cache_query(&sql, batches);
          
          Ok(result)
      }
      
      /// Invalidate cache for specific tables
      pub fn invalidate_cache(table_names: &[String]) {
          let mut cache = QUERY_CACHE.lock().unwrap();
          cache.invalidate(table_names);
      }
      
      /// Clear the entire query cache
      pub fn clear_cache() {
        let mut cache = QUERY_CACHE.lock().unwrap();
        let size_before = cache.cached_queries.len();
        cache.clear();
        println!("Cache cleared: {} queries removed from cache.", size_before);
    }
      
      /// Modify cache settings
      pub fn configure_cache(max_size: usize, ttl_seconds: Option<u64>) {
          *QUERY_CACHE.lock().unwrap() = QueryCache::new(max_size, ttl_seconds);
      }

     /// Helper function to register a DataFrame as a table provider in the given SessionContext
     async fn register_df_as_table(
        ctx: &SessionContext,
        table_name: &str,
        df: &DataFrame,
    ) -> ElusionResult<()> {
        let batches = df.clone().collect().await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Data Collection".to_string(),
            reason: format!("Failed to collect DataFrame: {}", e),
            suggestion: "💡 Check if DataFrame contains valid data".to_string()
        })?;

        let schema = df.schema();

        let mem_table = MemTable::try_new(schema.clone().into(), vec![batches])
        .map_err(|e| ElusionError::SchemaError {
            message: format!("Failed to create in-memory table: {}", e),
            schema: Some(schema.to_string()),
            suggestion: "💡 Verify schema compatibility and data types".to_string()
        })?;

        ctx.register_table(table_name, Arc::new(mem_table))
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Table Registration".to_string(),
            reason: format!("Failed to register table '{}': {}", table_name, e),
            suggestion: "💡 Check if table name is unique and valid".to_string()
        })?;

        Ok(())
    }

    //=========== SHARE POINT

    /// Load Excel file from SharePoint - handles everything automatically
    #[cfg(feature = "sharepoint")]
    pub async fn load_excel_from_sharepoint(
        tenant_id: &str,
        client_id: &str,
        site_url: &str,
        file_path: &str,
    ) -> ElusionResult<Self> {
        let config = SharePointConfig::new(
            tenant_id.to_string(),
            client_id.to_string(),
            site_url.to_string(),
        );
        
        let mut client = SharePointClient::new(config);
        let content = client.download_file(file_path).await?;
        
        let temp_dir = std::env::temp_dir();
        let temp_file = temp_dir.join(format!("sharepoint_{}.xlsx", chrono::Utc::now().timestamp()));
        std::fs::write(&temp_file, content)?;
        
        let aliased_df = Self::load_excel(temp_file.to_str().unwrap(), "sharepoint_data").await?;
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
        })
    }

    /// Load CSV file from SharePoint - handles everything automatically  
    #[cfg(feature = "sharepoint")]
    pub async fn load_csv_from_sharepoint(
        tenant_id: &str,
        client_id: &str,
        site_url: &str,
        file_path: &str,
    ) -> ElusionResult<Self> {
        let config = SharePointConfig::new(
            tenant_id.to_string(),
            client_id.to_string(),
            site_url.to_string(),
        );
        
        let mut client = SharePointClient::new(config);
        let content = client.download_file(file_path).await?;
        
        let temp_dir = std::env::temp_dir();
        let temp_file = temp_dir.join(format!("sharepoint_{}.csv", chrono::Utc::now().timestamp()));
        std::fs::write(&temp_file, content)?;
        
        let aliased_df = Self::load_csv(temp_file.to_str().unwrap(), "sharepoint_data").await?;
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
        })
    }

    /// Load JSON file from SharePoint - handles everything automatically
    #[cfg(feature = "sharepoint")]
    pub async fn load_json_from_sharepoint(
        tenant_id: &str,
        client_id: &str,
        site_url: &str,
        file_path: &str,
    ) -> ElusionResult<Self> {
        let config = SharePointConfig::new(
            tenant_id.to_string(),
            client_id.to_string(),
            site_url.to_string(),
        );
        
        let mut client = SharePointClient::new(config);
        let content = client.download_file(file_path).await?;
        
        let temp_dir = std::env::temp_dir();
        let temp_file = temp_dir.join(format!("sharepoint_{}.json", chrono::Utc::now().timestamp()));
        std::fs::write(&temp_file, content)?;
        
        let aliased_df = Self::load_json(temp_file.to_str().unwrap(), "sharepoint_data").await?;
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
        })
    }

    /// Load Parquet file from SharePoint - handles everything automatically
    #[cfg(feature = "sharepoint")]
    pub async fn load_parquet_from_sharepoint(
        tenant_id: &str,
        client_id: &str,
        site_url: &str,
        file_path: &str,
    ) -> ElusionResult<Self> {
        let config = SharePointConfig::new(
            tenant_id.to_string(),
            client_id.to_string(),
            site_url.to_string(),
        );
        
        let mut client = SharePointClient::new(config);
        let content = client.download_file(file_path).await?;
        
        let temp_dir = std::env::temp_dir();
        let temp_file = temp_dir.join(format!("sharepoint_{}.parquet", chrono::Utc::now().timestamp()));
        std::fs::write(&temp_file, content)?;
        
        let aliased_df = Self::load_parquet(temp_file.to_str().unwrap(), "sharepoint_data").await?;
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
        })
    }

    // Stub implementations 
    #[cfg(not(feature = "sharepoint"))]
    pub async fn load_excel_from_sharepoint(_tenant_id: &str, _client_id: &str, _site_url: &str, _file_path: &str) -> ElusionResult<Self> {
        Err(ElusionError::InvalidOperation {
            operation: "SharePoint Excel Loading".to_string(),
            reason: "SharePoint feature not enabled".to_string(),
            suggestion: "💡 Add 'sharepoint' to your features: features = [\"sharepoint\"]".to_string(),
        })
    }

    #[cfg(not(feature = "sharepoint"))]
    pub async fn load_csv_from_sharepoint(_tenant_id: &str, _client_id: &str, _site_url: &str, _file_path: &str) -> ElusionResult<Self> {
        Err(ElusionError::InvalidOperation {
            operation: "SharePoint CSV Loading".to_string(),
            reason: "SharePoint feature not enabled".to_string(),
            suggestion: "💡 Add 'sharepoint' to your features: features = [\"sharepoint\"]".to_string(),
        })
    }

    #[cfg(not(feature = "sharepoint"))]
    pub async fn load_json_from_sharepoint(_tenant_id: &str, _client_id: &str, _site_url: &str, _file_path: &str) -> ElusionResult<Self> {
        Err(ElusionError::InvalidOperation {
            operation: "SharePoint JSON Loading".to_string(),
            reason: "SharePoint feature not enabled".to_string(),
            suggestion: "💡 Add 'sharepoint' to your features: features = [\"sharepoint\"]".to_string(),
        })
    }

    #[cfg(not(feature = "sharepoint"))]
    pub async fn load_parquet_from_sharepoint(_tenant_id: &str, _client_id: &str, _site_url: &str, _file_path: &str) -> ElusionResult<Self> {
        Err(ElusionError::InvalidOperation {
            operation: "SharePoint Parquet Loading".to_string(),
            reason: "SharePoint feature not enabled".to_string(),
            suggestion: "💡 Add 'sharepoint' to your features: features = [\"sharepoint\"]".to_string(),
        })
    }

    /// Load all files from a SharePoint folder and union them if they have compatible schemas
    /// Supports CSV, Excel, JSON, and Parquet files
    #[cfg(feature = "sharepoint")]
    pub async fn load_folder_from_sharepoint(
        tenant_id: &str,
        client_id: &str,
        site_url: &str,
        folder_path: &str,
        file_extensions: Option<Vec<&str>>, // Filter by extensions, e.g., vec!["xlsx", "csv"]
        result_alias: &str,
    ) -> ElusionResult<Self> {
        let config = SharePointConfig::new(
            tenant_id.to_string(),
            client_id.to_string(),
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
                    match Self::load_csv_from_sharepoint(tenant_id, client_id, site_url, &file_path).await {
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
                    match Self::load_excel_from_sharepoint(tenant_id, client_id, site_url, &file_path).await {
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
                    match Self::load_json_from_sharepoint(tenant_id, client_id, site_url, &file_path).await {
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
                    match Self::load_parquet_from_sharepoint(tenant_id, client_id, site_url, &file_path).await {
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

    // Stub implementation for when sharepoint feature is not enabled
    #[cfg(not(feature = "sharepoint"))]
    pub async fn load_folder_from_sharepoint(
        _tenant_id: &str, 
        _client_id: &str, 
        _site_url: &str, 
        _folder_path: &str,
        _file_extensions: Option<Vec<&str>>,
        _result_alias: &str,
    ) -> ElusionResult<Self> {
        Err(ElusionError::InvalidOperation {
            operation: "SharePoint Folder Loading".to_string(),
            reason: "SharePoint feature not enabled".to_string(),
            suggestion: "💡 Add 'sharepoint' to your features: features = [\"sharepoint\"]".to_string(),
        })
    }

    /// Load all files from SharePoint folder and add filename as a column
    /// Same as load_folder_from_sharepoint but adds a "filename" column to track source files
    #[cfg(feature = "sharepoint")]
    pub async fn load_folder_from_sharepoint_with_filename_column(
        tenant_id: &str,
        client_id: &str,
        site_url: &str,
        folder_path: &str,
        file_extensions: Option<Vec<&str>>,
        result_alias: &str,
    ) -> ElusionResult<Self> {
        let config = SharePointConfig::new(
            tenant_id.to_string(),
            client_id.to_string(),
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
                    match Self::load_csv_from_sharepoint(tenant_id, client_id, site_url, &file_path).await {
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
                    match Self::load_excel_from_sharepoint(tenant_id, client_id, site_url, &file_path).await {
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
                    match Self::load_json_from_sharepoint(tenant_id, client_id, site_url, &file_path).await {
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
                    match Self::load_parquet_from_sharepoint(tenant_id, client_id, site_url, &file_path).await {
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

    // Stub implementation for when sharepoint feature is not enabled
    #[cfg(not(feature = "sharepoint"))]
    pub async fn load_folder_from_sharepoint_with_filename_column(
        _tenant_id: &str, 
        _client_id: &str, 
        _site_url: &str, 
        _folder_path: &str,
        _file_extensions: Option<Vec<&str>>,
        _result_alias: &str,
    ) -> ElusionResult<Self> {
        Err(ElusionError::InvalidOperation {
            operation: "SharePoint Folder Loading with Filename".to_string(),
            reason: "SharePoint feature not enabled".to_string(),
            suggestion: "💡 Add 'sharepoint' to your features: features = [\"sharepoint\"]".to_string(),
        })
    }

    // ====== POSTGRESS

    /// Create a DataFrame from a PostgreSQL query
    #[cfg(feature = "postgres")]
    pub async fn from_postgres(
        conn: &PostgresConnection,
        query: &str,
        alias: &str
    ) -> ElusionResult<Self> {
        let rows = conn.query(query, &[])
            .await
            .map_err(|e| ElusionError::Custom(format!("PostgreSQL query error: {}", e)))?;

        if rows.is_empty() {
            return Err(ElusionError::Custom("Query returned no rows".to_string()));
        }

        // Extract column info from the first row
        let first_row = &rows[0];
        let columns = first_row.columns();
        
        // Create schema from column metadata
        let mut fields = Vec::with_capacity(columns.len());
        for column in columns {
            let column_name = column.name();
            let pg_type = column.type_();
            
            // Map PostgreSQL types to Arrow types
            let arrow_type = match *pg_type {
                Type::BOOL => ArrowDataType::Boolean,
                Type::INT2 | Type::INT4 => ArrowDataType::Int64,
                Type::INT8 => ArrowDataType::Int64,
                Type::FLOAT4 => ArrowDataType::Float32,
                Type::FLOAT8 => ArrowDataType::Float64,
                Type::TEXT | Type::VARCHAR | Type::CHAR | Type::NAME | Type::UNKNOWN => ArrowDataType::Utf8,
                Type::NUMERIC => ArrowDataType::Float64, 
                Type::TIMESTAMP | Type::TIMESTAMPTZ => ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                Type::DATE => ArrowDataType::Date32,
                Type::TIME | Type::TIMETZ => ArrowDataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
                Type::UUID | Type::JSON | Type::JSONB => ArrowDataType::Utf8, 
                _ => ArrowDataType::Utf8, // Fallback for unsupported types
            };
            
            fields.push(Field::new(column_name, arrow_type, true));
        }
        
        let schema = Arc::new(Schema::new(fields));
        
        // Build arrays for each column
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
        
        for col_idx in 0..columns.len() {
            // let column = &columns[col_idx];
            let field = schema.field(col_idx);
            
            match field.data_type() {
                ArrowDataType::Boolean => {
                    let mut builder = BooleanBuilder::new();
                    for row in &rows {
                        match row.try_get::<_, Option<bool>>(col_idx) {
                            Ok(Some(value)) => builder.append_value(value),
                            Ok(None) => builder.append_null(),
                            Err(_) => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                ArrowDataType::Int32 => {
                    let mut builder = Int64Builder::new();
                    for row in &rows {
                        match row.try_get::<_, Option<i32>>(col_idx) {
                            Ok(Some(value)) => builder.append_value(value as i64),
                            Ok(None) => builder.append_null(),
                            Err(_) => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                ArrowDataType::Int64 => {
                    let mut builder = Int64Builder::new();
                    for row in &rows {
                        // Try multiple approaches to get the integer value
                        if let Ok(Some(value)) = row.try_get::<_, Option<i64>>(col_idx) {
                            builder.append_value(value);
                        } else if let Ok(Some(value)) = row.try_get::<_, Option<i32>>(col_idx) {
                            builder.append_value(value as i64);
                        } else if let Ok(Some(value)) = row.try_get::<_, Option<i16>>(col_idx) {
                            builder.append_value(value as i64);
                        } else {
                            builder.append_null();
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                ArrowDataType::Float64 => {
                    let mut builder = Float64Builder::new();
                    for row in &rows {
                        // First try to get as Decimal 
                        if let Ok(Some(decimal)) = row.try_get::<_, Option<Decimal>>(col_idx) {
                            if let Some(float_val) = decimal.to_f64() {
                                builder.append_value(float_val);
                            } else {
                                builder.append_null();
                            }
                        } else if let Ok(Some(value)) = row.try_get::<_, Option<f64>>(col_idx) {
                            builder.append_value(value);
                        } else if let Ok(Some(value)) = row.try_get::<_, Option<f32>>(col_idx) {
                            builder.append_value(value as f64);
                        } else if let Ok(Some(value)) = row.try_get::<_, Option<i64>>(col_idx) {
                            builder.append_value(value as f64);
                        } else if let Ok(Some(value)) = row.try_get::<_, Option<i32>>(col_idx) {
                            builder.append_value(value as f64);
                        } else if let Ok(Some(value_str)) = row.try_get::<_, Option<String>>(col_idx) {
                            if let Ok(num) = value_str.parse::<f64>() {
                                builder.append_value(num);
                            } else {
                                builder.append_null();
                            }
                        } else {
                            builder.append_null();
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                ArrowDataType::Float32 => {
                    let mut builder = Float64Builder::new();
                    for row in &rows {
                        match row.try_get::<_, Option<f64>>(col_idx) {
                            Ok(Some(value)) => builder.append_value(value),
                            Ok(None) => builder.append_null(),
                            Err(_) => match row.try_get::<_, Option<f32>>(col_idx) {
                                Ok(Some(value)) => builder.append_value(value as f64),
                                Ok(None) => builder.append_null(),
                                Err(_) => builder.append_null(),
                            },
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                ArrowDataType::Utf8 => {
                    let mut builder = StringBuilder::new();
                    for row in &rows {
                        match row.try_get::<_, Option<String>>(col_idx) {
                            Ok(Some(value)) => builder.append_value(value),
                            Ok(None) => builder.append_null(),
                            Err(_) => {
                                // Try as &str if String fails
                                match row.try_get::<_, Option<&str>>(col_idx) {
                                    Ok(Some(value)) => builder.append_value(value),
                                    Ok(None) => builder.append_null(),
                                    Err(_) => {
                                        // Instead of trying to use serde_json::Value which doesn't implement FromSql,
                                        // we'll use a simple generic way to get the value as a string
                                        if let Ok(value) = row.try_get::<_, Option<f64>>(col_idx) {
                                            if let Some(num) = value {
                                                builder.append_value(num.to_string());
                                            } else {
                                                builder.append_null();
                                            }
                                        } else if let Ok(value) = row.try_get::<_, Option<i64>>(col_idx) {
                                            if let Some(num) = value {
                                                builder.append_value(num.to_string());
                                            } else {
                                                builder.append_null();
                                            }
                                        } else if let Ok(value) = row.try_get::<_, Option<bool>>(col_idx) {
                                            if let Some(b) = value {
                                                builder.append_value(b.to_string());
                                            } else {
                                                builder.append_null();
                                            }
                                        } else {
                                            // Last resort: use a placeholder
                                            builder.append_value(format!("?column?_{}", col_idx));
                                        }
                                    }
                                }
                            },
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                // Handle other types as strings for now
                _ => {
                    let mut builder = StringBuilder::new();
                    for row in &rows {
                        // Get the value as text representation
                        match row.try_get::<_, Option<String>>(col_idx) {
                            Ok(Some(value)) => builder.append_value(value),
                            Ok(None) => builder.append_null(),
                            Err(_) => {
                                // Try common types that implement FromSql
                                if let Ok(value) = row.try_get::<_, Option<f64>>(col_idx) {
                                    if let Some(num) = value {
                                        builder.append_value(num.to_string());
                                    } else {
                                        builder.append_null();
                                    }
                                } else if let Ok(value) = row.try_get::<_, Option<i64>>(col_idx) {
                                    if let Some(num) = value {
                                        builder.append_value(num.to_string());
                                    } else {
                                        builder.append_null();
                                    }
                                } else if let Ok(value) = row.try_get::<_, Option<bool>>(col_idx) {
                                    if let Some(b) = value {
                                        builder.append_value(b.to_string());
                                    } else {
                                        builder.append_null();
                                    }
                                } else {
                                    // Last resort: use a placeholder
                                    builder.append_value(format!("?column?_{}", col_idx));
                                }
                            },
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
            }
        }
        
        // Create a record batch
        let batch = RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| ElusionError::Custom(format!("Failed to create record batch: {}", e)))?;
        
        // Create a DataFusion DataFrame
        let ctx = SessionContext::new();
        let mem_table = MemTable::try_new(schema, vec![vec![batch]])
            .map_err(|e| ElusionError::Custom(format!("Failed to create memory table: {}", e)))?;
        
        ctx.register_table(alias, Arc::new(mem_table))
            .map_err(|e| ElusionError::Custom(format!("Failed to register table: {}", e)))?;
        
        let df = ctx.table(alias).await
            .map_err(|e| ElusionError::Custom(format!("Failed to create DataFrame: {}", e)))?;
        
        // Return CustomDataFrame
        Ok(Self {
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

    #[cfg(not(feature = "postgres"))]
    pub async fn from_postgres(
        _conn: &PostgresConnection,
        _query: &str,
        _alias: &str
    ) -> ElusionResult<Self> {
        Err(ElusionError::Custom("*** Warning ***: Postgres feature not enabled. Add feature under [dependencies]".to_string()))
    }


    // ========== MYSQL ============
    #[cfg(feature = "mysql")]
    pub async fn from_mysql(
        conn: &MySqlConnection,
        query: &str,
        alias: &str
    ) -> ElusionResult<Self> {

        let rows = conn.query(query)
                .await
                .map_err(|e| ElusionError::Custom(format!("MySQL query error: {}", e)))?;

            if rows.is_empty() {
                return Err(ElusionError::Custom("Query returned no rows".to_string()));
            }


        // Get column names from the first row
        let first_row = &rows[0];
        let column_names: Vec<String> = first_row.columns_ref()
            .iter()
            .map(|col| col.name_str().to_string())
            .collect();
        
        // Create schema from column metadata
        let mut fields = Vec::with_capacity(column_names.len());
        for column_name in &column_names {
            // Get the column value from the first row to determine its type
            let value_opt = first_row.get_opt::<MySqlValue, _>(column_name.as_str());
            
            // Map MySQL types to Arrow types
            let arrow_type = match value_opt {
                Some(Ok(MySqlValue::NULL)) => ArrowDataType::Utf8, // Default to string for NULL values
                Some(Ok(MySqlValue::Bytes(_))) => ArrowDataType::Utf8,
                Some(Ok(MySqlValue::Int(_))) => ArrowDataType::Int64,
                Some(Ok(MySqlValue::UInt(_))) => ArrowDataType::UInt64,
                Some(Ok(MySqlValue::Float(_))) => ArrowDataType::Float32,
                Some(Ok(MySqlValue::Double(_))) => ArrowDataType::Float64,
                // MySQL Date format: YYYY-MM-DD
                Some(Ok(MySqlValue::Date(_, _, _, _, _, _, _))) => ArrowDataType::Date32,
                // MySQL Time format: HH:MM:SS
                Some(Ok(MySqlValue::Time(_, _, _, _, _, _))) => ArrowDataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
                _ => ArrowDataType::Utf8, // Default to string for any other type
            };
            
            fields.push(Field::new(column_name, arrow_type, true));
        }
        
        let schema = Arc::new(Schema::new(fields));
        
        // Build arrays for each column
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(column_names.len());
        
        for (col_idx, col_name) in column_names.iter().enumerate() {
            let field = schema.field(col_idx);
            
            match field.data_type() {
                ArrowDataType::Boolean => {
                    let mut builder = BooleanBuilder::new();
                    for row in &rows {
                        let value_opt = row.get_opt::<bool, _>(col_name.as_str());
                        match value_opt {
                            Some(Ok(value)) => builder.append_value(value),
                            _ => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                ArrowDataType::Int64 => {
                    let mut builder = Int64Builder::new();
                    for row in &rows {
                        let value_opt = row.get_opt::<i64, _>(col_name.as_str());
                        match value_opt {
                            Some(Ok(value)) => builder.append_value(value),
                            _ => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                ArrowDataType::UInt64 => {
                    let mut builder = UInt64Builder::new();
                    for row in &rows {
                        let value_opt = row.get_opt::<u64, _>(col_name.as_str());
                        match value_opt {
                            Some(Ok(value)) => builder.append_value(value),
                            _ => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                ArrowDataType::Float32 => {
                    let mut builder = Float64Builder::new();
                    for row in &rows {
                        let value_opt = row.get_opt::<f32, _>(col_name.as_str());
                        match value_opt {
                            Some(Ok(value)) => builder.append_value(value as f64),
                            _ => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                ArrowDataType::Float64 => {
                    let mut builder = Float64Builder::new();
                    for row in &rows {
                        let value_opt = row.get_opt::<f64, _>(col_name.as_str());
                        match value_opt {
                            Some(Ok(value)) => builder.append_value(value),
                            _ => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                ArrowDataType::Date32 => {
                    let mut builder = StringBuilder::new();
                    for row in &rows {
                        let value_opt = row.get_opt::<MySqlValue, _>(col_name.as_str());
                        match value_opt {
                            Some(Ok(MySqlValue::Date(year, month, day, _, _, _, _))) => {
                                builder.append_value(format!("{:04}-{:02}-{:02}", year, month, day));
                            },
                            _ => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                ArrowDataType::Timestamp(_, _) => {
                    let mut builder = StringBuilder::new();
                    for row in &rows {
                        let value_opt = row.get_opt::<MySqlValue, _>(col_name.as_str());
                        match value_opt {
                            Some(Ok(MySqlValue::Date(year, month, day, hour, minute, second, micros))) => {
                                builder.append_value(format!(
                                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
                                    year, month, day, hour, minute, second, micros
                                ));
                            },
                            _ => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                ArrowDataType::Time64(_) => {
                    let mut builder = StringBuilder::new();
                    for row in &rows {
                        let value_opt = row.get_opt::<MySqlValue, _>(col_name.as_str());
                        match value_opt {
                            Some(Ok(MySqlValue::Time(neg, hours, minutes, seconds, _, micros))) => {
                                let sign = if neg { "-" } else { "" };
                                builder.append_value(format!(
                                    "{}{}:{:02}:{:02}.{:06}", 
                                    sign, hours, minutes, seconds, micros
                                ));
                            },
                            _ => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
                _ => {
                    // Default to string for all other types
                    let mut builder = StringBuilder::new();
                    for row in &rows {
                        let value_opt = row.get_opt::<String, _>(col_name.as_str());
                        match value_opt {
                            Some(Ok(value)) => builder.append_value(value),
                            Some(Err(_)) => {
                                // Try to get the value as a byte array
                                let bytes_opt = row.get_opt::<Vec<u8>, _>(col_name.as_str());
                                match bytes_opt {
                                    Some(Ok(bytes)) => match String::from_utf8(bytes) {
                                        Ok(s) => builder.append_value(s),
                                        Err(_) => builder.append_value("[binary data]"),
                                    },
                                    _ => builder.append_null(),
                                }
                            },
                            None => builder.append_null(),
                        }
                    }
                    arrays.push(Arc::new(builder.finish()));
                },
            }
        }
        
        let batch = RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| ElusionError::Custom(format!("Failed to create record batch: {}", e)))?;
        
        let ctx = SessionContext::new();
        let mem_table = MemTable::try_new(schema, vec![vec![batch]])
            .map_err(|e| ElusionError::Custom(format!("Failed to create memory table: {}", e)))?;
        
        ctx.register_table(alias, Arc::new(mem_table))
            .map_err(|e| ElusionError::Custom(format!("Failed to register table: {}", e)))?;
        
        let df = ctx.table(alias).await
            .map_err(|e| ElusionError::Custom(format!("Failed to create DataFrame: {}", e)))?;
        
        Ok(Self {
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

    #[cfg(not(feature = "mysql"))]
    pub async fn from_mysql(
        _conn: &MySqlConnection,
        _query: &str,
        _alias: &str
    ) -> ElusionResult<Self> {
        Err(ElusionError::Custom("*** Warning ***: MySQL feature not enabled. Add feature = [\"mysql\"] under [dependencies]".to_string()))
    }

    // ==========NEW instance
    /// NEW method for loading and schema definition
    pub async fn new<'a>(
        file_path: &'a str,
        alias: &'a str,
    ) -> ElusionResult<Self> {
        let aliased_df = Self::load(file_path, alias).await?;

        Ok(CustomDataFrame {
            df: aliased_df.dataframe,
            table_alias: aliased_df.alias,
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
   
    // ==================== DATAFRAME Methods ====================

    /// Add JOIN clauses 
    pub fn join<const N: usize>(
        mut self,
        other: CustomDataFrame,
        conditions: [&str; N],  // Keeping single array of conditions with "="
        join_type: &str
    ) -> Self {
        let condition = conditions.iter()
            .map(|&cond| normalize_condition(cond))  // Keeping the "=" in condition
            .collect::<Vec<_>>()
            .join(" AND ");
    
        self.joins.push(Join {
            dataframe: other,
            condition,
            join_type: join_type.to_string(),
        });
        self
    }
    /// Add multiple JOIN clauses using const generics.
    /// Accepts Array of (DataFrame, conditions, join_type)
    pub fn join_many<const N: usize, const M: usize>(
        self,
        joins: [(CustomDataFrame, [&str; M], &str); N] 
    ) -> Self {
        let join_inputs = joins.into_iter()
            .map(|(df, conds, jt)| {
                let condition = conds.iter()
                    .map(|&cond| normalize_condition(cond))
                    .collect::<Vec<_>>()
                    .join(" AND ");
    
                Join {
                    dataframe: df,
                    condition,
                    join_type: jt.to_string(),
                }
            })
            .collect::<Vec<_>>();
        self.join_many_vec(join_inputs)
    }
    
    pub fn join_many_vec(mut self, joins: Vec<Join>) -> Self {
        self.joins.extend(joins);
        self
    }

    /// GROUP BY clause using const generics
    pub fn group_by<const N: usize>(self, group_columns: [&str; N]) -> Self {
        self.group_by_vec(group_columns.to_vec())
    }
    pub fn group_by_vec(mut self, columns: Vec<&str>) -> Self {
        self.group_by_columns = columns
            .into_iter()
            .map(|s| {
                if is_simple_column(s) {
                    normalize_column_name(s)
                } else if s.contains(" AS ") {
                    // Handle expressions with aliases
                    let expr_part = s.split(" AS ")
                        .next()
                        .unwrap_or(s);
                    normalize_expression(expr_part, &self.table_alias)
                } else {
                    // Handle expressions without aliases
                    normalize_expression(s,  &self.table_alias)
                }
            })
            .collect();
        self
    }

    /// GROUP_BY_ALL function that usifies all SELECT() olumns and reduces need for writing all columns
    pub fn group_by_all(mut self) -> Self {
        let mut all_group_by = Vec::new();
    
        // Process all selected columns
        for col in &self.selected_columns {
            if is_simple_column(&col) {
                // Simple column case
                all_group_by.push(col.clone());
            } else if is_expression(&col) {
                // Expression case - extract the part before AS
                if col.contains(" AS ") {
                    if let Some(expr_part) = col.split(" AS ").next() {
                        let expr = expr_part.trim().to_string();
                        if !all_group_by.contains(&expr) {
                            all_group_by.push(expr);
                        }
                    }
                } else {
                    // Expression without AS
                    if !all_group_by.contains(col) {
                        all_group_by.push(col.clone());
                    }
                }
            } else {
                // Table.Column case
                all_group_by.push(col.clone());
            }
        }
    
        self.group_by_columns = all_group_by;
        self
    }

    /// Add multiple WHERE conditions using const generics.
    /// Allows passing arrays like ["condition1", "condition2", ...]
    pub fn filter_many<const N: usize>(self, conditions: [&str; N]) -> Self {
        self.filter_vec(conditions.to_vec())
    }

    /// Add multiple WHERE conditions using a Vec<&str>
    pub fn filter_vec(mut self, conditions: Vec<&str>) -> Self {
        self.where_conditions.extend(conditions.into_iter().map(|c| normalize_condition_filter(c)));
        self
    }

    /// Add a single WHERE condition
    pub fn filter(mut self, condition: &str) -> Self {
        self.where_conditions.push(normalize_condition_filter(condition));
        self
    }
    
    /// Add multiple HAVING conditions using const generics.
    /// Allows passing arrays like ["condition1", "condition2", ...]
    pub fn having_many<const N: usize>(self, conditions: [&str; N]) -> Self {
        self.having_conditions_vec(conditions.to_vec())
    }

    /// Add multiple HAVING conditions using a Vec<&str>
    pub fn having_conditions_vec(mut self, conditions: Vec<&str>) -> Self {
        self.having_conditions.extend(conditions.into_iter().map(|c| normalize_condition(c)));
        self
    }

    /// Add a single HAVING condition
    pub fn having(mut self, condition: &str) -> Self {
        self.having_conditions.push(normalize_condition(condition));
        self
    }

    /// Add ORDER BY clauses using const generics.
    /// Allows passing arrays like ["column1", "column2"], [true, false]
    pub fn order_by<const N: usize>(self, columns: [&str; N], ascending: [bool; N]) -> Self {
        let normalized_columns: Vec<String> = columns.iter()
            .map(|c| normalize_column_name(c))
            .collect();
        self.order_by_vec(normalized_columns, ascending.to_vec())
    }

    /// Add ORDER BY clauses using vectors
    pub fn order_by_vec(mut self, columns: Vec<String>, ascending: Vec<bool>) -> Self {
        // Ensure that columns and ascending have the same length
        assert!(
            columns.len() == ascending.len(),
            "Columns and ascending flags must have the same length"
        );

        // Zip the columns and ascending flags into a Vec of tuples
        self.order_by_columns = columns.into_iter()
            .zip(ascending.into_iter())
            .collect();
        self
    }

    /// Add multiple ORDER BY clauses using const generics.
    /// Allows passing arrays of tuples: [ ("column1", true), ("column2", false); N ]
    pub fn order_by_many<const N: usize>(self, orders: [( &str, bool ); N]) -> Self {
        let orderings = orders.into_iter()
            .map(|(col, asc)| (normalize_column_name(col), asc))
            .collect::<Vec<_>>();
        self.order_by_many_vec(orderings)
    }

    /// Add multiple ORDER BY clauses using a Vec<(String, bool)>
    pub fn order_by_many_vec(mut self, orders: Vec<(String, bool)>) -> Self {
        self.order_by_columns = orders;
        self
    }

    /// Add LIMIT clause
    pub fn limit(mut self, count: u64) -> Self {
        self.limit_count = Some(count);
        self
    }

    // And in the CustomDataFrame implementation:
    pub fn window(mut self, window_expr: &str) -> Self {
        let normalized = normalize_window_function(window_expr);
        self.window_functions.push(normalized);
        self
    }

    /// Add CTEs using const generics.
    /// Allows passing arrays like ["cte1", "cte2", ...]
    pub fn with_ctes<const N: usize>(self, ctes: [&str; N]) -> Self {
        self.with_ctes_vec(ctes.to_vec())
    }

    /// Add CTEs using a Vec<&str>
    pub fn with_ctes_vec(mut self, ctes: Vec<&str>) -> Self {
        self.ctes.extend(ctes.into_iter().map(|c| c.to_string()));
        self
    }

    /// Add a single CTE
    pub fn with_cte_single(mut self, cte: &str) -> Self {
        self.ctes.push(cte.to_string());
        self
    }

    /// Add SET operations (UNION, INTERSECT, etc.)
    pub fn set_operation(mut self, set_op: &str) -> Self {
        self.set_operations.push(set_op.to_string());
        self
    }
   
    /// Apply multiple string functions to create new columns in the SELECT clause.
    pub fn string_functions<const N: usize>(mut self, expressions: [&str; N]) -> Self {
        for expr in expressions.iter() {
            // Add to SELECT clause
            self.selected_columns.push(normalize_expression(expr, &self.table_alias));
    
            // If GROUP BY is used, extract the expression part (before AS)
            // and add it to GROUP BY columns
            if !self.group_by_columns.is_empty() {
                let expr_part = expr.split(" AS ")
                    .next()
                    .unwrap_or(expr);
                self.group_by_columns.push(normalize_expression(expr_part, &self.table_alias));
            }
        }
        self
    }

     /// Add datetime functions to the SELECT clause
    /// Supports various date/time operations and formats
    pub fn datetime_functions<const N: usize>(mut self, expressions: [&str; N]) -> Self {
        for expr in expressions.iter() {
            // Add to SELECT clause
            self.selected_columns.push(normalize_expression(expr, &self.table_alias));
    
            // If GROUP BY is used, extract the expression part (before AS)
            if !self.group_by_columns.is_empty() {
                let expr_part = expr.split(" AS ")
                    .next()
                    .unwrap_or(expr);
                
                    self.group_by_columns.push(normalize_expression(expr_part, &self.table_alias));
            }
        }
        self
    }

    /// Adding aggregations to the SELECT clause using const generics.
    /// Ensures that only valid aggregate expressions are included.
    pub fn agg<const N: usize>(self, aggregations: [&str; N]) -> Self {
        self.clone().agg_vec(
            aggregations.iter()
                .filter(|&expr| is_aggregate_expression(expr))
                .map(|s| normalize_expression(s, &self.table_alias))
                .collect()
        )
    }

    /// Add aggregations to the SELECT clause using a `Vec<String>`
    /// Ensures that only valid aggregate expressions are included.
    pub fn agg_vec(mut self, aggregations: Vec<String>) -> Self {
        let valid_aggs = aggregations.into_iter()
            .filter(|expr| is_aggregate_expression(expr))
            .collect::<Vec<_>>();

        self.aggregations.extend(valid_aggs);
        self
    }

     /// Performs a APPEND with another DataFrame
     pub async fn append(self, other: CustomDataFrame) -> ElusionResult<Self> {

        // if self.df.schema() != other.df.schema() {
        //     return Err(ElusionError::SetOperationError {
        //         operation: "APPEND".to_string(),
        //         reason: "Schema mismatch between dataframes".to_string(),
        //         suggestion: "Ensure both dataframes have identical column names and types".to_string(),
        //     });
        // }

        let ctx = Arc::new(SessionContext::new());
        
        let mut batches_self = self.df.clone().collect().await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Collecting batches from first dataframe".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if the dataframe is valid and not empty".to_string(),
        })?;

        let batches_other = other.df.clone().collect().await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Collecting batches from second dataframe".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if the dataframe is valid and not empty".to_string(),
        })?;

        batches_self.extend(batches_other);
    
        let mem_table = MemTable::try_new(self.df.schema().clone().into(), vec![batches_self])
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Creating memory table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Verify data consistency, number of columns or memory availability".to_string(),
        })?;
    
        let alias = "append_result";

        ctx.register_table(alias, Arc::new(mem_table))
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Registering table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if table name is unique in context".to_string(),
        })?;
    
        let df = ctx.table(alias).await
            .map_err(|e| ElusionError::Custom(format!("Failed to create union DataFrame: {}", e)))?;
    
        Ok(CustomDataFrame {
            df,
            table_alias: alias.to_string(),
            from_table: alias.to_string(),
            selected_columns: self.selected_columns.clone(),
            alias_map: self.alias_map.clone(),
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
            original_expressions: self.original_expressions.clone(),
        })
    }
    /// Performs APPEND on multiple dataframes
    pub async fn append_many<const N: usize>(self, others: [CustomDataFrame; N]) -> ElusionResult<Self> {

        if N == 0 {
            return Err(ElusionError::SetOperationError {
                operation: "APPEND MANY".to_string(),
                reason: "No dataframes provided for append operation".to_string(),
                suggestion: "💡 Provide at least one dataframe to append".to_string(),
            });
        }

        // for (i, other) in others.iter().enumerate() {
        //     if self.df.schema() != other.df.schema() {
        //         return Err(ElusionError::SetOperationError {
        //             operation: "APPEND MANY".to_string(),
        //             reason: format!("Schema mismatch with dataframe at index {}", i),
        //             suggestion: "💡 Ensure all dataframes have identical column names and types".to_string(),
        //         });
        //     }
        // }

        let ctx = Arc::new(SessionContext::new());
        
        let mut all_batches = self.df.clone().collect().await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Collecting base dataframe".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if the dataframe is valid and not empty".to_string(),
        })?;
        
        for (i, other) in others.iter().enumerate() {
            let other_batches = other.df.clone().collect().await
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: format!("Collecting dataframe at index {}", i),
                    reason: e.to_string(),
                    suggestion: "💡 Check if the dataframe is valid and not empty".to_string(),
                })?;
            all_batches.extend(other_batches);
        }
    
        let mem_table = MemTable::try_new(self.df.schema().clone().into(), vec![all_batches])
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Creating memory table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Verify data consistency and memory availability".to_string(),
        })?;
    
        let alias = "union_many_result";

        ctx.register_table(alias, Arc::new(mem_table))
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Registering result table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if table name is unique in context".to_string(),
        })?;
    
        let df = ctx.table(alias).await
        .map_err(|e| ElusionError::SetOperationError {
            operation: "APPEND MANY".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Verify final table creation".to_string(),
        })?;
    
        Ok(CustomDataFrame {
            df,
            table_alias: alias.to_string(),
            from_table: alias.to_string(),
            selected_columns: self.selected_columns.clone(),
            alias_map: self.alias_map.clone(),
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
            original_expressions: self.original_expressions.clone(),
        })
    }
    /// Performs UNION on two dataframes
    pub async fn union(self, other: CustomDataFrame) -> ElusionResult<Self> {

        // if self.df.schema() != other.df.schema() {
        //     return Err(ElusionError::SetOperationError {
        //         operation: "UNION".to_string(),
        //         reason: "Schema mismatch between dataframes".to_string(),
        //         suggestion: "💡 Ensure both dataframes have identical column names and types".to_string(),
        //     });
        // }

        let ctx = Arc::new(SessionContext::new());
        
        Self::register_df_as_table(&ctx, &self.table_alias, &self.df).await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Registering first table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if table name is unique and data is valid".to_string(),
        })?;

        Self::register_df_as_table(&ctx, &other.table_alias, &other.df).await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Registering second table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if table name is unique and data is valid".to_string(),
        })?;
    
        
        let sql = format!(
            "SELECT DISTINCT * FROM {} UNION SELECT DISTINCT * FROM {}",
            normalize_alias(&self.table_alias),
            normalize_alias(&other.table_alias)
        );

        let df = ctx.sql(&sql).await
        .map_err(|e| ElusionError::SetOperationError {
            operation: "UNION".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Verify SQL syntax and schema compatibility".to_string(),
        })?;
    
        Ok(CustomDataFrame {
            df,
            table_alias: "union_result".to_string(),
            from_table: "union_result".to_string(),
            selected_columns: self.selected_columns.clone(),
            alias_map: self.alias_map.clone(),
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
            original_expressions: self.original_expressions.clone(),
        })
    }
    /// Performs UNION on multiple dataframes
    pub async fn union_many<const N: usize>(self, others: [CustomDataFrame; N]) -> ElusionResult<Self> {

        if N == 0 {
            return Err(ElusionError::SetOperationError {
                operation: "UNION MANY".to_string(),
                reason: "No dataframes provided for union operation".to_string(),
                suggestion: "💡 Provide at least one dataframe to union with".to_string(),
            });
        }

        // for (i, other) in others.iter().enumerate() {
        //     if self.df.schema() != other.df.schema() {
        //         return Err(ElusionError::SetOperationError {
        //             operation: "UNION MANY".to_string(),
        //             reason: format!("Schema mismatch with dataframe at index {}", i),
        //             suggestion: "💡 Ensure all dataframes have identical column names and types".to_string(),
        //         });
        //     }
        // }

        let ctx = Arc::new(SessionContext::new());
        
        Self::register_df_as_table(&ctx, &self.table_alias, &self.df).await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Registering base table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if table name is unique and data is valid".to_string(),
        })?;
        
        for (i, other) in others.iter().enumerate() {
            let alias = format!("union_source_{}", i);
            Self::register_df_as_table(&ctx, &alias, &other.df).await
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: format!("Registering table {}", i),
                    reason: e.to_string(),
                    suggestion: "💡 Check if table name is unique and data is valid".to_string(),
                })?;
        }
        
        let mut sql = format!("SELECT DISTINCT * FROM {}", normalize_alias(&self.table_alias));
        for i in 0..N {
            sql.push_str(&format!(" UNION SELECT DISTINCT * FROM {}", 
                normalize_alias(&format!("union_source_{}", i))));
        }
    
        let df = ctx.sql(&sql).await
        .map_err(|e| ElusionError::SetOperationError {
            operation: "UNION MANY".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Verify SQL syntax and schema compatibility".to_string(),
        })?;
    
        Ok(CustomDataFrame {
            df,
            table_alias: "union_many_result".to_string(),
            from_table: "union_many_result".to_string(),
            selected_columns: self.selected_columns.clone(),
            alias_map: self.alias_map.clone(),
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
            original_expressions: self.original_expressions.clone(),
        })
    }

    /// Performs UNION_ALL  on two dataframes
    pub async fn union_all(self, other: CustomDataFrame) -> ElusionResult<Self> {

        // if self.df.schema() != other.df.schema() {
        //     return Err(ElusionError::SetOperationError {
        //         operation: "UNION ALL".to_string(),
        //         reason: "Schema mismatch between dataframes".to_string(),
        //         suggestion: "💡 Ensure both dataframes have identical column names and types".to_string(),
        //     });
        // }

        let ctx = Arc::new(SessionContext::new());
        
        Self::register_df_as_table(&ctx, &self.table_alias, &self.df).await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Registering first table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if table name is unique and data is valid".to_string(),
        })?;

        Self::register_df_as_table(&ctx, &other.table_alias, &other.df).await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Registering second table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if table name is unique and data is valid".to_string(),
        })?;
        
        let sql = format!(
            "SELECT * FROM {} UNION ALL SELECT * FROM {}",
            normalize_alias(&self.table_alias),
            normalize_alias(&other.table_alias)
        );
    
        let df = ctx.sql(&sql).await
        .map_err(|e| ElusionError::SetOperationError {
            operation: "UNION ALL".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Verify SQL syntax and schema compatibility".to_string(),
        })?;
    
        Ok(CustomDataFrame {
            df,
            table_alias: "union_all_result".to_string(),
            from_table: "union_all_result".to_string(),
            selected_columns: self.selected_columns.clone(),
            alias_map: self.alias_map.clone(),
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
            original_expressions: self.original_expressions.clone(),
        })
    }
    /// Performs UNIONA_ALL on multiple dataframes
    pub async fn union_all_many<const N: usize>(self, others: [CustomDataFrame; N]) -> ElusionResult<Self> {

        if N == 0 {
            return Err(ElusionError::SetOperationError {
                operation: "UNION ALL MANY".to_string(),
                reason: "No dataframes provided for union operation".to_string(),
                suggestion: "💡 Provide at least one dataframe to union with".to_string(),
            });
        }

        // for (i, other) in others.iter().enumerate() {
        //     if self.df.schema() != other.df.schema() {
        //         return Err(ElusionError::SetOperationError {
        //             operation: "UNION ALL MANY".to_string(),
        //             reason: format!("Schema mismatch with dataframe at index {}", i),
        //             suggestion: "💡 Ensure all dataframes have identical column names and types".to_string(),
        //         });
        //     }
        // }

        let ctx = Arc::new(SessionContext::new());
        
        Self::register_df_as_table(&ctx, &self.table_alias, &self.df).await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Registering base table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if table name is unique and data is valid".to_string(),
        })?;
        
        for (i, other) in others.iter().enumerate() {
            let alias = format!("union_all_source_{}", i);
            Self::register_df_as_table(&ctx, &alias, &other.df).await
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: format!("Registering table {}", i),
                    reason: e.to_string(),
                    suggestion: "💡 Check if table name is unique and data is valid".to_string(),
                })?;
        }
        
        let mut sql = format!("SELECT * FROM {}", normalize_alias(&self.table_alias));
        for i in 0..N {
            sql.push_str(&format!(" UNION ALL SELECT * FROM {}", 
                normalize_alias(&format!("union_all_source_{}", i))));
        }
    
        let df = ctx.sql(&sql).await
        .map_err(|e| ElusionError::SetOperationError {
            operation: "UNION ALL MANY".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Verify SQL syntax and schema compatibility".to_string(),
        })?;
    
        Ok(CustomDataFrame {
            df,
            table_alias: "union_all_many_result".to_string(),
            from_table: "union_all_many_result".to_string(),
            selected_columns: self.selected_columns.clone(),
            alias_map: self.alias_map.clone(),
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
            original_expressions: self.original_expressions.clone(),
        })
    }
    /// Performs EXCEPT on two dataframes
    pub async fn except(self, other: CustomDataFrame) -> ElusionResult<Self> {

        // if self.df.schema() != other.df.schema() {
        //     return Err(ElusionError::SetOperationError {
        //         operation: "EXCEPT".to_string(),
        //         reason: "Schema mismatch between dataframes".to_string(),
        //         suggestion: "💡 Ensure both dataframes have identical column names and types".to_string(),
        //     });
        // }

        let ctx = Arc::new(SessionContext::new());
        
         Self::register_df_as_table(&ctx, &self.table_alias, &self.df).await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Registering first table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if table name is unique and data is valid".to_string(),
        })?;

        Self::register_df_as_table(&ctx, &other.table_alias, &other.df).await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Registering second table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if table name is unique and data is valid".to_string(),
        })?;
        
        let sql = format!(
            "SELECT * FROM {} EXCEPT SELECT * FROM {}",
            normalize_alias(&self.table_alias),
            normalize_alias(&other.table_alias)
        );
    
        let df = ctx.sql(&sql).await
        .map_err(|e| ElusionError::SetOperationError {
            operation: "EXCEPT".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Verify SQL syntax and schema compatibility".to_string(),
        })?;
    
        Ok(CustomDataFrame {
            df,
            table_alias: "except_result".to_string(),
            from_table: "except_result".to_string(),
            selected_columns: self.selected_columns.clone(),
            alias_map: self.alias_map.clone(),
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
            original_expressions: self.original_expressions.clone(),
        })
    }
    
    /// Performs INTERSECT on two dataframes
    pub async fn intersect(self, other: CustomDataFrame) -> ElusionResult<Self> {

        // if self.df.schema() != other.df.schema() {
        //     return Err(ElusionError::SetOperationError {
        //         operation: "INTERSECT".to_string(),
        //         reason: "Schema mismatch between dataframes".to_string(),
        //         suggestion: "💡 Ensure both dataframes have identical column names and types".to_string(),
        //     });
        // }

        let ctx = Arc::new(SessionContext::new());
        
        Self::register_df_as_table(&ctx, &self.table_alias, &self.df).await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Registering first table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if table name is unique and data is valid".to_string(),
        })?;

        Self::register_df_as_table(&ctx, &other.table_alias, &other.df).await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Registering second table".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check if table name is unique and data is valid".to_string(),
        })?;
        
        let sql = format!(
            "SELECT * FROM {} INTERSECT SELECT * FROM {}",
            normalize_alias(&self.table_alias),
            normalize_alias(&other.table_alias)
        );
    
        let df = ctx.sql(&sql).await
        .map_err(|e| ElusionError::SetOperationError {
            operation: "INTERSECT".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Verify SQL syntax and schema compatibility".to_string(),
        })?;
    
        Ok(CustomDataFrame {
            df,
            table_alias: "intersect_result".to_string(),
            from_table: "intersect_result".to_string(),
            selected_columns: self.selected_columns.clone(),
            alias_map: self.alias_map.clone(),
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
            original_expressions: self.original_expressions.clone(),
        })
    }
    
    /// Pivot the DataFrame
    pub async fn pivot<const N: usize>(
        mut self,
        row_keys: [&str; N],
        pivot_column: &str,
        value_column: &str,
        aggregate_func: &str,
    ) -> ElusionResult<Self> {
        let ctx = Arc::new(SessionContext::new());
        
        // current DataFrame
        Self::register_df_as_table(&ctx, &self.table_alias, &self.df).await?;

        let schema = self.df.schema();
        // println!("Current DataFrame schema fields: {:?}", 
        //     schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());

        // Find columns in current schema
        let exact_pivot_column = schema.fields().iter()
            .find(|f| f.name().to_uppercase() == pivot_column.to_uppercase())
            .ok_or_else(|| {
                let available = schema.fields().iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>();
                ElusionError::Custom(format!(
                    "Column {} not found in current data. Available columns: {:?}", 
                    pivot_column, available
                ))
            })?
            .name();
            
        let exact_value_column = schema.fields().iter()
            .find(|f| f.name().to_uppercase() == value_column.to_uppercase())
            .ok_or_else(|| {
                let available = schema.fields().iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>();
                ElusionError::Custom(format!(
                    "Column {} not found in current data. Available columns: {:?}", 
                    value_column, available
                ))
            })?
            .name();

        // let distinct_query = format!(
        //     "SELECT DISTINCT \"{}\" \
        //      FROM \"{}\" AS {} \
        //      WHERE \"{}\" IS NOT NULL \
        //      AND \"{}\" IS NOT NULL \
        //      GROUP BY \"{}\" \
        //      HAVING {}(\"{}\") > 0", 
        //     exact_pivot_column,
        //     self.from_table,
        //     self.table_alias,
        //     exact_pivot_column,
        //     exact_value_column,
        //     exact_pivot_column,
        //     aggregate_func,
        //     exact_value_column
        // );
        let distinct_query = format!(
            "SELECT DISTINCT \"{}\" \
             FROM \"{}\" AS {} \
             WHERE \"{}\" IS NOT NULL \
             AND \"{}\" IS NOT NULL \
             ORDER BY \"{}\"",
            exact_pivot_column,
            self.from_table,
            self.table_alias,
            exact_pivot_column,
            exact_value_column,
            exact_pivot_column
        );

        let distinct_df = ctx.sql(&distinct_query).await
            .map_err(|e| ElusionError::Custom(format!("Failed to execute distinct query: {}", e)))?;
        
        let distinct_batches = distinct_df.collect().await
            .map_err(|e| ElusionError::Custom(format!("Failed to collect distinct values: {}", e)))?;

        // Extract distinct values into a Vec<String>
        let distinct_values: Vec<String> = distinct_batches
            .iter()
            .flat_map(|batch| {
                let array = batch.column(0);
                match array.data_type() {
                    ArrowDataType::Utf8 => {
                        let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                        (0..batch.num_rows())
                            .map(|i| string_array.value(i).to_string())
                            .collect::<Vec<_>>()
                    },
                    _ => {
                        // For non-string types, convert to string representation
                        let string_array = compute::cast(array, &ArrowDataType::Utf8)
                            .unwrap();
                        let string_array = string_array.as_any().downcast_ref::<StringArray>().unwrap();
                        (0..batch.num_rows())
                            .map(|i| string_array.value(i).to_string())
                            .collect::<Vec<_>>()
                    }
                }
            })
            .collect();

        // Create pivot columns for each distinct value
        let pivot_cols: Vec<String> = distinct_values
            .iter()
            .map(|val| {
                // Generate pivoted column expression
                let value_expr = if schema.field_with_name(None, &exact_pivot_column)
                    .map(|f| matches!(f.data_type(), ArrowDataType::Int32 | ArrowDataType::Int64 | ArrowDataType::Float32 | ArrowDataType::Float64))
                    .unwrap_or(false) {
                    // Numeric comparison without quotes
                    format!(
                        "COALESCE({}(CASE WHEN \"{}\" = '{}' THEN \"{}\" END), 0)",
                        aggregate_func,
                        exact_pivot_column,
                        val,
                        exact_value_column
                    )
                } else {
                    // String comparison with quotes
                    format!(
                        "COALESCE({}(CASE WHEN \"{}\" = '{}' THEN \"{}\" END), 0)",
                        aggregate_func,
                        exact_pivot_column,
                        val.replace("'", "''"),  // Escape single quotes
                        exact_value_column
                    )
                };

                // Format the full column expression with alias
                format!(
                    "{} AS \"{}_{}\"",
                    value_expr,
                    exact_pivot_column,
                    val.replace("\"", "\"\"")  // Escape double quotes in alias
                )
            })
            .collect();

            let row_keys_str = row_keys.iter()
            .map(|&key| {
                let exact_key = schema.fields().iter()
                    .find(|f| f.name().to_uppercase() == key.to_uppercase())
                    .map_or(key.to_string(), |f| f.name().to_string());
                format!("\"{}\"", exact_key)
            })
            .collect::<Vec<_>>()
            .join(", ");
        
        // Create the final pivot query
        let pivot_subquery = format!(
            "(SELECT {}, {} FROM \"{}\" AS {} GROUP BY {})",
            row_keys_str,
            pivot_cols.join(", "),
            self.from_table,
            self.table_alias,
            row_keys_str
        );

        // Update the DataFrame state
        self.from_table = pivot_subquery;
        self.selected_columns.clear();
        self.group_by_columns.clear();
        
        // Add row keys to selected columns
        self.selected_columns.extend(row_keys.iter().map(|&s| s.to_string()));
        
        // Add pivot columns to selected columns
        for val in distinct_values {
            self.selected_columns.push(
                format!("{}_{}",
                    normalize_column_name(pivot_column),
                    normalize_column_name(&val)
                )
            );
        }

        

        Ok(self)
    }

    /// Unpivot the DataFrame (melt operation)
    pub async fn unpivot<const N: usize, const M: usize>(
        mut self,
        id_columns: [&str; N],
        value_columns: [&str; M],
        name_column: &str,
        value_column: &str,
    ) -> ElusionResult<Self> {
        let ctx = Arc::new(SessionContext::new());
        
        // Register the current DataFrame
        Self::register_df_as_table(&ctx, &self.table_alias, &self.df).await?;
    
        let schema = self.df.schema();
        // println!("Current DataFrame schema fields: {:?}", 
        //     schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>());
    
        // Find exact id columns from schema
        let exact_id_columns: Vec<String> = id_columns.iter()
            .map(|&id| {
                schema.fields().iter()
                    .find(|f| f.name().to_uppercase() == id.to_uppercase())
                    .map(|f| f.name().to_string())
                    .ok_or_else(|| {
                        let available = schema.fields().iter()
                            .map(|f| f.name())
                            .collect::<Vec<_>>();
                        ElusionError::Custom(format!(
                            "ID column '{}' not found in current data. Available columns: {:?}",
                            id, available
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
    
        // Find exact value columns from schema
        let exact_value_columns: Vec<String> = value_columns.iter()
            .map(|&val| {
                schema.fields().iter()
                    .find(|f| f.name().to_uppercase() == val.to_uppercase())
                    .map(|f| f.name().to_string())
                    .ok_or_else(|| {
                        let available = schema.fields().iter()
                            .map(|f| f.name())
                            .collect::<Vec<_>>();
                        ElusionError::Custom(format!(
                            "Value column '{}' not found in current data. Available columns: {:?}",
                            val, available
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
    
        // Create individual SELECT statements for each value column
        let selects: Vec<String> = exact_value_columns.iter().map(|val_col| {
            let id_cols_str = exact_id_columns.iter()
                .map(|id| format!("\"{}\"", id))
                .collect::<Vec<_>>()
                .join(", ");
    
            //  final part of the value column for the label
            // let label = if let Some(pos) = val_col.rfind('_') {
            //     &val_col[pos + 1..]
            // } else {
            //     val_col
            // };
            
            format!(
                "SELECT {}, '{}' AS \"{}\", \"{}\" AS \"{}\" FROM \"{}\" AS {}",
                id_cols_str,
                val_col,
                // label,
                name_column,
                val_col,
                value_column,
                self.from_table,
                self.table_alias
            )
        }).collect();
    
        // Combine all SELECT statements with UNION ALL
        let unpivot_subquery = format!(
            "({})",
            selects.join(" UNION ALL ")
        );
    
        // Update the DataFrame state
        self.from_table = unpivot_subquery;
        self.selected_columns.clear();
        
        // Add identifier columns with proper quoting
        self.selected_columns.extend(
            exact_id_columns.iter()
                .map(|id| format!("\"{}\"", id))
        );
        self.selected_columns.push(format!("\"{}\"", name_column));
        self.selected_columns.push(format!("\"{}\"", value_column));
    
        Ok(self)
    }

    /// Fill down null values (deferred execution - follows select/filter pattern)
    pub fn fill_down<const N: usize>(
        mut self, 
        columns: [&str; N]
    ) -> Self {
        let normalized_columns: Vec<String> = columns
            .iter()
            .map(|col| {
                col.trim().replace(" ", "_").to_lowercase()
            })
            .collect();
        
     //   println!("Debug: Adding fill_down for columns: {:?} -> normalized: {:?}", columns, normalized_columns);
        
        let operation = format!("FILL_DOWN:{}", normalized_columns.join(","));
        self.set_operations.push(operation);
        self
    }

    /// Alternative implementation using set_operations if you prefer that pattern
    pub fn fill_down_with_set_ops<const N: usize>(
        mut self, 
        columns: [&str; N]
    ) -> Self {
        let operation = format!("FILL_DOWN:{}", columns.join(","));
        self.set_operations.push(operation);
        self
    }

    /// Fill down null values (immediate execution - follows append/union pattern)  
    pub async fn fill_down_now<const N: usize>(
        self,
        columns: [&str; N], 
        alias: &str
    ) -> ElusionResult<CustomDataFrame> {
        self.fill_down_vec_now(columns.to_vec(), alias).await
    }

    pub async fn fill_down_vec_now(
        self,
        columns: Vec<&str>,
        alias: &str  
    ) -> ElusionResult<CustomDataFrame> {
        let ctx = SessionContext::new();
        
        // Register current DataFrame
        Self::register_df_as_table(&ctx, &self.table_alias, &self.df).await?;
        
        // Get all data first, then process it manually since DataFusion window functions don't work well
        let all_data_sql = format!("SELECT * FROM {}", normalize_alias(&self.table_alias));
        let temp_df = ctx.sql(&all_data_sql).await?;
        let batches = temp_df.clone().collect().await?;
        
        if batches.is_empty() {
            return Ok(CustomDataFrame {
                df: temp_df,
                table_alias: alias.to_string(),
                from_table: alias.to_string(),
                selected_columns: Vec::new(),
                alias_map: self.alias_map,
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
                query: all_data_sql,
                aggregated_df: None,
                union_tables: None,
                original_expressions: Vec::new(),
            });
        }
        
        // Process each batch to implement fill down manually
        let schema = batches[0].schema();
        let mut processed_batches = Vec::new();
        
        // Find column indices for fill down columns
        let fill_column_indices: Vec<usize> = columns
            .iter()
            .filter_map(|col_name| {
                schema.fields().iter().position(|field| field.name() == col_name)
            })
            .collect();
        
      //  println!("Debug: Fill column indices: {:?} for columns: {:?}", fill_column_indices, columns);
        
        for batch in batches {
            let mut new_columns = Vec::new();
            let mut fill_values: Vec<Option<String>> = vec![None; fill_column_indices.len()];
            
            // Process each column
            for (col_idx, _field) in schema.fields().iter().enumerate() {
                let array = batch.column(col_idx);
                
                if let Some(fill_idx) = fill_column_indices.iter().position(|&idx| idx == col_idx) {
                    // This is a fill-down column - process it
                    let string_array = array.as_any().downcast_ref::<arrow::array::StringArray>()
                        .ok_or_else(|| ElusionError::Custom("Expected string array".to_string()))?;
                    
                    let mut new_values = Vec::new();
                    for i in 0..string_array.len() {
                        // Fix the type error - string_array.value(i) returns &str, not Option<&str>
                        if string_array.is_null(i) {
                            new_values.push(fill_values[fill_idx].clone());
                        } else {
                            let value = string_array.value(i);
                            if !value.trim().is_empty() {
                                fill_values[fill_idx] = Some(value.to_string());
                                new_values.push(Some(value.to_string()));
                            } else {
                                new_values.push(fill_values[fill_idx].clone());
                            }
                        }
                    }
                    
                    let new_array = arrow::array::StringArray::from(new_values);
                    new_columns.push(Arc::new(new_array) as ArrayRef);
                } else {
                    // Regular column - keep as is
                    new_columns.push(array.clone());
                }
            }
            
            // Fix the error conversion issue
            let new_batch = RecordBatch::try_new(schema.clone(), new_columns)
                .map_err(|e| ElusionError::Custom(format!("Failed to create record batch: {}", e)))?;
            processed_batches.push(new_batch);
        }
        
        // Create new DataFrame from processed batches
        let result_mem_table = MemTable::try_new(schema.clone().into(), vec![processed_batches])
            .map_err(|e| ElusionError::Custom(format!("Failed to create mem table: {}", e)))?;
        ctx.register_table(alias, Arc::new(result_mem_table))
            .map_err(|e| ElusionError::Custom(format!("Failed to register table: {}", e)))?;
        let result_df = ctx.table(alias).await
            .map_err(|e| ElusionError::Custom(format!("Failed to get table: {}", e)))?;
        
        Ok(CustomDataFrame {
            df: result_df,
            table_alias: alias.to_string(),
            from_table: alias.to_string(),
            selected_columns: Vec::new(),
            alias_map: self.alias_map,
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
            query: format!("-- Manual fill down processing for columns: {:?}", columns),
            aggregated_df: None,
            union_tables: None,
            original_expressions: Vec::new(),
        })
    }

    /// Handle various set operations including FILL_DOWN
    fn handle_set_operation(&self, operation: &str, base_sql: String) -> String {
        if let Some(columns_str) = operation.strip_prefix("FILL_DOWN:") {
            self.handle_fill_down_operation(columns_str, base_sql)
        } else if operation.starts_with("UNION") {
            // Handle other set operations like UNION if needed in the future
            base_sql
        } else {
            base_sql
        }
    }

    /// Handle the FILL_DOWN operation by wrapping the base SQL in a CTE
    fn handle_fill_down_operation(&self, columns_str: &str, base_sql: String) -> String {
        let columns: Vec<&str> = columns_str.split(',').collect();
      //  println!("Debug: handle_fill_down_operation called with columns: {:?}", columns);
        
        let selected_cols = if self.selected_columns.is_empty() {
            self.df.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
        } else {
            self.selected_columns
                .iter()
                .map(|col| {
                    if col.contains(" AS ") {
                        col.split(" AS ")
                            .nth(1)
                            .unwrap_or(col)
                            .trim_matches('"')
                            .trim()
                            .to_string()
                    } else {
                        col.trim_matches('"')
                            .split('.')
                            .last()
                            .unwrap_or(col)
                            .trim_matches('"')
                            .to_string()
                    }
                })
                .collect()
        };
        
      //  println!("Debug: selected_cols after processing: {:?}", selected_cols);
      //  println!("Debug: columns to fill: {:?}", columns);
        
        // Handle both NULL and string "null" values by converting them to actual NULLs first
        let fill_expressions: Vec<String> = selected_cols
            .iter()
            .map(|col_name| {
                if columns.contains(&col_name.as_str()) {
                   // println!("Debug: Processing fill_down for column: {}", col_name);
                    // First convert "null" strings and empty strings to actual NULLs,
                    // then use LAST_VALUE with IGNORE NULLS
                    format!(
                        r#"LAST_VALUE(
                            CASE 
                                WHEN "{0}" IS NULL OR TRIM("{0}") = '' OR TRIM("{0}") = 'null' THEN NULL
                                ELSE "{0}"
                            END
                        ) IGNORE NULLS OVER (
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) AS "{0}""#,
                        col_name
                    )
                } else {
                    //println!("Debug: NOT processing fill_down for column: {} (not in fill list)", col_name);
                    format!(r#""{0}""#, col_name)
                }
            })
            .collect();
        
        let result_sql = format!(
            r#"WITH fill_down_base AS (
                {}
            )
            SELECT {} 
            FROM fill_down_base"#,
            base_sql,
            fill_expressions.join(", ")
        );
        
      //  println!("Debug: Final SQL: {}", result_sql);
        result_sql
    }

    /// SELECT clause using const generics
    pub fn select<const N: usize>(self, columns: [&str; N]) -> Self {
        self.select_vec(columns.to_vec())
    }

    /// Add selected columns to the SELECT clause using a Vec<&str>
    pub fn select_vec(mut self, columns: Vec<&str>) -> Self {
        // Store original expressions with AS clauses
        self.original_expressions = columns
            .iter()
            .filter(|&col| col.contains(" AS "))
            .map(|&s| s.to_string())
            .collect();
    
        // Instead of replacing selected_columns, merge with existing ones
        let mut all_columns = self.selected_columns.clone();
        
        if !self.group_by_columns.is_empty() {
            for col in columns {
                if is_expression(col) {
                    if is_aggregate_expression(col) {
                        all_columns.push(normalize_expression(col, &self.table_alias));
                    } else {
                        self.group_by_columns.push(col.to_string());
                        all_columns.push(normalize_expression(col, &self.table_alias));
                    }
                } else {
                    let normalized_col = normalize_column_name(col);
                    if self.group_by_columns.contains(&normalized_col) {
                        all_columns.push(normalized_col);
                    } else {
                        self.group_by_columns.push(normalized_col.clone());
                        all_columns.push(normalized_col);
                    }
                }
            }
        } else {
            // Handle non-GROUP BY case
            let aggregate_aliases: Vec<String> = self
                .aggregations
                .iter()
                .filter_map(|agg| {
                    agg.split(" AS ")
                        .nth(1)
                        .map(|alias| normalize_alias(alias))
                })
                .collect();
    
            all_columns.extend(
                columns
                    .into_iter()
                    .filter(|col| !aggregate_aliases.contains(&normalize_alias(col)))
                    .map(|s| {
                        if is_expression(s) {
                            normalize_expression(s, &self.table_alias)
                        } else {
                            normalize_column_name(s)
                        }
                    })
            );
        }
    
        // Remove duplicates while preserving order
        let mut seen = HashSet::new();
        self.selected_columns = all_columns
            .into_iter()
            .filter(|x| seen.insert(x.clone()))
            .collect();
    
        self
    }

    /// Extract JSON properties from a column containing JSON strings
    pub fn json<'a, const N: usize>(mut self, columns: [&'a str; N]) -> Self {
        let mut json_expressions = Vec::new();
        
        for expr in columns.iter() {
            // Parse the expression: "column.'$jsonPath' AS alias"
            // let parts: Vec<&str> = expr.split(" AS ").collect();
            let re = Regex::new(r"(?i)\s+AS\s+").unwrap();
            let parts: Vec<&str> = re.split(expr).collect();

            if parts.len() != 2 {
                continue; // skip invalid expressions, will be checked at .elusion() 
            }
            
            let path_part = parts[0].trim();
            let alias = parts[1].trim().to_lowercase();
            

            if !path_part.contains(".'$") {
                continue; // Skip invalid expressions
            }
            
            let col_path_parts: Vec<&str> = path_part.split(".'$").collect();
            let column_name = col_path_parts[0].trim();
            let json_path = col_path_parts[1].trim_end_matches('\'');
            
            let search_pattern = format!("\"{}\":", json_path);
            
            let sql_expr = format!(
                "CASE 
                    WHEN POSITION('{}' IN {}) > 0 THEN
                        TRIM(BOTH '\"' FROM 
                            SUBSTRING(
                                {}, 
                                POSITION('{}' IN {}) + {}, 
                                CASE
                                    WHEN POSITION(',\"' IN SUBSTRING({}, POSITION('{}' IN {}) + {})) > 0 THEN
                                        POSITION(',\"' IN SUBSTRING({}, POSITION('{}' IN {}) + {})) - 1
                                    WHEN POSITION('}}' IN SUBSTRING({}, POSITION('{}' IN {}) + {})) > 0 THEN
                                        POSITION('}}' IN SUBSTRING({}, POSITION('{}' IN {}) + {})) - 1
                                    ELSE 300 -- arbitrary large value
                                END
                            )
                        )
                    ELSE NULL
                 END as \"{}\"",
                search_pattern, column_name,
                column_name, 
                search_pattern, column_name, search_pattern.len(),
                column_name, search_pattern, column_name, search_pattern.len(),
                column_name, search_pattern, column_name, search_pattern.len(),
                column_name, search_pattern, column_name, search_pattern.len(),
                column_name, search_pattern, column_name, search_pattern.len(),
                alias
            );
            
            json_expressions.push(sql_expr);
        }

        self.selected_columns.extend(json_expressions);
        
        self
    }

    /// Extract values from JSON array objects using regexp_like and string functions
    pub fn json_array<'a, const N: usize>(mut self, columns: [&'a str; N]) -> Self {
        let mut json_expressions = Vec::new();
        
        for expr in columns.iter() {
            // Parse the expression: "column.'$ValueField:IdField=IdValue' AS alias"
            // let parts: Vec<&str> = expr.split(" AS ").collect();
            let re = Regex::new(r"(?i)\s+AS\s+").unwrap();
            let parts: Vec<&str> = re.split(expr).collect();

            if parts.len() != 2 {
                continue; // skip invalid expressions
            }
            
            let path_part = parts[0].trim();
            let alias = parts[1].trim().to_lowercase();
            
            if !path_part.contains(".'$") {
                continue; // Skip invalid expressions
            }
            
            let col_path_parts: Vec<&str> = path_part.split(".'$").collect();
            let column_name = col_path_parts[0].trim();
            let filter_expr = col_path_parts[1].trim_end_matches('\'');
         
            let filter_parts: Vec<&str> = filter_expr.split(':').collect();
            
            let sql_expr: String;
            
            if filter_parts.len() == 2 {
                // Format: "column.'$ValueField:IdField=IdValue' AS alias"
                let value_field = filter_parts[0].trim();
                let condition = filter_parts[1].trim();
                
                let condition_parts: Vec<&str> = condition.split('=').collect();
                if condition_parts.len() != 2 {
                    continue; // Skip invalid expressions
                }
                
                let id_field = condition_parts[0].trim();
                let id_value = condition_parts[1].trim();
      
                sql_expr = format!(
                    "CASE 
                        WHEN regexp_like({}, '\\{{\"{}\":\"{}\",[^\\}}]*\"{}\":(\"[^\"]*\"|[0-9.]+|true|false)', 'i') THEN
                            CASE
                                WHEN regexp_like(
                                    regexp_match(
                                        {},
                                        '\\{{\"{}\":\"{}\",[^\\}}]*\"{}\":(\"[^\"]*\")',
                                        'i'
                                    )[1],
                                    '\"[^\"]*\"'
                                ) THEN
                                    -- Handle string values by removing quotes
                                    regexp_replace(
                                        regexp_match(
                                            {},
                                            '\\{{\"{}\":\"{}\",[^\\}}]*\"{}\":\"([^\"]*)\"',
                                            'i'
                                        )[1],
                                        '\"',
                                        ''
                                    )
                                ELSE
                                    -- Handle numeric and boolean values
                                    regexp_match(
                                        {},
                                        '\\{{\"{}\":\"{}\",[^\\}}]*\"{}\":([0-9.]+|true|false)',
                                        'i'
                                    )[1]
                            END
                        ELSE NULL
                    END as \"{}\"",
                    column_name, id_field, id_value, value_field,
                    column_name, id_field, id_value, value_field,
                    column_name, id_field, id_value, value_field,
                    column_name, id_field, id_value, value_field,
                    alias
                );
            } else {
                continue; // Skip expressions that don't use the explicit format
            }
            
            json_expressions.push(sql_expr);
        }
    
        self.selected_columns.extend(json_expressions);
        
        self
    }

    /// Construct the SQL query based on the current state, including joins
    fn construct_sql(&self) -> String {
        let mut query = String::new();

        // WITH clause for CTEs
        if !self.ctes.is_empty() {
            query.push_str("WITH ");
            query.push_str(&self.ctes.join(", "));
            query.push_str(" ");
        }

        // Determine if it's a subquery with no selected columns
        let is_subquery = self.from_table.starts_with('(') && self.from_table.ends_with(')');
        let no_selected_columns = self.selected_columns.is_empty() && self.aggregations.is_empty() && self.window_functions.is_empty();

        if is_subquery && no_selected_columns {
            // Return only the subquery without wrapping it in SELECT * FROM
            query.push_str(&format!("{}", self.from_table));
        } else {
            // SELECT clause
            query.push_str("SELECT ");
            let mut select_parts = Vec::new();

            if !self.group_by_columns.is_empty() {
                // Add aggregations first
                select_parts.extend(self.aggregations.clone());

                // Add GROUP BY columns and selected columns
                for col in &self.selected_columns {
                    if !select_parts.contains(col) {
                        select_parts.push(col.clone());
                    }
                }
            } else {
                // No GROUP BY - add all parts
                select_parts.extend(self.aggregations.clone());
                select_parts.extend(self.selected_columns.clone());
            }

            // Add window functions last
            select_parts.extend(self.window_functions.clone());

            if select_parts.is_empty() {
                query.push_str("*");
            } else {
                query.push_str(&select_parts.join(", "));
            }

            // FROM clause
            query.push_str(" FROM ");
            if is_subquery {
                // It's a subquery; do not assign alias here
                query.push_str(&format!("{}", self.from_table));
            } else {
                // Regular table; quote as usual
                query.push_str(&format!(
                    "\"{}\" AS {}",
                    self.from_table.trim(),
                    self.table_alias
                ));
            }

            // Joins
            for join in &self.joins {
                query.push_str(&format!(
                    " {} JOIN \"{}\" AS {} ON {}",
                    join.join_type,
                    join.dataframe.from_table,
                    join.dataframe.table_alias,
                    join.condition
                ));
            }

            // WHERE clause
            if !self.where_conditions.is_empty() {
                query.push_str(" WHERE ");
                query.push_str(&self.where_conditions.join(" AND "));
            }

            // GROUP BY clause
            if !self.group_by_columns.is_empty() {
                query.push_str(" GROUP BY ");
                query.push_str(&self.group_by_columns.join(", "));
            }

            // HAVING clause
            if !self.having_conditions.is_empty() {
                query.push_str(" HAVING ");
                query.push_str(&self.having_conditions.join(" AND "));
            }

            // ORDER BY clause
            if !self.order_by_columns.is_empty() {
                query.push_str(" ORDER BY ");
                let orderings: Vec<String> = self.order_by_columns.iter()
                    .map(|(col, asc)| format!("{} {}", col, if *asc { "ASC" } else { "DESC" }))
                    .collect();
                query.push_str(&orderings.join(", "));
            }

            // LIMIT clause
            if let Some(limit) = self.limit_count {
                query.push_str(&format!(" LIMIT {}", limit));
            }
        }

        // set operations 
            let mut final_query = query;
            for operation in &self.set_operations {
                final_query = self.handle_set_operation(operation, final_query);
            }

        final_query
    }


    /// Execute the constructed SQL and return a new CustomDataFrame
    pub async fn elusion(&self, alias: &str) -> ElusionResult<Self> {
        let ctx = Arc::new(SessionContext::new());

        // Always register the base table first
        Self::register_df_as_table(&ctx, &self.table_alias, &self.df).await
        .map_err(|e| ElusionError::SchemaError {
            message: format!("Failed to register base table: {}", e),
            schema: Some(self.df.schema().to_string()),
            suggestion: "💡 Check table schema compatibility".to_string()
        })?;

        // For non-UNION queries, also register joined tables
        if self.union_tables.is_none() {
            for join in &self.joins {
                Self::register_df_as_table(&ctx, &join.dataframe.table_alias, &join.dataframe.df).await
                    .map_err(|e| ElusionError::JoinError {
                        message: format!("Failed to register joined table: {}", e),
                        left_table: self.table_alias.clone(),
                        right_table: join.dataframe.table_alias.clone(),
                        suggestion: "💡 Verify join table schemas are compatible".to_string()
                    })?;
            }
        }

        // For UNION queries with joins
        if let Some(tables) = &self.union_tables {
            for (table_alias, df, _) in tables {
                if ctx.table(table_alias).await.is_ok() {
                    continue;
                }
                Self::register_df_as_table(&ctx, table_alias, df).await
                    .map_err(|e| ElusionError::InvalidOperation {
                        operation: "Union Table Registration".to_string(),
                        reason: format!("Failed to register union table '{}': {}", table_alias, e),
                        suggestion: "💡 Check union table schema compatibility".to_string()
                    })?;
            }
        }

        let sql = if self.from_table.starts_with('(') && self.from_table.ends_with(')') {
            format!("SELECT * FROM {} AS {}", self.from_table, alias)
        } else {
            self.construct_sql()
        };

        // println!("Constructed SQL:\n{}", sql);
        
        // Execute the SQL query
        let df = ctx.sql(&sql).await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "SQL Execution".to_string(),
            reason: format!("Failed to execute SQL: {}", e),
            suggestion: "💡 Verify SQL syntax and table/column references".to_string()
        })?;

        // Collect the results into batches
        let batches = df.clone().collect().await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Data Collection".to_string(),
            reason: format!("Failed to collect results: {}", e),
            suggestion: "💡 Check if query returns valid data".to_string()
        })?;

        // Create a MemTable from the result batches
        let result_mem_table = MemTable::try_new(df.schema().clone().into(), vec![batches])
        .map_err(|e| ElusionError::SchemaError {
            message: format!("Failed to create result table: {}", e),
            schema: Some(df.schema().to_string()),
            suggestion: "💡 Verify result schema compatibility".to_string()
        })?;

        // Register the result as a new table with the provided alias
        ctx.register_table(alias, Arc::new(result_mem_table))
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Result Registration".to_string(),
            reason: format!("Failed to register result table: {}", e),
            suggestion: "💡 Try using a different alias name".to_string()
        })?;

        // Retrieve the newly registered table
        let result_df = ctx.table(alias).await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Result Retrieval".to_string(),
            reason: format!("Failed to retrieve final result: {}", e),
            suggestion: "💡 Check if result table was properly registered".to_string()
        })?;
        // Return a new CustomDataFrame with the new alias
        Ok(CustomDataFrame {
            df: result_df,
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
            query: sql,
            aggregated_df: Some(df.clone()),
            union_tables: None,
            original_expressions: self.original_expressions.clone(), 
        })
    }

    /// Display functions that display results to terminal
    pub async fn display(&self) -> ElusionResult<()> {
        self.df.clone().show().await.map_err(|e| 
            ElusionError::Custom(format!("Failed to display DataFrame: {}", e))
        )
    }
    /// DISPLAY Query Plan
    // pub fn display_query_plan(&self) {
    //     println!("Generated Logical Plan:");
    //     println!("{:?}", self.df.logical_plan());
    // }
    
    /// Displays the current schema for debugging purposes.
    // pub fn display_schema(&self) {
    //     println!("Current Schema for '{}': {:?}", self.table_alias, self.df.schema());
    // }

    /// Dipslaying query genereated from chained functions
    /// Displays the SQL query generated from the chained functions
    // pub fn display_query(&self) {
    //     let final_query = self.construct_sql();
    //     println!("Generated SQL Query: {}", final_query);
    // }


    // ================== STATISTICS FUNCS =================== //

    // helper functions for union
    fn find_actual_column_name(&self, column: &str) -> Option<String> {
        self.df
            .schema()
            .fields()
            .iter()
            .find(|f| f.name().to_lowercase() == column.to_lowercase())
            .map(|f| f.name().to_string())
    }
    /// Compute basic statistics for specified columns
    async fn compute_column_stats(&self, columns: &[&str]) -> ElusionResult<ColumnStats> {
        let mut stats = ColumnStats::default();
        let ctx = Arc::new(SessionContext::new());

        // Register the current dataframe as a temporary table
        Self::register_df_as_table(&ctx, &self.table_alias, &self.df).await?;

        for &column in columns {
            // Find the actual column name from schema
            let actual_column = self.find_actual_column_name(column)
                .ok_or_else(|| ElusionError::Custom(
                    format!("Column '{}' not found in schema", column)
                ))?;
            
            // Use the found column name in the normalized form
            let normalized_col = if actual_column.contains('.') {
                normalize_column_name(&actual_column)
            } else {
                normalize_column_name(&format!("{}.{}", self.table_alias, actual_column))
            };
    
            let sql = format!(
                "SELECT 
                    COUNT(*) as total_count,
                    COUNT({col}) as non_null_count,
                    AVG({col}::float) as mean,
                    MIN({col}) as min_value,
                    MAX({col}) as max_value,
                    STDDEV({col}::float) as std_dev
                FROM {}",
                normalize_alias(&self.table_alias),
                col = normalized_col
            );

            let result_df = ctx.sql(&sql).await.map_err(|e| {
                ElusionError::Custom(format!(
                    "Failed to compute statistics for column '{}': {}",
                    column, e
                ))
            })?;

            let batches = result_df.collect().await.map_err(ElusionError::DataFusion)?;
            
            if let Some(batch) = batches.first() {
                // Access columns directly instead of using row()
                let total_count = batch.column(0).as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| ElusionError::Custom("Failed to downcast total_count".to_string()))?
                    .value(0);
                
                let non_null_count = batch.column(1).as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| ElusionError::Custom("Failed to downcast non_null_count".to_string()))?
                    .value(0);
                
                let mean = batch.column(2).as_any().downcast_ref::<Float64Array>()
                    .ok_or_else(|| ElusionError::Custom("Failed to downcast mean".to_string()))?
                    .value(0);
                
                let min_value = ScalarValue::try_from_array(batch.column(3), 0)?;
                let max_value = ScalarValue::try_from_array(batch.column(4), 0)?;
                
                let std_dev = batch.column(5).as_any().downcast_ref::<Float64Array>()
                    .ok_or_else(|| ElusionError::Custom("Failed to downcast std_dev".to_string()))?
                    .value(0);

                stats.columns.push(ColumnStatistics {
                    name: column.to_string(),
                    total_count,
                    non_null_count,
                    mean: Some(mean),
                    min_value,
                    max_value,
                    std_dev: Some(std_dev),
                });
            }
        }

        Ok(stats)
    }

    /// Check for null values in specified columns
    async fn analyze_null_values(&self, columns: Option<&[&str]>) -> ElusionResult<NullAnalysis> {
        let ctx = Arc::new(SessionContext::new());
        Self::register_df_as_table(&ctx, &self.table_alias, &self.df).await?;

        let columns = match columns {
            Some(cols) => cols.to_vec(),
            None => {
                self.df
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect()
            }
        };

        let mut null_counts = Vec::new();
        for column in columns {
            // Find the actual column name from schema
            let actual_column = self.find_actual_column_name(column)
                .ok_or_else(|| ElusionError::Custom(
                    format!("Column '{}' not found in schema", column)
                ))?;
                
            // Use the found column name in the normalized form
            let normalized_col = if actual_column.contains('.') {
                normalize_column_name(&actual_column)
            } else {
                normalize_column_name(&format!("{}.{}", self.table_alias, actual_column))
            };
    
            let sql = format!(
                "SELECT 
                    '{}' as column_name,
                    COUNT(*) as total_rows,
                    COUNT(*) - COUNT({}) as null_count,
                    (COUNT(*) - COUNT({})) * 100.0 / COUNT(*) as null_percentage
                FROM {}",
                column, normalized_col, normalized_col, normalize_alias(&self.table_alias)
            );

            let result_df = ctx.sql(&sql).await.map_err(|e| {
                ElusionError::Custom(format!(
                    "Failed to analyze null values for column '{}': {}",
                    column, e
                ))
            })?;

            let batches = result_df.collect().await.map_err(ElusionError::DataFusion)?;
            
            if let Some(batch) = batches.first() {
                let column_name = batch.column(0).as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| ElusionError::Custom("Failed to downcast column_name".to_string()))?
                    .value(0);

                let total_rows = batch.column(1).as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| ElusionError::Custom("Failed to downcast total_rows".to_string()))?
                    .value(0);

                let null_count = batch.column(2).as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| ElusionError::Custom("Failed to downcast null_count".to_string()))?
                    .value(0);

                let null_percentage = batch.column(3).as_any().downcast_ref::<Float64Array>()
                    .ok_or_else(|| ElusionError::Custom("Failed to downcast null_percentage".to_string()))?
                    .value(0);

                null_counts.push(NullCount {
                    column_name: column_name.to_string(),
                    total_rows,
                    null_count,
                    null_percentage,
                });
            }
        }

        Ok(NullAnalysis { counts: null_counts })
    }

    /// Compute correlation between two numeric columns
    async fn compute_correlation(&self, col1: &str, col2: &str) -> ElusionResult<f64> {
        let ctx = Arc::new(SessionContext::new());
        Self::register_df_as_table(&ctx, &self.table_alias, &self.df).await?;

        let actual_col1 = self.find_actual_column_name(col1)
        .ok_or_else(|| ElusionError::Custom(
            format!("Column '{}' not found in schema", col1)
        ))?;

        let actual_col2 = self.find_actual_column_name(col2)
            .ok_or_else(|| ElusionError::Custom(
                format!("Column '{}' not found in schema", col2)
            ))?;
        
        // Use the found column names in normalized form
        let normalized_col1 = if actual_col1.contains('.') {
            normalize_column_name(&actual_col1)
        } else {
            normalize_column_name(&format!("{}.{}", self.table_alias, actual_col1))
        };
        
        let normalized_col2 = if actual_col2.contains('.') {
            normalize_column_name(&actual_col2)
        } else {
            normalize_column_name(&format!("{}.{}", self.table_alias, actual_col2))
        };
        
        let sql = format!(
            "SELECT corr({}::float, {}::float) as correlation 
            FROM {}",
            normalized_col1, normalized_col2, normalize_alias(&self.table_alias)
        );

        let result_df = ctx.sql(&sql).await.map_err(|e| {
            ElusionError::Custom(format!(
                "Failed to compute correlation between '{}' and '{}': {}",
                col1, col2, e
            ))
        })?;

        let batches = result_df.collect().await.map_err(ElusionError::DataFusion)?;
        
        if let Some(batch) = batches.first() {
            if let Some(array) = batch.column(0).as_any().downcast_ref::<Float64Array>() {
                if !array.is_null(0) {
                    return Ok(array.value(0));
                }
            }
        }

        Ok(0.0) // Return 0 if no correlation could be computed
    }


     /// Display statistical summary of specified columns
    pub async fn display_stats(&self, columns: &[&str]) -> ElusionResult<()> {
        let stats = self.compute_column_stats(columns).await?;
        
        println!("\n=== Column Statistics ===");
        println!("{:-<80}", "");
        
        for col_stat in stats.columns {
            println!("Column: {}", col_stat.name);
            println!("{:-<80}", "");
            println!("| {:<20} | {:>15} | {:>15} | {:>15} |", 
                "Metric", "Value", "Min", "Max");
            println!("{:-<80}", "");
            
            println!("| {:<20} | {:>15} | {:<15} | {:<15} |", 
                "Records", 
                col_stat.total_count,
                "-",
                "-");
                
            println!("| {:<20} | {:>15} | {:<15} | {:<15} |", 
                "Non-null Records", 
                col_stat.non_null_count,
                "-",
                "-");
                
            if let Some(mean) = col_stat.mean {
                println!("| {:<20} | {:>15.2} | {:<15} | {:<15} |", 
                    "Mean", 
                    mean,
                    "-",
                    "-");
            }
            
            if let Some(std_dev) = col_stat.std_dev {
                println!("| {:<20} | {:>15.2} | {:<15} | {:<15} |", 
                    "Standard Dev", 
                    std_dev,
                    "-",
                    "-");
            }
            
            println!("| {:<20} | {:>15} | {:<15} | {:<15} |", 
                "Value Range", 
                "-",
                format!("{}", col_stat.min_value),
                format!("{}", col_stat.max_value));
                
            println!("{:-<80}\n", "");
        }
        Ok(())
    }

    /// Display null value analysis
    pub async fn display_null_analysis(&self, columns: Option<&[&str]>) -> ElusionResult<()> {
        let analysis = self.analyze_null_values(columns).await?;
        
        println!("\n=== Null Value Analysis ===");
        println!("{:-<90}", "");
        println!("| {:<30} | {:>15} | {:>15} | {:>15} |", 
            "Column", "Total Rows", "Null Count", "Null Percentage");
        println!("{:-<90}", "");
        
        for count in analysis.counts {
            println!("| {:<30} | {:>15} | {:>15} | {:>14.2}% |", 
                count.column_name,
                count.total_rows,
                count.null_count,
                count.null_percentage);
        }
        println!("{:-<90}\n", "");
        Ok(())
    }

    /// Display correlation matrix for multiple columns
    pub async fn display_correlation_matrix(&self, columns: &[&str]) -> ElusionResult<()> {
        println!("\n=== Correlation Matrix ===");
        let col_width = 20;
        let total_width = (columns.len() + 1) * (col_width + 3) + 1;
        println!("{:-<width$}", "", width = total_width);
        
        // Print header with better column name handling
        print!("| {:<width$} |", "", width = col_width);
        for col in columns {
            let display_name = if col.len() > col_width {
                // Take first 12 chars and add "..." 
                format!("{}...", &col[..12])
            } else {
                col.to_string()
            };
            print!(" {:<width$} |", display_name, width = col_width);
        }
        println!();
        println!("{:-<width$}", "", width = total_width);
        
        // Calculate and print correlations with more decimal places
        for &col1 in columns {
            let display_name = if col1.len() > col_width {
                format!("{}...", &col1[..12])
            } else {
                col1.to_string()
            };
            print!("| {:<width$} |", display_name, width = col_width);
                    
            for &col2 in columns {
                let correlation = self.compute_correlation(col1, col2).await?;
                print!(" {:>width$.4} |", correlation, width = col_width);  // Changed to 4 decimal places
            }
            println!();
        }
        println!("{:-<width$}\n", "", width = total_width);
        Ok(())
    }

// ====================== WRITERS ==================== //

    /// Writes the DataFrame to a JSON file (always in overwrite mode)
    pub async fn write_to_json(
        &self,
        path: &str,
        pretty: bool,
    ) -> ElusionResult<()> {
        if let Some(parent) = LocalPath::new(path).parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).map_err(|e| ElusionError::WriteError {
                    path: parent.display().to_string(),
                    operation: "create_directory".to_string(),
                    reason: e.to_string(),
                    suggestion: "💡 Check if you have permissions to create directories".to_string(),
                })?;
            }
        }

        if fs::metadata(path).is_ok() {
            fs::remove_file(path).or_else(|_| fs::remove_dir_all(path)).map_err(|e| 
                ElusionError::WriteError {
                    path: path.to_string(),
                    operation: "overwrite".to_string(),
                    reason: format!("❌ Failed to delete existing file: {}", e),
                    suggestion: "💡 Check file permissions and ensure no other process is using the file".to_string(),
                }
            )?;
        }

        let batches = self.df.clone().collect().await.map_err(|e| 
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

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(|e| ElusionError::WriteError {
                path: path.to_string(),
                operation: "file_create".to_string(),
                reason: e.to_string(),
                suggestion: "💡 Check file permissions and path validity".to_string(),
            })?;

        let mut writer = BufWriter::new(file);
        
        // array opening bracket
        writeln!(writer, "[").map_err(|e| ElusionError::WriteError {
            path: path.to_string(),
            operation: "begin_json".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check disk space and write permissions".to_string(),
        })?;

        // Process each batch of records
        let mut first_row = true;
        let mut rows_written = 0;

        for batch in batches.iter() {
            let row_count = batch.num_rows();
            let column_count = batch.num_columns();
            
            // Skip empty batches
            if row_count == 0 || column_count == 0 {
                continue;
            }

            // Get column names
            let schema = batch.schema();
            let column_names: Vec<&str> = schema.fields().iter()
                .map(|f| f.name().as_str())
                .collect();

            // Process each row
            for row_idx in 0..row_count {
                if !first_row {
                    writeln!(writer, ",").map_err(|e| ElusionError::WriteError {
                        path: path.to_string(),
                        operation: "write_separator".to_string(),
                        reason: e.to_string(),
                        suggestion: "💡 Check disk space and write permissions".to_string(),
                    })?;
                }
                first_row = false;
                rows_written += 1;

                // Create a JSON object for the row
                let mut row_obj = serde_json::Map::new();
                
                // Add each column value to the row object
                for col_idx in 0..column_count {
                    let col_name = column_names[col_idx];
                    let array = batch.column(col_idx);
                    
                    // Convert arrow array value to serde_json::Value
                    let json_value = array_value_to_json(array, row_idx)?;
                    row_obj.insert(col_name.to_string(), json_value);
                }

                // Serialize the row to JSON
                let json_value = serde_json::Value::Object(row_obj);
                
                if pretty {
                    serde_json::to_writer_pretty(&mut writer, &json_value)
                        .map_err(|e| ElusionError::WriteError {
                            path: path.to_string(),
                            operation: format!("write_row_{}", rows_written),
                            reason: format!("JSON serialization error: {}", e),
                            suggestion: "💡 Check if row contains valid JSON data".to_string(),
                        })?;
                } else {
                    serde_json::to_writer(&mut writer, &json_value)
                        .map_err(|e| ElusionError::WriteError {
                            path: path.to_string(),
                            operation: format!("write_row_{}", rows_written),
                            reason: format!("JSON serialization error: {}", e),
                            suggestion: "💡 Check if row contains valid JSON data".to_string(),
                        })?;
                }
            }
        }

        // Write array closing bracket
        writeln!(writer, "\n]").map_err(|e| ElusionError::WriteError {
            path: path.to_string(),
            operation: "end_json".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check disk space and write permissions".to_string(),
        })?;

        // Ensure all data is written
        writer.flush().map_err(|e| ElusionError::WriteError {
            path: path.to_string(),
            operation: "flush".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Failed to flush data to file".to_string(),
        })?;

        println!("✅ Data successfully written to '{}'", path);
        
        if rows_written == 0 {
            println!("*** Warning ***: No rows were written to the file. Check if this is expected.");
        } else {
            println!("✅ Wrote {} rows to JSON file", rows_written);
        }

        Ok(())
    }


    /// Write the DataFrame to a Parquet file
    pub async fn write_to_parquet(
        &self,
        mode: &str,
        path: &str,
        options: Option<DataFrameWriteOptions>,
    ) -> ElusionResult<()> {
        let write_options = options.unwrap_or_else(DataFrameWriteOptions::new);

        if let Some(parent) = LocalPath::new(path).parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).map_err(|e| ElusionError::WriteError {
                    path: parent.display().to_string(),
                    operation: "create_directory".to_string(),
                    reason: e.to_string(),
                    suggestion: "💡 Check if you have permissions to create directories".to_string(),
                })?;
            }
        }
        match mode {
            "overwrite" => {
                if fs::metadata(path).is_ok() {
                    fs::remove_file(path).or_else(|_| fs::remove_dir_all(path)).map_err(|e| {
                        ElusionError::WriteError {
                            path: path.to_string(),
                            operation: "overwrite".to_string(),
                            reason: format!("❌ Failed to delete existing file/directory: {}", e),
                            suggestion: "💡 Check file permissions and ensure no other process is using the file".to_string()
                        }
                    })?;
                }
                
                self.df.clone().write_parquet(path, write_options, None).await
                    .map_err(|e| ElusionError::WriteError {
                        path: path.to_string(),
                        operation: "overwrite".to_string(),
                        reason: e.to_string(),
                        suggestion: "💡 Check file permissions and path validity".to_string()
                    })?;
            }
        "append" => {
            let ctx = SessionContext::new();
            
            if !fs::metadata(path).is_ok() {
                self.df.clone().write_parquet(path, write_options, None).await
                    .map_err(|e| ElusionError::WriteError {
                        path: path.to_string(),
                        operation: "append".to_string(),
                        reason: format!("❌ Failed to create initial file: {}", e),
                        suggestion: "💡 Check directory permissions and path validity".to_string()
                    })?;
                return Ok(());
            }

            // Read existing parquet file
            let existing_df = ctx.read_parquet(path, ParquetReadOptions::default()).await
                .map_err(|e| ElusionError::WriteError {
                    path: path.to_string(),
                    operation: "read_existing".to_string(),
                    reason: e.to_string(),
                    suggestion: "💡 Verify the file is a valid Parquet file".to_string()
                })?;

            // Print schemas for debugging
            // println!("Existing schema: {:?}", existing_df.schema());
            // println!("New schema: {:?}", self.df.schema());
            
            // Print column names for both DataFrames
            // println!("Existing columns ({}): {:?}", 
            //     existing_df.schema().fields().len(),
            //     existing_df.schema().field_names());
            // println!("New columns ({}): {:?}", 
            //     self.df.schema().fields().len(),
            //     self.df.schema().field_names());

            // Register existing data with a table alias
            ctx.register_table("existing_data", Arc::new(
                MemTable::try_new(
                    existing_df.schema().clone().into(),
                    vec![existing_df.clone().collect().await.map_err(|e| ElusionError::WriteError {
                        path: path.to_string(),
                        operation: "collect_existing".to_string(),
                        reason: e.to_string(),
                        suggestion: "💡 Failed to collect existing data".to_string()
                    })?]
                ).map_err(|e| ElusionError::WriteError {
                    path: path.to_string(),
                    operation: "create_mem_table".to_string(),
                    reason: e.to_string(),
                    suggestion: "💡 Failed to create memory table".to_string()
                })?
            )).map_err(|e| ElusionError::WriteError {
                path: path.to_string(),
                operation: "register_existing".to_string(),
                reason: e.to_string(),
                suggestion: "💡 Failed to register existing data".to_string()
            })?;

            // new data with a table alias
            ctx.register_table("new_data", Arc::new(
                MemTable::try_new(
                    self.df.schema().clone().into(),
                    vec![self.df.clone().collect().await.map_err(|e| ElusionError::WriteError {
                        path: path.to_string(),
                        operation: "collect_new".to_string(),
                        reason: e.to_string(),
                        suggestion: "💡 Failed to collect new data".to_string()
                    })?]
                ).map_err(|e| ElusionError::WriteError {
                    path: path.to_string(),
                    operation: "create_mem_table".to_string(),
                    reason: e.to_string(),
                    suggestion: "💡 Failed to create memory table".to_string()
                })?
            )).map_err(|e| ElusionError::WriteError {
                path: path.to_string(),
                operation: "register_new".to_string(),
                reason: e.to_string(),
                suggestion: "💡 Failed to register new data".to_string()
            })?;

            // SQL with explicit column list
            let column_list = existing_df.schema()
                .fields()
                .iter()
                .map(|f| format!("\"{}\"", f.name()))  
                .collect::<Vec<_>>()
                .join(", ");

            //  UNION ALL with explicit columns
            let sql = format!(
                "SELECT {} FROM existing_data UNION ALL SELECT {} FROM new_data",
                column_list, column_list
            );
            // println!("Executing SQL: {}", sql);

            let combined_df = ctx.sql(&sql).await
                .map_err(|e| ElusionError::WriteError {
                    path: path.to_string(),
                    operation: "combine_data".to_string(),
                    reason: e.to_string(),
                    suggestion: "💡 Failed to combine existing and new data".to_string()
                })?;

                // temporary path for writing
                let temp_path = format!("{}.temp", path);

                // Write combined data to temporary file
                combined_df.write_parquet(&temp_path, write_options, None).await
                    .map_err(|e| ElusionError::WriteError {
                        path: temp_path.clone(),
                        operation: "write_combined".to_string(),
                        reason: e.to_string(),
                        suggestion: "💡 Failed to write combined data".to_string()
                    })?;

                // Remove original file
                fs::remove_file(path).map_err(|e| ElusionError::WriteError {
                    path: path.to_string(),
                    operation: "remove_original".to_string(),
                    reason: format!("❌ Failed to remove original file: {}", e),
                    suggestion: "💡 Check file permissions".to_string()
                })?;

                // Rename temporary file to original path
                fs::rename(&temp_path, path).map_err(|e| ElusionError::WriteError {
                    path: path.to_string(),
                    operation: "rename_temp".to_string(),
                    reason: format!("❌ Failed to rename temporary file: {}", e),
                    suggestion: "💡 Check file system permissions".to_string()
                })?;
            }
            _ => return Err(ElusionError::InvalidOperation {
                operation: mode.to_string(),
                reason: "Invalid write mode".to_string(),
                suggestion: "💡 Use 'overwrite' or 'append'".to_string()
            })
        }

        match mode {
            "overwrite" => println!("✅ Data successfully overwritten to '{}'", path),
            "append" => println!("✅ Data successfully appended to '{}'", path),
            _ => unreachable!(),
        }
        
        Ok(())
    }

    /// Writes the DataFrame to a CSV file in either "overwrite" or "append" mode.
    pub async fn write_to_csv(
        &self,
        mode: &str,
        path: &str,
        csv_options: CsvWriteOptions,
    ) -> ElusionResult<()> {
        csv_options.validate()?;
        
        if let Some(parent) = LocalPath::new(path).parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).map_err(|e| ElusionError::WriteError {
                    path: parent.display().to_string(),
                    operation: "create_directory".to_string(),
                    reason: e.to_string(),
                    suggestion: "💡 Check if you have permissions to create directories".to_string(),
                })?;
            }
        }

        match mode {
            "overwrite" => {
                // Remove existing file if it exists
                if fs::metadata(path).is_ok() {
                    fs::remove_file(path).or_else(|_| fs::remove_dir_all(path)).map_err(|e| 
                        ElusionError::WriteError {
                            path: path.to_string(),
                            operation: "overwrite".to_string(),
                            reason: format!("Failed to delete existing file: {}", e),
                            suggestion: "💡 Check file permissions and ensure no other process is using the file".to_string(),
                        }
                    )?;
                }

                let batches = self.df.clone().collect().await.map_err(|e| 
                    ElusionError::InvalidOperation {
                        operation: "Data Collection".to_string(),
                        reason: format!("Failed to collect DataFrame: {}", e),
                        suggestion: "💡 Verify DataFrame is not empty and contains valid data".to_string(),
                    }
                )?;

                if batches.is_empty() {
                    return Err(ElusionError::InvalidOperation {
                        operation: "CSV Writing".to_string(),
                        reason: "No data to write".to_string(),
                        suggestion: "💡 Ensure DataFrame contains data before writing".to_string(),
                    });
                }

                let file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(path)
                    .map_err(|e| ElusionError::WriteError {
                        path: path.to_string(),
                        operation: "file_create".to_string(),
                        reason: e.to_string(),
                        suggestion: "💡 Check file permissions and path validity".to_string(),
                    })?;

                let writer = BufWriter::new(file);
                let mut csv_writer = WriterBuilder::new()
                    .with_header(true)
                    .with_delimiter(csv_options.delimiter)
                    .with_escape(csv_options.escape)
                    .with_quote(csv_options.quote)
                    .with_double_quote(csv_options.double_quote)
                    .with_null(csv_options.null_value.clone())
                    .build(writer);

                for batch in batches.iter() {
                    csv_writer.write(batch).map_err(|e| ElusionError::WriteError {
                        path: path.to_string(),
                        operation: "write_data".to_string(),
                        reason: e.to_string(),
                        suggestion: "💡 Failed to write data batch".to_string(),
                    })?;
                }
                
                csv_writer.into_inner().flush().map_err(|e| ElusionError::WriteError {
                    path: path.to_string(),
                    operation: "flush".to_string(),
                    reason: e.to_string(),
                    suggestion: "💡 Failed to flush data to file".to_string(),
                })?;
            },
            "append" => {
                if !fs::metadata(path).is_ok() {
                    // If file doesn't exist in append mode, just write directly
                    let batches = self.df.clone().collect().await.map_err(|e| 
                        ElusionError::InvalidOperation {
                            operation: "Data Collection".to_string(),
                            reason: format!("Failed to collect DataFrame: {}", e),
                            suggestion: "💡 Verify DataFrame is not empty and contains valid data".to_string(),
                        }
                    )?;

                    if batches.is_empty() {
                        return Err(ElusionError::InvalidOperation {
                            operation: "CSV Writing".to_string(),
                            reason: "No data to write".to_string(),
                            suggestion: "💡 Ensure DataFrame contains data before writing".to_string(),
                        });
                    }

                    let file = OpenOptions::new()
                        .write(true)
                        .create(true)
                        .open(path)
                        .map_err(|e| ElusionError::WriteError {
                            path: path.to_string(),
                            operation: "file_create".to_string(),
                            reason: e.to_string(),
                            suggestion: "💡 Check file permissions and path validity".to_string(),
                        })?;

                    let writer = BufWriter::new(file);
                    let mut csv_writer = WriterBuilder::new()
                        .with_header(true)
                        .with_delimiter(csv_options.delimiter)
                        .with_escape(csv_options.escape)
                        .with_quote(csv_options.quote)
                        .with_double_quote(csv_options.double_quote)
                        .with_null(csv_options.null_value.clone())
                        .build(writer);

                    for batch in batches.iter() {
                        csv_writer.write(batch).map_err(|e| ElusionError::WriteError {
                            path: path.to_string(),
                            operation: "write_data".to_string(),
                            reason: e.to_string(),
                            suggestion: "💡 Failed to write data batch".to_string(),
                        })?;
                    }
                    csv_writer.into_inner().flush().map_err(|e| ElusionError::WriteError {
                        path: path.to_string(),
                        operation: "flush".to_string(),
                        reason: e.to_string(),
                        suggestion: "💡 Failed to flush data to file".to_string(),
                    })?;
                } else {
                    let ctx = SessionContext::new();
                    let existing_df = ctx.read_csv(
                        path,
                        CsvReadOptions::new()
                            .has_header(true)
                            .schema_infer_max_records(1000),
                    ).await?;

                    // Verify columns match before proceeding
                    let existing_cols: HashSet<_> = existing_df.schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().to_string())
                        .collect();
                    
                    let new_cols: HashSet<_> = self.df.schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().to_string())
                        .collect();

                    if existing_cols != new_cols {
                        return Err(ElusionError::WriteError {
                            path: path.to_string(),
                            operation: "column_check".to_string(),
                            reason: "Column mismatch between existing file and new data".to_string(),
                            suggestion: "💡 Ensure both datasets have the same columns".to_string()
                        });
                    }

                    ctx.register_table("existing_data", Arc::new(
                        MemTable::try_new(
                            existing_df.schema().clone().into(),
                            vec![existing_df.clone().collect().await.map_err(|e| ElusionError::WriteError {
                                path: path.to_string(),
                                operation: "collect_existing".to_string(),
                                reason: e.to_string(),
                                suggestion: "💡 Failed to collect existing data".to_string()
                            })?]
                        ).map_err(|e| ElusionError::WriteError {
                            path: path.to_string(),
                            operation: "create_mem_table".to_string(),
                            reason: e.to_string(),
                            suggestion: "💡 Failed to create memory table".to_string()
                        })?
                    )).map_err(|e| ElusionError::WriteError {
                        path: path.to_string(),
                        operation: "register_existing".to_string(),
                        reason: e.to_string(),
                        suggestion: "💡 Failed to register existing data".to_string()
                    })?;

                    ctx.register_table("new_data", Arc::new(
                        MemTable::try_new(
                            self.df.schema().clone().into(),
                            vec![self.df.clone().collect().await.map_err(|e| ElusionError::WriteError {
                                path: path.to_string(),
                                operation: "collect_new".to_string(),
                                reason: e.to_string(),
                                suggestion: "💡 Failed to collect new data".to_string()
                            })?]
                        ).map_err(|e| ElusionError::WriteError {
                            path: path.to_string(),
                            operation: "create_mem_table".to_string(),
                            reason: e.to_string(),
                            suggestion: "💡 Failed to create memory table".to_string()
                        })?
                    )).map_err(|e| ElusionError::WriteError {
                        path: path.to_string(),
                        operation: "register_new".to_string(),
                        reason: e.to_string(),
                        suggestion: "💡 Failed to register new data".to_string()
                    })?;

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
                        .map_err(|e| ElusionError::WriteError {
                            path: path.to_string(),
                            operation: "combine_data".to_string(),
                            reason: e.to_string(),
                            suggestion: "💡 Failed to combine existing and new data".to_string()
                        })?;

                    let temp_path = format!("{}.temp", path);

                    // Clean up any existing temp file
                    if fs::metadata(&temp_path).is_ok() {
                        fs::remove_file(&temp_path).map_err(|e| ElusionError::WriteError {
                            path: temp_path.clone(),
                            operation: "cleanup_temp".to_string(),
                            reason: format!("Failed to delete temporary file: {}", e),
                            suggestion: "💡 Check file permissions and ensure no other process is using the file".to_string(),
                        })?;
                    }

                    let batches = combined_df.collect().await.map_err(|e| 
                        ElusionError::InvalidOperation {
                            operation: "Data Collection".to_string(),
                            reason: format!("Failed to collect DataFrame: {}", e),
                            suggestion: "💡 Verify DataFrame is not empty and contains valid data".to_string(),
                        }
                    )?;

                    if batches.is_empty() {
                        return Err(ElusionError::InvalidOperation {
                            operation: "CSV Writing".to_string(),
                            reason: "No data to write".to_string(),
                            suggestion: "💡 Ensure DataFrame contains data before writing".to_string(),
                        });
                    }

                    // Write to temporary file
                    {
                        let file = OpenOptions::new()
                            .write(true)
                            .create(true)
                            .truncate(true)
                            .open(&temp_path)
                            .map_err(|e| ElusionError::WriteError {
                                path: temp_path.clone(),
                                operation: "file_open".to_string(),
                                reason: e.to_string(),
                                suggestion: "💡 Check file permissions and path validity".to_string(),
                            })?;

                        let writer = BufWriter::new(file);
                        let mut csv_writer = WriterBuilder::new()
                            .with_header(true)
                            .with_delimiter(csv_options.delimiter)
                            .with_escape(csv_options.escape)
                            .with_quote(csv_options.quote)
                            .with_double_quote(csv_options.double_quote)
                            .with_null(csv_options.null_value.clone())
                            .build(writer);

                        for batch in batches.iter() {
                            csv_writer.write(batch).map_err(|e| ElusionError::WriteError {
                                path: temp_path.clone(),
                                operation: "write_data".to_string(),
                                reason: e.to_string(),
                                suggestion: "💡 Failed to write data batch".to_string(),
                            })?;
                        }

                        csv_writer.into_inner().flush().map_err(|e| ElusionError::WriteError {
                            path: temp_path.clone(),
                            operation: "flush".to_string(),
                            reason: e.to_string(),
                            suggestion: "💡 Check disk space and write permissions".to_string(),
                        })?;
                    } // Writer is dropped here

                    // Remove original file first if it exists
                    if fs::metadata(path).is_ok() {
                        fs::remove_file(path).map_err(|e| ElusionError::WriteError {
                            path: path.to_string(),
                            operation: "remove_original".to_string(),
                            reason: format!("Failed to remove original file: {}", e),
                            suggestion: "💡 Check file permissions".to_string()
                        })?;
                    }

                    // Now rename temp file to original path
                    fs::rename(&temp_path, path).map_err(|e| ElusionError::WriteError {
                        path: path.to_string(),
                        operation: "rename_temp".to_string(),
                        reason: format!("Failed to rename temporary file: {}", e),
                        suggestion: "💡 Check file system permissions".to_string()
                    })?;
                }
            },
            _ => return Err(ElusionError::InvalidOperation {
                operation: mode.to_string(),
                reason: "Invalid write mode".to_string(),
                suggestion: "💡 Use 'overwrite' or 'append'".to_string()
            })
        }

        match mode {
            "overwrite" => println!("✅ Data successfully overwritten to '{}'", path),
            "append" => println!("✅ Data successfully appended to '{}'", path),
            _ => unreachable!(),
        }

        Ok(())
    }

    /// Writes a DataFusion `DataFrame` to a Delta table at `path`
    pub async fn write_to_delta_table(
        &self,
        mode: &str,
        path: &str,
        partition_columns: Option<Vec<String>>,
    ) -> Result<(), DeltaTableError> {
        // Match on the user-supplied string to set `overwrite` and `write_mode`.
        let (overwrite, write_mode) = match mode {
            "overwrite" => {
                (true, WriteMode::Default)
            }
            "append" => {
                (false, WriteMode::Default)
            }
            "merge" => {
                //  "merge" to auto-merge schema
                (false, WriteMode::MergeSchema)
            }
            "default" => {
                // Another alias for (false, WriteMode::Default)
                (false, WriteMode::Default)
            }
            other => {
                return Err(DeltaTableError::Generic(format!(
                    "Unsupported write mode: {other}"
                )));
            }
        };

        write_to_delta_impl(
            &self.df,   // The underlying DataFusion DataFrame
            path,
            partition_columns,
            overwrite,
            write_mode,
        )
        .await
    }

    /// Writes the DataFrame to an Excel file with formatting options
    #[cfg(feature = "excel")]
    pub async fn write_to_excel(
        &self,
        path: &str,
        sheet_name: Option<&str>
    ) -> ElusionResult<()> {

        if let Some(parent) = LocalPath::new(path).parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent).map_err(|e| ElusionError::WriteError {
                    path: parent.display().to_string(),
                    operation: "create_directory".to_string(),
                    reason: e.to_string(),
                    suggestion: "💡 Check if you have permissions to create directories".to_string(),
                })?;
            }
        }
    
        if fs::metadata(path).is_ok() {
            fs::remove_file(path).map_err(|e| 
                ElusionError::WriteError {
                    path: path.to_string(),
                    operation: "overwrite".to_string(),
                    reason: format!("❌ Failed to delete existing file: {}", e),
                    suggestion: "💡 Check file permissions and ensure no other process is using the file".to_string(),
                }
            )?;
        }
    
        let batches = self.df.clone().collect().await.map_err(|e| 
            ElusionError::InvalidOperation {
                operation: "Data Collection".to_string(),
                reason: format!("Failed to collect DataFrame: {}", e),
                suggestion: "💡 Verify DataFrame is not empty and contains valid data".to_string(),
            }
        )?;
    
        if batches.is_empty() {
            return Err(ElusionError::InvalidOperation {
                operation: "Excel Writing".to_string(),
                reason: "No data to write".to_string(),
                suggestion: "💡 Ensure DataFrame contains data before writing".to_string(),
            });
        }
    
        let mut workbook = Workbook::new();
    
        let sheet_name = sheet_name.unwrap_or("Sheet1");
        let worksheet = workbook.add_worksheet().set_name(sheet_name).map_err(|e| ElusionError::WriteError {
            path: path.to_string(),
            operation: "worksheet_create".to_string(),
            reason: format!("Failed to create worksheet: {}", e),
            suggestion: "💡 Invalid sheet name or workbook error".to_string(),
        })?;
    
        let header_format = Format::new()
            .set_bold()
            .set_font_color(0xFFFFFF)
            .set_background_color(0x329A52)
            .set_align(rust_xlsxwriter::FormatAlign::Center);
        
        let date_format = Format::new()
            .set_num_format("yyyy-mm-dd");
        
        let schema = batches[0].schema();
        let column_count = schema.fields().len();
        
        for (col_idx, field) in schema.fields().iter().enumerate() {
            worksheet.write_string_with_format(0, col_idx as u16, field.name(), &header_format)
                .map_err(|e| ElusionError::WriteError {
                    path: path.to_string(),
                    operation: "write_header".to_string(),
                    reason: format!("Failed to write column header '{}': {}", field.name(), e),
                    suggestion: "💡 Check if the column name contains invalid characters".to_string(),
                })?;
                
            let width = (field.name().len() as f64 * 1.2).max(10.0).min(50.0);
            worksheet.set_column_width(col_idx as u16, width)
                .map_err(|e| ElusionError::WriteError {
                    path: path.to_string(),
                    operation: "set_column_width".to_string(),
                    reason: format!("Failed to set column width: {}", e),
                    suggestion: "💡 Failed to set column width".to_string(),
                })?;
        }
        
        // Write data rows
        let mut row_idx = 1; // Start from row 1 (after headers)
        
        for batch in batches.iter() {
            let row_count = batch.num_rows();
            
            for r in 0..row_count {
                for (c, field) in schema.fields().iter().enumerate() {
                    let col = batch.column(c);
                    
                    if col.is_null(r) {
                        // skip null values show as empty
                        continue;
                    }
                    
                    match field.data_type() {
                        ArrowDataType::Int8 | ArrowDataType::Int16 | ArrowDataType::Int32 | ArrowDataType::Int64 => {
                            let value = match field.data_type() {
                                ArrowDataType::Int8 => {
                                    if let Some(array) = col.as_any().downcast_ref::<Int8Array>() {
                                        if array.is_null(r) { 0.0 } else { array.value(r) as f64 }
                                    } else { 0.0 }
                                },
                                ArrowDataType::Int16 => {
                                    if let Some(array) = col.as_any().downcast_ref::<Int16Array>() {
                                        if array.is_null(r) { 0.0 } else { array.value(r) as f64 }
                                    } else { 0.0 }
                                },
                                ArrowDataType::Int32 => {
                                    if let Some(array) = col.as_any().downcast_ref::<Int32Array>() {
                                        if array.is_null(r) { 0.0 } else { array.value(r) as f64 }
                                    } else { 0.0 }
                                },
                                ArrowDataType::Int64 => {
                                    if let Some(array) = col.as_any().downcast_ref::<Int64Array>() {
                                        if array.is_null(r) { 0.0 } else { array.value(r) as f64 }
                                    } else { 0.0 }
                                },
                                _ => 0.0 
                            };
                            
                            worksheet.write_number(row_idx, c as u16, value)
                                .map_err(|e| ElusionError::WriteError {
                                    path: path.to_string(),
                                    operation: format!("write_number_r{}_c{}", row_idx, c),
                                    reason: format!("Failed to write number: {}", e),
                                    suggestion: "💡 Failed to write number value".to_string(),
                                })?;
                        },
                        ArrowDataType::UInt8 | ArrowDataType::UInt16 | ArrowDataType::UInt32 | ArrowDataType::UInt64 => {
                            let value = match field.data_type() {
                                ArrowDataType::UInt8 => {
                                    if let Some(array) = col.as_any().downcast_ref::<UInt8Array>() {
                                        if array.is_null(r) { 0.0 } else { array.value(r) as f64 }
                                    } else { 0.0 }
                                },
                                ArrowDataType::UInt16 => {
                                    if let Some(array) = col.as_any().downcast_ref::<UInt16Array>() {
                                        if array.is_null(r) { 0.0 } else { array.value(r) as f64 }
                                    } else { 0.0 }
                                },
                                ArrowDataType::UInt32 => {
                                    if let Some(array) = col.as_any().downcast_ref::<UInt32Array>() {
                                        if array.is_null(r) { 0.0 } else { array.value(r) as f64 }
                                    } else { 0.0 }
                                },
                                ArrowDataType::UInt64 => {
                                    if let Some(array) = col.as_any().downcast_ref::<UInt64Array>() {
                                        if array.is_null(r) { 0.0 } else { array.value(r) as f64 }
                                    } else { 0.0 }
                                },
                                _ => 0.0 // Shouldn't reach here
                            };
                            
                            worksheet.write_number(row_idx, c as u16, value)
                                .map_err(|e| ElusionError::WriteError {
                                    path: path.to_string(),
                                    operation: format!("write_number_r{}_c{}", row_idx, c),
                                    reason: format!("Failed to write number: {}", e),
                                    suggestion: "💡 Failed to write number value".to_string(),
                                })?;
                        },
                        ArrowDataType::Float32 | ArrowDataType::Float64 => {
                            let value = match array_value_to_json(col, r)? {
                                serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0),
                                _ => 0.0,
                            };
                            worksheet.write_number(row_idx, c as u16, value)
                                .map_err(|e| ElusionError::WriteError {
                                    path: path.to_string(),
                                    operation: format!("write_number_r{}_c{}", row_idx, c),
                                    reason: format!("Failed to write number: {}", e),
                                    suggestion: "💡 Failed to write number value".to_string(),
                                })?;
                        },
                        ArrowDataType::Boolean => {
                            let value = match array_value_to_json(col, r)? {
                                serde_json::Value::Bool(b) => b,
                                _ => false,
                            };
                            worksheet.write_boolean(row_idx, c as u16, value)
                                .map_err(|e| ElusionError::WriteError {
                                    path: path.to_string(),
                                    operation: format!("write_boolean_r{}_c{}", row_idx, c),
                                    reason: format!("Failed to write boolean: {}", e),
                                    suggestion: "💡 Failed to write boolean value".to_string(),
                                })?;
                        },
                        ArrowDataType::Date32 | ArrowDataType::Date64 => {
                            let date_str = match array_value_to_json(col, r)? {
                                serde_json::Value::String(s) => s,
                                _ => String::new(),
                            };
                            
                            // Format: YYYY-MM-DD
                            let date_parts: Vec<&str> = date_str.split('-').collect();
                            if date_parts.len() == 3 {
                                if let (Ok(year), Ok(month), Ok(day)) = (
                                    date_parts[0].parse::<u16>(),
                                    date_parts[1].parse::<u8>(),
                                    date_parts[2].parse::<u8>(),
                                ) {
                                    let excel_date = ExcelDateTime::from_ymd(year, month, day)
                                        .map_err(|e| ElusionError::WriteError {
                                            path: path.to_string(),
                                            operation: format!("create_date_r{}_c{}", row_idx, c),
                                            reason: format!("Invalid date: {}", e),
                                            suggestion: "💡 Failed to create Excel date".to_string(),
                                        })?;
                                        
                                    worksheet.write_datetime_with_format(row_idx, c as u16, &excel_date, &date_format)
                                        .map_err(|e| ElusionError::WriteError {
                                            path: path.to_string(),
                                            operation: format!("write_date_r{}_c{}", row_idx, c),
                                            reason: format!("Failed to write date: {}", e),
                                            suggestion: "💡 Failed to write date value".to_string(),
                                        })?;
                                } else {
                                    // Fallback to string if parsing fails
                                    worksheet.write_string(row_idx, c as u16, &date_str)
                                        .map_err(|e| ElusionError::WriteError {
                                            path: path.to_string(),
                                            operation: format!("write_date_str_r{}_c{}", row_idx, c),
                                            reason: format!("Failed to write date string: {}", e),
                                            suggestion: "💡 Failed to write date as string".to_string(),
                                        })?;
                                }
                            } else {
                                // Not a YYYY-MM-DD format, write as string
                                worksheet.write_string(row_idx, c as u16, &date_str)
                                    .map_err(|e| ElusionError::WriteError {
                                        path: path.to_string(),
                                        operation: format!("write_date_str_r{}_c{}", row_idx, c),
                                        reason: format!("Failed to write date string: {}", e),
                                        suggestion: "💡 Failed to write date as string".to_string(),
                                    })?;
                            }
                        },
                        _ => {
                            let value = match array_value_to_json(col, r)? {
                                serde_json::Value::String(s) => s,
                                other => other.to_string(),
                            };
                            worksheet.write_string(row_idx, c as u16, &value)
                                .map_err(|e| ElusionError::WriteError {
                                    path: path.to_string(),
                                    operation: format!("write_string_r{}_c{}", row_idx, c),
                                    reason: format!("Failed to write string: {}", e),
                                    suggestion: "💡 Failed to write string value".to_string(),
                                })?;
                        }
                    }
                }
                row_idx += 1;
            }
        }
        
        worksheet.autofilter(0, 0, row_idx - 1, (column_count - 1) as u16)
            .map_err(|e| ElusionError::WriteError {
                path: path.to_string(),
                operation: "add_autofilter".to_string(),
                reason: format!("Failed to add autofilter: {}", e),
                suggestion: "💡 Failed to add autofilter to worksheet".to_string(),
            })?;
 
        workbook.save(path).map_err(|e| ElusionError::WriteError {
            path: path.to_string(),
            operation: "save_workbook".to_string(),
            reason: format!("Failed to save workbook: {}", e),
            suggestion: "💡 Failed to save Excel file. Check if the file is open in another application.".to_string(),
        })?;
        
        println!("✅ Data successfully written to Excel file '{}'", path);
        println!("✅ Wrote {} rows and {} columns", row_idx - 1, column_count);
        
        Ok(())
    }
    #[cfg(not(feature = "excel"))]
    pub async fn write_to_excel(
        &self,
        _path: &str,
        _sheet_name: Option<&str>
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom("*** Warning ***: Excel feature not enabled. Add feature excel under [dependencies]".to_string()))
    }
    // ========= AZURE WRITING
    #[cfg(feature = "azure")]
    fn setup_azure_client(&self, url: &str, sas_token: &str) -> ElusionResult<(ContainerClient, String)> {
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
    pub async fn write_parquet_to_azure_with_sas(
        &self,
        mode: &str,
        url: &str,
        sas_token: &str,
    ) -> ElusionResult<()> {
        validate_azure_url(url)?;
        
        let (client, blob_name) = self.setup_azure_client(url, sas_token)?;
        let blob_client = client.blob_client(&blob_name);

        match mode {
            "overwrite" => {
                // Existing overwrite logic
                let batches: Vec<RecordBatch> = self.clone().df.collect().await
                    .map_err(|e| ElusionError::Custom(format!("Failed to collect DataFrame: {}", e)))?;

                let props = WriterProperties::builder()
                    .set_writer_version(WriterVersion::PARQUET_2_0)
                    .set_compression(Compression::SNAPPY)
                    .set_created_by("Elusion".to_string())
                    .build();

                let mut buffer = Vec::new();
                {
                    let schema = self.df.schema();
                    let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone().into(), Some(props))
                        .map_err(|e| ElusionError::Custom(format!("Failed to create Parquet writer: {}", e)))?;

                    for batch in batches {
                        writer.write(&batch)
                            .map_err(|e| ElusionError::Custom(format!("Failed to write batch to Parquet: {}", e)))?;
                    }
                    writer.close()
                        .map_err(|e| ElusionError::Custom(format!("Failed to close Parquet writer: {}", e)))?;
                }

                self.upload_to_azure(&blob_client, buffer).await?;
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
                                self.df.schema().clone().into(),
                                vec![self.df.clone().collect().await.map_err(|e|
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
                        self.upload_to_azure(&blob_client, buffer).await?;
                        println!("✅ Successfully appended parquet data to Azure blob: {}", url);
                    }
                    Err(_) => {
                        // If blob doesn't exist, create it with initial data
                        let batches: Vec<RecordBatch> = self.clone().df.collect().await
                            .map_err(|e| ElusionError::Custom(format!("Failed to collect DataFrame: {}", e)))?;

                        let props = WriterProperties::builder()
                            .set_writer_version(WriterVersion::PARQUET_2_0)
                            .set_compression(Compression::SNAPPY)
                            .set_created_by("Elusion".to_string())
                            .build();

                        let mut buffer = Vec::new();
                        {
                            let schema = self.df.schema();
                            let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone().into(), Some(props))
                                .map_err(|e| ElusionError::Custom(format!("Failed to create Parquet writer: {}", e)))?;

                            for batch in batches {
                                writer.write(&batch)
                                    .map_err(|e| ElusionError::Custom(format!("Failed to write batch to Parquet: {}", e)))?;
                            }
                            writer.close()
                                .map_err(|e| ElusionError::Custom(format!("Failed to close Parquet writer: {}", e)))?;
                        }

                        self.upload_to_azure(&blob_client, buffer).await?;
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

    #[cfg(not(feature = "azure"))]
    pub async fn write_parquet_to_azure_with_sas(
        &self,
        _mode: &str,
        _url: &str,
        _sas_token: &str,
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom("*** Warning ***: Azure feature not enabled. Add feature under [dependencies]".to_string()))
    }

    // Helper method for uploading data to Azure
    #[cfg(feature = "azure")]
    async fn upload_to_azure(&self, blob_client: &BlobClient, buffer: Vec<u8>) -> ElusionResult<()> {
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
    pub async fn write_json_to_azure_with_sas(
        &self,
        url: &str,
        sas_token: &str,
        pretty: bool,
    ) -> ElusionResult<()> {
        validate_azure_url(url)?;
        
        let (client, blob_name) = self.setup_azure_client(url, sas_token)?;
        let blob_client = client.blob_client(&blob_name);
    
        let batches = self.df.clone().collect().await.map_err(|e| 
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
        
        self.upload_json_to_azure(&blob_client, content).await?;
        
        println!("✅ Successfully wrote JSON data to Azure blob: {}", url);
        
        if rows_written == 0 {
            println!("*** Warning ***: No rows were written to the blob. Check if this is expected.");
        } else {
            println!("✅ Wrote {} rows to JSON blob", rows_written);
        }
        
        Ok(())
    }

    #[cfg(not(feature = "azure"))]
    pub async fn write_json_to_azure_with_sas(
        &self,
        _url: &str,
        _sas_token: &str,
        _pretty: bool
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom("*** Warning ***: Azure feature not enabled. Add feature under [dependencies]".to_string()))
    }
    
    // Helper method for uploading JSON data to Azure
    #[cfg(feature = "azure")]
    async fn upload_json_to_azure(&self, blob_client: &BlobClient, content: Bytes) -> ElusionResult<()> {
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

    //=================== LOADERS ============================= //
    /// LOAD function for CSV file type
    pub async fn load_csv(file_path: &str, alias: &str) -> ElusionResult<AliasedDataFrame> {
        let ctx = SessionContext::new();

        if !LocalPath::new(file_path).exists() {
            return Err(ElusionError::WriteError {
                path: file_path.to_string(),
                operation: "read".to_string(),
                reason: "File not found".to_string(),
                suggestion: "💡 Check if the file path is correct".to_string()
            });
        }

        let df = match ctx
            .read_csv(
                file_path,
                CsvReadOptions::new()
                    .has_header(true)
                    .schema_infer_max_records(1000),
            )
            .await
        {
            Ok(df) => df,
            Err(err) => {
                eprintln!(
                    "Error reading CSV file '{}': {}. Ensure the file is UTF-8 encoded and free of corrupt data.",
                    file_path, err
                );
                return Err(ElusionError::DataFusion(err));
            }
        };

        Ok(AliasedDataFrame {
            dataframe: df,
            alias: alias.to_string(),
        })
    }

    /// LOAD function for Parquet file type
    pub fn load_parquet<'a>(
        file_path: &'a str,
        alias: &'a str,
    ) -> BoxFuture<'a, ElusionResult<AliasedDataFrame>> {
        Box::pin(async move {
            let ctx = SessionContext::new();

            if !LocalPath::new(file_path).exists() {
                return Err(ElusionError::WriteError {
                    path: file_path.to_string(),
                    operation: "read".to_string(),
                    reason: "File not found".to_string(),
                    suggestion: "💡 Check if the file path is correct".to_string(),
                });
            }

            let df = match ctx.read_parquet(file_path, ParquetReadOptions::default()).await {
                Ok(df) => {
                    df
                }
                Err(err) => {
                    return Err(ElusionError::DataFusion(err));
                }
            };

            
            let batches = df.clone().collect().await.map_err(ElusionError::DataFusion)?;
            let schema = df.schema().clone();
            let mem_table = MemTable::try_new(schema.clone().into(), vec![batches])
                .map_err(|e| ElusionError::SchemaError {
                    message: e.to_string(),
                    schema: Some(schema.to_string()),
                    suggestion: "💡 Check if the parquet file schema is valid".to_string(),
                })?;

            let normalized_alias = normalize_alias_write(alias);
            ctx.register_table(&normalized_alias, Arc::new(mem_table))
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: "Table Registration".to_string(),
                    reason: e.to_string(),
                    suggestion: "💡 Try using a different alias name".to_string(),
                })?;

            let aliased_df = ctx.table(alias).await
                .map_err(|_| ElusionError::InvalidOperation {
                    operation: "Table Creation".to_string(),
                    reason: format!("Failed to create table with alias '{}'", alias),
                    suggestion: "💡 Check if the alias is valid and unique".to_string(),
                })?;

            Ok(AliasedDataFrame {
                dataframe: aliased_df,
                alias: alias.to_string(),
            })
        })
    }

    /// Loads a JSON file into a DataFusion DataFrame
    pub fn load_json<'a>(
        file_path: &'a str,
        alias: &'a str,
    ) -> BoxFuture<'a, ElusionResult<AliasedDataFrame>> {
        Box::pin(async move {
            // Open file with BufReader for efficient reading
            let file = File::open(file_path).map_err(|e| ElusionError::WriteError {
                path: file_path.to_string(),
                operation: "read".to_string(),
                reason: e.to_string(),
                suggestion: "💡 heck if the file exists and you have proper permissions".to_string(),
            })?;
            
            let file_size = file.metadata().map_err(|e| ElusionError::WriteError {
                path: file_path.to_string(),
                operation: "metadata reading".to_string(),
                reason: e.to_string(),
                suggestion: "💡 Check file permissions and disk status".to_string(),
            })?.len() as usize;
                
            let reader = BufReader::with_capacity(32 * 1024, file); // 32KB buffer
            let stream = Deserializer::from_reader(reader).into_iter::<Value>();
            
            let mut all_data = Vec::with_capacity(file_size / 3); // Pre-allocate with estimated size
            
            // Process the first value to determine if it's an array or object
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
                                                all_data.push(row.into_iter().collect());
                                            }
                                        } else {
                                            all_data.push(base_map.into_iter().collect());
                                        }
                                    }
                                }
                            }
                            Ok(_) => continue,
                            Err(e) => return Err(ElusionError::InvalidOperation {
                                operation: "JSON parsing".to_string(),
                                reason: format!("Failed to parse JSON array: {}", e),
                                suggestion: "💡 Ensure the JSON file is properly formatted and contains valid data".to_string(),
                            }),
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
                                    all_data.push(row.into_iter().collect());
                                }
                            } else {
                                all_data.push(base_map.into_iter().collect());
                            }
                        }
                    }
                }
                Some(Err(e)) => return Err(ElusionError::InvalidOperation {
                    operation: "JSON parsing".to_string(),
                    reason: format!("Invalid JSON format: {}", e),
                    suggestion: "💡 Check if the JSON file is well-formed and valid".to_string(),
                }),
                _ => return Err(ElusionError::InvalidOperation {
                    operation: "JSON reading".to_string(),
                    reason: "Empty or invalid JSON file".to_string(),
                    suggestion: "💡 Ensure the JSON file contains valid data in either array or object format".to_string(),
                }),
            }

            if all_data.is_empty() {
                return Err(ElusionError::InvalidOperation {
                    operation: "JSON processing".to_string(),
                    reason: "No valid JSON data found".to_string(),
                    suggestion: "💡 Check if the JSON file contains the expected data structure".to_string(),
                });
            }

            let schema = infer_schema_from_json(&all_data);
            let record_batch = build_record_batch(&all_data, schema.clone())
                .map_err(|e| ElusionError::SchemaError {
                    message: format!("Failed to build RecordBatch: {}", e),
                    schema: Some(schema.to_string()),
                    suggestion: "💡 Check if the JSON data structure is consistent".to_string(),
                })?;

            let ctx = SessionContext::new();
            let mem_table = MemTable::try_new(schema.clone(), vec![vec![record_batch]])
                .map_err(|e| ElusionError::SchemaError {
                    message: format!("Failed to create MemTable: {}", e),
                    schema: Some(schema.to_string()),
                    suggestion: "💡 Verify data types and schema compatibility".to_string(),
                })?;

            ctx.register_table(alias, Arc::new(mem_table))
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: "Table registration".to_string(),
                    reason: format!("Failed to register table: {}", e),
                    suggestion: "💡 Try using a different alias or check table compatibility".to_string(),
                })?;

            let df = ctx.table(alias).await.map_err(|e| ElusionError::InvalidOperation {
                operation: "Table creation".to_string(),
                reason: format!("Failed to create table: {}", e),
                suggestion: "💡 Verify table creation parameters and permissions".to_string(),
            })?;

            Ok(AliasedDataFrame {
                dataframe: df,
                alias: alias.to_string(),
            })
        })
    }

    /// Load a Delta table at `file_path` into a DataFusion DataFrame and wrap it in `AliasedDataFrame`
    pub fn load_delta<'a>(
        file_path: &'a str,
        alias: &'a str,
    ) -> BoxFuture<'a, ElusionResult<AliasedDataFrame>> {
        Box::pin(async move {
            let ctx = SessionContext::new();

            // path manager
            let path_manager = DeltaPathManager::new(file_path);

            // Open Delta table using path manager
            let table = open_table(&path_manager.table_path())
            .await
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Delta Table Opening".to_string(),
                reason: e.to_string(),
                suggestion: "💡 Ensure the path points to a valid Delta table".to_string(),
            })?;

            
            let file_paths: Vec<String> = {
                let raw_uris = table.get_file_uris()
                    .map_err(|e| ElusionError::InvalidOperation {
                        operation: "Delta File Listing".to_string(),
                        reason: e.to_string(),
                        suggestion: "💡 Check Delta table permissions and integrity".to_string(),
                    })?;
                
                raw_uris.map(|uri| path_manager.normalize_uri(&uri))
                    .collect()
            };
            
            // ParquetReadOptions
            let parquet_options = ParquetReadOptions::new()
                // .schema(&combined_schema)
                // .table_partition_cols(partition_columns.clone())
                .parquet_pruning(false)
                .skip_metadata(false);

            let df = ctx.read_parquet(file_paths, parquet_options).await?;


            let batches = df.clone().collect().await?;
            // println!("Number of batches: {}", batches.len());
            // for (i, batch) in batches.iter().enumerate() {
            //     println!("Batch {} row count: {}", i, batch.num_rows());
            // }
            let schema = df.schema().clone().into();
            // Build M  emTable
            let mem_table = MemTable::try_new(schema, vec![batches])?;
            let normalized_alias = normalize_alias_write(alias);
            ctx.register_table(&normalized_alias, Arc::new(mem_table))?;
            
            // Create final DataFrame
            let aliased_df = ctx.table(&normalized_alias).await?;
            
            // Verify final row count
            // let final_count = aliased_df.clone().count().await?;
            // println!("Final row count: {}", final_count);

            Ok(AliasedDataFrame {
                dataframe: aliased_df,
                alias: alias.to_string(),
            })
        })
    }

    // ========== EXCEL
    /// Load an Excel file (XLSX) into a CustomDataFrame
    pub fn load_excel<'a>(
        file_path: &'a str,
        alias: &'a str,
    ) -> BoxFuture<'a, ElusionResult<AliasedDataFrame>> {
        Box::pin(async move {

            if !LocalPath::new(file_path).exists() {
                return Err(ElusionError::WriteError {
                    path: file_path.to_string(),
                    operation: "read".to_string(),
                    reason: "File not found".to_string(),
                    suggestion: "💡 Check if the file path is correct".to_string(),
                });
            }
            
            let mut workbook: Xlsx<_> = open_workbook(file_path)
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: "Excel Reading".to_string(),
                    reason: format!("Failed to open Excel file: {}", e),
                    suggestion: "💡 Ensure the file is a valid Excel (XLSX) file and not corrupted".to_string(),
                })?;
            
            let sheet_names = workbook.sheet_names().to_owned();
            if sheet_names.is_empty() {
                return Err(ElusionError::InvalidOperation {
                    operation: "Excel Reading".to_string(),
                    reason: "Excel file does not contain any sheets".to_string(),
                    suggestion: "💡 Ensure the Excel file contains at least one sheet with data".to_string(),
                });
            }
            
            let sheet_name = &sheet_names[0];
            
            let range = workbook.worksheet_range(sheet_name)
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: "Excel Reading".to_string(),
                    reason: format!("Failed to read sheet '{}': {}", sheet_name, e),
                    suggestion: "💡 The sheet may be corrupted or empty".to_string(),
                })?;
            
            if range.is_empty() {
                return Err(ElusionError::InvalidOperation {
                    operation: "Excel Reading".to_string(),
                    reason: format!("Sheet '{}' is empty", sheet_name),
                    suggestion: "💡 Ensure the sheet contains data".to_string(),
                });
            }

            let headers_row = range.rows().next().ok_or_else(|| ElusionError::InvalidOperation {
                operation: "Excel Reading".to_string(),
                reason: "Failed to read headers from Excel file".to_string(),
                suggestion: "💡 Ensure the first row contains column headers".to_string(),
            })?;
            
            // Convert headers to strings, sanitizing as needed
            let headers: Vec<String> = headers_row.iter()
                .enumerate()
                .map(|(column_index, cell)|  {
                    let header = cell.to_string().trim().to_string();
                    if header.is_empty() {
                        format!("Column_{}", column_index)
                    } else {
                        // Replace spaces and special characters with underscores
                        let sanitized = header.replace(' ', "_")
                            .replace(|c: char| !c.is_alphanumeric() && c != '_', "_");
                        
                        // Ensure header starts with a letter
                        if sanitized.chars().next().map_or(true, |c| !c.is_alphabetic()) {
                            format!("col_{}", sanitized)
                        } else {
                            sanitized
                        }
                    }
                })
                .enumerate()
                .map(|(column_index, header)| {
                    if header.is_empty() {
                        format!("Column_{}", column_index)
                    } else {
                        header
                    }
                })
                .collect();
            
            // Check for duplicates in headers
            let mut seen = HashSet::new();
            let mut has_duplicates = false;
            for header in &headers {
                if !seen.insert(header.clone()) {
                    has_duplicates = true;
                    break;
                }
            }
            
            // If duplicates found, make headers unique
            let final_headers = if has_duplicates {
                let mut seen = HashSet::new();
                let mut unique_headers = Vec::with_capacity(headers.len());
                
                for header in headers {
                    let mut unique_header = header.clone();
                    let mut counter = 1;
                    
                    while !seen.insert(unique_header.clone()) {
                        unique_header = format!("{}_{}", header, counter);
                        counter += 1;
                    }
                    
                    unique_headers.push(unique_header);
                }
                
                unique_headers
            } else {
                headers
            };
            
            // Process the data rows
            let mut all_data: Vec<HashMap<String, Value>> = Vec::new();
            
            // Skip the header row
            for row in range.rows().skip(1) {
                let mut row_map = HashMap::new();
                
                for (i, cell) in row.iter().enumerate() {
                    if i >= final_headers.len() {
                        continue; // Skip cells without headers
                    }
                    
                    // Convert cell value to serde_json::Value based on its type
                    let value = match cell {
                        CalamineDataType::Empty => Value::Null,
                        
                        CalamineDataType::String(s) => Value::String(s.clone()),
                        
                        CalamineDataType::Float(f) => {
                            if f.fract() == 0.0 {
                                // It's an integer
                                Value::Number(serde_json::Number::from(f.round() as i64))
                            } else {
                                // It's a floating point
                                serde_json::Number::from_f64(*f)
                                    .map(Value::Number)
                                    .unwrap_or(Value::Null)
                            }
                        },
                        
                        CalamineDataType::Int(i) => Value::Number((*i).into()),
                        
                        CalamineDataType::Bool(b) => Value::Bool(*b),
                        
                        CalamineDataType::DateTime(dt) => {
                          
                            if let Some(naive_date) = excel_date_to_naive_date(*dt) {
                                Value::String(naive_date.format("%Y-%m-%d").to_string())
                            } else {
                                // Fallback to original representation if conversion fails
                                Value::String(format!("DateTime({})", dt))
                            }
                        },
                        
                        CalamineDataType::Duration(d) => {
                            let hours = (d * 24.0) as i64;
                            let minutes = ((d * 24.0 * 60.0) % 60.0) as i64;
                            let seconds = ((d * 24.0 * 60.0 * 60.0) % 60.0) as i64;
                            Value::String(format!("{}h {}m {}s", hours, minutes, seconds))
                        },
                        
                        CalamineDataType::DateTimeIso(dt_iso) => {
                            // ISO 8601 formatted date/time string
                            Value::String(dt_iso.clone())
                        },
                        
                        CalamineDataType::DurationIso(d_iso) => {
                            // ISO 8601 formatted duration string
                            Value::String(d_iso.clone())
                        },
                        
                        CalamineDataType::Error(_) => Value::Null,
                    };
                    
                    row_map.insert(final_headers[i].clone(), value);
                }
                
                all_data.push(row_map);
            }
            
            if all_data.is_empty() {
                return Err(ElusionError::InvalidOperation {
                    operation: "Excel Processing".to_string(),
                    reason: "No valid data rows found in Excel file".to_string(),
                    suggestion: "💡 Ensure the Excel file contains data rows after the header row".to_string(),
                });
            }
            
            let schema = infer_schema_from_json(&all_data);
            
            let record_batch = build_record_batch(&all_data, schema.clone())
                .map_err(|e| ElusionError::SchemaError {
                    message: format!("Failed to build RecordBatch: {}", e),
                    schema: Some(schema.to_string()),
                    suggestion: "💡 Check if the Excel data structure is consistent".to_string(),
                })?;
            
            let ctx = SessionContext::new();
            let mem_table = MemTable::try_new(schema.clone(), vec![vec![record_batch]])
                .map_err(|e| ElusionError::SchemaError {
                    message: format!("Failed to create MemTable: {}", e),
                    schema: Some(schema.to_string()),
                    suggestion: "💡 Verify data types and schema compatibility".to_string(),
                })?;
            
            ctx.register_table(alias, Arc::new(mem_table))
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: "Table Registration".to_string(),
                    reason: format!("Failed to register table: {}", e),
                    suggestion: "💡 Try using a different alias name".to_string(),
                })?;
            
            let df = ctx.table(alias).await
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: "Table Creation".to_string(),
                    reason: format!("Failed to create table: {}", e),
                    suggestion: "💡 Verify table creation parameters".to_string(),
                })?;
            
            Ok(AliasedDataFrame {
                dataframe: df,
                alias: alias.to_string(),
            })
        })
    }

   
    // ================= AZURE 
    /// Aazure function that connects to Azure blob storage
    #[cfg(feature = "azure")]
    pub async fn from_azure_with_sas_token(
        url: &str,
        sas_token: &str,
        filter_keyword: Option<&str>, 
        alias: &str,
    ) -> ElusionResult<Self> {

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


        let container = url_parts.last()
            .ok_or_else(|| ElusionError::Custom("Invalid URL format".to_string()))?
            .to_string();

        // info!("Extracted account='{}', container='{}'", account, container);

        let credentials = StorageCredentials::sas_token(sas_token.to_string())
            .map_err(|e| ElusionError::Custom(format!("Invalid SAS token: {}", e)))?;

        // info!("Created StorageCredentials with SAS token");

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

        let mut blobs = Vec::new();
        let mut total_size = 0;
        let mut stream = client.list_blobs().into_stream();
        // info!("Listing blobs...");
        
        while let Some(response) = stream.next().await {
            let response = response.map_err(|e| 
                ElusionError::Custom(format!("Failed to list blobs: {}", e)))?;
            
            for blob in response.blobs.blobs() {
                if (blob.name.ends_with(".json") || blob.name.ends_with(".csv")) && 
                filter_keyword.map_or(true, |keyword| blob.name.contains(keyword)) // && blob.properties.content_length > 2048 
                {
                    println!("Adding blob '{}' to the download list", blob.name);
                    total_size += blob.properties.content_length as usize;
                    blobs.push(blob.name.clone());
                }
            }
        }

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

    #[cfg(not(feature = "azure"))]
    pub async fn from_azure_with_sas_token(
        _url: &str,
        _sas_token: &str,
        _filter_keyword: Option<&str>, 
        _alias: &str,
    ) -> ElusionResult<Self> {
        Err(ElusionError::Custom("*** Warning ***: Azure feature not enabled. Add feature under [dependencies]".to_string()))
    }

    /// Unified load function that determines the file type based on extension
    pub async fn load(
        file_path: &str,
        alias: &str,
    ) -> ElusionResult<AliasedDataFrame> {
        let path_manager = DeltaPathManager::new(file_path);
        if path_manager.is_delta_table() {
            let aliased_df = Self::load_delta(file_path, alias).await?;
            // Apply lowercase transformation
            let df_lower = lowercase_column_names(aliased_df.dataframe).await?;
            return Ok(AliasedDataFrame {
                dataframe: df_lower,
                alias: alias.to_string(),
            });
        }

        let ext = file_path
            .split('.')
            .last()
            .unwrap_or_default()
            .to_lowercase();

        let aliased_df = match ext.as_str() {
            "csv" => Self::load_csv(file_path, alias).await?,
            "json" => Self::load_json(file_path, alias).await?,
            "parquet" => Self::load_parquet(file_path, alias).await?,
            "xlsx" | "xls" => Self::load_excel(file_path, alias).await?,
            "" => return Err(ElusionError::InvalidOperation {
                operation: "File Loading".to_string(),
                reason: format!("Directory is not a Delta table and has no recognized extension: {file_path}"),
                suggestion: "💡 Provide a file with a supported extension (.csv, .json, .parquet, .xlsx, .xls) or a valid Delta table directory".to_string(),
            }),
            other => return Err(ElusionError::InvalidOperation {
                operation: "File Loading".to_string(),
                reason: format!("Unsupported file extension: {other}"),
                suggestion: "💡 Use one of the supported file types: .csv, .json, .parquet, or Delta table".to_string(),
            }),
        };

        let df_lower = lowercase_column_names(aliased_df.dataframe).await?;
        Ok(AliasedDataFrame {
            dataframe: df_lower,
            alias: alias.to_string(),
        })
    }

// -------------------- PLOTING -------------------------- //
    ///Create line plot
     #[cfg(feature = "dashboard")]
    pub async fn plot_linee(
        &self, 
        x_col: &str, 
        y_col: &str,
        show_markers: bool,
        title: Option<&str>
    ) -> ElusionResult<Plot> {
        let batches = self.df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        let batch = &batches[0];
        
        let x_idx = batch.schema().index_of(x_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", x_col, e)))?;
        let y_idx = batch.schema().index_of(y_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", y_col, e)))?;

        // Get arrays and convert to vectors
        let x_values: Vec<f64> = convert_to_f64_vec(batch.column(x_idx))?;
        let y_values: Vec<f64> = convert_to_f64_vec(batch.column(y_idx))?;

        // Sort values chronologically
        let (sorted_x, sorted_y) = sort_by_date(&x_values, &y_values);

        // Create trace with appropriate mode
        let trace = if show_markers {
            Scatter::new(sorted_x, sorted_y)
                .mode(Mode::LinesMarkers)
                .name(&format!("{} vs {}", y_col, x_col))
                .line(Line::new()
                    .color(Rgb::new(55, 128, 191))
                    .width(2.0))
                .marker(Marker::new()
                    .color(Rgb::new(55, 128, 191))
                    .size(8))
        } else {
            Scatter::new(sorted_x, sorted_y)
                .mode(Mode::Lines)
                .name(&format!("{} vs {}", y_col, x_col))
                .line(Line::new()
                    .color(Rgb::new(55, 128, 191))
                    .width(2.0))
        };
            
        let mut plot = Plot::new();
        plot.add_trace(trace);
        
        // Check if x column is a date type and set axis accordingly
        let x_axis = if matches!(batch.column(x_idx).data_type(), ArrowDataType::Date32) {
            Axis::new()
                .title(x_col.to_string())
                .grid_color(Rgb::new(229, 229, 229))
                .show_grid(true)
                .type_(plotly::layout::AxisType::Date)
        } else {
            Axis::new()
                .title(x_col.to_string())
                .grid_color(Rgb::new(229, 229, 229))
                .show_grid(true)
        };

        let layout = Layout::new()
            .title(title.unwrap_or(&format!("{} vs {}", y_col, x_col))) 
            .x_axis(x_axis)
            .y_axis(Axis::new()
                .title(y_col.to_string())     
                .grid_color(Rgb::new(229, 229, 229))
                .show_grid(true));

        plot.set_layout(layout);
        Ok(plot)
    }


    /// Create time series Plot
    #[cfg(feature = "dashboard")]
    pub async fn plot_time_seriess(
        &self,
        date_col: &str,
        value_col: &str,
        show_markers: bool,
        title: Option<&str>,
    ) -> ElusionResult<Plot> {
        let batches = self.df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        let batch = &batches[0];
        
        let x_idx = batch.schema().index_of(date_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", date_col, e)))?;
        let y_idx = batch.schema().index_of(value_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", value_col, e)))?;

        // Check if x column is a date type
        if !matches!(batch.column(x_idx).data_type(), ArrowDataType::Date32) {
            return Err(ElusionError::Custom(
                format!("Column {} must be a Date32 type for time series plot", date_col)
            ));
        }

        let x_values = convert_to_f64_vec(batch.column(x_idx))?;
        let y_values = convert_to_f64_vec(batch.column(y_idx))?;

        // Sort values chronologically
        let (sorted_x, sorted_y) = sort_by_date(&x_values, &y_values);

        let trace = if show_markers {
            Scatter::new(sorted_x, sorted_y)
                .mode(Mode::LinesMarkers)
                .name(value_col)
                .line(Line::new()
                    .color(Rgb::new(55, 128, 191))
                    .width(2.0))
                .marker(Marker::new()
                    .color(Rgb::new(55, 128, 191))
                    .size(8))
        } else {
            Scatter::new(sorted_x, sorted_y)
                .mode(Mode::Lines)
                .name(value_col)
                .line(Line::new()
                    .color(Rgb::new(55, 128, 191))
                    .width(2.0))
        };

        let mut plot = Plot::new();
        plot.add_trace(trace);

        let layout = Layout::new()
            .title(title.unwrap_or(&format!("{} over Time", value_col)))
            .x_axis(Axis::new()
                .title(date_col.to_string())
                .grid_color(Rgb::new(229, 229, 229))
                .show_grid(true)
                .type_(plotly::layout::AxisType::Date))
            .y_axis(Axis::new()
                .title(value_col.to_string())
                .grid_color(Rgb::new(229, 229, 229))
                .show_grid(true));

        plot.set_layout(layout);
        Ok(plot)
    }

    /// Create a scatter plot from two columns
    #[cfg(feature = "dashboard")]
    pub async fn plot_scatterr(
        &self,
        x_col: &str,
        y_col: &str,
        marker_size: Option<usize>,
    ) -> ElusionResult<Plot> {
        let batches = self.df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        let batch = &batches[0];
        
        let x_idx = batch.schema().index_of(x_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", x_col, e)))?;
        let y_idx = batch.schema().index_of(y_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", y_col, e)))?;

        let x_values: Vec<f64> = convert_to_f64_vec(batch.column(x_idx))?;
        let y_values: Vec<f64> = convert_to_f64_vec(batch.column(y_idx))?;

        let trace = Scatter::new(x_values, y_values)
            .mode(Mode::Markers)
            .name(&format!("{} vs {}", y_col, x_col))
            .marker(Marker::new()
                .color(Rgb::new(55, 128, 191))
                .size(marker_size.unwrap_or(8)));

        let mut plot = Plot::new();
        plot.add_trace(trace);
        
        let layout = Layout::new()
            .title(format!("Scatter Plot: {} vs {}", y_col, x_col))
            .x_axis(Axis::new().title(x_col.to_string()))
            .y_axis(Axis::new().title(y_col.to_string()));

        plot.set_layout(layout);
        Ok(plot)
    }

    /// Create a bar chart from two columns
    #[cfg(feature = "dashboard")]
    pub async fn plot_barr(
        &self,
        x_col: &str,
        y_col: &str,
        orientation: Option<&str>, 
        title: Option<&str>,
    ) -> ElusionResult<Plot> {
        let batches = self.df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        let batch = &batches[0];

        let x_idx = batch.schema().index_of(x_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", x_col, e)))?;
        let y_idx = batch.schema().index_of(y_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", y_col, e)))?;

        let (x_values, y_values) = if batch.column(x_idx).data_type() == &ArrowDataType::Utf8 {
            (convert_to_string_vec(batch.column(x_idx))?, convert_to_f64_vec(batch.column(y_idx))?)
        } else {
            (convert_to_string_vec(batch.column(y_idx))?, convert_to_f64_vec(batch.column(x_idx))?)
        };

        let trace = match orientation.unwrap_or("v") {
            "h" => {
                Bar::new(x_values.clone(), y_values.clone())
                    .orientation(Orientation::Horizontal)
                    .name(&format!("{} by {}", y_col, x_col))
            },
            _ => {
                Bar::new(x_values, y_values)
                    .orientation(Orientation::Vertical)
                    .name(&format!("{} by {}", y_col, x_col))
            }
        };

        let mut plot = Plot::new();
        plot.add_trace(trace);

        let layout = Layout::new()
            .title(title.unwrap_or(&format!("Bar Chart: {} by {}", y_col, x_col)))
            .x_axis(Axis::new().title(x_col.to_string()))
            .y_axis(Axis::new().title(y_col.to_string()));

        plot.set_layout(layout);
        Ok(plot)
    }

    /// Create a histogram from a single column
    #[cfg(feature = "dashboard")]
    pub async fn plot_histogramm(
        &self,
        col: &str,
        bins: Option<usize>,
        title: Option<&str>
    ) -> ElusionResult<Plot> {
        let batches = self.df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        let batch = &batches[0];

        let idx = batch.schema().index_of(col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", col, e)))?;

        let values = convert_to_f64_vec(batch.column(idx))?;

        let trace = Histogram::new(values)
            .name(col)
            .n_bins_x(bins.unwrap_or(30));

        let mut plot = Plot::new();
        plot.add_trace(trace);

        let layout = Layout::new()
            .title(title.unwrap_or(&format!("Histogram of {}", col)))
            .x_axis(Axis::new().title(col.to_string()))
            .y_axis(Axis::new().title("Count".to_string()));

        plot.set_layout(layout);
        Ok(plot)
    }

    /// Create a box plot from a column
    #[cfg(feature = "dashboard")]
    pub async fn plot_boxx(
        &self,
        value_col: &str,
        group_by_col: Option<&str>,
        title: Option<&str>,
    ) -> ElusionResult<Plot> {
        let batches = self.df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        let batch = &batches[0];
        
        // Get value column index
        let value_idx = batch.schema().index_of(value_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", value_col, e)))?;

        // Convert values column
        let values = convert_to_f64_vec(batch.column(value_idx))?;

        let trace = if let Some(group_col) = group_by_col {
            // Get group column index
            let group_idx = batch.schema().index_of(group_col)
                .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", group_col, e)))?;

            // Convert group column to strings
            let groups = convert_to_f64_vec(batch.column(group_idx))?;

            BoxPlot::new(values)
                .x(groups) // Groups on x-axis
                .name(value_col)
        } else {
            BoxPlot::new(values)
                .name(value_col)
        };

        let mut plot = Plot::new();
        plot.add_trace(trace);

        let layout = Layout::new()
            .title(title.unwrap_or(&format!("Distribution of {}", value_col)))
            .y_axis(Axis::new()
                .title(value_col.to_string())
                .grid_color(Rgb::new(229, 229, 229))
                .show_grid(true))
            .x_axis(Axis::new()
                .title(group_by_col.unwrap_or("").to_string())
                .grid_color(Rgb::new(229, 229, 229))
                .show_grid(true));

        plot.set_layout(layout);
        Ok(plot)
    }

     /// Create a pie chart from two columns: labels and values
     #[cfg(feature = "dashboard")]
     pub async fn plot_piee(
        &self,
        label_col: &str,
        value_col: &str,
        title: Option<&str>,
    ) -> ElusionResult<Plot> {
        let batches = self.df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        let batch = &batches[0];

        // Get column indices
        let label_idx = batch.schema().index_of(label_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", label_col, e)))?;
        let value_idx = batch.schema().index_of(value_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", value_col, e)))?;

        // Convert columns to appropriate types
        let labels = convert_to_string_vec(batch.column(label_idx))?;
        let values = convert_to_f64_vec(batch.column(value_idx))?;

        // Create the pie chart trace
        let trace = Pie::new(values)
            .labels(labels)
            .name(value_col)
            .hole(0.0);

        let mut plot = Plot::new();
        plot.add_trace(trace);

        // Create layout
        let layout = Layout::new()
            .title(title.unwrap_or(&format!("Distribution of {}", value_col)))
            .show_legend(true);

        plot.set_layout(layout);
        Ok(plot)
    }

    /// Create a donut chart (pie chart with a hole)
    #[cfg(feature = "dashboard")]
    pub async fn plot_donutt(
        &self,
        label_col: &str,
        value_col: &str,
        title: Option<&str>,
        hole_size: Option<f64>, // Value between 0 and 1
    ) -> ElusionResult<Plot> {
        let batches = self.df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        let batch = &batches[0];

        let label_idx = batch.schema().index_of(label_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", label_col, e)))?;
        let value_idx = batch.schema().index_of(value_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", value_col, e)))?;

        let labels = convert_to_string_vec(batch.column(label_idx))?;
        let values = convert_to_f64_vec(batch.column(value_idx))?;

        // Ensure hole size is between 0 and 1
        let hole_size = hole_size.unwrap_or(0.5).max(0.0).min(1.0);

        let trace = Pie::new(values)
            .labels(labels)
            .name(value_col)
            .hole(hole_size); 

        let mut plot = Plot::new();
        plot.add_trace(trace);

        let layout = Layout::new()
            .title(title.unwrap_or(&format!("Distribution of {}", value_col)))
            .show_legend(true);

        plot.set_layout(layout);
        Ok(plot)
    }

    // -------------Interactive Charts
    #[cfg(feature = "dashboard")]
    pub async fn plot_line(
        &self,
        date_col: &str,
        value_col: &str,
        show_markers: bool,
        title: Option<&str>,
    ) -> ElusionResult<Plot> {
        let mut plot = self.plot_linee(date_col, value_col, show_markers, title).await?;

        // Create range selector buttons
        let buttons = vec![
            Button::new()
                .name("1m")
                .args(json!({
                    "xaxis.range": ["now-1month", "now"]
                }))
                .label("1m"),
            Button::new()
                .name("6m")
                .args(json!({
                    "xaxis.range": ["now-6months", "now"]
                }))
                .label("6m"),
            Button::new()
                .name("1y")
                .args(json!({
                    "xaxis.range": ["now-1year", "now"]
                }))
                .label("1y"),
            Button::new()
                .name("YTD")
                .args(json!({
                    "xaxis.range": ["now-ytd", "now"]
                }))
                .label("YTD"),
            Button::new()
                .name("all")
                .args(json!({
                    "xaxis.autorange": true
                }))
                .label("All")
        ];

        // Update layout with range selector and slider
        let layout = plot.layout().clone()
            .x_axis(Axis::new()
                .title(date_col.to_string())
                .grid_color(Rgb::new(229, 229, 229))
                .show_grid(true)
                .type_(plotly::layout::AxisType::Date)
                .range_slider(RangeSlider::new().visible(true)))
            .update_menus(vec![
                UpdateMenu::new()
                    .buttons(buttons)
                    .direction(UpdateMenuDirection::Down)
                    .show_active(true)
            ])
            .drag_mode(DragMode::Zoom);

        plot.set_layout(layout);
        Ok(plot)
    }

    #[cfg(not(feature = "dashboard"))]
    pub async fn plot_line(
        &self,
        _date_col: &str,
        _value_col: &str,
        _show_markers: bool,
        _title: Option<&str>,
    ) -> ElusionResult<Plot> {
        println!("*** Warning ***: Dashboard feature not enabled. Add feature under [dependencies]");
        Ok(Plot)
    }


     /// Create an enhanced time series plot with range selector buttons
     #[cfg(feature = "dashboard")]
     pub async fn plot_time_series(
        &self,
        date_col: &str,
        value_col: &str,
        show_markers: bool,
        title: Option<&str>,
    ) -> ElusionResult<Plot> {
        let mut plot = self.plot_time_seriess(date_col, value_col, show_markers, title).await?;

        // Create range selector buttons
        let buttons = vec![
            Button::new()
                .name("1m")
                .args(json!({
                    "xaxis.range": ["now-1month", "now"]
                }))
                .label("1m"),
            Button::new()
                .name("6m")
                .args(json!({
                    "xaxis.range": ["now-6months", "now"]
                }))
                .label("6m"),
            Button::new()
                .name("1y")
                .args(json!({
                    "xaxis.range": ["now-1year", "now"]
                }))
                .label("1y"),
            Button::new()
                .name("YTD")
                .args(json!({
                    "xaxis.range": ["now-ytd", "now"]
                }))
                .label("YTD"),
            Button::new()
                .name("all")
                .args(json!({
                    "xaxis.autorange": true
                }))
                .label("All")
        ];

        // Update layout with range selector and slider
        let layout = plot.layout().clone()
            .x_axis(Axis::new()
                .title(date_col.to_string())
                .grid_color(Rgb::new(229, 229, 229))
                .show_grid(true)
                .type_(plotly::layout::AxisType::Date)
                .range_slider(RangeSlider::new().visible(true)))
            .update_menus(vec![
                UpdateMenu::new()
                    .buttons(buttons)
                    .direction(UpdateMenuDirection::Down)
                    .show_active(true)
            ])
            .drag_mode(DragMode::Zoom);

        plot.set_layout(layout);
        Ok(plot)
    }

    #[cfg(not(feature = "dashboard"))]
    pub async fn plot_time_series(
        &self,
        _date_col: &str,
        _value_col: &str,
        _show_markers: bool,
        _title: Option<&str>,
    ) -> ElusionResult<Plot> {
        println!("*** Warning ***: Dashboard feature not enabled. Add feature under [dependencies]");
        Ok(Plot)
    }

    /// Create an enhanced bar chart with sort buttons
    #[cfg(feature = "dashboard")]
    pub async fn plot_bar(
        &self,
        x_col: &str,
        y_col: &str,
        title: Option<&str>,
    ) -> ElusionResult<Plot> {
        let mut plot = self.plot_barr(x_col, y_col, None, title).await?;

        // Create sort buttons
        let update_menu_buttons = vec![
            Button::new()
                .name("reset")
                .args(json!({
                    "xaxis.type": "category",
                    "xaxis.categoryorder": "trace"
                }))
                .label("Reset"),
            Button::new()
                .name("ascending")
                .args(json!({
                    "xaxis.type": "category",
                    "xaxis.categoryorder": "total ascending"
                }))
                .label("Sort Ascending"),
            Button::new()
                .name("descending")
                .args(json!({
                    "xaxis.type": "category",
                    "xaxis.categoryorder": "total descending"
                }))
                .label("Sort Descending")
        ];

        // Update layout with buttons
        let layout = plot.layout().clone()
            .show_legend(true)
            .update_menus(vec![
                UpdateMenu::new()
                    .buttons(update_menu_buttons)
                    .direction(UpdateMenuDirection::Down)
                    .show_active(true)
            ]);

        plot.set_layout(layout);
        Ok(plot)
    }
    #[cfg(not(feature = "dashboard"))]
    pub async fn plot_bar(
        &self,
        _x_col: &str,
        _y_col: &str,
        _title: Option<&str>,
    ) -> ElusionResult<Plot> {
        println!("*** Warning ***: Dashboard feature not enabled. Add feature under [dependencies]");
        Ok(Plot)
    }

    /// Create an enhanced scatter plot with zoom and selection modes
    #[cfg(feature = "dashboard")]
    pub async fn plot_scatter(
        &self,
        x_col: &str,
        y_col: &str,
        marker_size: Option<usize>,
    ) -> ElusionResult<Plot> {
        let mut plot = self.plot_scatterr(x_col, y_col, marker_size).await?;

        // Create mode buttons
        let mode_buttons = vec![
            Button::new()
                .name("zoom")
                .args(json!({
                    "dragmode": "zoom"
                }))
                .label("Zoom"),
            Button::new()
                .name("select")
                .args(json!({
                    "dragmode": "select"
                }))
                .label("Select"),
            Button::new()
                .name("pan")
                .args(json!({
                    "dragmode": "pan"
                }))
                .label("Pan")
        ];

        // Update layout with buttons
        let layout = plot.layout().clone()
            .show_legend(true)
            .drag_mode(DragMode::Zoom)
            .update_menus(vec![
                UpdateMenu::new()
                    .buttons(mode_buttons)
                    .direction(UpdateMenuDirection::Down)
                    .show_active(true)
            ]);
            
        plot.set_layout(layout);
        Ok(plot)
    }

    #[cfg(not(feature = "dashboard"))]
    pub async fn plot_scatter(
        &self,
        _x_col: &str,
        _y_col: &str,
        _marker_size: Option<usize>,
    ) -> ElusionResult<Plot> {
        println!("*** Warning ***: Dashboard feature not enabled. Add feature under [dependencies]");
        Ok(Plot)
    }

    ///Interactive histogram plot
    #[cfg(feature = "dashboard")]
    pub async fn plot_histogram(
        &self,
        col: &str,
        title: Option<&str>,
    ) -> ElusionResult<Plot> {
        let mut plot = self.plot_histogramm(col, None, title).await?;
    
        // Create binning control buttons
        let bin_buttons = vec![
            Button::new()
                .name("bins10")
                .args(json!({
                    "xbins.size": 10
                }))
                .label("10 Bins"),
            Button::new()
                .name("bins20")
                .args(json!({
                    "xbins.size": 20
                }))
                .label("20 Bins"),
            Button::new()
                .name("bins30")
                .args(json!({
                    "xbins.size": 30
                }))
                .label("30 Bins")
        ];
    
        let layout = plot.layout().clone()
            .show_legend(true)
            .update_menus(vec![
                UpdateMenu::new()
                    .buttons(bin_buttons)
                    .direction(UpdateMenuDirection::Down)
                    .show_active(true)
            ]);
    
        plot.set_layout(layout);
        Ok(plot)
    }

    #[cfg(not(feature = "dashboard"))]
    pub async fn plot_histogram(
        &self,
        _col: &str,
        _title: Option<&str>
    ) -> ElusionResult<Plot> {
        println!("*** Warning ***: Dashboard feature not enabled. Add feature under [dependencies]");
        Ok(Plot)
    }

    ///Interactive Box plot
    #[cfg(feature = "dashboard")]
    pub async fn plot_box(
        &self,
        value_col: &str,
        group_by_col: Option<&str>,
        title: Option<&str>,
    ) -> ElusionResult<Plot> {
        let mut plot = self.plot_boxx(value_col, group_by_col, title).await?;
    
        // Create outlier control buttons
        let outlier_buttons = vec![
            Button::new()
                .name("show_outliers")
                .args(json!({
                    "boxpoints": "outliers"
                }))
                .label("Show Outliers"),
            Button::new()
                .name("hide_outliers")
                .args(json!({
                    "boxpoints": false
                }))
                .label("Hide Outliers")
        ];
    
        let layout = plot.layout().clone()
            .show_legend(true)
            .update_menus(vec![
                UpdateMenu::new()
                    .buttons(outlier_buttons)
                    .direction(UpdateMenuDirection::Down)
                    .show_active(true)
            ]);
    
        plot.set_layout(layout);
        Ok(plot)
    }

    #[cfg(not(feature = "dashboard"))]
    pub async fn plot_box(
        &self,
        _value_col: &str,
        _group_by_col: Option<&str>,
        _title: Option<&str>
    ) -> ElusionResult<Plot> {
        println!("*** Warning ***: Dashboard feature not enabled. Add feature under [dependencies]");
        Ok(Plot)
    }

    ///Interactive Pie Plot
    #[cfg(feature = "dashboard")]
    pub async fn plot_pie(
        &self,
        label_col: &str,
        value_col: &str,
        title: Option<&str>,
    ) -> ElusionResult<Plot> {
        let mut plot = self.plot_piee(label_col, value_col, title).await?;
    
        // Create display mode buttons
        let display_buttons = vec![
            Button::new()
                .name("percentage")
                .args(json!({
                    "textinfo": "percent"
                }))
                .label("Show Percentages"),
            Button::new()
                .name("values")
                .args(json!({
                    "textinfo": "value"
                }))
                .label("Show Values"),
            Button::new()
                .name("both")
                .args(json!({
                    "textinfo": "value+percent"
                }))
                .label("Show Both")
        ];
    
        let layout = plot.layout().clone()
            .show_legend(true)
            .update_menus(vec![
                UpdateMenu::new()
                    .buttons(display_buttons)
                    .direction(UpdateMenuDirection::Down)
                    .show_active(true)
            ]);
    
        plot.set_layout(layout);
        Ok(plot)
    }

    #[cfg(not(feature = "dashboard"))]
    pub async fn plot_pie(
        &self,
        _label_col: &str,
        _value_col: &str,
        _title: Option<&str>
    ) -> ElusionResult<Plot> {
        println!("*** Warning ***: Dashboard feature not enabled. Add feature under [dependencies]");
        Ok(Plot)
    }

    ///Interactive Donut Plot
    #[cfg(feature = "dashboard")]
    pub async fn plot_donut(
        &self,
        label_col: &str,
        value_col: &str,
        title: Option<&str>,
    ) -> ElusionResult<Plot> {
        let mut plot = self.plot_donutt(label_col, value_col, title, Some(0.5)).await?;
    
        // Create hole size control buttons
        let hole_buttons = vec![
            Button::new()
                .name("small")
                .args(json!({
                    "hole": 0.3
                }))
                .label("Small Hole"),
            Button::new()
                .name("medium")
                .args(json!({
                    "hole": 0.5
                }))
                .label("Medium Hole"),
            Button::new()
                .name("large")
                .args(json!({
                    "hole": 0.7
                }))
                .label("Large Hole")
        ];
    
        let layout = plot.layout().clone()
            .show_legend(true)
            .update_menus(vec![
                UpdateMenu::new()
                    .buttons(hole_buttons)
                    .direction(UpdateMenuDirection::Down)
                    .show_active(true)
            ]);
    
        plot.set_layout(layout);
        Ok(plot)
    }

    #[cfg(not(feature = "dashboard"))]
    pub async fn plot_donut(
        &self,
        _label_col: &str,
        _value_col: &str,
        _title: Option<&str>
    ) -> ElusionResult<Plot> {
        println!("*** Warning ***: Dashboard feature not enabled. Add feature under [dependencies]");
        Ok(Plot)
    }

    /// Create an enhanced report with interactive features
    #[cfg(feature = "dashboard")]
    pub async fn create_report(
        plots: Option<&[(&Plot, &str)]>,
        tables: Option<&[(&CustomDataFrame, &str)]>,
        report_title: &str,
        filename: &str,  // Full path including filename
        layout_config: Option<ReportLayout>,
        table_options: Option<TableOptions>, 
    ) -> ElusionResult<()> {
        
        if let Some(parent) = LocalPath::new(filename).parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)?;
            }
        }
    
        let file_path_str = LocalPath::new(filename).to_str()
            .ok_or_else(|| ElusionError::Custom("Invalid path".to_string()))?;
    
        // Get layout configuration
        let layout = layout_config.unwrap_or_default();
    
        // Create plot containers HTML if plots are provided
        let plot_containers = plots.map(|plots| {
            plots.iter().enumerate()
                .map(|(i, (plot, title))| format!(
                    r#"<div class="plot-container" 
                        data-plot-data='{}'
                        data-plot-layout='{}'>
                        <div class="plot-title">{}</div>
                        <div id="plot_{}" style="width:100%;height:{}px;"></div>
                    </div>"#,
                    serde_json::to_string(plot.data()).unwrap(),
                    serde_json::to_string(plot.layout()).unwrap(),
                    title,
                    i,
                    layout.plot_height
                ))
                .collect::<Vec<_>>()
                .join("\n")
        }).unwrap_or_default();
    
        // Create table containers HTML if tables are provided
        let table_containers = if let Some(tables) = tables {

            let table_op = TableOptions::default();
            let table_opts = table_options.as_ref().unwrap_or(&table_op);

            let mut containers = Vec::new();
            for (i, (df, title)) in tables.iter().enumerate() {
                // Access the inner DataFrame with df.df
                let batches = df.df.clone().collect().await?;
                let schema = df.df.schema();
                let columns = schema.fields().iter()
                    .map(|f| {
                        let base_def = format!(
                            r#"{{
                                field: "{}",
                                headerName: "{}",
                                sortable: true,
                                filter: true,
                                resizable: true"#,
                            f.name(),
                            f.name()
                        );

                        // Add date-specific formatting for date columns and potential string dates
                        let column_def = match f.data_type() {
                            ArrowDataType::Date32 | ArrowDataType::Date64 | ArrowDataType::Timestamp(_, _) => {
                                format!(
                                    r#"{},
                                    filter: 'agDateColumnFilter',
                                    filterParams: {{
                                        browserDatePicker: true,
                                        minValidYear: 1000,
                                        maxValidYear: 9999
                                    }}"#,
                                    base_def
                                )
                            },
                            ArrowDataType::Utf8 if f.name().to_lowercase().contains("date") || 
                                                f.name().to_lowercase().contains("time") => {
                                format!(
                                    r#"{},
                                    filter: 'agDateColumnFilter',
                                    filterParams: {{
                                        browserDatePicker: true,
                                        minValidYear: 1000,
                                        maxValidYear: 9999,
                                        comparator: (filterValue, cellValue) => {{
                                            try {{
                                                const filterDate = new Date(filterValue);
                                                const cellDate = new Date(cellValue);
                                                if (!isNaN(filterDate) && !isNaN(cellDate)) {{
                                                    return cellDate - filterDate;
                                                }}
                                            }} catch (e) {{}}
                                            return 0;
                                        }}
                                    }}"#,
                                    base_def
                                )
                            },
                            _ => base_def,
                        };

                        // Close the column definition object
                        format!("{}}}", column_def)
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                
                // Convert batches to rows
                let mut rows = Vec::new();
                for batch in &batches {
                    for row_idx in 0..batch.num_rows() {
                        let mut row = serde_json::Map::new();
                        for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                            let col = batch.column(col_idx);
                            let value = match col.data_type() {
                                ArrowDataType::Int32 => {
                                    let array = col.as_any().downcast_ref::<Int32Array>().unwrap();
                                    if array.is_null(row_idx) {
                                        serde_json::Value::Null
                                    } else {
                                        serde_json::Value::Number(array.value(row_idx).into())
                                    }
                                },
                                ArrowDataType::Int64 => {
                                    let array = col.as_any().downcast_ref::<Int64Array>().unwrap();
                                    if array.is_null(row_idx) {
                                        serde_json::Value::Null
                                    } else {
                                        serde_json::Value::Number(array.value(row_idx).into())
                                    }
                                },
                                ArrowDataType::Float64 => {
                                    let array = col.as_any().downcast_ref::<Float64Array>().unwrap();
                                    if array.is_null(row_idx) {
                                        serde_json::Value::Null
                                    } else {
                                        let num = array.value(row_idx);
                                        if num.is_finite() {
                                            serde_json::Number::from_f64(num)
                                                .map(serde_json::Value::Number)
                                                .unwrap_or(serde_json::Value::Null)
                                        } else {
                                            serde_json::Value::Null
                                        }
                                    }
                                },
                                ArrowDataType::Date32 => {
                                    let array = col.as_any().downcast_ref::<Date32Array>().unwrap();
                                    if array.is_null(row_idx) {
                                        serde_json::Value::Null
                                    } else {
                                        let days = array.value(row_idx);
                                        // Using your convert_date32_to_timestamps logic but adapting for a single value
                                        let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163)
                                            .unwrap_or(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
                                        let datetime = date.and_hms_opt(0, 0, 0).unwrap();
                                        serde_json::Value::String(datetime.format("%Y-%m-%d").to_string())
                                    }
                                },
                                ArrowDataType::Date64 => {
                                    let array = col.as_any().downcast_ref::<Date64Array>().unwrap();
                                    if array.is_null(row_idx) {
                                        serde_json::Value::Null
                                    } else {
                                        let ms = array.value(row_idx);
                                        let datetime = chrono::DateTime::from_timestamp_millis(ms)
                                            .unwrap_or_default()
                                            .naive_utc();
                                        serde_json::Value::String(datetime.format("%Y-%m-%d %H:%M:%S").to_string())
                                    }
                                },
                                ArrowDataType::Timestamp(time_unit, None) => {
                                    let array = col.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                                    if array.is_null(row_idx) {
                                        serde_json::Value::Null
                                    } else {
                                        let ts = array.value(row_idx);
                                        let datetime = match time_unit {
                                            TimeUnit::Second => chrono::DateTime::from_timestamp(ts, 0),
                                            TimeUnit::Millisecond => chrono::DateTime::from_timestamp_millis(ts),
                                            TimeUnit::Microsecond => chrono::DateTime::from_timestamp_micros(ts),
                                            TimeUnit::Nanosecond => chrono::DateTime::from_timestamp(
                                                ts / 1_000_000_000,
                                                (ts % 1_000_000_000) as u32
                                            ),
                                        }.unwrap_or_default().naive_utc();
                                        serde_json::Value::String(datetime.format("%Y-%m-%d %H:%M:%S").to_string())
                                    }
                                },
                                ArrowDataType::Utf8 => {
                                    let array = col.as_any().downcast_ref::<StringArray>().unwrap();
                                    if array.is_null(row_idx) {
                                        serde_json::Value::Null
                                    } else {
                                        let value = array.value(row_idx);
                                        // Try to parse as date if the column name suggests it might be a date
                                        if field.name().to_lowercase().contains("date") || 
                                           field.name().to_lowercase().contains("time") {
                                            if let Some(datetime) = parse_date_string(value) {
                                                serde_json::Value::String(
                                                    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
                                                )
                                            } else {
                                                serde_json::Value::String(value.to_string())
                                            }
                                        } else {
                                            serde_json::Value::String(value.to_string())
                                        }
                                    }
                                },
                                _ => serde_json::Value::Null,
                            };
                            row.insert(field.name().clone(), value);
                        }
                        rows.push(serde_json::Value::Object(row));
                    }
                }

                let container = format!(
                    r#"<div class="table-container">
                        <div class="table-title">{0}</div>
                        <div id="grid_{1}" class="{2}" style="width:100%;height:{3}px;">
                            <!-- AG Grid will be rendered here -->
                        </div>
                        <script>
                            (function() {{
                                console.log('Initializing grid_{1}');
                                
                                // Column definitions with more detailed configuration
                                const columnDefs = [{4}];
                                console.log('Column definitions:', columnDefs);
                
                                const rowData = {5};
                                console.log('Row data:', rowData);
                
                                // Grid options with more features
                                const gridOptions = {{
                                    columnDefs: columnDefs,
                                    rowData: rowData,
                                    pagination: {6},
                                    paginationPageSize: {7},
                                    defaultColDef: {{
                                        flex: 1,
                                        minWidth: 100,
                                        sortable: {8},
                                        filter: {9},
                                        floatingFilter: true,
                                        resizable: true,
                                        cellClass: 'ag-cell-font-size'
                                    }},
                                    onGridReady: function(params) {{
                                        console.log('Grid Ready event fired for grid_{1}');
                                        params.api.sizeColumnsToFit();
                                        const event = new CustomEvent('gridReady');
                                        gridDiv.dispatchEvent(event);
                                    }},
                                    enableRangeSelection: true,
                                    enableCharts: true,
                                    popupParent: document.body,
                                    // Add styling options
                                    headerClass: "ag-header-cell",
                                    rowClass: "ag-row-font-size",
                                    sideBar: {{
                                        toolPanels: ['columns', 'filters'],
                                        defaultToolPanel: '',
                                        hiddenByDefault: {10}
                                    }}
                                }};
                
                                // Initialize AG Grid
                                const gridDiv = document.querySelector('#grid_{1}');
                                console.log('Grid container:', gridDiv);
                                console.log('AG Grid loaded:', typeof agGrid !== 'undefined');
                                
                                if (!gridDiv) {{
                                    console.error('Grid container not found for grid_{1}');
                                    return;
                                }}
                                
                                try {{
                                    new agGrid.Grid(gridDiv, gridOptions);
                                    gridDiv.gridOptions = gridOptions;
                                }} catch (error) {{
                                    console.error('Error initializing AG Grid:', error);
                                }}
                            }})();
                        </script>
                    </div>"#,
                    title,                   // {0}
                    i,                      // {1}
                    table_opts.theme,       // {2}
                    layout.table_height,    // {3}
                    columns,               // {4} Column definitions
                    serde_json::to_string(&rows).unwrap_or_default(),  // {5}
                    table_opts.pagination,  // {6}
                    table_opts.page_size,   // {7}
                    table_opts.enable_sorting,  // {8}
                    table_opts.enable_filtering,  // {9}
                    !table_opts.enable_column_menu  // {10}
                );
                containers.push(container);
            }
            containers.join("\n")
        } else {
            String::new()
        };
    
        let html_content = format!(
            r#"<!DOCTYPE html>
            <html>
            <head>
                <title>{0}</title>
                {1}
                {2}
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                        margin: 0;
                        padding: 20px;
                        background-color: #f5f5f5;
                    }}
                    .container {{
                        max-width: {3}px;
                        margin: 0 auto;
                        background-color: white;
                        padding: 20px;
                        border-radius: 8px;
                        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                    }}
                    h1 {{
                        color: #333;
                        text-align: center;
                        margin-bottom: 30px;
                    }}
                    .controls {{
                        margin-bottom: 20px;
                        padding: 15px;
                        background: #f8f9fa;
                        border-radius: 8px;
                        display: flex;
                        gap: 10px;
                        justify-content: center;
                    }}
                    .controls button {{
                        padding: 8px 16px;
                        border: none;
                        border-radius: 4px;
                        background: #007bff;
                        color: white;
                        cursor: pointer;
                        transition: background 0.2s;
                    }}
                    .controls button:hover {{
                        background: #0056b3;
                    }}
                    .controls button {{
                        padding: 8px 16px;
                        border: none;
                        border-radius: 4px;
                        background: #007bff;
                        color: white;
                        cursor: pointer;
                        transition: background 0.2s;
                    }}
                    .controls button:hover {{
                        background: #0056b3;
                    }}
                    .controls button.export-button {{
                        background: #28a745; 
                    }}
                    .controls button.export-button:hover {{
                        background: #218838; 
                    }}
                    .grid {{
                        display: grid;
                        grid-template-columns: repeat({4}, 1fr);
                        gap: {5}px;
                    }}
                    .plot-container, .table-container {{
                        background: white;
                        padding: 15px;
                        border-radius: 8px;
                        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
                    }}
                    .plot-title, .table-title {{
                        font-size: 18px;
                        font-weight: bold;
                        margin-bottom: 10px;
                        color: #444;
                    }}
                    @media (max-width: 768px) {{
                        .grid {{
                            grid-template-columns: 1fr;
                        }}
                    }}
                    .loading {{
                        display: none;
                        position: fixed;
                        top: 50%;
                        left: 50%;
                        transform: translate(-50%, -50%);
                        background: rgba(255,255,255,0.9);
                        padding: 20px;
                        border-radius: 8px;
                        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                    }}
                    .ag-cell-font-size {{
                        font-size: 17px;
                    }}

                    .ag-cell-bold {{
                        font-weight: bold;
                    }}

                    .ag-header-cell {{
                        font-weight: bold;
                        border-bottom: 1px solid #3fdb59;
                    }}

                    .align-right {{
                        text-align: left;
                    }}

                    .ag-theme-alpine {{
                        --ag-font-size: 17px;
                        --ag-header-height: 40px;
                    }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>{0}</h1>
                    <div class="controls">
                        {6}
                    </div>
                    <div id="loading" class="loading">Processing...</div>
                    {7}
                    {8}
                </div>
                <script>
                    {9}
                </script>
            </body>
            </html>"#,
            report_title,                // {0}
            if plots.is_some() {         // {1}
                r#"<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>"#
            } else { "" },
            if tables.is_some() {        // {2}
                r#"
                <script>
                    // Check if AG Grid is already loaded
                    console.log('AG Grid script loading status:', typeof agGrid !== 'undefined');
                </script>
                <script src="https://cdn.jsdelivr.net/npm/ag-grid-community@31.0.1/dist/ag-grid-community.min.js"></script>
                <script src="https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js"></script>
                <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/ag-grid-community@31.0.1/styles/ag-grid.css">
                <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/ag-grid-community@31.0.1/styles/ag-theme-alpine.css">
                <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/ag-grid-community@31.0.1/styles/ag-theme-quartz.css">
                <script>
                    // Verify AG Grid loaded correctly
                    document.addEventListener('DOMContentLoaded', function() {
                        console.log('AG Grid loaded check:', typeof agGrid !== 'undefined');
                        console.log('XLSX loaded check:', typeof XLSX !== 'undefined');
                    });
                </script>
                "#
            } else { "" },
            layout.max_width,            // {3}
            layout.grid_columns,         // {4}
            layout.grid_gap,            // {5}
            generate_controls(plots.is_some(), tables.is_some()),  // {6}
            if !plot_containers.is_empty() {  // {7}
                format!(r#"<div class="grid">{}</div>"#, plot_containers)
            } else { String::new() },
            if !table_containers.is_empty() {  // {8}
                format!(r#"<div class="tables">{}</div>"#, table_containers)
            } else { String::new() },
            generate_javascript(plots.is_some(), tables.is_some(), layout.grid_columns)  // {9}
        );
    
        std::fs::write(file_path_str, html_content)?;
        println!("✅ Interactive Dashboard created at {}", file_path_str);
        Ok(())
    }

    #[cfg(not(feature = "dashboard"))]
    pub async fn create_report(
        _plots: Option<&[(&Plot, &str)]>,
        _tables: Option<&[(&CustomDataFrame, &str)]>,
        _report_title: &str,
        _filename: &str,
        _layout_config: Option<ReportLayout>,
        _table_options: Option<TableOptions>,
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom("*** Warning ***: Dashboard feature not enabled. Add feature under [dependencies]".to_string()))
    }
    
}
// ============== PIPELINE SCHEDULER
#[derive(Clone)]
pub struct PipelineScheduler {
    scheduler: JobScheduler,
}

#[derive(Debug)]
pub enum SchedulerError {
    InvalidTime(String),
    InvalidFrequency(String),
    JobFailed(String),
}

impl PipelineScheduler {
    /// Creates new Pipeline Scheduler
    pub async fn new<F, Fut>(frequency: &str, job: F) -> ElusionResult<Self> 
    where
    F: Fn() -> Fut + Send + Sync + 'static,
   Fut: Future<Output = ElusionResult<()>> + Send + 'static
{
    println!("Initializing JobScheduler");

    let scheduler = JobScheduler::new().await
        .map_err(|e| ElusionError::Custom(format!("Scheduler init failed: {}", e)))?;
    println!("Jobs are scheduled, and will run with frequency: '{}'", frequency);
        
    let cron = Self::parse_schedule(frequency)?;
    // debug!("Cron expression: {}", cron);

    let job_fn = Arc::new(job);

    let job = Job::new_async(&cron, move |uuid, mut l| {
        let job_fn = job_fn.clone();
        Box::pin(async move {
            let future = job_fn();
            future.await.unwrap_or_else(|e| eprintln!("❌ Job execution failed: {}", e));
            
            let next_tick = l.next_tick_for_job(uuid).await;
            match next_tick {
                Ok(Some(ts)) => println!("Next job execution: {:?} UTC Time", ts),
                _ => println!("Could not determine next job execution"),
            }
        })
    }).map_err(|e| ElusionError::Custom(format!("❌ Job creation failed: {}", e)))?;


        scheduler.add(job).await
            .map_err(|e| ElusionError::Custom(format!("❌ Job scheduling failed: {}", e)))?;
            
        scheduler.start().await
            .map_err(|e| ElusionError::Custom(format!("❌ Scheduler start failed: {}", e)))?;
        
       println!("JobScheduler successfully initialized and started.");

        Ok(Self { scheduler })
    }

    fn parse_schedule(frequency: &str) -> ElusionResult<String> {
        let cron = match frequency.to_lowercase().as_str() {
            "1min" => "0 */1 * * * *".to_string(),
            "2min" => "0 */2 * * * *".to_string(),
            "5min" => "0 */5 * * * *".to_string(),
            "10min" => "0 */10 * * * *".to_string(),
            "15min" => "0 */15 * * * *".to_string(),
            "30min" => "0 */30 * * * *".to_string(),
            "1h" => "0 0 * * * *".to_string(),
            "2h" => "0 0 */2 * * *".to_string(),
            "3h" => "0 0 */3 * * *".to_string(),
            "4h" => "0 0 */4 * * *".to_string(),
            "5h" => "0 0 */5 * * *".to_string(),
            "6h" => "0 0 */6 * * *".to_string(),
            "7h" => "0 0 */7 * * *".to_string(),
            "8h" => "0 0 */8 * * *".to_string(),
            "9h" => "0 0 */9 * * *".to_string(),
            "10h" => "0 0 */10 * * *".to_string(),
            "11h" => "0 0 */11 * * *".to_string(),
            "12h" => "0 0 */12 * * *".to_string(),
            "24h" => "0 0 0 * * *".to_string(),
            "2days" => "0 0 0 */2 * *".to_string(),
            "3days" => "0 0 0 */3 * *".to_string(),
            "4days" => "0 0 0 */4 * *".to_string(),
            "5days" => "0 0 0 */5 * *".to_string(),
            "6days" => "0 0 0 */6 * *".to_string(),
            "7days" => "0 0 0 */7 * *".to_string(),
            "14days" => "0 0 0 */14 * *".to_string(),
            "30days" => "0 0 1 */1 * *".to_string(),
            _ => return Err(ElusionError::Custom(
                "Invalid frequency. Use: 1min,2min,5min,10min,15min,30min,
                1h,2h,3h,4h,5h,6h,7h,8h,9h,10h,11h,12h,24h,
                2days,3days,4days,5days,6days,7days,14days,30days".into()
            ))
        };

        Ok(cron)
    }
    /// Shuts down pipeline job execution
    pub async fn shutdown(mut self) -> ElusionResult<()> {
        println!("Shutdown is ready if needed with -> Ctr+C");
        tokio::signal::ctrl_c().await
            .map_err(|e| ElusionError::Custom(format!("❌ Ctrl+C handler failed: {}", e)))?;
        self.scheduler.shutdown().await
            .map_err(|e| ElusionError::Custom(format!("❌ Shutdown failed: {}", e)))
    }
}


// ================ ELUSION API
#[derive(Clone)]
pub struct ElusionApi;

#[cfg(feature = "api")]
enum JsonType {
    Array,
    Object,
}

#[cfg(feature = "api")]
fn validate_https_url(url: &str) -> ElusionResult<()> {
    if !url.starts_with("https://") {
        return Err(ElusionError::Custom("URL must start with 'https://'".to_string()));
    }
    Ok(())
}

impl ElusionApi{

    pub fn new () -> Self {
        Self
    }

/// Create a JSON from a REST API endpoint that returns JSON
#[cfg(feature = "api")]
pub async fn from_api(
    &self,  
    url: &str,
    file_path: &str
) -> ElusionResult<()> {
    validate_https_url(url)?;
    let client = Client::new();
    let response = client.get(url)
        .send()
        .await
        .map_err(|e| ElusionError::Custom(format!("❌ HTTP request failed: {}", e)))?;
    
    let content = response.bytes()
        .await
        .map_err(|e| ElusionError::Custom(format!("Failed to get response content: {}", e)))?;

    println!("Generated URL: {}", url);

    Self::save_json_to_file(content, file_path).await
}

#[cfg(not(feature = "api"))]
pub async fn from_api(
    &self,  
    _url: &str,
    _file_path: &str
) -> ElusionResult<()> {
    Err(ElusionError::Custom("*** Warning ***: API feature not enabled. Add feature under [dependencies]".to_string()))
}

/// Create a JSON from a REST API endpoint with custom headers
#[cfg(feature = "api")]
pub async fn from_api_with_headers(
    &self,
    url: &str, 
    headers: HashMap<String, String>,
    file_path: &str
) -> ElusionResult<()> {
    validate_https_url(url)?;
    let client = Client::new();
    let mut request = client.get(url);
    
    for (key, value) in headers {
        request = request.header(&key, value);
    }
    
    let response = request
        .send()
        .await
        .map_err(|e| ElusionError::Custom(format!("❌ HTTP request failed: {}", e)))?;

    let content = response.bytes()
        .await
        .map_err(|e| ElusionError::Custom(format!("Failed to get response content: {}", e)))?;
    println!("Generated URL: {}", url);

    Self::save_json_to_file(content, file_path).await
}
#[cfg(not(feature = "api"))]
    pub async fn from_api_with_headers(
        &self,
        _url: &str, 
        _headers: HashMap<String, String>,
        _file_path: &str
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom("*** Warning ***: API feature not enabled. Add feature under [dependencies]".to_string()))
    }

/// Create JSON from API with custom query parameters
#[cfg(feature = "api")]
pub async fn from_api_with_params(
    &self,
    base_url: &str, 
    params: HashMap<&str, &str>,
    file_path: &str
) -> ElusionResult<()> {
    validate_https_url(base_url)?;

    if params.is_empty() {
        return Self::from_api( &self, base_url, file_path).await;
    }

    let query_string: String = params
        .iter()
        .map(|(k, v)| {
            // Only encode if the value contains a space
            if v.contains(' ') {
                format!("{}={}", k, v)
            } else {
                format!("{}={}", urlencoding::encode(k), urlencoding::encode(v))
            }
        })
        .collect::<Vec<String>>()
        .join("&");

    let url = format!("{}?{}", base_url, query_string);

    Self::from_api( &self, &url, file_path).await
}
#[cfg(not(feature = "api"))]
pub async fn from_api_with_params(
    &self,
    _base_url: &str, 
    _params: HashMap<&str, &str>,
    _file_path: &str
) -> ElusionResult<()> {
    Err(ElusionError::Custom("*** Warning ***: API feature not enabled. Add feature under [dependencies]".to_string()))
}

/// Create JSON from API with parameters and headers
#[cfg(feature = "api")]
pub async fn from_api_with_params_and_headers(
    &self,
    base_url: &str,
    params: HashMap<&str, &str>,
    headers: HashMap<String, String>,
    file_path: &str
) -> ElusionResult<()> {
    if params.is_empty() {
        return Self::from_api_with_headers( &self, base_url, headers, file_path).await;
    }

    let query_string: String = params
        .iter()
        .map(|(k, v)| {
            // Only encode if the key or value contains no spaces
            if !k.contains(' ') && !v.contains(' ') {
                format!("{}={}", 
                    urlencoding::encode(k), 
                    urlencoding::encode(v)
                )
            } else {
                format!("{}={}", k, v)
            }
        })
        .collect::<Vec<String>>()
        .join("&");

    let url = format!("{}?{}", base_url, query_string);

    Self::from_api_with_headers( &self, &url, headers, file_path).await
}

#[cfg(not(feature = "api"))]
    pub async fn from_api_with_params_and_headers(
        &self,
        _base_url: &str,
        _params: HashMap<&str, &str>,
        _headers: HashMap<String, String>,
        _file_path: &str
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom("*** Warning ***: API feature not enabled. Add feature under [dependencies]".to_string()))
    }

/// Create JSON from API with date range parameters
#[cfg(feature = "api")]
pub async fn from_api_with_dates(
    &self,
    base_url: &str, 
    from_date: &str, 
    to_date: &str,
    file_path: &str
) -> ElusionResult<()> {
    let url = format!("{}?from={}&to={}", 
        base_url,
        // Only encode if the date contains a space
        if from_date.contains(' ') { from_date.to_string() } else { urlencoding::encode(from_date).to_string() },
        if to_date.contains(' ') { to_date.to_string() } else { urlencoding::encode(to_date).to_string() }
    );

    Self::from_api( &self, &url, file_path).await
}
#[cfg(not(feature = "api"))]
pub async fn from_api_with_dates(
    &self,
    _base_url: &str, 
    _from_date: &str, 
    _to_date: &str,
    _file_path: &str
) -> ElusionResult<()> {
    Err(ElusionError::Custom("*** Warning ***: API feature not enabled. Add feature under [dependencies]".to_string()))
}

/// Create JSON from API with pagination
#[cfg(feature = "api")]
pub async fn from_api_with_pagination(
    &self,
    base_url: &str,
    page: u32,
    per_page: u32,
    file_path: &str
) -> ElusionResult<()> {
    let url = format!("{}?page={}&per_page={}", base_url, page, per_page);
 
    Self::from_api( &self, &url, file_path).await
}

#[cfg(not(feature = "api"))]
pub async fn from_api_with_pagination(
    &self,
    _base_url: &str,
    _page: u32,
    _per_page: u32,
    _file_path: &str
) -> ElusionResult<()> {
    Err(ElusionError::Custom("*** Warning ***: API feature not enabled. Add feature under [dependencies]".to_string()))
}

/// Create JSON from API with sorting
#[cfg(feature = "api")]
pub async fn from_api_with_sort(
    &self,
    base_url: &str,
    sort_field: &str,
    order: &str,
    file_path: &str
) -> ElusionResult<()> {
    let url = format!("{}?sort={}&order={}", 
        base_url,
        if sort_field.contains(' ') { sort_field.to_string() } else { urlencoding::encode(sort_field).to_string() },
        if order.contains(' ') { order.to_string() } else { urlencoding::encode(order).to_string() }
    );

    Self::from_api( &self, &url, file_path).await
}
#[cfg(not(feature = "api"))]
pub async fn from_api_with_sort(
    &self,
    _base_url: &str,
    _sort_field: &str,
    _order: &str,
    _file_path: &str
) -> ElusionResult<()> {
    Err(ElusionError::Custom("*** Warning ***: API feature not enabled. Add feature under [dependencies]".to_string()))
}

/// Create JSON from API with sorting and headers
#[cfg(feature = "api")]
pub async fn from_api_with_headers_and_sort(
    &self,
    base_url: &str,
    headers: HashMap<String, String>,
    sort_field: &str,
    order: &str,
    file_path: &str
) -> ElusionResult<()> {
    validate_https_url(base_url)?;
    
    let url = format!("{}?sort={}&order={}", 
        base_url,
        if sort_field.contains(' ') { sort_field.to_string() } else { urlencoding::encode(sort_field).to_string() },
        if order.contains(' ') { order.to_string() } else { urlencoding::encode(order).to_string() }
    );

    Self::from_api_with_headers(&self, &url, headers, file_path).await
}
#[cfg(not(feature = "api"))]
    pub async fn from_api_with_headers_and_sort(
        &self,
        _base_url: &str,
        _headers: HashMap<String, String>,
        _sort_field: &str,
        _order: &str,
        _file_path: &str
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom("*** Warning ***: API feature not enabled. Add feature under [dependencies]".to_string()))
    }

/// Process JSON response into JSON 
#[cfg(feature = "api")]
async fn save_json_to_file(content: Bytes, file_path: &str) -> ElusionResult<()> {

    if content.is_empty() {
        return Err(ElusionError::InvalidOperation {
            operation: "JSON Processing".to_string(),
            reason: "Empty content provided".to_string(),
            suggestion: "💡 Ensure API response contains data".to_string(),
        });
    }

    let reader = std::io::BufReader::new(content.as_ref());
    let stream = serde_json::Deserializer::from_reader(reader).into_iter::<Value>();
    let mut stream = stream.peekable();

    let json_type = match stream.peek() {
        Some(Ok(Value::Array(_))) => JsonType::Array,
        Some(Ok(Value::Object(_))) => JsonType::Object,
        Some(Ok(_)) => return Err(ElusionError::InvalidOperation {
            operation: "JSON Validation".to_string(),
            reason: "Invalid JSON structure".to_string(),
            suggestion: "💡 JSON must be either an array or object at root level".to_string(),
        }),
        Some(Err(e)) => return Err(ElusionError::InvalidOperation {
            operation: "JSON Parsing".to_string(),
            reason: format!("JSON syntax error: {}", e),
            suggestion: "💡 Check if the JSON content is well-formed".to_string(),
        }),
        None => return Err(ElusionError::InvalidOperation {
            operation: "JSON Reading".to_string(),
            reason: "Empty or invalid JSON content".to_string(),
            suggestion: "💡 Verify the API response contains valid JSON data".to_string(),
        }),
    };

    // validating file path and create directories
    let path = LocalPath::new(file_path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| ElusionError::WriteError {
            path: parent.display().to_string(),
            operation: "Directory Creation".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check directory permissions and path validity".to_string(),
        })?;
    }

    // Open file for writing
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(file_path)
        .map_err(|e| ElusionError::WriteError {
            path: file_path.to_string(),
            operation: "File Creation".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Verify file path and write permissions".to_string(),
        })?;

    let mut writer = std::io::BufWriter::new(file);
    let mut first = true;
    let mut items_written = 0;

    match json_type {
        JsonType::Array => {
            writeln!(writer, "[").map_err(|e| ElusionError::WriteError {
                path: file_path.to_string(),
                operation: "Array Start".to_string(),
                reason: e.to_string(),
                suggestion: "💡 Check disk space and write permissions".to_string(),
            })?;

            for value in stream {
                match value {
                    Ok(Value::Array(array)) => {
                        for item in array {
                            if !first {
                                writeln!(writer, ",").map_err(|e| ElusionError::WriteError {
                                    path: file_path.to_string(),
                                    operation: "Array Separator".to_string(),
                                    reason: e.to_string(),
                                    suggestion: "💡 Check disk space and write permissions".to_string(),
                                })?;
                            }
                            first = false;
                            items_written += 1;

                            serde_json::to_writer_pretty(&mut writer, &item)
                                .map_err(|e| ElusionError::WriteError {
                                    path: file_path.to_string(),
                                    operation: format!("Write Array Item {}", items_written),
                                    reason: format!("JSON serialization error: {}", e),
                                    suggestion: "💡 Check if item contains valid JSON data".to_string(),
                                })?;
                        }
                    }
                    Ok(_) => continue,
                    Err(e) => return Err(ElusionError::InvalidOperation {
                        operation: "Array Processing".to_string(),
                        reason: format!("Failed to parse array item: {}", e),
                        suggestion: "💡 Verify JSON array structure is valid".to_string(),
                    }),
                }
            }
            writeln!(writer, "\n]").map_err(|e| ElusionError::WriteError {
                path: file_path.to_string(),
                operation: "Array End".to_string(),
                reason: e.to_string(),
                suggestion: "💡 Check disk space and write permissions".to_string(),
            })?;
        }
        JsonType::Object => {
            for value in stream {
                match value {
                    Ok(Value::Object(map)) => {
                        items_written += 1;
                        serde_json::to_writer_pretty(&mut writer, &Value::Object(map))
                            .map_err(|e| ElusionError::WriteError {
                                path: file_path.to_string(),
                                operation: format!("Write Object {}", items_written),
                                reason: format!("JSON serialization error: {}", e),
                                suggestion: "💡 Check if object contains valid JSON data".to_string(),
                            })?;
                    }
                    Ok(_) => return Err(ElusionError::InvalidOperation {
                        operation: "Object Processing".to_string(),
                        reason: "Non-object value in object stream".to_string(),
                        suggestion: "💡 Ensure all items are valid JSON objects".to_string(),
                    }),
                    Err(e) => return Err(ElusionError::InvalidOperation {
                        operation: "Object Processing".to_string(),
                        reason: format!("Failed to parse object: {}", e),
                        suggestion: "💡 Verify JSON object structure is valid".to_string(),
                    }),
                }
            }
        }
    }

    writer.flush().map_err(|e| ElusionError::WriteError {
        path: file_path.to_string(),
        operation: "File Finalization".to_string(),
        reason: e.to_string(),
        suggestion: "💡 Check disk space and write permissions".to_string(),
    })?;

    println!("✅ Successfully created {} with {} items", file_path, items_written);
    
    if items_written == 0 {
        println!("*** Warning ***: No items were written to the file. Check if this is expected.");
    }

    Ok(())

}



}


// ReportLayout with all fields when dashboard feature is enabled
#[cfg(feature = "dashboard")]
#[derive(Debug, Clone)]
pub struct ReportLayout {
    pub grid_columns: usize,     // Number of columns in the grid
    pub grid_gap: usize,         // Gap between plots in pixels
    pub max_width: usize,        // Maximum width of the container
    pub plot_height: usize,      // Height of each plot
    pub table_height: usize,     // Height of each table
}

#[cfg(feature = "dashboard")]
impl Default for ReportLayout {
    fn default() -> Self {
        Self {
            grid_columns: 2,
            grid_gap: 20,
            max_width: 1200,
            plot_height: 400,
            table_height: 400,
        }
    }
}

// ReportLayout stub with all the same fields when dashboard feature is not enabled
#[cfg(not(feature = "dashboard"))]
#[derive(Debug, Clone)]
pub struct ReportLayout {
    pub grid_columns: usize,
    pub grid_gap: usize, 
    pub max_width: usize,
    pub plot_height: usize,
    pub table_height: usize,
}


#[cfg(feature = "dashboard")]
#[derive(Debug, Clone)]
pub struct TableOptions {
    pub pagination: bool,
    pub page_size: usize,
    pub enable_sorting: bool,
    pub enable_filtering: bool,
    pub enable_column_menu: bool,
    pub theme: String,           // "ag-theme-alpine", "ag-theme-balham", etc.
}

#[cfg(feature = "dashboard")]
impl Default for TableOptions {
    fn default() -> Self {
        Self {
            pagination: true,
            page_size: 10,
            enable_sorting: true,
            enable_filtering: true,
            enable_column_menu: true,
            theme: "ag-theme-alpine".to_string(),
        }
    }
}

#[cfg(not(feature = "dashboard"))]
#[derive(Debug, Clone)]
pub struct TableOptions {
    pub pagination: bool,
    pub page_size: usize,
    pub enable_sorting: bool,
    pub enable_filtering: bool,
    pub enable_column_menu: bool,
    pub theme: String,
}

#[cfg(feature = "dashboard")]
fn generate_controls(has_plots: bool, has_tables: bool) -> String {
    let mut controls = Vec::new();
    
    if has_plots {
        controls.extend_from_slice(&[
            r#"<button onclick="toggleGrid()">Toggle Layout</button>"#
        ]);
    }
    
    if has_tables {
        controls.extend_from_slice(&[
            r#"<button onclick="exportAllTables()" class="export-button">Export to CSV</button>"#,
            r#"<button onclick="exportToExcel()" class="export-button">Export to Excel</button>"#
        ]);
    }
    
    controls.join("\n")
}

#[cfg(feature = "dashboard")]
fn generate_javascript(has_plots: bool, has_tables: bool, grid_columns: usize) -> String {
    let mut js = String::new();
    
    // Common utilities
    js.push_str(r#"
    document.addEventListener('DOMContentLoaded', function() {
        console.log('DOMContentLoaded event fired');
        showLoading();
        
        const promises = [];
        
        // Wait for plots if they exist
        const plotContainers = document.querySelectorAll('.plot-container');
        console.log('Found plot containers:', plotContainers.length);
        
        if (plotContainers.length > 0) {
            promises.push(...Array.from(plotContainers).map(container => 
                new Promise(resolve => {
                    const observer = new MutationObserver((mutations, obs) => {
                        if (container.querySelector('.js-plotly-plot')) {
                            obs.disconnect();
                            resolve();
                        }
                    });
                    observer.observe(container, { childList: true, subtree: true });
                })
            ));
        }
        
        // Wait for grids if they exist
        const gridContainers = document.querySelectorAll('[id^="grid_"]');
        console.log('Found grid containers:', gridContainers.length);
        
        if (gridContainers.length > 0) {
            promises.push(...Array.from(gridContainers).map(container => 
                new Promise(resolve => {
                    container.addEventListener('gridReady', () => {
                        console.log('Grid ready event received for:', container.id);
                        resolve();
                    }, { once: true });
                    // Add a timeout to prevent infinite waiting
                    setTimeout(() => {
                        console.log('Grid timeout for:', container.id);
                        resolve();
                    }, 5000);
                })
            ));
        }
        
        // If no async content to wait for, hide loading immediately
        if (promises.length === 0) {
            console.log('No async content to wait for');
            hideLoading();
            return;
        }
        
        // Wait for all content to load or timeout
        Promise.all(promises)
            .then(() => {
                console.log('All content loaded successfully');
                hideLoading();
                showNotification('Report loaded successfully', 'info');
            })
            .catch(error => {
                console.error('Error loading report:', error);
                hideLoading();
                showNotification('Error loading some components', 'error');
            });
        });
    "#);

    // Plots JavaScript
    if has_plots {
        js.push_str(&format!(r#"
            const plots = [];
            let currentGridColumns = {};
            
            document.querySelectorAll('.plot-container').forEach((container, index) => {{
                const plotDiv = container.querySelector(`#plot_${{index}}`);
                const data = JSON.parse(container.dataset.plotData);
                const layout = JSON.parse(container.dataset.plotLayout);
                
                layout.autosize = true;
                
                Plotly.newPlot(plotDiv, data, layout, {{
                    responsive: true,
                    scrollZoom: true,
                    modeBarButtonsToAdd: [
                        'hoverClosestCartesian',
                        'hoverCompareCartesian'
                    ],
                    displaylogo: false
                }}).then(gd => {{
                    plots.push(gd);
                    gd.on('plotly_click', function(data) {{
                        highlightPoint(data, index);
                    }});
                }}).catch(error => {{
                    console.error('Error creating plot:', error);
                    showNotification('Error creating plot', 'error');
                }});
            }});


            function toggleGrid() {{
                const grid = document.querySelector('.grid');
                currentGridColumns = currentGridColumns === {0} ? 1 : {0};
                grid.style.gridTemplateColumns = `repeat(${{currentGridColumns}}, 1fr)`;
                showNotification(`Layout changed to ${{currentGridColumns}} column(s)`);
            }}

            function highlightPoint(data, plotIndex) {{
                if (!data.points || !data.points[0]) return;
                
                const point = data.points[0];
                const pointColor = 'red';
                
                plots.forEach((plot, idx) => {{
                    if (idx !== plotIndex) {{
                        const trace = plot.data[0];
                        if (trace.x && trace.y) {{
                            const matchingPoints = trace.x.map((x, i) => {{
                                return {{x, y: trace.y[i]}};
                            }}).filter(p => p.x === point.x && p.y === point.y);
                            
                            if (matchingPoints.length > 0) {{
                                Plotly.restyle(plot, {{'marker.color': pointColor}}, [0]);
                            }}
                        }}
                    }}
                }});
            }}
        "#, grid_columns));
    }

    // Tables JavaScript
    if has_tables {
        js.push_str(r#"
            // Table utility functions
            function exportAllTables() {
                try {
                    document.querySelectorAll('.ag-theme-alpine').forEach((container, index) => {
                        const gridApi = container.gridOptions.api;
                        const csvContent = gridApi.getDataAsCsv({
                            skipHeader: false,
                            skipFooters: true,
                            skipGroups: true,
                            fileName: `table_${index}.csv`
                        });
                        
                        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
                        const link = document.createElement('a');
                        link.href = URL.createObjectURL(blob);
                        link.download = `table_${index}.csv`;
                        link.click();
                    });
                    showNotification('Exported all tables');
                } catch (error) {
                    console.error('Error exporting tables:', error);
                    showNotification('Error exporting tables', 'error');
                }
            }

            function exportToExcel() {
            try {
                document.querySelectorAll('.ag-theme-alpine').forEach((container, index) => {
                    const gridApi = container.gridOptions.api;
                    const columnApi = container.gridOptions.columnApi;
                    
                    // Get displayed columns
                    const columns = columnApi.getAllDisplayedColumns();
                    const columnDefs = columns.map(col => ({
                        header: col.colDef.headerName || col.colDef.field,
                        field: col.colDef.field
                    }));
                    
                    // Get all rows data
                    const rowData = [];
                    gridApi.forEachNode(node => {
                        const row = {};
                        columnDefs.forEach(col => {
                            row[col.header] = node.data[col.field];
                        });
                        rowData.push(row);
                    });
                    
                    // Create workbook and worksheet
                    const worksheet = XLSX.utils.json_to_sheet(rowData);
                    const workbook = XLSX.utils.book_new();
                    XLSX.utils.book_append_sheet(workbook, worksheet, `Table_${index}`);
                    
                    // Generate Excel file
                    const excelBuffer = XLSX.write(workbook, { bookType: 'xlsx', type: 'array' });
                    const blob = new Blob([excelBuffer], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
                    const url = window.URL.createObjectURL(blob);
                    const link = document.createElement('a');
                    link.href = url;
                    link.download = `table_${index}.xlsx`;
                    document.body.appendChild(link);
                    link.click();
                    
                    // Cleanup
                    setTimeout(() => {
                        document.body.removeChild(link);
                        window.URL.revokeObjectURL(url);
                    }, 0);
                });
                showNotification('Exported tables to Excel');
            } catch (error) {
                console.error('Error exporting to Excel:', error);
                showNotification('Error exporting to Excel', 'error');
            }
        }
    
            // Initialize AG Grid Quick Filter after DOM is loaded
            document.addEventListener('DOMContentLoaded', function() {
                document.querySelectorAll('.ag-theme-alpine').forEach(container => {
                    const gridOptions = container.gridOptions;
                    if (gridOptions) {
                        console.log('Initializing quick filter for container:', container.id);
                        const quickFilterInput = document.createElement('input');
                        quickFilterInput.type = 'text';
                        quickFilterInput.placeholder = 'Quick Filter...';
                        quickFilterInput.className = 'quick-filter';
                        quickFilterInput.style.cssText = 'margin: 10px 0; padding: 5px; width: 200px;';
                        
                        quickFilterInput.addEventListener('input', function(e) {
                            gridOptions.api.setQuickFilter(e.target.value);
                        });
                        
                        container.parentNode.insertBefore(quickFilterInput, container);
                    }
                });
            });
        "#);
    }

    // Add notification styles
    js.push_str(r#"
        const style = document.createElement('style');
        style.textContent = `
            .notification {
                position: fixed;
                bottom: 20px;
                right: 20px;
                padding: 10px 20px;
                border-radius: 4px;
                color: white;
                font-weight: bold;
                z-index: 1000;
                animation: slideIn 0.5s ease-out;
            }
            .notification.info {
                background-color: #007bff;
            }
            .notification.error {
                background-color: #dc3545;
            }
            @keyframes slideIn {
                from { transform: translateX(100%); }
                to { transform: translateX(0); }
            }
        `;
        document.head.appendChild(style);
    "#);

    js
}

/// Extract row from a DataFrame as a HashMap based on row index
pub async fn extract_row_from_df(df: &CustomDataFrame, row_index: usize) -> ElusionResult<HashMap<String, String>> {
    let ctx = SessionContext::new();
   
    let batches = df.df.clone().collect().await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Data Collection".to_string(),
            reason: format!("Failed to collect DataFrame: {}", e),
            suggestion: "💡 Check if DataFrame contains valid data".to_string()
        })?;
    
    let schema = df.df.schema();
    let mem_table = MemTable::try_new(schema.clone().into(), vec![batches])
        .map_err(|e| ElusionError::SchemaError {
            message: format!("Failed to create in-memory table: {}", e),
            schema: Some(schema.to_string()),
            suggestion: "💡 Verify schema compatibility and data types".to_string()
        })?;
    
    ctx.register_table("temp_extract", Arc::new(mem_table))
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Table Registration".to_string(),
            reason: format!("Failed to register table: {}", e),
            suggestion: "💡 Check if table name is unique and valid".to_string()
        })?;
    
    let row_df = ctx.sql(&format!("SELECT * FROM temp_extract LIMIT 1 OFFSET {}", row_index)).await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "SQL Execution".to_string(),
            reason: format!("Failed to execute SQL: {}", e),
            suggestion: "💡 Verify DataFrame is valid".to_string()
        })?;
    
    let batches = row_df.collect().await
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Result Collection".to_string(),
            reason: format!("Failed to collect result: {}", e),
            suggestion: "💡 Check if query returns valid data".to_string()
        })?;
    
    if batches.is_empty() || batches[0].num_rows() == 0 {
        return Err(ElusionError::Custom(format!("No data found at row {}", row_index)));
    }
    
    let mut row_values = HashMap::new();
    let batch = &batches[0];
    
    for (i, field) in schema.fields().iter().enumerate() {
        let col = batch.column(i);
        let value = match col.data_type() {
            ArrowDataType::Utf8 => {
                let array = col.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| ElusionError::Custom("Failed to downcast to StringArray".to_string()))?;
                
                if array.is_null(0) {
                    "".to_string()
                } else {
                    array.value(0).to_string()
                }
            },
            _ => {
                format!("{:?}", col.as_ref())
            }
        };
        row_values.insert(field.name().to_string(), value);
    }
    
    Ok(row_values)
}

/// Extract a Value from a DataFrame based on column name and row index
pub async fn extract_value_from_df(df: &CustomDataFrame, column_name: &str, row_index: usize) -> ElusionResult<String>{

    let ctx = SessionContext::new();

    let batches = df.df.clone().collect().await 
        .map_err(|e| ElusionError::InvalidOperation { 
            operation: "Data Colleciton".to_string(), 
            reason: format!("Failed to collect DataFrame: {}", e), 
            suggestion: "💡 Check if DataFrame contains valid data".to_string() 
        })?;

    let schema = df.df.schema();
    let mem_table = MemTable::try_new(schema.clone().into(), vec![batches])
        .map_err(|e| ElusionError::SchemaError { 
            message: format!("Failed to create in-memory table: {}", e), 
            schema: Some(schema.to_string()), 
            suggestion: "💡 Verify schema compatibility and data types".to_string()
        })?;

    ctx.register_table("temp_extract", Arc::new(mem_table))
        .map_err(|e| ElusionError::InvalidOperation { 
            operation: "Table Registration".to_string(), 
            reason: format!("Failed to register Table: {}", e), 
            suggestion: "💡 Check if table is unique or valid".to_string() 
        })?;

    let value_df = ctx.sql(&format!("SELECT\"{}\" FROM temp_extract LIMIT 1 OFFSET {}", column_name, row_index)).await
        .map_err(|e| ElusionError::InvalidOperation { 
            operation: "SQL Execution".to_string(), 
            reason: format!("Failed to Execute SQL: {}", e), 
            suggestion: "💡 Verify column name exists in DataFrame".to_string() 
        })?;

    let batches = value_df.collect().await
        .map_err(|e| ElusionError::InvalidOperation { 
            operation: "Result Collection".to_string(), 
            reason: format!("Failed to collect Result: {}", e), 
            suggestion: "💡 Check if Query returns valid data".to_string() 
        })?;

    if batches.is_empty() || batches[0].num_rows() == 0 {
        return Err(ElusionError::Custom(format!("No data found for column '{}' at row {}", column_name, row_index)));
    }

    let col = batches[0].column(0);
    let value = match col.data_type(){
        ArrowDataType::Utf8=>{
            let array = col.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| ElusionError::Custom("Failed to downcast to StringArray".to_string()))?;

            if array.is_null(0){
                "".to_string()
            } else {
                array.value(0).to_string()
            }
        },
        _ => {
            format!("{:?}", col.as_ref())
        }
    };

    Ok(value)
}