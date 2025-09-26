#[cfg(feature = "ftp")]
use std::time::Duration;
#[cfg(feature = "ftp")]
use suppaftp::{FtpStream, Mode};
#[cfg(feature = "ftp")]
use suppaftp::{NativeTlsFtpStream, NativeTlsConnector};
#[cfg(feature = "ftp")]
use suppaftp::native_tls::TlsConnector;
#[cfg(feature = "ftp")]
use crate::prelude::*;
#[cfg(feature = "ftp")]
use crate::ElusionError;
#[cfg(feature = "ftp")]
use crate::ElusionResult;
#[cfg(feature = "ftp")]
use crate::CustomDataFrame;

/// Internal FTP connection
#[cfg(feature = "ftp")]
pub struct FtpConnection {
    host: String,
    port: u16,
    username: String,
    password: String,
    use_tls: bool,
    passive_mode: bool,
    timeout: Duration,
    working_directory: Option<String>,
}
#[cfg(feature = "ftp")]
impl FtpConnection {
    pub fn new(
        host: String,
        username: String,
        password: String,
    ) -> Self {
        Self {
            host,
            port: 21, 
            username,
            password,
            use_tls: false,
            passive_mode: true,  
            timeout: Duration::from_secs(30),
            working_directory: None,
        }
    }

    pub fn with_tls(mut self) -> Self {
        self.use_tls = true;
        self
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn with_directory(mut self, working_directory: String) -> Self {
        self.working_directory = Some(working_directory);
        self
    }

    pub fn with_passive_mode(mut self, passive: bool) -> Self {
        self.passive_mode = passive;
        self
    }

    pub fn with_timeout(mut self, timeout_secs: u64) -> Self {
        self.timeout = Duration::from_secs(timeout_secs);
        self
    }

    pub fn connect(&self) -> ElusionResult<FtpStreamEnum> {
        print!("🔌 Connecting to FTP server... \n");
        let address = format!("{}:{}", self.host, self.port);

        if self.use_tls {
            // For TLS connections, use NativeTlsFtpStream directly
            let ftp_stream = NativeTlsFtpStream::connect(&address)
                .map_err(|e| ElusionError::Custom(format!("Failed to connect to FTP server at {}: {}", address, e)))?;

            // Create TLS connector
            let tls_connector = TlsConnector::builder()
                .danger_accept_invalid_certs(true)
                .build()
                .map_err(|e| ElusionError::Custom(format!("Failed to create TLS connector: {}", e)))?;
            
            let native_tls_connector = NativeTlsConnector::from(tls_connector);
            
            // Upgrade to secure connection
            let mut ftp_stream = ftp_stream
                .into_secure(native_tls_connector, &self.host)
                .map_err(|e| ElusionError::Custom(format!("Failed to establish secure TLS connection: {}", e)))?;

            ftp_stream
                .login(&self.username, &self.password)
                .map_err(|e| ElusionError::Custom(format!("Failed to authenticate with FTP server: {}", e)))?;

            // Set transfer mode (modifies in place, returns ())
            if self.passive_mode {
                ftp_stream.set_mode(Mode::Passive);
            } else {
                ftp_stream.set_mode(Mode::Active);
            }

            // Change to working directory if specified
            if let Some(dir) = &self.working_directory {
                ftp_stream.cwd(dir)
                    .map_err(|e| ElusionError::Custom(format!("Failed to change to working directory {}: {}", dir, e)))?;
            }

            Ok(FtpStreamEnum::Tls(ftp_stream))
        } else {
            // regular (non-TLS) connection
            let mut ftp_stream = FtpStream::connect(&address)
                .map_err(|e| ElusionError::Custom(format!("Failed to connect to FTP server at {}: {}", address, e)))?;

            // Login
            ftp_stream
                .login(&self.username, &self.password)
                .map_err(|e| ElusionError::Custom(format!("Failed to authenticate with FTP server: {}", e)))?;

            // Set transfer mode (modifies in place, returns ())
            if self.passive_mode {
                ftp_stream.set_mode(Mode::Passive);
            } else {
                ftp_stream.set_mode(Mode::Active);
            }

            // Change to working directory if specified
            if let Some(dir) = &self.working_directory {
                ftp_stream.cwd(dir)
                    .map_err(|e| ElusionError::Custom(format!("Failed to change to working directory {}: {}", dir, e)))?;
            }
            print!("✅ Connected and authenticated successfully to FTP Server\n");
            Ok(FtpStreamEnum::Plain(ftp_stream))
        }
    }

    pub fn download_to_temp(&self, remote_path: &str) -> ElusionResult<String> {
        let mut ftp = self.connect()?;
        
        // Extract filename from path
        let filename = remote_path.split('/').last().unwrap_or(remote_path);
        
        let temp_dir = std::env::temp_dir();
        let temp_path = temp_dir.join(format!("elusion_ftp_{}", filename));
        
        let file = std::fs::File::create(&temp_path)
            .map_err(|e| ElusionError::Custom(format!("Failed to create temporary file {:?}: {}", temp_path, e)))?;
        let mut writer = BufWriter::new(file);
        
        match &mut ftp {
            FtpStreamEnum::Plain(stream) => {
                stream.retr(remote_path, |reader| {
                    std::io::copy(reader, &mut writer)
                        .map(|_| ())
                        .map_err(suppaftp::FtpError::ConnectionError)
                })
                .map_err(|e| ElusionError::Custom(format!("Failed to download file {}: {}", remote_path, e)))?;
            },
            FtpStreamEnum::Tls(stream) => {
                stream.retr(remote_path, |reader| {
                    std::io::copy(reader, &mut writer)
                        .map(|_| ())
                        .map_err(suppaftp::FtpError::ConnectionError)
                })
                .map_err(|e| ElusionError::Custom(format!("Failed to download file {}: {}", remote_path, e)))?;
            }
        }
        
        ftp.quit().ok();
        Ok(temp_path.to_string_lossy().to_string())
    }

    pub fn upload_file(&self, local_path: &LocalPath, remote_path: &str) -> ElusionResult<()> {
        let mut ftp = self.connect()?;
        
        let file = std::fs::File::open(local_path)
            .map_err(|e| ElusionError::Custom(format!("Failed to open local file {:?}: {}", local_path, e)))?;
        let mut reader = BufReader::new(file);
        
        match &mut ftp {
            FtpStreamEnum::Plain(stream) => {
                stream.put_file(remote_path, &mut reader)
                    .map_err(|e| ElusionError::Custom(format!("Failed to upload file to {}: {}", remote_path, e)))?;
            },
            FtpStreamEnum::Tls(stream) => {
                stream.put_file(remote_path, &mut reader)
                    .map_err(|e| ElusionError::Custom(format!("Failed to upload file to {}: {}", remote_path, e)))?;
            }
        }
        
        ftp.quit().ok();
        Ok(())
    }

    pub fn list_files(&self, path: Option<&str>) -> ElusionResult<Vec<String>> {
        let mut ftp = self.connect()?;
        
        if let Some(dir) = path {
            match &mut ftp {
                FtpStreamEnum::Plain(stream) => {
                    stream.cwd(dir)
                        .map_err(|e| ElusionError::Custom(format!("Failed to change directory to {}: {}", dir, e)))?;
                },
                FtpStreamEnum::Tls(stream) => {
                    stream.cwd(dir)
                        .map_err(|e| ElusionError::Custom(format!("Failed to change directory to {}: {}", dir, e)))?;
                }
            }
        }

        let files = match &mut ftp {
            FtpStreamEnum::Plain(stream) => {
                stream.nlst(None)
                    .map_err(|e| ElusionError::Custom(format!("Failed to list files: {}", e)))?
            },
            FtpStreamEnum::Tls(stream) => {
                stream.nlst(None)
                    .map_err(|e| ElusionError::Custom(format!("Failed to list files: {}", e)))?
            }
        };
        
        ftp.quit().ok();
        Ok(files)
    }

    pub fn filter_files_by_extension(&self, path: Option<&str>, extensions: &[&str]) -> ElusionResult<Vec<String>> {
        let all_files = self.list_files(path)?;
        
        let filtered: Vec<String> = all_files
            .into_iter()
            .filter(|file| {
                extensions.iter().any(|ext| {
                    file.to_lowercase().ends_with(&format!(".{}", ext.to_lowercase()))
                })
            })
            .collect();
        
        Ok(filtered)
    }
}

// Enum to handle both plain and TLS FTP streams
#[cfg(feature = "ftp")]
pub enum FtpStreamEnum {
    Plain(FtpStream),
    Tls(NativeTlsFtpStream),
}
#[cfg(feature = "ftp")]
impl FtpStreamEnum {
    pub fn quit(&mut self) -> suppaftp::FtpResult<()> {
        match self {
            FtpStreamEnum::Plain(stream) => stream.quit(),
            FtpStreamEnum::Tls(stream) => stream.quit(),
        }
    }

   pub fn size(&mut self, remote_path: &str) -> suppaftp::FtpResult<usize> {
        match self {
            FtpStreamEnum::Plain(stream) => stream.size(remote_path),
            FtpStreamEnum::Tls(stream) => stream.size(remote_path),
        }
    }

    pub fn rm(&mut self, remote_path: &str) -> suppaftp::FtpResult<()> {
        match self {
            FtpStreamEnum::Plain(stream) => stream.rm(remote_path),
            FtpStreamEnum::Tls(stream) => stream.rm(remote_path),
        }
    }

    pub fn mkdir(&mut self, remote_path: &str) -> suppaftp::FtpResult<()> {
        match self {
            FtpStreamEnum::Plain(stream) => stream.mkdir(remote_path),
            FtpStreamEnum::Tls(stream) => stream.mkdir(remote_path),
        }
    }
}

// FOLDER implementation
#[cfg(feature = "ftp")]
pub async fn from_ftp_folder_impl(
    server: &str,
    username: &str,
    password: &str,
    port: Option<u16>,
    use_tls: bool,
    folder_path: &str,
    file_extensions: Option<Vec<&str>>,
    result_alias: &str,
) -> ElusionResult<CustomDataFrame> {
    // List files in the folder
    let files = FtpUtils::list_files(
        server,
        username,
        password,
        Some(folder_path),
        port,
        use_tls,
    )?;

    let mut dataframes = Vec::new();

    for file_name in files {
        let file_ext = file_name
            .split('.')
            .last()
            .unwrap_or("")
            .to_lowercase();

        if !["csv", "json", "parquet", "xml", "xlsx", "xls"].contains(&file_ext.as_str()) {
            println!("⏭️ Skipping unsupported file type: {}", file_name);
            continue;
        }

        if let Some(ref extensions) = file_extensions {
            if !extensions.iter().any(|ext| ext.to_lowercase() == file_ext) {
                continue;
            }
        }

        let remote_path = format!("{}/{}", folder_path.trim_end_matches('/'), file_name);

        let load_result = if use_tls {
            CustomDataFrame::from_ftps(
                server,
                username,
                password,
                &remote_path,
                &format!("ftp_file_{}", dataframes.len()),
            ).await
        } else {
            CustomDataFrame::from_ftp(
                server,
                username,
                password,
                &remote_path,
                &format!("ftp_file_{}", dataframes.len()),
            ).await
        };

        match load_result {
            Ok(df) => {
                println!("✅ Loaded file: {}", file_name);
                dataframes.push(df);
            },
            Err(e) => {
                eprintln!("⚠️ Failed to load file {}: {}", file_name, e);
                continue;
            }
        }
    }

    if dataframes.is_empty() {
        return Err(ElusionError::InvalidOperation {
            operation: "FTP Folder Loading".to_string(),
            reason: "No supported files found or all files failed to load".to_string(),
            suggestion: "💡 Check folder path and ensure it contains CSV, Excel, JSON, Parquet, or XML files".to_string(),
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
                if first_field.name().to_lowercase() != current_field.name().to_lowercase() {
                    compatible_schemas = false;
                    schema_issues.push(format!("File {} column {} name is '{}', but first file has '{}'", 
                        file_idx + 1, col_idx + 1, current_field.name(), first_field.name()));
                }

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
    println!("🔗 Unioning {} files with compatible schemas...", dataframes.len());

    let total_files = dataframes.len();
     let mut result = dataframes.clone().into_iter().next().unwrap();

    for (i, df) in dataframes.into_iter().enumerate().skip(1) {
            result = result.union_all(df).await
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: "SharePoint Folder Union with Filename".to_string(),
                    reason: format!("Failed to union file {}: {}", i + 1, e),
                    suggestion: "💡 Check that all files have compatible schemas".to_string(),
                })?;
            
            println!("✅ Unioned file {}/{}", i + 1, total_files - 1);
        }

    println!("🎉 Successfully combined {} files using UNION ALL", total_files);

    result.elusion(result_alias).await
}

#[cfg(feature = "ftp")]
pub async fn from_ftp_folder_with_filename_column_impl(
    server: &str,
    username: &str,
    password: &str,
    port: Option<u16>,
    use_tls: bool,
    folder_path: &str,
    file_extensions: Option<Vec<&str>>,
    result_alias: &str,
) -> ElusionResult<CustomDataFrame> {
    // List files in the folder
    let files = FtpUtils::list_files(
        server,
        username,
        password,
        Some(folder_path),
        port,
        use_tls,
    )?;

    let mut dataframes = Vec::new();

    for file_name in files {
        let file_ext = file_name
            .split('.')
            .last()
            .unwrap_or("")
            .to_lowercase();

        if !["csv", "json", "parquet", "xml", "xlsx", "xls"].contains(&file_ext.as_str()) {
            println!("⏭️ Skipping unsupported file type: {}", file_name);
            continue;
        }

        if let Some(ref extensions) = file_extensions {
            if !extensions.iter().any(|ext| ext.to_lowercase() == file_ext) {
                continue;
            }
        }

        let remote_path = format!("{}/{}", folder_path.trim_end_matches('/'), file_name);

        let load_result = if use_tls {
            CustomDataFrame::from_ftps(
                server,
                username,
                password,
                &remote_path,
                &format!("ftp_file_{}", dataframes.len()),
            ).await
        } else {
            CustomDataFrame::from_ftp(
                server,
                username,
                password,
                &remote_path,
                &format!("ftp_file_{}", dataframes.len()),
            ).await
        };

        let mut loaded_df = match load_result {
            Ok(df) => {
                println!("✅ Loaded file: {}", file_name);
                df
            },
            Err(e) => {
                eprintln!("⚠️ Failed to load file {}: {}", file_name, e);
                continue;
            }
        };

        // Add filename as a new column using select with literal value
        loaded_df = loaded_df.select_vec(vec![
            &format!("'{}' AS filename_added", file_name), 
            "*"
        ]);
        
        // Execute the selection to create the dataframe with filename column
        let temp_alias = format!("file_with_filename_{}", dataframes.len());
        loaded_df = match loaded_df.elusion(&temp_alias).await {
            Ok(filename_df) => {
                println!("✅ Added filename column to {}", file_name);
                filename_df
            },
            Err(e) => {
                eprintln!("⚠️ Failed to add filename to {}: {}", file_name, e);
                continue;
            }
        };

        dataframes.push(loaded_df);
    }

    if dataframes.is_empty() {
        return Err(ElusionError::InvalidOperation {
            operation: "FTP Folder Loading with Filename".to_string(),
            reason: "No supported files found or all files failed to load".to_string(),
            suggestion: "💡 Check folder path and ensure it contains CSV, Excel, JSON, Parquet, or XML files".to_string(),
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
                if first_field.name().to_lowercase() != current_field.name().to_lowercase() {
                    compatible_schemas = false;
                    schema_issues.push(format!("File {} column {} name is '{}', but first file has '{}'", 
                        file_idx + 1, col_idx + 1, current_field.name(), first_field.name()));
                }

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
    println!("🔗 Unioning {} files with compatible schemas...", dataframes.len());

    let total_files = dataframes.len();
     let mut result = dataframes.clone().into_iter().next().unwrap();

    for (i, df) in dataframes.into_iter().enumerate().skip(1) {
            result = result.union_all(df).await
                .map_err(|e| ElusionError::InvalidOperation {
                    operation: "FTP Folder Union with Filename".to_string(),
                    reason: format!("Failed to union file {}: {}", i + 1, e),
                    suggestion: "💡 Check that all files have compatible schemas".to_string(),
                })?;
            
            println!("✅ Unioned file {}/{}", i + 1, total_files - 1);
        }

    println!("🎉 Successfully combined {} files using UNION ALL", total_files);

    result.elusion(result_alias).await
}

// impls 

    /// Load data from FTP server
    #[cfg(feature = "ftp")]
    pub async fn from_ftp_impl(
        server: &str,
        username: &str,
        password: &str,
        remote_path: &str,
        alias: &str
    ) -> ElusionResult<CustomDataFrame> {
        let connection = FtpConnection::new(
            server.to_string(),
            username.to_string(),
            password.to_string()
        );
        
        let temp_path = connection.download_to_temp(remote_path)
            .map_err(|e| ElusionError::Custom(format!("Failed to download from FTP: {}", e)))?;
        
        let df = CustomDataFrame::new(&temp_path, alias).await?;
        
        std::fs::remove_file(&temp_path).ok();
        
        Ok(df)
    }

    /// Load data from FTPS server (secure FTP)
    #[cfg(feature = "ftp")]
    pub async fn from_ftps_impl(
        server: &str,
        username: &str,
        password: &str,
        remote_path: &str,
        alias: &str
    ) -> ElusionResult<CustomDataFrame> {
        let connection = FtpConnection::new(
            server.to_string(),
            username.to_string(),
            password.to_string()
        ).with_tls();

        let temp_path = connection.download_to_temp(remote_path)
            .map_err(|e| ElusionError::Custom(format!("Failed to download from FTPS: {}", e)))?;
        
        let df = CustomDataFrame::new(&temp_path, alias).await?;
        
        std::fs::remove_file(&temp_path).ok();
        
        Ok(df)
    }

    /// Load data from FTP server with custom port
    #[cfg(feature = "ftp")]
    pub async fn from_ftp_with_port_impl(
        server: &str,
        port: u16,
        username: &str,
        password: &str,
        remote_path: &str,
        alias: &str
    ) -> ElusionResult<CustomDataFrame> {
        let connection = FtpConnection::new(
            server.to_string(),
            username.to_string(),
            password.to_string()
        ).with_port(port);
        
        let temp_path = connection.download_to_temp(remote_path)
            .map_err(|e| ElusionError::Custom(format!("Failed to download from FTP: {}", e)))?;
        
        let df = CustomDataFrame::new(&temp_path, alias).await?;
        
        std::fs::remove_file(&temp_path).ok();
        
        Ok(df)
    }

    /// Load data from FTP server with working directory
    #[cfg(feature = "ftp")]
    pub async fn from_ftp_with_directory_impl(
        server: &str,
        username: &str,
        password: &str,
        directory: &str,
        remote_path: &str,
        alias: &str
    ) -> ElusionResult<CustomDataFrame> {
        let connection = FtpConnection::new(
            server.to_string(),
            username.to_string(),
            password.to_string()
        ).with_directory(directory.to_string());
        
        let temp_path = connection.download_to_temp(remote_path)
            .map_err(|e| ElusionError::Custom(format!("Failed to download from FTP: {}", e)))?;
        
        let df = CustomDataFrame::new(&temp_path, alias).await?;
        
        std::fs::remove_file(&temp_path).ok();
        
        Ok(df)
    }

    /// Write DataFrame result to FTP server as CSV
    #[cfg(feature = "ftp")]
    pub async fn write_csv_to_ftp_impl(
        df: &CustomDataFrame,
        server: &str,
        username: &str,
        password: &str,
        remote_path: &str
    ) -> ElusionResult<()> {
        let connection = FtpConnection::new(
            server.to_string(),
            username.to_string(),
            password.to_string()
        );
        
        let temp_dir = std::env::temp_dir();
        let temp_filename = format!("elusion_ftp_temp_{}.csv", Utc::now().timestamp());
        let temp_path = temp_dir.join(&temp_filename);
        
        let csv_options = CsvWriteOptions {
            delimiter: b',',
            escape: b'\\',
            quote: b'"',
            double_quote: false,
            null_value: "NULL".to_string(),
        };
        
        df.write_to_csv("overwrite", &temp_path.to_string_lossy(), csv_options).await?;
        
        connection.upload_file(&temp_path, remote_path)
            .map_err(|e| ElusionError::Custom(format!("Failed to upload to FTP: {}", e)))?;
        
        std::fs::remove_file(&temp_path).ok();
        
        Ok(())
    }

    /// Write DataFrame result to FTP server as Excel
    #[cfg(feature = "ftp")]
    pub async fn write_excel_to_ftp_impl(
        df: &CustomDataFrame,
        server: &str,
        username: &str,
        password: &str,
        remote_path: &str,
        sheet_name: Option<&str>
    ) -> ElusionResult<()> {
        let connection = FtpConnection::new(
            server.to_string(),
            username.to_string(),
            password.to_string()
        );
 
        let temp_dir = std::env::temp_dir();
        let temp_filename = format!("elusion_ftp_temp_{}.xlsx", Utc::now().timestamp());
        let temp_path = temp_dir.join(&temp_filename);
        
        df.write_to_excel(&temp_path.to_string_lossy(), sheet_name).await?;
        
        connection.upload_file(&temp_path, remote_path)
            .map_err(|e| ElusionError::Custom(format!("Failed to upload to FTP: {}", e)))?;
        
        std::fs::remove_file(&temp_path).ok();
        
        Ok(())
    }

    /// Write DataFrame result to FTP server as Parquet
    #[cfg(feature = "ftp")]
    pub async fn write_parquet_to_ftp_impl(
        df: &CustomDataFrame,
        server: &str,
        username: &str,
        password: &str,
        remote_path: &str
    ) -> ElusionResult<()> {
        let connection = FtpConnection::new(
            server.to_string(),
            username.to_string(),
            password.to_string()
        );

        let temp_dir = std::env::temp_dir();
        let temp_filename = format!("elusion_ftp_temp_{}.parquet", Utc::now().timestamp());
        let temp_path = temp_dir.join(&temp_filename);
        
        df.write_to_parquet("overwrite", &temp_path.to_string_lossy(), None).await?;
        
        connection.upload_file(&temp_path, remote_path)
            .map_err(|e| ElusionError::Custom(format!("Failed to upload to FTP: {}", e)))?;
        
        std::fs::remove_file(&temp_path).ok();
        
        Ok(())
    }

    /// Write DataFrame result to FTP server as JSON
    #[cfg(feature = "ftp")]
    pub async fn write_json_to_ftp_impl(
        df: &CustomDataFrame,
        server: &str,
        username: &str,
        password: &str,
        remote_path: &str,
        pretty: bool
    ) -> ElusionResult<()> {
        let connection = FtpConnection::new(
            server.to_string(),
            username.to_string(),
            password.to_string()
        );
        
        let temp_dir = std::env::temp_dir();
        let temp_filename = format!("elusion_ftp_temp_{}.json", Utc::now().timestamp());
        let temp_path = temp_dir.join(&temp_filename);
        
        df.write_to_json(&temp_path.to_string_lossy(), pretty).await?;

        connection.upload_file(&temp_path, remote_path)
            .map_err(|e| ElusionError::Custom(format!("Failed to upload to FTP: {}", e)))?;
        
        std::fs::remove_file(&temp_path).ok();
        
        Ok(())
    }


    
// ---- UTILS

#[cfg(feature = "ftp")]
pub struct FtpUtils;

#[cfg(feature = "ftp")]
impl FtpUtils {
    pub fn list_files(
        server: &str,
        username: &str,
        password: &str,
        path: Option<&str>,
        port: Option<u16>,
        use_tls: bool,
    ) -> ElusionResult<Vec<String>> {
        let mut connection = FtpConnection::new(
            server.to_string(),
            username.to_string(),
            password.to_string(),
        );
        if let Some(p) = port {
            connection = connection.with_port(p);
        }
        if use_tls {
            connection = connection.with_tls();
        }
        connection.list_files(path)
    }
}

