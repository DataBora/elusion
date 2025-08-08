use crate::prelude::*;

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
pub struct DeltaPathManager {
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
pub async fn write_to_delta_impl(
    df: &DataFrame,
    path: &str,
    partition_columns: Option<Vec<String>>,
    overwrite: bool,
    write_mode: WriteMode,
) -> Result<(), DeltaTableError> {
    validate_delta_path_simple(path)?;

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

    println!("✅ Wrote data to Delta table at version: {version}");
    Ok(())
}

fn validate_delta_path_simple(path: &str) -> Result<(), DeltaTableError> {
    let common_file_extensions = [
        ".csv", ".json", ".parquet", ".txt", ".xlsx", 
        ".xml", ".avro", ".orc", ".sql", ".yaml", ".yml"
    ];
    
    let path_lower = path.to_lowercase();
    
    for ext in &common_file_extensions {
        if path_lower.ends_with(ext) {
            return Err(DeltaTableError::Generic(
                format!("❌ Invalid Delta table path. Delta tables are directories, not files. Remove the '{}' extension from '{}'", 
                    ext, path)
            ));
        }
    }
    
    Ok(())
}
