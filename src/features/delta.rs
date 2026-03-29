use crate::prelude::*;

/// Attempt to glean the Arrow schema of a DataFusion `DataFrame` by collecting
/// a **small sample** (up to 1 row). If there's **no data**, returns an empty schema
/// or an error
async fn glean_arrow_schema(df: &DataFrame) -> ElusionResult<DeltaSchemaRef> {

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
        _ => DeltaType::STRING,
    }
}

/// Helper struct to manage path conversions between different path types
#[derive(Clone)]
pub struct DeltaPathManager {
    base_path: PathBuf,
}

impl DeltaPathManager {
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

    pub fn base_path_str(&self) -> String {
        self.base_path.to_string_lossy().replace('\\', "/")
    }

    pub fn delta_log_path(&self) -> DeltaPath {
        let base = self.base_path_str();
        DeltaPath::from(format!("{base}/_delta_log"))
    }

    pub fn table_path(&self) -> String {
        self.base_path_str()
    }

    pub fn drive_prefix(&self) -> String {
        let base_path = self.base_path_str();
        if let Some(colon_pos) = base_path.find(':') {
            base_path[..colon_pos + 2].to_string()
        } else {
            "/".to_string()
        }
    }

    pub fn normalize_uri(&self, uri: &str) -> String {
        let drive_prefix = self.drive_prefix();
        let path = uri.trim_start_matches(|c| c != '/' && c != '\\')
            .trim_start_matches(['/', '\\']);
        format!("{}{}", drive_prefix, path).replace('\\', "/")
    }

    pub fn is_delta_table(&self) -> bool {
        let delta_log = self.base_path.join("_delta_log");
        let delta_log_exists = delta_log.is_dir();
        if delta_log_exists {
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

    /// Convert table path string to a file:// Url for deltalake 0.31 APIs
    pub fn table_url(&self) -> Result<url::Url, DeltaTableError> {
        let path = self.base_path_str();
        let abs_path = std::path::Path::new(&path);
        
        let url = url::Url::from_file_path(abs_path)
            .map_err(|_| DeltaTableError::Generic(
                format!("Invalid table path '{}': could not convert to file URL", path)
            ))?;
        
       // println!(" DEBUG table_url: {}", url); 
        Ok(url)
    }
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
    let table_url = path_manager.table_url()?;

    //println!(" Delta table URL: {}", table_url);

    // Collect data first so we know the schema
    let batches = df
        .clone()
        .collect()
        .await
        .map_err(|e| DeltaTableError::Generic(format!("DataFusion collect error: {e}")))?;

    if batches.is_empty() {
        return Err(DeltaTableError::Generic("No data to write".to_string()));
    }

    // Get Arrow schema from datafusion
    let arrow_schema_ref = glean_arrow_schema(df)
        .await
        .map_err(|e| DeltaTableError::Generic(format!("Could not glean Arrow schema: {e}")))?;

    // Convert Arrow schema fields to Delta schema fields
    let delta_fields: Vec<StructField> = arrow_schema_ref
        .fields()
        .iter()
        .map(|field| {
            StructField::new(
                field.name().clone(),
                arrow_to_delta_type(field.data_type()),
                field.is_nullable(),
            )
        })
        .collect();

    let parts = partition_columns.clone().unwrap_or_default();

    let mut config: HashMap<String, Option<String>> = HashMap::new();
    config.insert("delta.minWriterVersion".to_string(), Some("7".to_string()));
    config.insert("delta.minReaderVersion".to_string(), Some("3".to_string()));

    // ── Step 1: Create or open the table ──────────────────────────────────
    let table: DeltaTable = if overwrite {
        // Remove existing data so we start fresh
        if let Err(e) = fs::remove_dir_all(&path_manager.base_path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                return Err(DeltaTableError::Generic(format!(
                    "Failed to remove existing directory at '{}': {e}", path
                )));
            }
        }

        println!("🗑️  Cleared existing Delta table, creating fresh...");

        // DeltaOps::try_from_url on a non-existent path creates the table
        #[allow(deprecated)]
        DeltaOps::try_from_url(table_url.clone())
            .await
            .map_err(|e| DeltaTableError::Generic(format!("Failed to init DeltaOps: {e}")))?
            .create()
            .with_columns(delta_fields.clone())
            .with_partition_columns(parts.clone())
            .with_save_mode(SaveMode::Overwrite)
            .with_configuration(config.clone())
            .await
            .map_err(|e| DeltaTableError::Generic(format!("Failed to create Delta table: {e}")))?

    } else {
        // Check if table exists
        let table_exists = DeltaTableBuilder::from_url(table_url.clone())
            .map(|b| b.build().map(|t| t.version().is_some()).unwrap_or(false))
            .unwrap_or(false);

        if table_exists {
            println!("📂 Appending to existing Delta table...");
            DeltaTableBuilder::from_url(table_url.clone())
                .map_err(|e| DeltaTableError::Generic(format!("Failed to build Delta table: {e}")))?
                .load()
                .await
                .map_err(|e| DeltaTableError::Generic(format!("Failed to load Delta table: {e}")))?
        } else {
            println!("🆕 Creating new Delta table for append...");
            #[allow(deprecated)]
            DeltaOps::try_from_url(table_url.clone())
                .await
                .map_err(|e| DeltaTableError::Generic(format!("Failed to init DeltaOps: {e}")))?
                .create()
                .with_columns(delta_fields.clone())
                .with_partition_columns(parts.clone())
                .with_save_mode(SaveMode::Append)
                .with_configuration(config.clone())
                .await
                .map_err(|e| DeltaTableError::Generic(format!("Failed to create Delta table: {e}")))?
        }
    };

    println!("✅ Delta table ready at version: {:?}", table.version());


    let delta_schema_ref = convert_schema_to_delta(&arrow_schema_ref)
        .map_err(|e| DeltaTableError::Generic(format!("Schema conversion failed: {e}")))?;

    let mut writer_config = HashMap::new();
    writer_config.insert("delta.protocol.minWriterVersion".to_string(), "7".to_string());
    writer_config.insert("delta.protocol.minReaderVersion".to_string(), "3".to_string());

    let mut writer = RecordBatchWriter::try_new(
        table_url.as_str(),
        delta_schema_ref,
        partition_columns,
        Some(writer_config),
    )?;

    let batch_count = batches.len();
    for (i, batch) in batches.into_iter().enumerate() {
        let delta_batch = convert_batch_to_delta(&batch)
            .map_err(|e| DeltaTableError::Generic(format!("Batch {i} conversion failed: {e}")))?;
        writer.write_with_mode(delta_batch, write_mode).await?;
        println!("  ✍️  Wrote batch {}/{}", i + 1, batch_count);
    }

    let mut table = table;
    let version = writer
        .flush_and_commit(&mut table)
        .await
        .map_err(|e| DeltaTableError::Generic(format!("Failed to flush and commit: {e}")))?;

    println!("✅ Wrote data to Delta table at version: {version}");
    Ok(())
}

/// Convert a datafusion arrow SchemaRef (arrow 54) to deltalake arrow SchemaRef (arrow 57)
/// by round-tripping through IPC bytes.
fn convert_schema_to_delta(
    schema: &DeltaSchemaRef,
) -> Result<deltalake::arrow::datatypes::SchemaRef, String> {
    use datafusion::arrow::ipc::writer::StreamWriter as DFStreamWriter;

    // Serialize schema via an empty IPC stream
    let mut buf = Vec::new();
    {
        let mut writer = DFStreamWriter::try_new(&mut buf, schema.as_ref())
            .map_err(|e| format!("IPC writer error: {e}"))?;
        writer.finish().map_err(|e| format!("IPC finish error: {e}"))?;
    }

    // Deserialize with deltalake's arrow
    let cursor = std::io::Cursor::new(buf);
    let reader = deltalake::arrow::ipc::reader::StreamReader::try_new(cursor, None)
        .map_err(|e| format!("IPC reader error: {e}"))?;

    Ok(reader.schema())
}

/// Convert a datafusion RecordBatch (arrow 54) to a deltalake RecordBatch (arrow 57)
/// by round-tripping through IPC bytes.
fn convert_batch_to_delta(
    batch: &datafusion::arrow::array::RecordBatch,
) -> Result<deltalake::arrow::array::RecordBatch, String> {
    use datafusion::arrow::ipc::writer::StreamWriter as DFStreamWriter;
    use deltalake::arrow::ipc::reader::StreamReader as DeltaStreamReader;

    let mut buf = Vec::new();
    {
        let mut writer = DFStreamWriter::try_new(&mut buf, batch.schema().as_ref())
            .map_err(|e| format!("IPC writer error: {e}"))?;
        writer.write(batch).map_err(|e| format!("IPC write error: {e}"))?;
        writer.finish().map_err(|e| format!("IPC finish error: {e}"))?;
    }

    let cursor = std::io::Cursor::new(buf);
    let mut reader = DeltaStreamReader::try_new(cursor, None)
        .map_err(|e| format!("IPC reader error: {e}"))?;

    reader
        .next()
        .ok_or_else(|| "No batch in IPC stream".to_string())?
        .map_err(|e| format!("IPC batch read error: {e}"))
}

fn validate_delta_path_simple(path: &str) -> Result<(), DeltaTableError> {
    let common_file_extensions = [
        ".csv", ".json", ".parquet", ".txt", ".xlsx",
        ".xml", ".avro", ".orc", ".sql", ".yaml", ".yml",
    ];
    let path_lower = path.to_lowercase();
    for ext in &common_file_extensions {
        if path_lower.ends_with(ext) {
            return Err(DeltaTableError::Generic(format!(
                "❌ Invalid Delta table path. Delta tables are directories, not files. \
                Remove the '{}' extension from '{}'",
                ext, path
            )));
        }
    }
    Ok(())
}