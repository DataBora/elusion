#[cfg(feature = "copydata")]
use crate::prelude::*;
#[cfg(feature = "copydata")]
use datafusion::arrow::csv::ReaderBuilder as CsvReaderBuilder;
#[cfg(feature = "copydata")]
use datafusion::arrow::json::ReaderBuilder as JsonReaderBuilder;
#[cfg(feature = "copydata")]
use parquet::arrow::ArrowWriter;
#[cfg(feature = "copydata")]
use parquet::file::properties::{WriterProperties, WriterVersion};
#[cfg(feature = "copydata")]
use parquet::basic::Compression;
#[cfg(feature = "copydata")]
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
#[cfg(feature = "copydata")]
use std::pin::Pin;
#[cfg(feature = "copydata")]
use futures::Stream;
#[cfg(feature = "copydata")]
use datafusion::arrow::csv::reader::Format;
#[cfg(feature = "fabric")]
pub use crate::features::fabric::FabricAuthMethod;
#[cfg(feature = "fabric")]
use crate::features::fabric::OneLakeClient;
#[cfg(feature = "fabric")]
use crate::features::fabric::OneLakeConfig;
#[cfg(feature = "copydata")]
use crate::helper_funcs::infer_schema_json::infer_schema_from_json;
#[cfg(feature = "copydata")]
use crate::build_record_batch;
#[cfg(feature = "copydata")]
use crate::features::csv::array_value_to_string;

use crate::ElusionResult;
use crate::ElusionError;

#[cfg(feature = "copydata")]
#[derive(Debug, Clone, PartialEq)]
pub enum OutputFormat {
    Parquet,
    Csv,
}

#[cfg(not(feature = "copydata"))]
#[derive(Debug, Clone, PartialEq)]
pub enum OutputFormat {
    Parquet,
    Csv,
}

/// Configuration for copy operations
#[cfg(feature = "copydata")]
#[derive(Debug, Clone)]
pub struct CopyConfig {
    pub batch_size: usize,
    pub compression: Option<ParquetCompression>,
    pub csv_delimiter: Option<u8>,
    pub infer_schema: bool,
    pub output_format: OutputFormat,
}

#[cfg(not(feature = "copydata"))]
#[derive(Debug, Clone)]
pub struct CopyConfig {
    pub batch_size: usize,
    pub compression: Option<ParquetCompression>,
    pub csv_delimiter: Option<u8>,
    pub infer_schema: bool,
    pub output_format: OutputFormat,
}

#[cfg(feature = "copydata")]
#[derive(Debug, Clone)]
pub enum ParquetCompression {
    Uncompressed,
    Snappy,
}

#[cfg(not(feature = "copydata"))]
#[derive(Debug, Clone)]
pub enum ParquetCompression {
    Uncompressed,
    Snappy,
}

#[cfg(feature = "copydata")]
impl Default for CopyConfig {
    fn default() -> Self {
        Self {
            batch_size: 100_000,
            compression: Some(ParquetCompression::Snappy),
             csv_delimiter: Some(b','),
             infer_schema: false,
             output_format: OutputFormat::Parquet,
        }
    }
}

#[cfg(not(feature = "copydata"))]
impl Default for CopyConfig {
    fn default() -> Self {
        Self {
            batch_size: 100_000,
            compression: Some(ParquetCompression::Snappy),
            csv_delimiter: Some(b','),
            infer_schema: false,
            output_format: OutputFormat::Parquet, 
        }
    }
}

/// Source configurations for data copy
#[cfg(feature = "copydata")]
pub enum CopySource<'a> {
    File {
        path: &'a str,
        csv_delimiter: Option<u8>,
    },
    #[cfg(feature = "fabric")]
    FabricOneLake {
        abfss_path: &'a str,
        file_path: &'a str,
        auth: FabricAuthMethod,
    },
}

#[cfg(not(feature = "copydata"))]
pub enum CopySource<'a> {
    File {
        path: &'a str,
        csv_delimiter: Option<u8>,
    },
    FabricOneLake {
        abfss_path: &'a str,
        file_path: &'a str,
        auth: FabricAuthMethod,
    },
}

#[cfg(feature = "copydata")]
pub enum CopyDestination<'a> {
    File {
        path: &'a str,
    },
    #[cfg(feature = "fabric")]
    FabricOneLake {
        abfss_path: &'a str,
        file_path: &'a str,
        auth: FabricAuthMethod,
    },
}

#[cfg(not(feature = "copydata"))]
pub enum CopyDestination<'a> {
    File {
        path: &'a str,
    },
    FabricOneLake {
        abfss_path: &'a str,
        file_path: &'a str,
        auth: FabricAuthMethod,
    },
}

#[cfg(not(feature = "fabric"))]
#[derive(Debug, Clone)]
pub enum FabricAuthMethod {
    AzureCLI,
    ServicePrincipal {
        tenant_id: String,
        client_id: String,
        client_secret: String,
    },
}
/// Core copy_data implementation
#[cfg(feature = "copydata")]
pub struct CopyDataEngine;

#[cfg(not(feature = "copydata"))]
pub struct CopyDataEngine;

#[cfg(feature = "copydata")]
enum DataWriter {
    ParquetFile(ArrowWriter<File>),
    #[cfg(feature = "fabric")]
    ParquetBuffer(ArrowWriter<Vec<u8>>),
    CsvFile(csv::Writer<File>),
    #[cfg(feature = "fabric")]
    CsvBuffer(csv::Writer<Vec<u8>>),
}


#[cfg(feature = "copydata")]
impl DataWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> ElusionResult<()> {
        match self {
            DataWriter::ParquetFile(w) => {
                w.write(batch)
                    .map_err(|e| ElusionError::Custom(format!("Parquet write error: {}", e)))
            }
            #[cfg(feature = "fabric")]
            DataWriter::ParquetBuffer(w) => {
                w.write(batch)
                    .map_err(|e| ElusionError::Custom(format!("Parquet write error: {}", e)))
            }
            DataWriter::CsvFile(w) => Self::write_batch_to_csv(w, batch),
            #[cfg(feature = "fabric")]
            DataWriter::CsvBuffer(w) => Self::write_batch_to_csv(w, batch),
        }
    }
    
    fn write_batch_to_csv<W: std::io::Write>(writer: &mut csv::Writer<W>, batch: &RecordBatch) -> ElusionResult<()> {
        for row_idx in 0..batch.num_rows() {
            let mut record = Vec::new();
            
            for col_idx in 0..batch.num_columns() {
                let column = batch.column(col_idx);
                let value = array_value_to_string(column.as_ref(), row_idx);
                record.push(value);
            }
            
            writer.write_record(&record)
                .map_err(|e| ElusionError::Custom(format!("CSV write error: {}", e)))?;
        }
        
        Ok(())
    }
}

#[cfg(feature = "copydata")]
pub fn stream_json_array_batches(
    path: &str,
    batch_size: usize,
    schema: SchemaRef,
) -> ElusionResult<impl Stream<Item = ElusionResult<RecordBatch>>> {
    let file = File::open(path)
        .map_err(|e| ElusionError::Custom(format!("Failed to open JSON file: {}", e)))?;
    let mut reader = BufReader::new(file);

    let mut json_str = String::new();
    reader.read_to_string(&mut json_str)
        .map_err(|e| ElusionError::Custom(format!("Failed to read JSON file: {}", e)))?;

    let json_value: Value = serde_json::from_str(&json_str)
        .map_err(|e| ElusionError::Custom(format!("Failed to parse JSON file: {}", e)))?;

    let json_array = match json_value {
        Value::Array(arr) => arr,
        _ => return Err(ElusionError::Custom("Expected a JSON array at top level".to_string())),
    };

    let index = 0;
    let schema_clone = schema.clone();

    let stream = futures::stream::unfold((json_array, index), move |(data, mut i)| {
        let schema = schema_clone.clone(); 
        async move {
            if i >= data.len() {
                return None;
            }

            let mut rows = Vec::with_capacity(batch_size);
            while i < data.len() && rows.len() < batch_size {
                if let Value::Object(obj) = &data[i] {
                    rows.push(obj.clone().into_iter().collect::<HashMap<_, _>>());
                }
                i += 1;
            }

            let batch = build_record_batch(&rows, schema);
            Some((
                batch.map_err(|e| ElusionError::Custom(format!("Arrow error: {}", e))),
                (data, i),
            ))
        }
    });

    Ok(Box::pin(stream))
}

#[cfg(feature = "copydata")]
impl CopyDataEngine {

    /// Copy data directly from source to Parquet file without loading into memory
    pub async fn copy_data_enhanced(
        source: CopySource<'_>,
        destination: CopyDestination<'_>,
        config: Option<CopyConfig>,
    ) -> ElusionResult<()> {
        let start_time = std::time::Instant::now();
        let config = config.unwrap_or_default();
        
        println!("🚀 Starting data copy operation...");
        println!("⚙️  Batch size: {} rows", config.batch_size);
        println!("🗜️  Compression: {:?}", config.compression);
        println!();
        
        // Create streaming reader from source
        let mut stream = Self::create_source_stream(source, config.batch_size, &config).await?;
        
        let mut rows_read = 0;
        let mut rows_written = 0;
        let mut batches_read = 0;
        let mut batches_written = 0;
        
        // Create writer based on destination type
        let mut writer: Option<DataWriter> = None;  
        
        #[cfg(feature = "fabric")]
        let mut fabric_client: Option<OneLakeClient> = None;
        #[cfg(feature = "fabric")]
        let mut fabric_file_path: Option<String> = None;
        
        match &destination {
            CopyDestination::File { path } => {
                println!("📁 Output: {}", path);
            }
            #[cfg(feature = "fabric")]
            CopyDestination::FabricOneLake { abfss_path, file_path, auth } => {
                println!("📁 Output: Fabric OneLake");
                println!("   ABFSS: {}", abfss_path);
                println!("   File: {}", file_path);
                
                // Parse ABFSS path and create client
                let parsed = OneLakeClient::parse_abfss_path(abfss_path)?;
                
                let onelake_config = OneLakeConfig {
                    workspace_id: parsed.workspace_id,
                    lakehouse_id: parsed.lakehouse_id,
                    warehouse_id: parsed.warehouse_id,
                    auth_method: auth.clone(),
                };
                
                let mut client = OneLakeClient::new(onelake_config);
                client.authenticate().await?;
                fabric_client = Some(client);
                
                // Build full file path
                let full_path = if parsed.base_path == "Files" || parsed.base_path.is_empty() {
                    file_path.to_string()
                } else {
                    format!("{}/{}", parsed.base_path.trim_start_matches("Files/"), file_path)
                };
                fabric_file_path = Some(full_path);
            }
        }
        
        // Stream data in batches
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            
            let batch_rows = batch.num_rows();
            rows_read += batch_rows;
            batches_read += 1;
            
            println!("📖 Read batch #{}: {} rows (total: {} rows)", 
                batches_read, batch_rows, rows_read);
            
            // Initialize writer with schema from first batch
            if writer.is_none() {
                let schema = batch.schema();
                
                match &config.output_format {
                    OutputFormat::Parquet => {
                        let props = Self::create_writer_properties(&config);
                        
                        match &destination {
                            CopyDestination::File { path } => {
                                let file = File::create(path)
                                    .map_err(|e| ElusionError::Custom(format!("Failed to create output file: {}", e)))?;
                                
                                writer = Some(DataWriter::ParquetFile(ArrowWriter::try_new(file, schema, Some(props))
                                    .map_err(|e| ElusionError::Custom(format!("Failed to create Parquet writer: {}", e)))?));
                            }
                            #[cfg(feature = "fabric")]
                            CopyDestination::FabricOneLake { .. } => {
                                writer = Some(DataWriter::ParquetBuffer(ArrowWriter::try_new(Vec::new(), schema, Some(props))
                                    .map_err(|e| ElusionError::Custom(format!("Failed to create Parquet writer: {}", e)))?));
                            }
                        }
                    }
                    OutputFormat::Csv => {
                        let delimiter = config.csv_delimiter.unwrap_or(b',');
                        
                        match &destination {
                            CopyDestination::File { path } => {
                                let file = File::create(path)
                                    .map_err(|e| ElusionError::Custom(format!("Failed to create CSV file: {}", e)))?;
                                
                                let mut csv_writer = csv::WriterBuilder::new()
                                    .delimiter(delimiter)
                                    .from_writer(file);
                                
                                // Write headers
                                let headers: Vec<String> = schema.fields().iter()
                                    .map(|f| f.name().to_string())
                                    .collect();
                                csv_writer.write_record(&headers)
                                    .map_err(|e| ElusionError::Custom(format!("Failed to write CSV headers: {}", e)))?;
                                
                                writer = Some(DataWriter::CsvFile(csv_writer));
                            }
                            #[cfg(feature = "fabric")]
                            CopyDestination::FabricOneLake { .. } => {
                                let mut csv_writer = csv::WriterBuilder::new()
                                    .delimiter(delimiter)
                                    .from_writer(Vec::new());
                                
                                // Write headers
                                let headers: Vec<String> = schema.fields().iter()
                                    .map(|f| f.name().to_string())
                                    .collect();
                                csv_writer.write_record(&headers)
                                    .map_err(|e| ElusionError::Custom(format!("Failed to write CSV headers: {}", e)))?;
                                
                                writer = Some(DataWriter::CsvBuffer(csv_writer));
                            }
                        }
                    }
                }
                
                println!("📝 Initialized {} writer with schema", 
                    if config.output_format == OutputFormat::Csv { "CSV" } else { "Parquet" });
                println!();
            }
            
            // Write batch to Parquet
            if let Some(ref mut w) = writer {
            w.write_batch(&batch)
                    .map_err(|e| ElusionError::Custom(format!("Failed to write batch: {}", e)))?;
                
                rows_written += batch_rows;
                batches_written += 1;
                
                println!("✍️  Wrote batch #{}: {} rows (total: {} rows)", 
                    batches_written, batch_rows, rows_written);
                println!();
            }
        }
        
        // Finalizing file
        let bytes_written = if let Some(w) = writer {
            println!("🔒 Finalizing output file...");
            
            match w {
                // ====== PARQUET FILE ======
                DataWriter::ParquetFile(file_writer) => {
                    file_writer.close()
                        .map_err(|e| ElusionError::Custom(format!("Failed to close Parquet writer: {}", e)))?;
                    
                    if let CopyDestination::File { path } = &destination {
                        std::fs::metadata(path)
                            .map(|m| m.len() as usize)
                            .unwrap_or(0)
                    } else {
                        0
                    }
                }
                
                // ====== PARQUET BUFFER (Fabric) ======
                #[cfg(feature = "fabric")]
                DataWriter::ParquetBuffer(buffer_writer) => {
                    println!("📦 Finalizing in-memory Parquet buffer for Fabric upload...");
                    
                    let final_buffer = buffer_writer.into_inner()
                        .map_err(|e| ElusionError::Custom(format!("Failed to finalize buffer: {}", e)))?;
                    
                    let buffer_size = final_buffer.len();
                    let buffer_mb = buffer_size as f64 / 1_048_576.0;
                    
                    println!("✅ Buffer finalized: {:.2} MB", buffer_mb);
                    
                    // Upload buffer to Fabric
                    if let (Some(mut client), Some(file_path)) = (fabric_client, fabric_file_path) {
                        println!("📤 Uploading {:.2} MB to Fabric OneLake...", buffer_mb);
                        println!("   ⏳ Please wait, upload in progress (single operation)...");
                        
                        let upload_start = std::time::Instant::now();
                        client.upload_file(&file_path, final_buffer).await?;
                        
                        let upload_duration = upload_start.elapsed().as_secs_f64();
                        let upload_speed = buffer_mb / upload_duration;
                        
                        println!("✅ Upload completed in {:.2}s ({:.2} MB/s)", upload_duration, upload_speed);
                    }
                    
                    buffer_size
                }
                
                // ====== CSV FILE ======
                DataWriter::CsvFile(mut csv_writer) => {
                    csv_writer.flush()
                        .map_err(|e| ElusionError::Custom(format!("Failed to flush CSV writer: {}", e)))?;
                    
                    if let CopyDestination::File { path } = &destination {
                        std::fs::metadata(path)
                            .map(|m| m.len() as usize)
                            .unwrap_or(0)
                    } else {
                        0
                    }
                }
                
                // ====== CSV BUFFER (Fabric) ======
                #[cfg(feature = "fabric")]
                DataWriter::CsvBuffer(mut csv_writer) => {
                    println!("📦 Finalizing in-memory CSV buffer for Fabric upload...");
                    
                    csv_writer.flush()
                        .map_err(|e| ElusionError::Custom(format!("Failed to flush CSV writer: {}", e)))?;
                    
                    let final_buffer = csv_writer.into_inner()
                        .map_err(|e| ElusionError::Custom(format!("Failed to get CSV buffer: {}", e)))?;
                    
                    let buffer_size = final_buffer.len();
                    let buffer_mb = buffer_size as f64 / 1_048_576.0;
                    
                    println!("✅ CSV buffer finalized: {:.2} MB", buffer_mb);
                    
                    if let (Some(mut client), Some(file_path)) = (fabric_client, fabric_file_path) {
                        println!("📤 Uploading {:.2} MB CSV to Fabric OneLake...", buffer_mb);
                        println!("   ⏳ Please wait, upload in progress (single operation)...");
                        
                        let upload_start = std::time::Instant::now();
                        client.upload_file(&file_path, final_buffer).await?;
                        
                        let upload_duration = upload_start.elapsed().as_secs_f64();
                        let upload_speed = buffer_mb / upload_duration;
                        
                        println!("✅ Upload completed in {:.2}s ({:.2} MB/s)", upload_duration, upload_speed);
                    }
                    
                    buffer_size
                }
            }
        } else {
            0
        };
        
        let duration_ms = start_time.elapsed().as_millis();
        
        println!("✅ Copy operation completed!");
        println!("📊 Summary:");
        println!("   • Rows read: {}", rows_read);
        println!("   • Rows written: {}", rows_written);
        println!("   • Batches processed: {}", batches_read);
        println!("   • Output size: {:.2} MB", bytes_written as f64 / 1_048_576.0);
        println!("   • Duration: {:.2}s", duration_ms as f64 / 1000.0);
        println!();
        
        Ok(())
    }
    
    /// Create Parquet writer properties based on configuration
    fn create_writer_properties(config: &CopyConfig) -> WriterProperties {

        let compression = match config.compression {
            Some(ParquetCompression::Uncompressed) => Compression::UNCOMPRESSED,
            Some(ParquetCompression::Snappy) => Compression::SNAPPY,
            None => Compression::UNCOMPRESSED,
        };
        
        WriterProperties::builder()
            .set_compression(compression)
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_created_by("Elusion".to_string())
            .build()
    }
    
    /// Create a streaming source from file
    async fn create_source_stream(
        source: CopySource<'_>,
        batch_size: usize,
        config: &CopyConfig,
    ) -> ElusionResult<Pin<Box<dyn Stream<Item = ElusionResult<RecordBatch>> + Send>>> {
        match source {
            CopySource::File { path, csv_delimiter } => {
                Self::file_stream(path.to_string(), csv_delimiter, batch_size, config).await
            }
            #[cfg(feature = "fabric")]
            CopySource::FabricOneLake { abfss_path, file_path, auth } => {
                Self::fabric_stream(abfss_path.to_string(), file_path.to_string(), auth, batch_size, config).await
            }
        }
    }
    
    /// Stream data from local files (CSV, Parquet, JSON) with explicit batch control
    async fn file_stream(
        path: String,
        csv_delimiter: Option<u8>,  
        batch_size: usize,
        config: &CopyConfig,  
    ) -> ElusionResult<Pin<Box<dyn Stream<Item = ElusionResult<RecordBatch>> + Send>>> {
        
        let extension = path.split('.').last().unwrap_or("").to_lowercase();
        
        match extension.as_str() {
            "parquet" => {
                println!("📦 Reading Parquet file...");
                
                // Open Parquet file with streaming reader
                let file = File::open(&path)
                    .map_err(|e| ElusionError::Custom(format!("Failed to open Parquet file: {}", e)))?;
                
                let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                    .map_err(|e| ElusionError::Custom(format!("Failed to create Parquet reader: {}", e)))?;
                
                let reader = builder
                    .with_batch_size(batch_size)
                    .build()
                    .map_err(|e| ElusionError::Custom(format!("Failed to build Parquet reader: {}", e)))?;
                
                // Convert iterator to stream
                let stream = stream::iter(reader.into_iter())
                    .map(|result| {
                        result.map_err(|e| ElusionError::Custom(format!("Parquet read error: {}", e)))
                    });
                
                Ok(Box::pin(stream))
            }
            
            "csv" => {
                let delimiter = csv_delimiter
                    .or(config.csv_delimiter)
                    .unwrap_or(b',');

                if config.infer_schema {
                    // INFER SCHEMA MODE: Detect data types (for clean CSVs)
                    println!("🔍 Inferring CSV schema with type detection...");
                    
                    let file = File::open(&path)
                        .map_err(|e| ElusionError::Custom(format!("Failed to open CSV file: {}", e)))?;
        
                    let format = Format::default()
                        .with_delimiter(delimiter)
                        .with_header(true);
                    
                    let (inferred_schema, _records_read) = format  
                        .infer_schema(&file, Some(50000)) 
                        .map_err(|e| ElusionError::Custom(format!("Failed to infer CSV schema: {}", e)))?;
                                    
                    // Clean column names (remove newlines, carriage returns, extra spaces)
                    let cleaned_fields: Vec<Field> = inferred_schema
                        .fields()
                        .iter()
                        .map(|field| {
                            let clean_name = field.name()
                                .trim()
                                .replace('\n', " ")
                                .replace('\r', "")
                                .replace(" ", "_")
                                .replace("  ", "_"); 
                            Field::new(clean_name, field.data_type().clone(), field.is_nullable())
                        })
                        .collect();
                    
                    let cleaned_schema = Arc::new(Schema::new(cleaned_fields));
                    
                    println!("📋 Inferred {} columns with detected types", cleaned_schema.fields().len());
                    
                    // Reopen file for reading
                    let file = File::open(&path)
                        .map_err(|e| ElusionError::Custom(format!("Failed to open CSV file: {}", e)))?;
                    
                    // Build reader with cleaned schema
                    let reader = CsvReaderBuilder::new(cleaned_schema)
                        .with_delimiter(delimiter)
                        .with_batch_size(batch_size)
                        .with_header(true)
                        .build(file)
                        .map_err(|e| ElusionError::Custom(format!("Failed to create CSV reader: {}", e)))?;
                    
                    let stream = stream::iter(reader.into_iter())
                        .map(|result| {
                            result.map_err(|e| ElusionError::Custom(format!("CSV read error: {}", e)))
                        });
                    
                    Ok(Box::pin(stream))
                    
                } else {
                    // SAFE STRING MODE: All columns as Utf8 (for messy CSVs)
                    println!("📋 Reading CSV with all columns as strings (safe mode)");
                    
                    // Use Rust CSV crate for schema inference with better buffer handling
                    let mut csv_reader = csv::ReaderBuilder::new()
                        .delimiter(delimiter)
                        .has_headers(true)
                        .flexible(true) 
                        .buffer_capacity(8 * 1024 * 1024)  // 8MB buffer for wide CSVs
                        .from_path(&path)
                        .map_err(|e| ElusionError::Custom(format!("Failed to open CSV for schema inference: {}", e)))?;
                    
                    // Get headers
                    let headers = csv_reader.headers()
                        .map_err(|e| ElusionError::Custom(format!("Failed to read CSV headers: {}", e)))?
                        .clone();
                    
                    println!("📋 Detected {} columns in CSV", headers.len());
                    
                    // Build Arrow schema - treat all columns as Utf8 for maximum compatibility
                    let fields: Vec<Field> = headers.iter()
                        .map(|name| {
                            let clean_name = name
                                .trim()
                                .replace('\n', " ")
                                .replace('\r', "")
                                .replace(" ", "_")
                                .replace("  ", "_"); 
                            Field::new(clean_name, ArrowDataType::Utf8, true)
                        })
                        .collect();
                    
                    let schema = Arc::new(Schema::new(fields));
                    
                    // Reopen file for Arrow CSV reader
                    let file = File::open(&path)
                        .map_err(|e| ElusionError::Custom(format!("Failed to open CSV file: {}", e)))?;
                    
                    //   Arrow CSV reader with cleaned schema
                    let reader = CsvReaderBuilder::new(schema)
                        .with_delimiter(delimiter)
                        .with_batch_size(batch_size)
                        .with_header(true)
                        .build(file)
                        .map_err(|e| ElusionError::Custom(format!("Failed to create CSV reader: {}", e)))?;
                    
                    let stream = stream::iter(reader.into_iter())
                        .map(|result| {
                            result.map_err(|e| ElusionError::Custom(format!("CSV read error: {}", e)))
                        });
                    
                    Ok(Box::pin(stream))
                }
            }
            
            "json" | "ndjson" => {
                println!("📄 Reading JSON/NDJSON file...");
                
                // First, detect if this is NDJSON by reading first line
                let file_peek = File::open(&path)
                    .map_err(|e| ElusionError::Custom(format!("Failed to open JSON file: {}", e)))?;
                
                let mut peek_reader = BufReader::new(file_peek);
                let mut first_line = String::new();
                peek_reader.read_line(&mut first_line)
                    .map_err(|e| ElusionError::Custom(format!("Failed to read first line: {}", e)))?;
                
                let is_ndjson = first_line.trim().starts_with('{') && !first_line.trim().starts_with('[');
                
                if is_ndjson {
                    println!("🔍 Detected NDJSON format (newline-delimited JSON objects)");
                    
                    // Parse first line to get the first row for inference
                    let first_obj: Value = serde_json::from_str(first_line.trim())
                        .map_err(|e| ElusionError::Custom(format!("Failed to parse first JSON object: {}", e)))?;
                    
                    let first_map = if let Value::Object(map) = first_obj {
                        map.into_iter().collect::<HashMap<String, Value>>()
                    } else {
                        return Err(ElusionError::Custom("Expected JSON object in NDJSON file".to_string()));
                    };
                    
                    // For better inference, optionally read more rows (e.g., up to 10)
                    // Here, using just the first for simplicity; increase if types vary across rows
                    let rows: Vec<HashMap<String, Value>> = vec![first_map];
                    
                    // Infer schema using your custom function
                    let schema = infer_schema_from_json(&rows);
                    
                    if schema.fields().is_empty() {
                        return Err(ElusionError::Custom("No fields found in JSON object".to_string()));
                    }
                    
                    println!("📋 Inferred {} columns from NDJSON", schema.fields().len());
                    
                    // Create reader with inferred schema
                    let file = File::open(&path)
                        .map_err(|e| ElusionError::Custom(format!("Failed to open NDJSON file: {}", e)))?;
                    
                    let buf_reader = BufReader::new(file);
                    
                    let reader = JsonReaderBuilder::new(schema)
                        .with_batch_size(batch_size)
                        .build(buf_reader)
                        .map_err(|e| ElusionError::Custom(format!("Failed to create NDJSON reader: {}", e)))?;
                    
                    // Streaming
                    let stream = futures::stream::unfold(reader, |mut reader| async move {
                        match reader.next() {
                            Some(Ok(batch)) => Some((Ok(batch), reader)),
                            Some(Err(e)) => Some((Err(ElusionError::Custom(format!("NDJSON read error: {}", e))), reader)),
                            None => None,
                        }
                    });
                    
                    Ok(Box::pin(stream))
                    
                } else {
                    println!("🔍 Detected JSON array format");
                    println!("📖 Reading file for schema inference (this may take a moment for large files)...");

                    // Step 1: Read only a few rows to infer schema
                    let file = File::open(&path)
                        .map_err(|e| ElusionError::Custom(format!("Failed to open JSON file: {}", e)))?;
                    
                    let file_size = file.metadata()
                        .map(|m| m.len() as f64 / 1_048_576.0)
                        .unwrap_or(0.0);
                    
                    println!("   📏 File size: {:.2} MB", file_size);
                    
                    let buf_reader = BufReader::new(file);

                    let json_value: Value = serde_json::from_reader(buf_reader)
                        .map_err(|e| ElusionError::Custom(format!("Failed to parse JSON array: {}", e)))?;

                    let json_array = match json_value {
                        Value::Array(arr) => {
                            println!("   ✅ Parsed JSON array with {} objects", arr.len());
                            arr
                        }
                        _ => return Err(ElusionError::Custom("Expected a JSON array at top level".to_string())),
                    };

                
                    println!("🔍 Inferring schema from first 100 objects...");
                    let mut rows = Vec::new();
                    for val in json_array.iter().take(100) {
                        if let Value::Object(map) = val {
                            rows.push(map.clone().into_iter().collect::<HashMap<_, _>>());
                        } else {
                            return Err(ElusionError::Custom("Expected JSON objects inside array".to_string()));
                        }
                    }

                    let schema = infer_schema_from_json(&rows);

                    if schema.fields().is_empty() {
                        return Err(ElusionError::Custom("No fields found in JSON array".to_string()));
                    }

                    println!("✅ Inferred {} columns from JSON array", schema.fields().len());
                    println!("📦 Building streaming batches...");

                    // Step 3: Use custom streaming logic (not arrow-json)
                    let stream = stream_json_array_batches(&path, batch_size, schema)?;

                    Ok(Box::pin(stream))
                }

            }
            
            _ => {
                Err(ElusionError::Custom(
                    format!("Unsupported file format: '{}'. Supported formats: CSV, Parquet, JSON/NDJSON.", extension)
                ))
            }
        }
    }

    #[cfg(feature = "fabric")]
    async fn fabric_stream(
        abfss_path: String,
        file_path: String,
        auth: FabricAuthMethod,
        batch_size: usize,
        config: &CopyConfig,
    ) -> ElusionResult<Pin<Box<dyn Stream<Item = ElusionResult<RecordBatch>> + Send>>> {
        println!("📥 Reading from Fabric OneLake: {}", file_path);
        
        // Parse ABFSS path
        let parsed = OneLakeClient::parse_abfss_path(&abfss_path)?;
        
        // Create and authenticate client
        let onelake_config = OneLakeConfig {
            workspace_id: parsed.workspace_id,
            lakehouse_id: parsed.lakehouse_id,
            warehouse_id: parsed.warehouse_id,
            auth_method: auth,
        };
        
        let mut client = OneLakeClient::new(onelake_config);
        client.authenticate().await?;
        
        // Build full file path
        let full_file_path = if parsed.base_path == "Files" || parsed.base_path.is_empty() {
            file_path.clone()
        } else {
            format!("{}/{}", parsed.base_path.trim_start_matches("Files/"), file_path)
        };
        
        // Download file
        let content = client.download_file(&full_file_path).await?;
        
        // Write to temp file
        let temp_dir = std::env::temp_dir();
        let file_extension = file_path.split('.').last().unwrap_or("tmp").to_lowercase();
        let temp_file = temp_dir.join(format!(
            "fabric_copy_{}_{}.{}", 
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            std::process::id(),
            file_extension
        ));
        
        std::fs::write(&temp_file, content)
            .map_err(|e| ElusionError::Custom(format!("Failed to write temporary file: {}", e)))?;
        
        // Stream from temp file
        let temp_path = temp_file.to_string_lossy().to_string();
        let stream = Self::file_stream(temp_path.clone(), None, batch_size, config).await?;
        
        // Clean up temp file after streaming (best effort)
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            let _ = std::fs::remove_file(&temp_file);
        });
        
        Ok(stream)
    }
}

// USERS FUNCS
  /// Copy data from file source to Parquet file
    #[cfg(feature = "copydata")]
    pub async fn copy_data(
        source: CopySource<'_>,
        destination: CopyDestination<'_>,
        config: Option<CopyConfig>,
    ) -> ElusionResult<()> {
        CopyDataEngine::copy_data_enhanced(source, destination, config).await
    }

    #[cfg(not(feature = "copydata"))]
    pub async fn copy_data(
        _source: CopySource<'_>,
        _destination: CopyDestination<'_>,
        _config: Option<CopyConfig>,
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom("*** Warning ***: copydata feature not enabled. Add feature copydata under [dependencies]".to_string()))
    }
    
    /// Copy from local file to Parquet (useful for format conversion)
    #[cfg(feature = "copydata")]
    pub async fn copy_file_to_parquet(
        input_path: &str,
        output_path: &str,
        compression: Option<ParquetCompression>,
    ) -> ElusionResult<()> {
        CopyDataEngine::copy_data_enhanced( 
            CopySource::File {
                path: input_path,
                csv_delimiter: None,
            },
            CopyDestination::File {  
                path: output_path,
            },
            compression.map(|c| CopyConfig {
                compression: Some(c),
                ..Default::default()
            }),
        ).await
    }

    #[cfg(not(feature = "copydata"))]
    pub async fn copy_file_to_parquet(
        _input_path: &str,
        _output_path: &str,
        _compression:  Option<ParquetCompression>,
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom("*** Warning ***: copydata feature not enabled. Add feature copydata under [dependencies]".to_string()))
    }

    /// Copy from local file to CSV
    #[cfg(feature = "copydata")]
    pub async fn copy_file_to_csv(
        input_path: &str,
        output_path: &str,
        csv_delimiter: Option<u8>,
    ) -> ElusionResult<()> {
        CopyDataEngine::copy_data_enhanced(
            CopySource::File {
                path: input_path,
                csv_delimiter: None,
            },
            CopyDestination::File {
                path: output_path,
            },
            Some(CopyConfig {
                output_format: OutputFormat::Csv,
                csv_delimiter,
                ..Default::default()
            }),
        ).await
    }

    #[cfg(not(feature = "copydata"))]
    pub async fn copy_file_to_csv(
        _input_path: &str,
        _output_path: &str,
        _csv_delimiter: Option<u8>,
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom("*** Warning ***: copydata feature not enabled. Add feature copydata under [dependencies]".to_string()))
    }

    /// Copy file to Fabric OneLake with Azure CLI auth
    #[cfg(all(feature = "copydata", feature = "fabric"))]
    pub async fn copy_file_to_fabric(
        input_path: &str,
        abfss_path: &str,
        output_file: &str,
        compression: Option<ParquetCompression>,
    ) -> ElusionResult<()> {
        copy_data(
            CopySource::File {
                path: input_path,
                csv_delimiter: None,
            },
            CopyDestination::FabricOneLake {
                abfss_path: abfss_path,
                file_path: output_file,
                auth: FabricAuthMethod::AzureCLI,
            },
            compression.map(|c| CopyConfig {
                compression: Some(c),
                ..Default::default()
            }),
        ).await
    }

    #[cfg(not(all(feature = "copydata", feature = "fabric")))]
    pub async fn copy_file_to_fabric(
        _input_path: &str,
        _abfss_path: &str,
        _output_file: &str,
        _compression: Option<ParquetCompression>,
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom(
            "*** Warning ***: 'copydata' and 'fabric' features not enabled. Add features = [\"copydata\", \"fabric\"] under [dependencies]".to_string()
        ))
    }

    /// Copy file to Fabric OneLake with Service Principal auth
    #[cfg(all(feature = "copydata", feature = "fabric"))]
    pub async fn copy_file_to_fabric_with_sp(
        input_path: &str,
        tenant_id: &str,
        client_id: &str,
        client_secret: &str,
        abfss_path: &str,
        output_file: &str,
        compression: Option<ParquetCompression>,
    ) -> ElusionResult<()> {
        copy_data(
            CopySource::File {
                path: input_path,
                csv_delimiter: None,
            },
            CopyDestination::FabricOneLake {
                abfss_path: abfss_path,
                file_path: output_file,
                auth: FabricAuthMethod::ServicePrincipal {
                    tenant_id: tenant_id.to_string(),
                    client_id: client_id.to_string(),
                    client_secret: client_secret.to_string(),
                },
            },
            compression.map(|c| CopyConfig {
                compression: Some(c),
                ..Default::default()
            }),
        ).await
    }

    #[cfg(not(all(feature = "copydata", feature = "fabric")))]
    pub async fn copy_file_to_fabric_with_sp(
        _input_path: &str,
        _tenant_id: &str,
        _client_id: &str,
        _client_secret: &str,
        _abfss_path: &str,
        _output_file: &str,
        _compression: Option<ParquetCompression>,
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom(
            "*** Warning ***: 'copydata' and 'fabric' features not enabled. Add features = [\"copydata\", \"fabric\"] under [dependencies]".to_string()
        ))
    }