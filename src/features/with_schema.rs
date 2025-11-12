
use std::path::Path as LocalPathCheck;
use std::sync::Arc;
use datafusion::prelude::{SessionContext, CsvReadOptions, ParquetReadOptions};
use datafusion::datasource::MemTable;
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef, TimeUnit};
use serde_json;

use crate::custom_error::cust_error::{ElusionError, ElusionResult};
use crate::AliasedDataFrame;

/// Represents a user-defined schema for loading files
#[derive(Clone, Debug)]
pub struct FileSchema {
    /// Arrow schema
    pub schema: SchemaRef,
}

/// Schema builder for fluent construction of schemas
pub struct SchemaBuilder {
    fields: Vec<Field>,
}

impl SchemaBuilder {
    /// Create a new schema builder
    pub fn new() -> Self {
        SchemaBuilder {
            fields: Vec::new(),
        }
    }

    /// Add a field to the schema
    pub fn field(mut self, name: &str, data_type: ArrowDataType, nullable: bool) -> Self {
        self.fields.push(Field::new(name, data_type, nullable));
        self
    }

    /// Build the schema
    pub fn build(self) -> FileSchema {
        FileSchema {
            schema: Arc::new(Schema::new(self.fields)),
        }
    }
}

impl Default for SchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl FileSchema {
    /// Create a new schema from fields
    pub fn new(fields: Vec<Field>) -> Self {
        FileSchema {
            schema: Arc::new(Schema::new(fields)),
        }
    }

    /// Create a schema from a SchemaRef
    pub fn from_schema_ref(schema: SchemaRef) -> Self {
        FileSchema { schema }
    }

    /// Get a reference to the schema
    pub fn schema_ref(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Create a new schema builder for fluent construction
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::new()
    }
}

/// Detect file extension from path
fn get_file_extension(file_path: &str) -> ElusionResult<String> {
    let path = LocalPathCheck::new(file_path);
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|s| s.to_lowercase())
        .ok_or_else(|| ElusionError::InvalidOperation {
            operation: "File Extension Detection".to_string(),
            reason: format!("Unable to determine file extension for '{}'", file_path),
            suggestion: "💡 Ensure the file has a valid extension (.csv, .parquet, .json, .xlsx, .xml, .delta)".to_string(),
        })
}

/// Load a file with a predefined schema 
pub async fn load_with_schema(
    file_path: &str,
    schema: FileSchema,
    alias: &str,
) -> ElusionResult<AliasedDataFrame> {

    if !LocalPathCheck::new(file_path).exists() {
        return Err(ElusionError::WriteError {
            path: file_path.to_string(),
            operation: "read".to_string(),
            reason: "File not found".to_string(),
            suggestion: "💡 Check if the file path is correct".to_string(),
        });
    }

    let extension = get_file_extension(file_path)?;
    
    println!("📂 Loading file with custom schema: {}", file_path);
    println!("📋 Schema: {} fields", schema.schema.fields().len());

    match extension.as_str() {
        "csv" => load_csv_with_schema(file_path, schema, alias).await,
        "parquet" => load_parquet_with_schema(file_path, schema, alias).await,
        "json" => load_json_with_schema(file_path, schema, alias).await,
        "xlsx" => load_excel_with_schema(file_path, schema, alias).await,
        "xml" => load_xml_with_schema(file_path, schema, alias).await,
        "delta" => load_delta_with_schema(file_path, schema, alias).await,
        _ => Err(ElusionError::InvalidOperation {
            operation: "File Format Detection".to_string(),
            reason: format!("Unsupported file format: '.{}'", extension),
            suggestion: "💡 Supported formats: .csv, .parquet, .json, .xlsx, .xml, .delta".to_string(),
        }),
    }
}

/// Load CSV with predefined schema
async fn load_csv_with_schema(
    file_path: &str,
    schema: FileSchema,
    alias: &str,
) -> ElusionResult<AliasedDataFrame> {
    let ctx = SessionContext::new();
    
    let schema_ref = schema.schema_ref();
    let csv_options = CsvReadOptions::new()
        .has_header(true)
        .schema(schema_ref.as_ref());

    let df = ctx
        .read_csv(file_path, csv_options)
        .await
        .map_err(|e| ElusionError::DataFusion(e))?;

    println!("✅ CSV loaded with custom schema: {} columns", schema.schema.fields().len());

    Ok(AliasedDataFrame {
        dataframe: df,
        alias: alias.to_string(),
    })
}

/// Load Parquet with predefined schema
async fn load_parquet_with_schema(
    file_path: &str,
    schema: FileSchema,
    alias: &str,
) -> ElusionResult<AliasedDataFrame> {
    let ctx = SessionContext::new();

    let schema_ref = schema.schema_ref();
    // DataFusion's parquet reader will validate schema compatibility
    let df = ctx
        .read_parquet(file_path, ParquetReadOptions::new().schema(schema_ref.as_ref()))
        .await
        .map_err(|e| ElusionError::DataFusion(e))?;

    println!("✅ Parquet loaded with custom schema: {} columns", schema.schema.fields().len());

    Ok(AliasedDataFrame {
        dataframe: df,
        alias: alias.to_string(),
    })
}

/// Load JSON with predefined schema
async fn load_json_with_schema(
    file_path: &str,
    schema: FileSchema,
    alias: &str,
) -> ElusionResult<AliasedDataFrame> {
    let ctx = SessionContext::new();

    let df_raw = ctx
        .read_json(file_path, Default::default())
        .await
        .map_err(|e| ElusionError::DataFusion(e))?;

    let batches = df_raw
        .clone()
        .collect()
        .await
        .map_err(|e| ElusionError::DataFusion(e))?;

    let mem_table = MemTable::try_new(schema.schema_ref(), vec![batches])
        .map_err(|e| ElusionError::Custom(format!("Failed to create table with schema: {}", e)))?;

    ctx.register_table(alias, Arc::new(mem_table))
        .map_err(|e| ElusionError::DataFusion(e))?;

    let df = ctx
        .table(alias)
        .await
        .map_err(|e| ElusionError::DataFusion(e))?;

    println!("✅ JSON loaded with custom schema: {} columns", schema.schema.fields().len());

    Ok(AliasedDataFrame {
        dataframe: df,
        alias: alias.to_string(),
    })
}

/// Load Excel with predefined schema
async fn load_excel_with_schema(
    file_path: &str,
    schema: FileSchema,
    alias: &str,
) -> ElusionResult<AliasedDataFrame> {
    use crate::features::excel::load_excel;

    let temp_df = load_excel(file_path, "temp_excel")
        .await?;

    let ctx = SessionContext::new();

    let batches = temp_df
        .dataframe
        .clone()
        .collect()
        .await
        .map_err(|e| ElusionError::DataFusion(e))?;

    let mem_table = MemTable::try_new(schema.schema_ref(), vec![batches])
        .map_err(|e| ElusionError::Custom(format!("Failed to create table with schema: {}", e)))?;

    ctx.register_table(alias, Arc::new(mem_table))
        .map_err(|e| ElusionError::DataFusion(e))?;

    let df = ctx
        .table(alias)
        .await
        .map_err(|e| ElusionError::DataFusion(e))?;

    println!("✅ Excel loaded with custom schema: {} columns", schema.schema.fields().len());

    Ok(AliasedDataFrame {
        dataframe: df,
        alias: alias.to_string(),
    })
}

/// Load XML with predefined schema
async fn load_xml_with_schema(
    file_path: &str,
    schema: FileSchema,
    alias: &str,
) -> ElusionResult<AliasedDataFrame> {
    use crate::features::xml::load_xml_with_mode;
    use crate::features::xml::XmlProcessingMode;

    let temp_df = load_xml_with_mode(file_path, "temp_xml", XmlProcessingMode::Standard)
        .await?;

    let ctx = SessionContext::new();

    let batches = temp_df
        .dataframe
        .clone()
        .collect()
        .await
        .map_err(|e| ElusionError::DataFusion(e))?;

    let mem_table = MemTable::try_new(schema.schema_ref(), vec![batches])
        .map_err(|e| ElusionError::Custom(format!("Failed to create table with schema: {}", e)))?;

    ctx.register_table(alias, Arc::new(mem_table))
        .map_err(|e| ElusionError::DataFusion(e))?;

    let df = ctx
        .table(alias)
        .await
        .map_err(|e| ElusionError::DataFusion(e))?;

    println!("✅ XML loaded with custom schema: {} columns", schema.schema.fields().len());

    Ok(AliasedDataFrame {
        dataframe: df,
        alias: alias.to_string(),
    })
}

/// Load Delta Lake table with predefined schema
async fn load_delta_with_schema(
    file_path: &str,
    schema: FileSchema,
    alias: &str,
) -> ElusionResult<AliasedDataFrame> {
    use deltalake::DeltaTableBuilder;
    use crate::features::delta::DeltaPathManager;
    
    let ctx = SessionContext::new();
    let path_manager = DeltaPathManager::new(file_path);

    // Open the Delta table
    let mut table = DeltaTableBuilder::from_uri(&path_manager.table_path())
        .build()
        .map_err(|e| ElusionError::Custom(format!("Failed to build Delta table: {}", e)))?;

    table.load()
        .await
        .map_err(|e| ElusionError::Custom(format!("Failed to load Delta table: {}", e)))?;

    // Get file URIs and read as parquet
    let raw_uris = table.get_file_uris()
        .map_err(|e| ElusionError::Custom(format!("Failed to get file URIs: {}", e)))?;

    let file_paths: Vec<String> = raw_uris
        .map(|uri| path_manager.normalize_uri(&uri))
        .collect();

    let schema_ref = schema.schema_ref();
    let parquet_options = ParquetReadOptions::new()
        .schema(schema_ref.as_ref());

    let df = ctx
        .read_parquet(file_paths, parquet_options)
        .await
        .map_err(|e| ElusionError::DataFusion(e))?;

    println!("✅ Delta table loaded with custom schema: {} columns", schema.schema.fields().len());

    Ok(AliasedDataFrame {
        dataframe: df,
        alias: alias.to_string(),
    })
}

/// Helper function to create a schema from a JSON specification
pub fn schema_from_json(json_spec: &str) -> ElusionResult<FileSchema> {
    let parsed: serde_json::Value = serde_json::from_str(json_spec)
        .map_err(|e| ElusionError::Custom(format!("Invalid JSON schema specification: {}", e)))?;

    let fields_array = parsed
        .get("fields")
        .and_then(|v| v.as_array())
        .ok_or_else(|| ElusionError::Custom(
            "Schema must contain 'fields' array".to_string()
        ))?;

    let mut fields = Vec::new();

    for field_spec in fields_array {
        let name = field_spec
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ElusionError::Custom("Field must have 'name'".to_string()))?;

        let data_type_str = field_spec
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ElusionError::Custom("Field must have 'type'".to_string()))?;

        let nullable = field_spec
            .get("nullable")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        let arrow_type = match data_type_str.to_lowercase().as_str() {
            "int64" | "i64" => ArrowDataType::Int64,
            "int32" | "i32" => ArrowDataType::Int32,
            "int16" | "i16" => ArrowDataType::Int16,
            "int8" | "i8" => ArrowDataType::Int8,
            "uint64" | "u64" => ArrowDataType::UInt64,
            "uint32" | "u32" => ArrowDataType::UInt32,
            "uint16" | "u16" => ArrowDataType::UInt16,
            "uint8" | "u8" => ArrowDataType::UInt8,
            "float64" | "f64" | "double" => ArrowDataType::Float64,
            "float32" | "f32" | "float" => ArrowDataType::Float32,
            "string" | "text" | "varchar" => ArrowDataType::Utf8,
            "bool" | "boolean" => ArrowDataType::Boolean,
            "date" | "date32" => ArrowDataType::Date32,
            "timestamp" => ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
            "binary" => ArrowDataType::Binary,
            _ => return Err(ElusionError::Custom(
                format!("Unsupported data type: '{}'", data_type_str)
            )),
        };

        fields.push(Field::new(name, arrow_type, nullable));
    }

    Ok(FileSchema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::TempDir;

    fn create_test_csv(content: &str, filename: &str, temp_dir: &TempDir) -> String {
        let file_path = temp_dir.path().join(filename);
        let mut file = File::create(&file_path).unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file_path.to_str().unwrap().to_string()
    }

    #[test]
    fn test_file_schema_creation() {
        let fields = vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, true),
            Field::new("amount", ArrowDataType::Float64, true),
        ];
        
        let schema = FileSchema::new(fields);
        assert_eq!(schema.schema.fields().len(), 3);
        assert_eq!(schema.schema.field(0).name(), "id");
        assert_eq!(schema.schema.field(1).name(), "name");
        assert_eq!(schema.schema.field(2).name(), "amount");
    }

    #[test]
    fn test_file_extension_detection() {
        assert_eq!(get_file_extension("test.csv").unwrap(), "csv");
        assert_eq!(get_file_extension("data.parquet").unwrap(), "parquet");
        assert_eq!(get_file_extension("file.json").unwrap(), "json");
        assert_eq!(get_file_extension("sheet.xlsx").unwrap(), "xlsx");
        assert_eq!(get_file_extension("document.xml").unwrap(), "xml");
        assert_eq!(get_file_extension("table.delta").unwrap(), "delta");
    }

    #[test]
    fn test_schema_from_json() {
        let json_spec = r#"{
            "fields": [
                {"name": "id", "type": "int64", "nullable": false},
                {"name": "name", "type": "string", "nullable": true},
                {"name": "score", "type": "float64", "nullable": true}
            ]
        }"#;

        let schema = schema_from_json(json_spec).unwrap();
        assert_eq!(schema.schema.fields().len(), 3);
        
        assert_eq!(schema.schema.field(0).name(), "id");
        assert!(!schema.schema.field(0).is_nullable());
        
        assert_eq!(schema.schema.field(1).name(), "name");
        assert!(schema.schema.field(1).is_nullable());
        
        assert_eq!(schema.schema.field(2).name(), "score");
        assert!(schema.schema.field(2).is_nullable());
    }

    #[test]
    fn test_schema_from_json_all_types() {
        let json_spec = r#"{
            "fields": [
                {"name": "col_int64", "type": "int64"},
                {"name": "col_int32", "type": "int32"},
                {"name": "col_uint64", "type": "uint64"},
                {"name": "col_float64", "type": "float64"},
                {"name": "col_float32", "type": "float32"},
                {"name": "col_string", "type": "string"},
                {"name": "col_bool", "type": "boolean"},
                {"name": "col_date", "type": "date32"},
                {"name": "col_timestamp", "type": "timestamp"},
                {"name": "col_binary", "type": "binary"}
            ]
        }"#;

        let schema = schema_from_json(json_spec).unwrap();
        assert_eq!(schema.schema.fields().len(), 10);
        
        // Verify each type is correctly parsed
        assert!(matches!(schema.schema.field(0).data_type(), ArrowDataType::Int64));
        assert!(matches!(schema.schema.field(1).data_type(), ArrowDataType::Int32));
        assert!(matches!(schema.schema.field(2).data_type(), ArrowDataType::UInt64));
        assert!(matches!(schema.schema.field(3).data_type(), ArrowDataType::Float64));
        assert!(matches!(schema.schema.field(4).data_type(), ArrowDataType::Float32));
        assert!(matches!(schema.schema.field(5).data_type(), ArrowDataType::Utf8));
        assert!(matches!(schema.schema.field(6).data_type(), ArrowDataType::Boolean));
        assert!(matches!(schema.schema.field(7).data_type(), ArrowDataType::Date32));
        assert!(matches!(schema.schema.field(9).data_type(), ArrowDataType::Binary));
    }

    #[test]
    fn test_schema_from_json_invalid_json() {
        let invalid_json = "{ invalid json }";
        let result = schema_from_json(invalid_json);
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_from_json_missing_fields() {
        let json_spec = r#"{"data": "no fields"}"#;
        let result = schema_from_json(json_spec);
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_from_json_missing_name() {
        let json_spec = r#"{
            "fields": [
                {"type": "int64"}
            ]
        }"#;
        let result = schema_from_json(json_spec);
        assert!(result.is_err());
    }

    #[test]
    fn test_schema_from_json_missing_type() {
        let json_spec = r#"{
            "fields": [
                {"name": "id"}
            ]
        }"#;
        let result = schema_from_json(json_spec);
        assert!(result.is_err());
    }

    #[test]
    fn test_unsupported_file_extension() {
        let result = get_file_extension("file.unsupported");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "unsupported");
    }

    #[test]
    fn test_schema_from_json_type_aliases() {
        let json_spec = r#"{
            "fields": [
                {"name": "col_i64", "type": "i64"},
                {"name": "col_f64", "type": "f64"},
                {"name": "col_text", "type": "text"},
                {"name": "col_varchar", "type": "varchar"},
                {"name": "col_bool", "type": "boolean"},
                {"name": "col_double", "type": "double"}
            ]
        }"#;

        let schema = schema_from_json(json_spec).unwrap();
        assert_eq!(schema.schema.fields().len(), 6);
        
        // Verify aliases are correctly mapped
        assert!(matches!(schema.schema.field(0).data_type(), ArrowDataType::Int64));
        assert!(matches!(schema.schema.field(1).data_type(), ArrowDataType::Float64));
        assert!(matches!(schema.schema.field(2).data_type(), ArrowDataType::Utf8));
        assert!(matches!(schema.schema.field(3).data_type(), ArrowDataType::Utf8));
        assert!(matches!(schema.schema.field(4).data_type(), ArrowDataType::Boolean));
        assert!(matches!(schema.schema.field(5).data_type(), ArrowDataType::Float64));
    }

    #[test]
    fn test_schema_case_insensitive_types() {
        let json_spec = r#"{
            "fields": [
                {"name": "col1", "type": "INT64"},
                {"name": "col2", "type": "String"},
                {"name": "col3", "type": "FLOAT64"},
                {"name": "col4", "type": "Boolean"}
            ]
        }"#;

        let schema = schema_from_json(json_spec).unwrap();
        assert_eq!(schema.schema.fields().len(), 4);
        
        assert!(matches!(schema.schema.field(0).data_type(), ArrowDataType::Int64));
        assert!(matches!(schema.schema.field(1).data_type(), ArrowDataType::Utf8));
        assert!(matches!(schema.schema.field(2).data_type(), ArrowDataType::Float64));
        assert!(matches!(schema.schema.field(3).data_type(), ArrowDataType::Boolean));
    }

    #[test]
    fn test_file_schema_clone() {
        let fields = vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, true),
        ];
        let schema = FileSchema::new(fields);
        let cloned = schema.clone();

        assert_eq!(schema.schema.fields().len(), cloned.schema.fields().len());
    }

    #[test]
    fn test_schema_builder_basic() {
        let schema = FileSchema::builder()
            .field("order_id", ArrowDataType::Int64, false)
            .field("customer_name", ArrowDataType::Utf8, true)
            .field("order_amount", ArrowDataType::Float64, true)
            .field("order_date", ArrowDataType::Date32, true)
            .build();

        assert_eq!(schema.schema.fields().len(), 4);
        assert_eq!(schema.schema.field(0).name(), "order_id");
        assert_eq!(schema.schema.field(1).name(), "customer_name");
        assert_eq!(schema.schema.field(2).name(), "order_amount");
        assert_eq!(schema.schema.field(3).name(), "order_date");

        // Verify types
        assert!(matches!(schema.schema.field(0).data_type(), ArrowDataType::Int64));
        assert!(matches!(schema.schema.field(1).data_type(), ArrowDataType::Utf8));
        assert!(matches!(schema.schema.field(2).data_type(), ArrowDataType::Float64));
        assert!(matches!(schema.schema.field(3).data_type(), ArrowDataType::Date32));

        // Verify nullable
        assert!(!schema.schema.field(0).is_nullable());
        assert!(schema.schema.field(1).is_nullable());
        assert!(schema.schema.field(2).is_nullable());
        assert!(schema.schema.field(3).is_nullable());
    }

    #[test]
    fn test_schema_builder_empty() {
        let schema = FileSchema::builder().build();
        assert_eq!(schema.schema.fields().len(), 0);
    }

    #[test]
    fn test_schema_builder_all_types() {
        let schema = FileSchema::builder()
            .field("col_i64", ArrowDataType::Int64, false)
            .field("col_i32", ArrowDataType::Int32, false)
            .field("col_u64", ArrowDataType::UInt64, true)
            .field("col_f64", ArrowDataType::Float64, true)
            .field("col_f32", ArrowDataType::Float32, true)
            .field("col_str", ArrowDataType::Utf8, true)
            .field("col_bool", ArrowDataType::Boolean, false)
            .field("col_date", ArrowDataType::Date32, true)
            .field("col_binary", ArrowDataType::Binary, true)
            .build();

        assert_eq!(schema.schema.fields().len(), 9);
        
        // Verify types are preserved
        assert!(matches!(schema.schema.field(0).data_type(), ArrowDataType::Int64));
        assert!(matches!(schema.schema.field(1).data_type(), ArrowDataType::Int32));
        assert!(matches!(schema.schema.field(2).data_type(), ArrowDataType::UInt64));
        assert!(matches!(schema.schema.field(3).data_type(), ArrowDataType::Float64));
        assert!(matches!(schema.schema.field(4).data_type(), ArrowDataType::Float32));
        assert!(matches!(schema.schema.field(5).data_type(), ArrowDataType::Utf8));
        assert!(matches!(schema.schema.field(6).data_type(), ArrowDataType::Boolean));
        assert!(matches!(schema.schema.field(7).data_type(), ArrowDataType::Date32));
        assert!(matches!(schema.schema.field(8).data_type(), ArrowDataType::Binary));
    }

    #[test]
    fn test_schema_builder_vs_vec_creation() {
        // Using builder pattern
        let schema_from_builder = FileSchema::builder()
            .field("id", ArrowDataType::Int64, false)
            .field("name", ArrowDataType::Utf8, true)
            .field("amount", ArrowDataType::Float64, true)
            .build();

        // Using vec! pattern
        let fields = vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, true),
            Field::new("amount", ArrowDataType::Float64, true),
        ];
        let schema_from_vec = FileSchema::new(fields);

        // Both should have same structure
        assert_eq!(
            schema_from_builder.schema.fields().len(),
            schema_from_vec.schema.fields().len()
        );
        
        for i in 0..3 {
            assert_eq!(
                schema_from_builder.schema.field(i).name(),
                schema_from_vec.schema.field(i).name()
            );
            assert_eq!(
                schema_from_builder.schema.field(i).data_type(),
                schema_from_vec.schema.field(i).data_type()
            );
        }
    }

    #[tokio::test]
    async fn test_load_csv_with_schema_builder() {
        let temp_dir = TempDir::new().unwrap();
        let csv_content = "order_id,customer_name,order_amount,order_date\n1,Alice,100.50,2024-01-15\n2,Bob,250.75,2024-01-16\n";
        let file_path = create_test_csv(csv_content, "orders_builder.csv", &temp_dir);

        // Build schema using fluent builder
        let schema = FileSchema::builder()
            .field("order_id", ArrowDataType::Int64, false)
            .field("customer_name", ArrowDataType::Utf8, true)
            .field("order_amount", ArrowDataType::Float64, true)
            .field("order_date", ArrowDataType::Utf8, true)
            .build();

        let result = load_with_schema(&file_path, schema, "orders_table").await;
        assert!(result.is_ok());

        let aliased_df = result.unwrap();
        assert_eq!(aliased_df.alias, "orders_table");
        assert_eq!(aliased_df.dataframe.schema().fields().len(), 4);
    }

    #[tokio::test]
    async fn test_load_csv_builder_vs_vec_equivalence() {
        let temp_dir = TempDir::new().unwrap();
        let csv_content = "id,name,value\n1,test,42.5\n2,demo,99.9\n";
        let file_path = create_test_csv(csv_content, "test_equiv.csv", &temp_dir);

        // Schema using builder
        let schema_builder = FileSchema::builder()
            .field("id", ArrowDataType::Int64, false)
            .field("name", ArrowDataType::Utf8, true)
            .field("value", ArrowDataType::Float64, true)
            .build();

        let result_builder = load_with_schema(&file_path, schema_builder, "table_builder").await;
        assert!(result_builder.is_ok());

        // Schema using vec!
        let fields = vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, true),
            Field::new("value", ArrowDataType::Float64, true),
        ];
        let schema_vec = FileSchema::new(fields);

        let result_vec = load_with_schema(&file_path, schema_vec, "table_vec").await;
        assert!(result_vec.is_ok());

        // Both should load same number of columns
        let df_builder = result_builder.unwrap();
        let df_vec = result_vec.unwrap();
        assert_eq!(
            df_builder.dataframe.schema().fields().len(),
            df_vec.dataframe.schema().fields().len()
        );
    }

    #[test]
    fn test_schema_builder_chaining_style() {
        // Test that builder can be used in various chaining styles
        let schema = FileSchema::builder()
            .field("first", ArrowDataType::Int32, true)
            .field("second", ArrowDataType::Utf8, false)
            .field("third", ArrowDataType::Boolean, true)
            .field("fourth", ArrowDataType::Float64, false)
            .build();

        assert_eq!(schema.schema.fields().len(), 4);
        assert_eq!(schema.schema.field(0).name(), "first");
        assert_eq!(schema.schema.field(1).name(), "second");
        assert_eq!(schema.schema.field(2).name(), "third");
        assert_eq!(schema.schema.field(3).name(), "fourth");

        // Check nullability alternates as expected
        assert!(schema.schema.field(0).is_nullable());
        assert!(!schema.schema.field(1).is_nullable());
        assert!(schema.schema.field(2).is_nullable());
        assert!(!schema.schema.field(3).is_nullable());
    }

    #[test]
    fn test_schema_builder_with_all_numeric_types() {
        let schema = FileSchema::builder()
            .field("int8_col", ArrowDataType::Int8, false)
            .field("int16_col", ArrowDataType::Int16, false)
            .field("int32_col", ArrowDataType::Int32, false)
            .field("int64_col", ArrowDataType::Int64, false)
            .field("uint8_col", ArrowDataType::UInt8, true)
            .field("uint16_col", ArrowDataType::UInt16, true)
            .field("uint32_col", ArrowDataType::UInt32, true)
            .field("uint64_col", ArrowDataType::UInt64, true)
            .field("float32_col", ArrowDataType::Float32, false)
            .field("float64_col", ArrowDataType::Float64, false)
            .build();

        assert_eq!(schema.schema.fields().len(), 10);

        // Verify signed integers
        assert!(matches!(schema.schema.field(0).data_type(), ArrowDataType::Int8));
        assert!(matches!(schema.schema.field(1).data_type(), ArrowDataType::Int16));
        assert!(matches!(schema.schema.field(2).data_type(), ArrowDataType::Int32));
        assert!(matches!(schema.schema.field(3).data_type(), ArrowDataType::Int64));

        // Verify unsigned integers
        assert!(matches!(schema.schema.field(4).data_type(), ArrowDataType::UInt8));
        assert!(matches!(schema.schema.field(5).data_type(), ArrowDataType::UInt16));
        assert!(matches!(schema.schema.field(6).data_type(), ArrowDataType::UInt32));
        assert!(matches!(schema.schema.field(7).data_type(), ArrowDataType::UInt64));

        // Verify floating point
        assert!(matches!(schema.schema.field(8).data_type(), ArrowDataType::Float32));
        assert!(matches!(schema.schema.field(9).data_type(), ArrowDataType::Float64));
    }
}

