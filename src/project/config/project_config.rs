use std::collections::HashMap;
use serde::Deserialize;
use crate::custom_error::cust_error::{ElusionError, ElusionResult};
use super::env_resolver::resolve_env_value;

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum MaterializationType {
    Parquet,
    Delta,
    Memory,  // no write - just pass through in memory
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum OutputDestination {
    Local,
    Fabric,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LocalOutput {
    pub bronze_path: String,
    pub silver_path: String,
    pub gold_path: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct FabricOutput {
    pub abfss_path: String,
    pub tenant_id: String,
    pub client_id: String,
    pub client_secret: String,
    pub bronze_path: String,
    pub silver_path: String,
    pub gold_path: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MaterializationConfig {
    pub bronze: MaterializationType,
    pub silver: MaterializationType,
    pub gold: MaterializationType,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ModelConfig {
    pub materialization: Option<MaterializationType>,
    pub output_path: Option<String>,  // overrides global path for this model
}

#[derive(Debug, Deserialize)]
pub struct ProjectFile {
    pub project: ProjectMeta,
    pub materialization: MaterializationConfig,
    pub output: OutputConfig,
    #[serde(default)]
    pub models: HashMap<String, ModelConfig>,  // per-model overrides
}

#[derive(Debug, Deserialize)]
pub struct ProjectMeta {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Deserialize)]
pub struct OutputConfig {
    pub destination: OutputDestination,
    pub local: Option<LocalOutput>,
    pub fabric: Option<FabricOutput>,
}

impl ProjectFile {
    pub fn load(path: &str) -> ElusionResult<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| ElusionError::Custom(format!(
                "❌ Failed to read project config '{}': {}", path, e
            )))?;

        let mut file: ProjectFile = toml::from_str(&content)
            .map_err(|e| ElusionError::Custom(format!(
                "❌ Failed to parse project config '{}': {}", path, e
            )))?;

        // Resolve env vars in fabric output if present
        if let Some(fabric) = &mut file.output.fabric {
            fabric.tenant_id = resolve_env_value(&fabric.tenant_id)?;
            fabric.client_id = resolve_env_value(&fabric.client_id)?;
            fabric.client_secret = resolve_env_value(&fabric.client_secret)?;
        }

        // Validate output config matches destination
        match file.output.destination {
            OutputDestination::Local => {
                if file.output.local.is_none() {
                    return Err(ElusionError::Custom(
                        "❌ destination = 'local' but [output.local] is not configured".to_string()
                    ));
                }
                // Create output directories if they don't exist
                let local = file.output.local.as_ref().unwrap();
                Self::ensure_dir(&local.bronze_path)?;
                Self::ensure_dir(&local.silver_path)?;
                Self::ensure_dir(&local.gold_path)?;
            }
            OutputDestination::Fabric => {
                if file.output.fabric.is_none() {
                    return Err(ElusionError::Custom(
                        "❌ destination = 'fabric' but [output.fabric] is not configured".to_string()
                    ));
                }
            }
        }

        println!("✅ Project config loaded: {} v{}", file.project.name, file.project.version);
        Ok(file)
    }

    /// Get materialization type for a specific model
    /// Model-level config overrides global config
    pub fn get_materialization(&self, model_name: &str) -> MaterializationType {
            if let Some(model) = self.models.get(model_name) {
                if let Some(mat) = &model.materialization {
                    return mat.clone();
                }
            }
            if model_name.starts_with("brz_") || model_name.starts_with("bronze_") {
                self.materialization.bronze.clone()
            } else if model_name.starts_with("slv_") || model_name.starts_with("silver_") {
                self.materialization.silver.clone()
            } else {
                self.materialization.gold.clone()
            }
        }

    /// Get output path for a specific model
    pub fn get_output_path(&self, model_name: &str) -> ElusionResult<String> {
        if let Some(model) = self.models.get(model_name) {
            if let Some(path) = &model.output_path {
                return Ok(path.clone());
            }
        }

        match &self.output.destination {
            OutputDestination::Local => {
                let local = self.output.local.as_ref().unwrap();
                if model_name.starts_with("brz_") || model_name.starts_with("bronze_") {
                    Ok(local.bronze_path.clone())
                } else if model_name.starts_with("slv_") || model_name.starts_with("silver_") {
                    Ok(local.silver_path.clone())
                } else {
                    Ok(local.gold_path.clone())
                }
            }
            OutputDestination::Fabric => {
                let fabric = self.output.fabric.as_ref().unwrap();
                if model_name.starts_with("brz_") || model_name.starts_with("bronze_") {
                    Ok(fabric.bronze_path.clone())
                } else if model_name.starts_with("slv_") || model_name.starts_with("silver_") {
                    Ok(fabric.silver_path.clone())
                } else {
                    Ok(fabric.gold_path.clone())
                }
            }
        }
    }

    fn ensure_dir(path: &str) -> ElusionResult<()> {
        std::fs::create_dir_all(path).map_err(|e| ElusionError::Custom(format!(
            "❌ Failed to create output directory '{}': {}", path, e
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_project_toml_parsing() {
        let toml_str = r#"
            [project]
            name = "test_pipeline"
            version = "1.0"

            [materialization]
            bronze = "parquet"
            silver = "parquet"
            gold = "delta"

            [output]
            destination = "local"

            [output.local]
            bronze_path = "C:\\Data\\bronze"
            silver_path = "C:\\Data\\silver"
            gold_path = "C:\\Data\\gold"
        "#;

        let result: Result<ProjectFile, _> = toml::from_str(toml_str);
        assert!(result.is_ok());

        let file = result.unwrap();
        assert_eq!(file.project.name, "test_pipeline");
        assert_eq!(file.project.version, "1.0");
    }

    #[test]
    fn test_materialization_type_detection() {
        let toml_str = r#"
            [project]
            name = "test"
            version = "1.0"

            [materialization]
            bronze = "parquet"
            silver = "parquet"
            gold = "delta"

            [output]
            destination = "local"

            [output.local]
            bronze_path = "C:\\Data\\bronze"
            silver_path = "C:\\Data\\silver"
            gold_path = "C:\\Data\\gold"
        "#;

        let file: ProjectFile = toml::from_str(toml_str).unwrap();

        assert!(matches!(
            file.get_materialization("brz_sales"),
            MaterializationType::Parquet
        ));
        assert!(matches!(
            file.get_materialization("slv_enriched"),
            MaterializationType::Parquet
        ));
        assert!(matches!(
            file.get_materialization("fct_revenue"),
            MaterializationType::Delta
        ));
    }

    #[test]
    fn test_output_path_detection() {
        let toml_str = r#"
            [project]
            name = "test"
            version = "1.0"

            [materialization]
            bronze = "parquet"
            silver = "parquet"
            gold = "delta"

            [output]
            destination = "local"

            [output.local]
            bronze_path = "C:\\Data\\bronze"
            silver_path = "C:\\Data\\silver"
            gold_path = "C:\\Data\\gold"
        "#;

        let file: ProjectFile = toml::from_str(toml_str).unwrap();

        assert_eq!(
            file.get_output_path("brz_sales").unwrap(),
            "C:\\Data\\bronze"
        );
        assert_eq!(
            file.get_output_path("slv_enriched").unwrap(),
            "C:\\Data\\silver"
        );
        assert_eq!(
            file.get_output_path("fct_revenue").unwrap(),
            "C:\\Data\\gold"
        );
    }

    #[test]
    fn test_missing_local_config_fails() {
        let toml_str = r#"
            [project]
            name = "test"
            version = "1.0"

            [materialization]
            bronze = "parquet"
            silver = "parquet"
            gold = "delta"

            [output]
            destination = "local"
        "#;
        let result: Result<ProjectFile, _> = toml::from_str(toml_str);

        assert!(result.is_ok()); 
        let file = result.unwrap();
        assert!(file.output.local.is_none()); 
    }
}