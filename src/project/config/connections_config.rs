use std::collections::HashMap;
use std::path::Path;
use serde::Deserialize;
use crate::custom_error::cust_error::{ElusionError, ElusionResult};
use super::env_resolver::resolve_required;


#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SourceConfig {
    Csv {
        path: String,
    },
    Parquet {
        path: String,
    },
    Delta {
        path: String,
    },
    Fabric {
        abfss_path: String,
        file_path: String,
        tenant_id: String,
        client_id: String,
        client_secret: String,
    },
    FabricSas {
        url: String,
        sas_token: String,
        filter_keyword: Option<String>,
    },
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConnectionsFile {
    pub sources: HashMap<String, SourceConfig>,
}

impl ConnectionsFile {
    /// Parse connections.toml and resolve all env vars
    /// Fails fast if any source file/path doesn't exist or env var is missing
    pub fn load(path: &str) -> ElusionResult<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| ElusionError::Custom(format!(
                "❌ Failed to read connections file '{}': {}", path, e
            )))?;

        let mut file: ConnectionsFile = toml::from_str(&content)
            .map_err(|e| ElusionError::Custom(format!(
                "❌ Failed to parse connections file '{}': {}", path, e
            )))?;

        for (name, source) in file.sources.iter_mut() {
            match source {
                SourceConfig::Csv { path } |
                SourceConfig::Parquet { path } |
                SourceConfig::Delta { path } => {
                    if !Path::new(&path).exists() {
                        return Err(ElusionError::Custom(format!(
                            "❌ Source '{}' path does not exist: '{}'", name, path
                        )));
                    }
                    println!("✅ Source '{}' validated: {}", name, path);
                }
                SourceConfig::Fabric {
                    abfss_path,
                    file_path,
                    tenant_id,
                    client_id,
                    client_secret,
                } => {
                    tenant_id.clone_from(&resolve_required("tenant_id", tenant_id)?);
                    client_id.clone_from(&resolve_required("client_id", client_id)?);
                    client_secret.clone_from(&resolve_required("client_secret", client_secret)?);
                    println!("✅ Source '{}' validated: {}/{}", name, abfss_path, file_path);
                }
                SourceConfig::FabricSas { url, sas_token, .. } => {
                    sas_token.clone_from(&resolve_required("sas_token", sas_token)?);
                    println!("✅ Source '{}' validated: {}", name, url);
                }
            }
        }

        Ok(file)
    }

    pub fn get_source(&self, name: &str) -> ElusionResult<&SourceConfig> {
        self.sources.get(name).ok_or_else(|| ElusionError::Custom(format!(
            "❌ Source '{}' not found in connections.toml. Available sources: [{}]",
            name,
            self.sources.keys().cloned().collect::<Vec<_>>().join(", ")
        )))
    }
}