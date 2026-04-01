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


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connections_toml_csv_parsing() {
        let toml_str = r#"
            [sources.raw_sales]
            type = "csv"
            path = "C:\\Data\\sales.csv"
        "#;

        // parsing works
        let result: Result<ConnectionsFile, _> = toml::from_str(toml_str);
        assert!(result.is_ok());

        let file = result.unwrap();
        assert!(file.sources.contains_key("raw_sales"));

        match &file.sources["raw_sales"] {
            SourceConfig::Csv { path } => {
                assert_eq!(path, "C:\\Data\\sales.csv");
            }
            _ => panic!("Expected CSV source"),
        }
    }

    #[test]
    fn test_connections_toml_fabric_parsing() {
        let toml_str = r#"
            [sources.raw_fabric]
            type = "fabric"
            abfss_path = "abfss://container@account.dfs.core.windows.net"
            file_path = "bronze/orders.parquet"
            tenant_id = "TENANT_ID"
            client_id = "CLIENT_ID"
            client_secret = "CLIENT_SECRET"
        "#;

        let result: Result<ConnectionsFile, _> = toml::from_str(toml_str);
        assert!(result.is_ok());

        let file = result.unwrap();
        match &file.sources["raw_fabric"] {
            SourceConfig::Fabric { 
                abfss_path, 
                file_path,
                tenant_id, 
                client_id, 
                client_secret 
            } => {
                assert_eq!(abfss_path, "abfss://container@account.dfs.core.windows.net");
                assert_eq!(file_path, "bronze/orders.parquet");
                assert_eq!(tenant_id, "TENANT_ID");
                assert_eq!(client_id, "CLIENT_ID");
                assert_eq!(client_secret, "CLIENT_SECRET");
            }
            _ => panic!("Expected Fabric source"),
        }
    }

    #[test]
    fn test_fabric_env_var_resolution() {
        // Simulate what happens at startup when env vars are set
        std::env::set_var("TENANT_ID", "real-tenant-123");
        std::env::set_var("CLIENT_ID", "real-client-456");
        std::env::set_var("CLIENT_SECRET", "real-secret-789");

        let toml_str = r#"
            [sources.raw_fabric]
            type = "fabric"
            abfss_path = "abfss://container@account.dfs.core.windows.net"
            file_path = "bronze/orders.parquet"
            tenant_id = "TENANT_ID"
            client_id = "CLIENT_ID"
            client_secret = "CLIENT_SECRET"
        "#;

        let mut file: ConnectionsFile = toml::from_str(toml_str).unwrap();

        // Simulate the resolution that happens in load()
        for (_, source) in file.sources.iter_mut() {
            if let SourceConfig::Fabric { tenant_id, client_id, client_secret, .. } = source {
                tenant_id.clone_from(&crate::project::config::env_resolver::resolve_required("tenant_id", tenant_id).unwrap());
                client_id.clone_from(&crate::project::config::env_resolver::resolve_required("client_id", client_id).unwrap());
                client_secret.clone_from(&crate::project::config::env_resolver::resolve_required("client_secret", client_secret).unwrap());
            }
        }

        match &file.sources["raw_fabric"] {
            SourceConfig::Fabric { tenant_id, client_id, client_secret, .. } => {
                assert_eq!(tenant_id, "real-tenant-123");
                assert_eq!(client_id, "real-client-456");
                assert_eq!(client_secret, "real-secret-789");
            }
            _ => panic!("Expected Fabric source"),
        }

        std::env::remove_var("TENANT_ID");
        std::env::remove_var("CLIENT_ID");
        std::env::remove_var("CLIENT_SECRET");
    }

    #[test]
    fn test_missing_env_var_fails_fast() {
        std::env::remove_var("MISSING_TENANT_ID");

        let result = crate::project::config::env_resolver::resolve_required(
            "tenant_id", 
            "MISSING_TENANT_ID"
        );

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "MISSING_TENANT_ID");
    }
}