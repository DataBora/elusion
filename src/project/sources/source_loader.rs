use crate::CustomDataFrame;
use crate::custom_error::cust_error::ElusionResult;
use crate::project::config::connections_config::SourceConfig;
use super::local_source::{load_local_csv, load_local_parquet, load_local_delta};
use super::fabric_source::{load_fabric_service_principal, load_fabric_sas};

pub async fn load_source(
    name: &str,
    config: &SourceConfig,
) -> ElusionResult<CustomDataFrame> {
    match config {
        SourceConfig::Csv { path } => {
            load_local_csv(path, name).await
        }
        SourceConfig::Parquet { path } => {
            load_local_parquet(path, name).await
        }
        SourceConfig::Delta { path } => {
            load_local_delta(path, name).await
        }
        SourceConfig::Fabric {
            abfss_path,
            file_path,
            tenant_id,
            client_id,
            client_secret,
        } => {
            load_fabric_service_principal(
                abfss_path,
                file_path,
                tenant_id,
                client_id,
                client_secret,
                name,
            ).await
        }
        SourceConfig::FabricSas {
            url,
            sas_token,
            filter_keyword,
        } => {
            load_fabric_sas(
                url,
                sas_token,
                filter_keyword.as_deref(),
                name,
            ).await
        }
    }
}