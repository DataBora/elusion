use crate::CustomDataFrame;
use crate::custom_error::cust_error::{ElusionError, ElusionResult};
use crate::project::config::project_config::{
    MaterializationType, OutputDestination, ProjectFile,
};
use super::local_writer::{write_parquet_local, write_delta_local};
use super::fabric_writer::write_parquet_fabric;

pub async fn materialize(
    df: &CustomDataFrame,
    model_name: &str,
    project_config: &ProjectFile,
) -> ElusionResult<()> {
    let mat_type = project_config.get_materialization(model_name);
    let output_path = project_config.get_output_path(model_name)?;

    // Memory = no write, just pass through
    if mat_type == MaterializationType::Memory {
        println!("💭 Memory only: '{}' not persisted", model_name);
        return Ok(());
    }

    match &project_config.output.destination {
        OutputDestination::Local => {
            match mat_type {
                MaterializationType::Parquet => {
                    write_parquet_local(df, &output_path, model_name).await
                }
                MaterializationType::Delta => {
                    write_delta_local(df, &output_path, model_name).await
                }
                MaterializationType::Memory => Ok(()),
            }
        }
        OutputDestination::Fabric => {
            let fabric = project_config.output.fabric.as_ref()
                .ok_or_else(|| ElusionError::Custom(
                    "❌ Fabric output not configured in elusion.toml".to_string()
                ))?;

            match mat_type {
                MaterializationType::Parquet | MaterializationType::Delta => {
                    write_parquet_fabric(
                        df,
                        &fabric.abfss_path,
                        &output_path,
                        &fabric.tenant_id,
                        &fabric.client_id,
                        &fabric.client_secret,
                        model_name,
                    ).await
                }
                MaterializationType::Memory => Ok(()),
            }
        }
    }
}