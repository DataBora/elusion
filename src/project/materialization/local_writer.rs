use std::path::Path;
use crate::CustomDataFrame;
use crate::custom_error::cust_error::{ElusionError, ElusionResult};

pub async fn write_parquet_local(
    df: &CustomDataFrame,
    output_path: &str,
    model_name: &str,
) -> ElusionResult<()> {
    let file_path = Path::new(output_path)
        .join(format!("{}.parquet", model_name))
        .to_string_lossy()
        .into_owned();
    println!("💾 Writing Parquet: {}", file_path);
    df.write_to_parquet("overwrite", &file_path, None).await?;
    println!("✅ Written: {}", file_path);
    Ok(())
}

pub async fn write_delta_local(
    df: &CustomDataFrame,
    output_path: &str,
    model_name: &str,
) -> ElusionResult<()> {
    let table_path = Path::new(output_path)
        .join(model_name)
        .to_string_lossy()
        .into_owned();
    println!("💾 Writing Delta: {}", table_path);
    df.write_to_delta_table("overwrite", &table_path, None)
        .await
        .map_err(|e| ElusionError::Custom(format!(
            "❌ Failed to write Delta table '{}': {}", table_path, e
        )))?;
    println!("✅ Written: {}", table_path);
    Ok(())
}
