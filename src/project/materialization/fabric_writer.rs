use crate::CustomDataFrame;
use crate::custom_error::cust_error::ElusionResult;

pub async fn write_parquet_fabric(
    df: &CustomDataFrame,
    abfss_path: &str,
    file_path: &str,
    tenant_id: &str,
    client_id: &str,
    client_secret: &str,
    model_name: &str,
) -> ElusionResult<()> {
    let full_path = format!("{}/{}.parquet", file_path, model_name);
    println!("☁️  Writing Parquet to Fabric: {}/{}", abfss_path, full_path);
    df.write_parquet_to_fabric_with_service_principal(
        tenant_id,
        client_id,
        client_secret,
        abfss_path,
        &full_path,
    ).await?;
    println!("✅ Written to Fabric: {}/{}", abfss_path, full_path);
    Ok(())
}