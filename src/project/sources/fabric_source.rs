use crate::CustomDataFrame;
use crate::custom_error::cust_error::ElusionResult;

pub async fn load_fabric_service_principal(
    abfss_path: &str,
    file_path: &str,
    tenant_id: &str,
    client_id: &str,
    client_secret: &str,
    alias: &str,
) -> ElusionResult<CustomDataFrame> {
    println!("☁️  Loading Fabric source: {}/{}", abfss_path, file_path);
    CustomDataFrame::from_fabric_with_service_principal(
        tenant_id,
        client_id,
        client_secret,
        abfss_path,
        file_path,
        alias,
    ).await
}

pub async fn load_fabric_sas(
    url: &str,
    sas_token: &str,
    filter_keyword: Option<&str>,
    alias: &str,
) -> ElusionResult<CustomDataFrame> {
    println!("☁️  Loading Fabric SAS source: {}", url);
    CustomDataFrame::from_azure_with_sas_token(
        url,
        sas_token,
        filter_keyword,
        alias,
    ).await
}