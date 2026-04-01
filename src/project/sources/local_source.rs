use crate::CustomDataFrame;
use crate::custom_error::cust_error::ElusionResult;

pub async fn load_local_csv(path: &str, alias: &str) -> ElusionResult<CustomDataFrame> {
    println!("📂 Loading CSV source: {}", path);
    CustomDataFrame::new(path, alias).await
}

pub async fn load_local_parquet(path: &str, alias: &str) -> ElusionResult<CustomDataFrame> {
    println!("📂 Loading Parquet source: {}", path);
    CustomDataFrame::new(path, alias).await
}

pub async fn load_local_delta(path: &str, alias: &str) -> ElusionResult<CustomDataFrame> {
    println!("📂 Loading Delta source: {}", path);
    CustomDataFrame::new(path, alias).await
}