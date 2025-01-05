
// use log::debug;
use elusion::prelude::*;


#[tokio::main]
async fn main() -> ElusionResult<()> {

    let csv_path = "C:\\Borivoj\\RUST\\Elusion\\sales_order_report.csv";
    let sales_df = CustomDataFrame::new(csv_path, "sales").await;

    let result_df = sales_df
    .aggregation(vec![
        AggregationBuilder::new("billable_value").sum().alias("total_sales"),
        AggregationBuilder::new("billable_value").avg().alias("avg_sales")
    ])
    .group_by(vec!["customer_name", "order_date"])
    .having("total_sales > 1000")
    .select(vec!["customer_name", "order_date", "total_sales", "avg_sales"]) // SELECT is used with Final columns after aggregation
    .order_by(vec!["total_sales"], vec![false])
    .limit(10);

    result_df.display().await?;

    // Overwrite
    // result_df
    //     .write_to_delta_table(
    //         "overwrite",
    //         "C:\\Borivoj\\RUST\\Elusion\\agg_sales",
    //         Some(vec!["order_date".into()]),
    //     )
    //     .await
    //     .expect("Failed to overwrite Delta table");

    // // Append
    // result_df
    //     .write_to_delta_table(
    //         "append",
    //         "C:\\Borivoj\\RUST\\Elusion\\agg_sales",
    //         None,
    //     )
    //     .await
    //     .expect("Failed to append to Delta table");

    //  // Or "merge" if you want neither removing existing data nor schema merging
    //  result_df
    //  .write_to_delta_table(
    //      "merge",
    //      "C:\\Borivoj\\RUST\\Elusion\\agg_sales",
    //      None,
    //  )
    //  .await
    //  .expect("Failed to use merge mode");

    // // Or "default" if you want neither removing existing data nor schema merging
    // result_df
    //     .write_to_delta_table(
    //         "default",
    //         "C:\\Borivoj\\RUST\\Elusion\\agg_sales",
    //         None,
    //     )
    //     .await
    //     .expect("Failed to use default mode");

    Ok(())


}

