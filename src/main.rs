pub mod datatypes;
pub mod loaders;
pub mod select;

use std::sync::Arc;

use loaders::csv_loader::{create_schema_from_str, CsvLoader};

use select::select_queries::CustomDataFrame;
// use display::display_dataframe::display;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    
    let columns = vec![

        ("sales_order_num", "VARCHAR", false),
        ("sales_order_line_num", "INT", false),
        ("order_date", "DATE", false),
        ("customer_name", "VARCHAR", false),
        ("email_address", "VARCHAR", false),
        ("item", "VARCHAR", false),
        ("quantity", "INT", false),
        ("unit_price", "DOUBLE", false),
        ("tax_amount", "DOUBLE", false),
    ];

    let schema = Arc::new(create_schema_from_str(columns));
    let path = "C:\\Borivoj\\RUST\\Elusion\\elusion\\sales.csv";

    // Load the CSV with alias
    let aliased_df = path.load(path, schema, "sales").await?;
    let custom_df = CustomDataFrame::new(aliased_df);

    // Perform operations
    let result_df = custom_df
    .select(vec![
        "order_date",
        "customer_name",
        "SUM(unit_price) AS unit_price_summed"
    ])
    .filter("customer_name = 'Curtis Lu'")
    .group_by(vec!["order_date", "customer_name"])
    .order_by(vec!["order_date"], vec![true])
    .limit(10);


    result_df.display_query();
    result_df.display().await?;
    

    Ok(())

}