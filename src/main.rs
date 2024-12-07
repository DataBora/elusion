pub mod datatypes;
pub mod loaders;
pub mod select;

use std::sync::Arc;
use log::{debug, error};
use loaders::csv_loader::{create_schema_from_str, CsvLoader};

use select::select_queries::CustomDataFrame;
use crate::select::aggregation::AggregationBuilder;
use datafusion::logical_expr::col;
// use display::display_dataframe::display;


#[tokio::main]
async fn main() -> datafusion::error::Result<()> {

    // Initialize logging
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();

    debug!("Initializing schema and setting up column definitions.");
    
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

    // Create schema
    let schema = Arc::new(create_schema_from_str(columns));
    let path = "C:\\Borivoj\\RUST\\Elusion\\elusion\\sales.csv";

    // Load CSV data into a DataFrame
    
    let aliased_df = match path.load(path, schema, "sales").await {
        Ok(df) => {
            debug!("CSV loaded successfully.");
            df
        },
        Err(err) => {
            error!("Failed to load CSV: {:?}", err);
            return Err(err);
        },
    };

    let custom_df = CustomDataFrame::new(aliased_df);
    println!("Using table alias: {}", custom_df.table_alias); // Debug alias


    let result_df = custom_df
        .select(vec!["customer_name", "order_date", "unit_price", "quantity"]) // Explicitly selected columns
        .aggregation(vec![
            AggregationBuilder::new("unit_price").sum().alias("total_price"),
            AggregationBuilder::new("quantity").avg().alias("average_quantity"),
        ]) // Aggregation columns with aliasing
        .group_by(vec!["customer_name", "order_date"]) // Grouping
        .order_by(vec!["order_date"], vec![true]) // Sorting
        .limit(20); // Limiting




 
    //  result_df.display_query();
     result_df.display().await?;
    //  result_df.display_query_plan();
     result_df.display_query(); // Show SQL equivalent
    


    Ok(())
}

