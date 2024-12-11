pub mod datatypes;
pub mod aggregation_builder;
pub mod data_query_loader;

// use log::debug;

use data_query_loader::load_query::CustomDataFrame;
use crate::aggregation_builder::aggregation::AggregationBuilder;


#[tokio::main]
async fn main() -> datafusion::error::Result<()> {

    // Initialize logging
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();

    // debug!("Initializing schema and setting up column definitions.");
    
    // let columns = vec![
    //     ("sales_order_num", "VARCHAR", false),
    //     ("sales_order_line_num", "INT", false),
    //     ("order_date", "DATE", false),
    //     ("customer_name", "VARCHAR", false),
    //     ("email_address", "VARCHAR", false),
    //     ("item", "VARCHAR", false),
    //     ("quantity", "INT", false),
    //     ("unit_price", "DOUBLE", false),
    //     ("tax_amount", "DOUBLE", false),
    // ];

    // let csv_path = "C:\\Borivoj\\RUST\\Elusion\\sales.csv";

    // let custom_df = CustomDataFrame::new(csv_path, columns, "sales").await; 


    // let result_df = custom_df
    //     .select(vec!["customer_name", "order_date", "unit_price", "quantity"]) 
    //     .aggregation(vec![
    //         AggregationBuilder::new("unit_price").sum().alias("total_spent"),
    //         AggregationBuilder::new("quantity").avg().alias("average_quantity"),
    //     ]) 
    //     .filter("customer_name = 'Ruben Prasad'")
    //     .group_by(vec!["customer_name", "order_date"]) 
    //     // .having("total_spent < 700")
    //     .order_by(vec!["order_date"], vec![true]) 
    //     .limit(10); 

    //  result_df.display_query();
    //  result_df.display().await?;
    //  result_df.display_query_plan();
    //  result_df.display_query(); 

// PARQUET --------------------------
    let parquet_columns = vec![
        ("row_id", "INT", false),
        ("timestamp", "INT", true),
        ("user_id", "INT", false),
        ("content_id", "INT", false),
        ("content_type_id", "BOOLEAN", false),
        ("task_container_id", "INT", false),
        ("user_answer", "INT", false),
        ("answered_correctly", "INT", false),
        ("prior_question_elapsed_time", "DOUBLE", true),
        ("prior_question_had_explanation", "BOOLEAN", true),
    ];

    // File path
    let parquet_path = "C:\\Borivoj\\RUST\\Elusion\\riiid_train.parquet";

    // Load file and define schema in a single call
    let parquet_df = CustomDataFrame::new(parquet_path, parquet_columns, "riid").await;

    let result_df2 = parquet_df
        .select(vec![
            "row_id",
            "timestamp",
            "user_id",
            "content_id",
            "content_type_id",
            "task_container_id",
            "user_answer",
            "answered_correctly",
            "prior_question_elapsed_time",
            "prior_question_had_explanation",
        ])
        .limit(10);

    result_df2.display().await?;


    Ok(())
}

