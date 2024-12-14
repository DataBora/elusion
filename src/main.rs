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
    
    let sales_columns = vec![
        ("OrderDate", "DATE", false),
        ("StockDate", "DATE", false),
        ("OrderNumber", "VARCHAR", false),
        ("ProductKey", "INT", false),
        ("CustomerKey", "INT", false),
        ("TerritoryKey", "INT", false),
        ("OrderLineItem", "INT", false),
        ("OrderQuantity", "INT", false)
    ];

    let customers_columns = vec![
        ("CustomerKey", "INT", false),
        ("Prefix", "VARCHAR", true),
        ("FirstName", "VARCHAR", false),
        ("LastName", "VARCHAR", false),
        ("BirthDate", "DATE", false),
        ("MaritialStatus", "CHAR", false),
        ("Gender", "VARCHAR", false),
        ("EmailAddress", "VARCHAR", false),
        ("AnnualIncome", "INT", false),
        ("TotalChildren", "INT", false),
        ("EducationLevel", "VARCHAR", false),
        ("Occupation", "VARCHAR", false),
        ("HomeOwner","CHAR", false)
    ];

    let products_columns = vec![
        ("ProductKey", "INT", false),
        ("ProductSubcategoryKey", "INT", false),
        ("ProductSKU", "VARCHAR", false),
        ("ProductName", "VARCHAR", false),
        ("ModelName", "VARCHAR", false),
        ("ProductDescription", "VARCHAR", false),
        ("ProductColor", "VARCHAR", false),
        ("ProductSize", "VARCHAR", false),
        ("ProductStyle", "VARCHAR", false),
        ("ProductCost", "DOUBLE", false),
        ("ProductPrice", "DOUBLE", false)
    ];

    let subcategory_columns = vec![
        ("ProductSubcategoryKey", "INT", false),
        ("SubcategoryName", "VARCHAR", false),
        ("ProducCateforyKey", "INT", false)
    ];

    let sales_data = "C:\\Borivoj\\RUST\\Elusion\\SalesData2022.csv";
    // let customers_data = "C:\\Borivoj\\RUST\\Elusion\\Customers.csv";
    // let products = "C:\\Borivoj\\RUST\\Elusion\\Products.csv";
    // let subcategory = "C:\\Borivoj\\RUST\\Elusion\\ProductSubcategory.csv";

    let df_sales = CustomDataFrame::new(sales_data, sales_columns, "sales").await; 
    // let df_customers = CustomDataFrame::new(customers_data, customers_columns, "customers").await; 
    // let df_products = CustomDataFrame::new(products, products_columns, "products").await; 
    // let df_subcategory = CustomDataFrame::new(subcategory, subcategory_columns, "subcategory").await; 

    
    df_sales
        .select(vec!["OrderDate", "OrderQuantity"])
        .limit(10)
        .display().await?; 

    


    //  result_df.display_query();
    //  result_df.display().await?;
    //  result_df.display_query_plan();
    //  result_df.display_query(); 

// PARQUET --------------------------
    // let parquet_columns = vec![
    //     ("row_id", "INT", false),
    //     ("timestamp", "INT", true),
    //     ("user_id", "INT", false),
    //     ("content_id", "INT", false),
    //     ("content_type_id", "BOOLEAN", false),
    //     ("task_container_id", "INT", false),
    //     ("user_answer", "INT", false),
    //     ("answered_correctly", "INT", false),
    //     ("prior_question_elapsed_time", "DOUBLE", true),
    //     ("prior_question_had_explanation", "BOOLEAN", true),
    // ];

    // File path
    // let parquet_path = "C:\\Borivoj\\RUST\\Elusion\\riiid_train.parquet";

    // // Load file and define schema in a single call
    // let parquet_df = CustomDataFrame::new(parquet_path, parquet_columns, "riid").await;

    // let result_df2 = parquet_df
    //     .aggregation(vec![
    //         AggregationBuilder::new("row_id").count().alias("df_count")
    //     ]);
        // .select(vec![
        //     "row_id",
        //     "timestamp",
        //     "user_id",
        //     "content_id",
        //     "content_type_id",
        //     "task_container_id",
        //     "user_answer",
        //     "answered_correctly",
        //     "prior_question_elapsed_time",
        //     "prior_question_had_explanation",
        // ])
        // .limit(10);

    // result_df2.display().await?;


    Ok(())
}

