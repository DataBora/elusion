mod elusion;

use elusion::AggregationBuilder;
use elusion::CustomDataFrame;
// use log::debug;


#[tokio::main]
async fn main() -> datafusion::error::Result<()> {

   
    // std::env::set_var("RUST_LOG", "debug");
    // env_logger::init();

    
    let sales_columns = vec![
        ("OrderDate", "DATE", false),
        ("StockDate", "DATE", false),
        ("OrderNumber", "VARCHAR", false),
        ("ProductKey", "INT", false),
        ("CustomerKey", "INT", true),
        ("TerritoryKey", "INT", false),
        ("OrderLineItem", "INT", false),
        ("OrderQuantity", "INT", false)
    ];

    let customers_columns = vec![
        ("CustomerKey", "INT", true),
        ("Prefix", "VARCHAR", true),
        ("FirstName", "VARCHAR", true),
        ("LastName", "VARCHAR", true),
        ("BirthDate", "DATE", true),
        ("MaritialStatus", "CHAR", true),
        ("Gender", "VARCHAR", true),
        ("EmailAddress", "VARCHAR", true),
        ("AnnualIncome", "INT", true),
        ("TotalChildren", "INT", true),
        ("EducationLevel", "VARCHAR", true),
        ("Occupation", "VARCHAR", true),
        ("HomeOwner","CHAR", true)
    ];

    // let products_columns = vec![
    //     ("ProductKey", "INT", false),
    //     ("ProductSubcategoryKey", "INT", false),
    //     ("ProductSKU", "VARCHAR", false),
    //     ("ProductName", "VARCHAR", false),
    //     ("ModelName", "VARCHAR", false),
    //     ("ProductDescription", "VARCHAR", false),
    //     ("ProductColor", "VARCHAR", false),
    //     ("ProductSize", "VARCHAR", false),
    //     ("ProductStyle", "VARCHAR", false),
    //     ("ProductCost", "DOUBLE", false),
    //     ("ProductPrice", "DOUBLE", false)
    // ];

    // let subcategory_columns = vec![
    //     ("ProductSubcategoryKey", "INT", false),
    //     ("SubcategoryName", "VARCHAR", false),
    //     ("ProducCateforyKey", "INT", false)
    // ];

    let sales_data = "C:\\Borivoj\\RUST\\Elusion\\SalesData2022.csv";
    let customers_data = "C:\\Borivoj\\RUST\\Elusion\\Customers.csv";
    // let products = "C:\\Borivoj\\RUST\\Elusion\\Products.csv";
    // let subcategory = "C:\\Borivoj\\RUST\\Elusion\\ProductSubcategory.csv";

    let df_sales = CustomDataFrame::new(sales_data, sales_columns, "sales").await; 
    let df_customers = CustomDataFrame::new(customers_data, customers_columns, "customers").await; 
    // let df_products = CustomDataFrame::new(products, products_columns, "products").await; 
    // let df_subcategory = CustomDataFrame::new(subcategory, subcategory_columns, "subcategory").await; 


    let join_df = df_sales
        .join(
            df_customers,
            "sales.CustomerKey == customers.CustomerKey",
            "INNER",
        )
        .select(vec![
            "sales.OrderDate",
            "sales.OrderQuantity",
            "customers.FirstName",
            "customers.LastName",
        ])
        .limit(10);
        
    join_df.display_query();
    join_df.display().await?;
    // join_df.display_query_plan();

//==================================

    let sales_order_columns = vec![
        ("customer_name", "VARCHAR", false),
        ("customer_contact_name", "VARCHAR", false),
        ("customer_country", "VARCHAR", false),
        ("employee_name", "VARCHAR", false),
        ("employee_title", "VARCHAR", false),
        ("shipper_name", "VARCHAR", false),
        ("ship_name", "VARCHAR", false),
        ("order_date", "DATE", false),
        ("delivery_date", "DATE", false),
        ("freight_value", "DOUBLE", false),
        ("order_value", "DOUBLE", false),
        ("billable_value", "NUMBER", false),
    ];

    let sales_path = "C:\\Borivoj\\RUST\\Elusion\\sales_order_report.csv";

    let sales_order_data = CustomDataFrame::new(sales_path, sales_order_columns, "sales_orders").await;

    let result_sales = sales_order_data.clone()
            .select(vec!["customer_name", "order_date", "billable_value"])
            .filter("billable_value > 100.0")
            .order_by(vec!["order_date"], vec![true])
            .limit(10);

    result_sales.display_query();   
    result_sales.display().await?;

//=============================================
let result_df = sales_order_data
    .aggregation(vec![
        AggregationBuilder::new("billable_value").sum().alias("total_sales"),
        AggregationBuilder::new("billable_value").avg().alias("avg_sales")
    ])
    .group_by(vec!["customer_name", "order_date"])
    .having("total_sales > 1000")
    .select(vec!["customer_name", "order_date", "total_sales", "avg_sales"]) // Final columns after aggregation
    .order_by(vec!["total_sales"], vec![false])
    .limit(10);


    result_df.display_query();
    result_df.display().await?;

    Ok(())
}

