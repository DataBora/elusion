
// use log::debug;
use elusion::prelude::*;


#[tokio::main]
async fn main() -> ElusionResult<()> {

   
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


    let join_df = df_sales.clone()
        .join(
            df_customers.clone(),
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

// ============ CTE
  //  CTE for aggregated sales data
 
        // let aggregated_sales = df_sales.clone()
        //     .aggregation(vec![
        //         AggregationBuilder::new("OrderQuantity").sum().alias("total_order_quantity"),
        //         AggregationBuilder::new("OrderLineItem").count().alias("total_orders"),
        //     ])
        //     .group_by(vec!["sales.CustomerKey"])
        //     .select(vec!["CustomerKey", "total_order_quantity", "total_orders"]);
            
        // // println!("Aggregated Sales Schema: {:?}", aggregated_sales.df.schema());

        // // for cte in &aggregated_sales.ctes {
        // // println!("CTE Registered: {:?}", cte.schema.fields());
        // // }

        // let cte_query = aggregated_sales.df_alias("agg_sales")
        // .join(
        //     df_customers,
        //     "agg_sales.CustomerKey == customers.CustomerKey",
        //     "INNER",
        // )
        // .select(vec![
        //     "total_order_quantity", 
        //     "cust.FirstName",
        //     "cust.LastName",
        //     "cust.EmailAddress",
        // ]);

        // cte_query.limit(10).display().await?;


// ========== WINDOW Function

    // let window_query = df_sales
    //     .select(vec!["CustomerKey", "OrderDate", "OrderQuantity"])
    //     .window(
    //         "RANK",                  // Window function: RANK
    //         "OrderQuantity",         // Column to calculate rank
    //         vec!["CustomerKey"],     // PARTITION BY CustomerKey
    //         vec!["OrderDate"],       // ORDER BY OrderDate
    //         Some("OrderRank"),       // Alias for the window function
    //     )
    //     .order_by(vec!["CustomerKey", "OrderRank"], vec![true, true]) // Order by CustomerKey and rank
    //     .limit(10);

    // window_query.display_query();
    // window_query.display().await?;

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

// // //=============================================
let result_df = sales_order_data
    .aggregation(vec![
        AggregationBuilder::new("billable_value").sum().alias("total_sales"),
        AggregationBuilder::new("billable_value").avg().alias("avg_sales")
    ])
    .group_by(vec!["customer_name", "order_date"])
    .having("total_sales > 1000")
    .select(vec!["customer_name", "order_date", "total_sales", "avg_sales"]) 
    .order_by(vec!["total_sales"], vec![false])
    .limit(10);


    result_df.display_query();
    result_df.display().await?;

    let sales_columns = df_sales
        .select(vec!["CustomerKey as customer_key"])
        .limit(10);

    sales_columns.display().await?;
    

//     //======= writing parquet

    

//     result_df
//         .write_to_parquet("overwrite", "C:\\Borivoj\\RUST\\Elusion\\test.parquet", None)
//         .await
//         .expect("Failed to write to Parquet");

//     result_df
//         .write_to_parquet(
//             "append",
//             "C:\\Borivoj\\RUST\\Elusion\\test.parquet",
//             None
//         )
//         .await
//         .expect("Failed to append to Parquet");

    Ok(())
}

