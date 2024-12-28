
// use log::debug;
use elusion::prelude::*;


#[tokio::main]
async fn main() -> ElusionResult<()> {

   
    // std::env::set_var("RUST_LOG", "debug");
    env_logger::init();

    
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
        ("ProductPrice", "DOUBLE", false),
    ];



    let sales_data = "C:\\Borivoj\\RUST\\Elusion\\SalesData2022.csv";
    let customers_data = "C:\\Borivoj\\RUST\\Elusion\\Customers.csv";
    let products_data = "C:\\Borivoj\\RUST\\Elusion\\Products.csv";

    let df_sales = CustomDataFrame::new(sales_data, sales_columns, "sales").await; 
    let df_customers = CustomDataFrame::new(customers_data, customers_columns, "customers").await; 
    let df_products = CustomDataFrame::new(products_data, products_columns, "products").await; 


//     let join_df = df_sales.clone()
//         .join(
//             df_customers.clone(),
//             "sales.CustomerKey == customers.CustomerKey",
//             "INNER",
//         )
//         .select(vec![
//             "sales.OrderDate",
//             "sales.OrderQuantity",
//             "customers.FirstName",
//             "customers.LastName",
//         ])
//         .limit(10);
        
//     join_df.display_query();
//     join_df.display().await?;
//     // join_df.display_query_plan();


// //==================================

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

    // ========= FILTERING

    let result_sales = sales_order_data.clone()
            .select(vec!["customer_name", "order_date", "billable_value"])
            .filter("billable_value > 100.0")
            .order_by(vec!["order_date"], vec![true])
            .limit(10);

    result_sales.display_query();   
    result_sales.display().await?;

    // AGGREGATION =============================================
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

//     //=================== ALIAS IN SELECT ==================
//     let sales_columns = df_sales.clone()
//         .select(vec!["CustomerKey as customer_key","StockDate AS date"])
//         .limit(10);

//     sales_columns.display().await?;

//     // ============ raw sql

    let sql_three = "
        SELECT
            c.CustomerKey,
            c.FirstName,
            c.LastName,
            p.ProductName,
            SUM(s.OrderQuantity) AS TotalQuantity
        FROM
            sales s
        INNER JOIN
            customers c
        ON
            s.CustomerKey = c.CustomerKey
        INNER JOIN
            products p
        ON
            s.ProductKey = p.ProductKey
        GROUP BY
            c.CustomerKey,
            c.FirstName,
            c.LastName,
            p.ProductName
        ORDER BY
            TotalQuantity DESC
        LIMIT 10;
    ";

    // Execute the SQL query on sales, customers, and products DataFrames
    let result_three = df_sales.raw_sql(sql_three, "customer_product_sales_summary", &[&df_customers, &df_products]).await?;

    // Display the results
    result_three.display().await?;

    // =================== JSON
    let json_columns = vec![
        ("movie_title", "VARCHAR", true), 
        ("genre", "VARCHAR", true),          
        ("gross", "VARCHAR", true),          
        ("imdbmetascore", "VARCHAR", true), 
        ("popcornscore", "VARCHAR", true),  
        ("rating", "VARCHAR", true),        
        ("tomatoscore", "VARCHAR", true),   
    ];
    let json_path = "C:\\Borivoj\\RUST\\Elusion\\movies.json";
    let json_df = CustomDataFrame::new(json_path, json_columns, "movies").await;

    // let json_result = json_df
    //         .select(vec!["movie_title", "genre", "imdb_metascore"]);

    //     json_result.display().await?;

    let json_sql = "
        SELECT
            * 
        FROM
         movies
    ";

    let result_json = json_df.raw_sql(json_sql, "movies_all", &[]).await?;

    result_json.display().await?;


    // ======= writing parquet

    // raw_cte
    //     .write_to_parquet("overwrite", "C:\\Borivoj\\RUST\\Elusion\\raw_cte.parquet", None)
    //     .await
    //     .expect("Failed to write to Parquet");

    // result_df
    //     .write_to_parquet(
    //         "append",
    //         "C:\\Borivoj\\RUST\\Elusion\\test.parquet",
    //         None
    //     )
    //     .await
    //     .expect("Failed to append to Parquet");

    Ok(())
}

