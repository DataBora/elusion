
// use log::debug;
use elusion::prelude::*;


#[tokio::main]
async fn main() -> ElusionResult<()> {

   

    let sales_data = "C:\\Borivoj\\RUST\\Elusion\\SalesData2022.csv";
    let customers_data = "C:\\Borivoj\\RUST\\Elusion\\Customers.csv";
    let products_data = "C:\\Borivoj\\RUST\\Elusion\\Products.csv";

    let df_sales = CustomDataFrame::new(sales_data, "s").await?; 
    let df_customers = CustomDataFrame::new(customers_data, "c").await?; 
    let df_products = CustomDataFrame::new(products_data, "p").await?; 

    // let many_joins = df_sales
    //     .join_many([
    //         (df_customers, "s.CustomerKey = c.CustomerKey", "INNER"),
    //         (df_products, "s.ProductKey = p.ProductKey", "INNER"),
    //     ]) 
    //     .select([
    //         "c.CustomerKey",
    //         "c.FirstName",
    //         "c.LastName",
    //         "p.ProductName",
    //     ]) 
    //     .agg([
    //         "SUM(s.OrderQuantity) AS total_quantity",
    //         "AVG(s.OrderQuantity) AS avg_quantity",
    //     ]) 
    //     .group_by(["c.CustomerKey", "c.FirstName", "c.LastName", "p.ProductName"]) 
    //     .having_many([("SUM(s.OrderQuantity) > 10"), ("AVG(s.OrderQuantity) < 100")]) 
    //     .order_by_many([
    //         ("total_quantity", true), ("p.ProductName", false) // true is ascending, false is descending
    //     ])
    //     .limit(10); 

    // let join_df3 = many_joins.elusion("result_three_joins").await?;
    // join_df3.display().await?;

   
    

//     // three_joins.display().await?;

//      // 3. Performing a Single Join
//    // Build the query using method chaining
//    let single_join = df_sales
//         .join(df_customers, "s.CustomerKey = c.CustomerKey", "INNER")
//         .select([
//             "s.OrderDate","c.FirstName", "c.LastName",
//         ])
//         .agg([
//             "SUM(s.OrderQuantity) AS total_quantity",
//             "AVG(s.OrderQuantity) AS avg_quantity",
//         ])
//         .group_by([   
//             "s.OrderDate","c.FirstName","c.LastName"
//         ])
//         .having("SUM(s.OrderQuantity) > 10") 
//         .order_by(["total_quantity"], [false]) 
//         .limit(10);

//     let join_df1 = single_join.elusion("result_query").await?;
//     join_df1.display().await?;

    // let csv_path = "C:\\Borivoj\\RUST\\Elusion\\sales_order_report.csv";
    // let sales_order_df = CustomDataFrame::new(csv_path, "sales").await?;

    // // Build the query using method chaining
    // let sales_df = sales_order_df.clone()
    //     .select([
    //         "customer_name", 
    //         "order_date"
    //     ])
    //     .agg([
    //         "SUM(billable_value) AS total_sales",
    //         "AVG(billable_value) AS avg_sales",
    //     ])
    //     .group_by(["customer_name", "order_date"])
    //     .having("total_sales > 1000")
    //     .order_by(["total_sales"], [false]) // true is ascending, false is descending
    //     .limit(10);
    
    // Display the generated SQL query
    // result_df.display_query();
    
    // // Execute the query
    // let agg_sales_df = sales_df.elusion("result_sales").await?;
    
    // // Optionally display the results
    // agg_sales_df.display().await?;

    // let filter_df = sales_order_df
    //     .select(["customer_name", "order_date", "billable_value"])
    //     .filter("order_date > '2021-07-04'")
    //     .order_by(["order_date"], [true])
    //     .limit(10);

    // let filtered = filter_df.elusion("result_sales").await?;
    // filtered.display().await?;

    // print_json
    // .write_to_parquet(
    //     "append",
    //     "C:\\Borivoj\\RUST\\Elusion\\test.parquet",
    //     None // I've set WriteOptions to default for writing Parquet files, so keep it None
    // )
    // .await
    // .expect("Failed to write to Parquet");

//     //============= advanced window
//     // Build the query with multiple window functions
    let window_query = df_sales
        .join(df_customers, "s.CustomerKey = c.CustomerKey", "INNER")
        .select([
            "s.OrderDate",
            "c.FirstName",
            "c.LastName",
            "s.OrderQuantity",
        ])
        .window(
            "ROW_NUMBER() OVER (PARTITION BY c.CustomerKey ORDER BY s.OrderDate) as row_num"
        )
        .window(
            "SUM(s.OrderQuantity) OVER (PARTITION BY c.CustomerKey ORDER BY s.OrderDate) as running_total"
        )
        .limit(10);

    let window_df = window_query.elusion("result_window").await?;
    window_df.display().await?;

    // SQL\

//     let sql_one = "
//     SELECT
//         CAST(BirthDate AS DATE) as date_of_birth,
//         CONCAT(firstname, ' ',lastname) as full_name
//         FROM CUSTOMERS
//     LIMIT 10;
//     ";

// let result_one = df_customers.raw_sql(sql_one, "customers_data", &[]).await?;

// result_one.display().await?;

// // Query on 2 DataFrames
// let sql_two = "
//     WITH agg_sales AS (
//         SELECT
//             CustomerKey,
//             SUM(OrderQuantity) AS total_order_quantity,
//             COUNT(OrderLineItem) AS total_orders
//         FROM sales
//         GROUP BY CustomerKey
//     ),
//     customer_details AS (
//         SELECT
//             *
//         FROM customers
//     )
//     SELECT
//         cd.*,
//         asales.total_order_quantity,
//         asales.total_orders
//     FROM agg_sales asales
//     INNER JOIN customer_details cd ON asales.CustomerKey = cd.CustomerKey
//     ORDER BY asales.total_order_quantity DESC
//     LIMIT 100;
// ";

// let result_two = df_sales.raw_sql(sql_two, "top_customers", &[&df_customers]).await?;

// result_two.display().await?;

// // Query on 3 DataFrames (same approach is used on any number of DataFrames)
// let sql_three = "
//     SELECT c.CustomerKey, c.FirstName, c.LastName, p.ProductName,
//             SUM(s.OrderQuantity) AS TotalQuantity
//     FROM
//         sales s
//     INNER JOIN
//         customers c
//     ON
//         s.CustomerKey = c.CustomerKey
//     INNER JOIN
//         products p
//     ON
//         s.ProductKey = p.ProductKey
//     GROUP BY c.CustomerKey, c.FirstName, c.LastName, p.ProductName
//     ORDER BY
//         TotalQuantity DESC
//     LIMIT 100;
//     ";
//     // we need to provide dataframe names in raw_sql that are included in query, as well as new alias ex:"sales_summary" for further use of dataframe if needed
//     let result_three = df_sales.raw_sql(sql_three, "sales_summary", &[&df_customers, &df_products]).await?;

//     result_three.display().await?;

   


    Ok(())


}

