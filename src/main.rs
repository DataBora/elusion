
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



    let sales_data = "C:\\Users\\BorivojGrujičić\\RUST\\divcibare\\SalesData2022.csv";
    let customers_data = "C:\\Users\\BorivojGrujičić\\RUST\\divcibare\\Customers.csv";

    let df_sales = CustomDataFrame::new(sales_data, sales_columns, "sales").await; 
    let df_customers = CustomDataFrame::new(customers_data, customers_columns, "customers").await; 


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

    let sales_path = "C:\\Users\\BorivojGrujičić\\RUST\\divcibare\\sales_order_report.csv";
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

    //=================== ALIAS IN SELECT ==================
    let sales_columns = df_sales.clone()
        .select(vec!["CustomerKey as customer_key","StockDate AS date"])
        .limit(10);

    sales_columns.display().await?;

    // ============ raw sql

    let raw_sql = "
        WITH agg_sales AS (
            SELECT
                CustomerKey,
                SUM(OrderQuantity) AS total_order_quantity,
                COUNT(OrderLineItem) AS total_orders
            FROM sales
            GROUP BY CustomerKey
        ),
        customer_details AS (
            SELECT
                *
            FROM customers
        )
        SELECT
            cd.*,
            asales.total_order_quantity,
            asales.total_orders
        FROM agg_sales asales
        INNER JOIN customer_details cd ON asales.CustomerKey = cd.CustomerKey
        ORDER BY asales.total_order_quantity DESC
        LIMIT 100;
    ";

    let raw_cte = df_sales.execute_sql(raw_sql, "top_customers", &[&df_customers]).await?;
    raw_cte.display().await?;

    // =================== JSON

    // let json_schema = vec![
    //     "","",""
    // ];
    // let json_file_path = "C:\\Users\\BorivojGrujičić\\RUST\\divcibare\\test.json";

    // let df_json = CustomDataFrame::load(json_file_path, json_schema, "my_json").await;

    // df_json.display().await?;

    // =============== CTE

    // let aggregated_sales = df_sales.clone()
    //         .aggregation(vec![
    //             AggregationBuilder::new("OrderQuantity").sum().alias("total_order_quantity"),
    //             AggregationBuilder::new("OrderLineItem").count().alias("total_orders"),
    //         ])
    //         .group_by(vec!["sales.CustomerKey"])
    //         .with_cte( "agg_sales");

    // let joined_df = agg_sales
    //         .join(
    //             df_customers,
    //             "agg_sales.CustomerKey == cust.CustomerKey",
    //             "INNER",
    //         )
    //         .select(vec![
    //             "agg_sales.total_order_quantity", 
    //             "cust.FirstName",
    //             "cust.LastName",
    //             "cust.EmailAddress",
    //         ]);

    //     joined_df.display().await?;

    //======= writing parquet

    raw_cte
        .write_to_parquet("overwrite", "C:\\Users\\BorivojGrujičić\\RUST\\divcibare\\raw_cte.parquet", None)
        .await
        .expect("Failed to write to Parquet");

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

