
// use log::debug;
use elusion::prelude::*;


#[tokio::main]
async fn main() -> ElusionResult<()> {


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

    // Query on 1 DataFrame
    let sql_one = "
        SELECT
            CAST(BirthDate AS DATE) as date_of_birth,
            CONCAT(firstname, ' ',lastname) as full_name
            FROM CUSTOMERS
        LIMIT 10;
        ";

    let result_one = df_customers.raw_sql(sql_one, "customers_data", &[]).await?;
    result_one.display().await?;

    // Query on 2 DataFrames
    let sql_two = "
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
        LIMIT 10;
    ";

    let result_two = df_sales.raw_sql(sql_two, "top_customers", &[&df_customers]).await?;
    result_two.display().await?;

    // Query on 3 DataFrames (same approach is used on any number of DataFrames)
    let sql_three = "
        SELECT c.CustomerKey, c.FirstName, c.LastName, p.ProductName,
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
        GROUP BY c.CustomerKey, c.FirstName, c.LastName, p.ProductName
        ORDER BY
            TotalQuantity DESC
        LIMIT 10;
        ";

    let result_three = df_sales.raw_sql(sql_three, "customer_product_sales_summary", &[&df_customers, &     df_products]).await?;
        result_three.display().await?;

     //=============================================   
    let result = df_sales.clone()
        .select(vec!["OrderDate as koji_kurac", "OrderNumber AS picka"])
        .limit(10);

    result.display().await?;
    //=============================

        // DATA FRAME
    let join_df = df_sales.clone()
        .join(
            df_customers.clone(),
            "sales.CustomerKey == customers.CustomerKey",
            "INNER",
        )
        .select(vec![
            "sales.OrderDate",
            "sales.ProductKey",
            "sales.OrderQuantity",
            "customers.FirstName",
            "customers.LastName",
        ])
        .limit(10);
            
        // join_df.display_query(); // if you want to see generated sql query
        join_df.display().await?;
//===============================================================================
    let sales_order_columns = vec![
        ("customer_name", "VARCHAR", false),
            ("customer_contact_name", "VARCHAR",false),
            ("customer_country", "VARCHAR",false),
            ("employee_name", "VARCHAR",false),	
            ("employee_title", "VARCHAR",false),	
            ("shipper_name", "VARCHAR",false),	
            ("ship_name"	, "VARCHAR",false),
            ("order_date", "DATE",false	),	
            ("delivery_date", "DATE",false),	
            ("freight_value", "DOUBLE",false),
            ("order_value"	, "DOUBLE",false),
            ("billable_value", "DOUBLE",false),
        
    ];
    let sales_orders_path  = "C:\\Borivoj\\RUST\\Elusion\\sales_order_report.csv";

    let sales_order_data = CustomDataFrame::new(sales_orders_path,sales_order_columns,  "sales_orders").await;

    let result_df = sales_order_data
        .aggregation(vec![
            AggregationBuilder::new("billable_value").sum().alias("total_sales"),
            AggregationBuilder::new("billable_value").avg().alias("avg_sales")
        ])
        .group_by(vec!["customer_name", "order_date"])
        .having("total_sales > 11000.00")
        .select(vec!["customer_name", "order_date", "total_sales", "avg_sales"]) // SELECT is used with Final columns after aggregation
        .order_by(vec!["total_sales"], vec![false])
        .limit(10);

        // result_df.display_query(); // if you want to see generated sql query
        result_df.display().await?;

    //=======================================
//     let joined_df = df_sales
//     .join(
//         df_customers,
//         "sales.customerkey == customers.customerkey",
//         "INNER"
//     )
//     .join(
//         df_products,
//         "sales.productkey == products.productkey",
//         "INNER"
//     );

// // Print intermediate schema to debug
// println!("Joined Schema: {:?}", joined_df.display_schema());

    // Perform the first join
    // let joined_df = df_sales
    // .simple_join(
    //     df_customers,
    //     "sales.customerkey == customers.customerkey",
    //     "INNER"
    // )
    // .simple_join(
    //     df_products,
    //     "sales.productkey == products.productkey",
    //     "INNER"
    // );

// println!("Final Joined Schema: {:?}", joined_df.df.schema());



    let result_three = df_sales
        .join(
            df_customers,
            "sales.CustomerKey == customers.CustomerKey",
            "INNER"
        )
        .join(
            df_products,
            "sales.ProductKey == products.ProductKey",
            "INNER"
        )
        .aggregation(vec![
            AggregationBuilder::new("OrderQuantity") 
                .sum()
                .alias("total_quantity") 
        ])
        .group_by(vec![
            "customers.customerkey",     
            "customers.firstname",       
            "customers.lastname",       
            "products.productname" 
        ])
        .having("total_quantity > 10")
        .select(vec![
            "customers.customerkey", 
            "customers.firstname",
            "customers.lastname",
            "products.productname",
            "total_quantity"  
        ])
        .order_by(vec!["total_quantity"], vec![false])
        .limit(10);

    // println!("Final Schema: {:?}", result_three.display_schema());

    match result_three.display().await {
        Ok(_) => println!("Query executed successfully"),
        Err(e) => println!("Error: {:?}", e),
    }
    Ok(())
}

