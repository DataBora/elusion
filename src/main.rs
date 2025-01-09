
// use log::debug;
use elusion::prelude::*;


#[tokio::main]
async fn main() -> ElusionResult<()> {

    let sales_path = "C:\\Borivoj\\RUST\\Elusion\\sales_order_report.csv";
    let sales_order_df = CustomDataFrame::new(sales_path, "sales").await?;

    let filter_df = sales_order_df.clone()
    .select(["customer_name", "order_date", "billable_value"])
    .filter("order_date > '2021-07-04'") //you can use fileter_many() as well
    .order_by(["order_date"], [true])
    .limit(10);

    let filtered = filter_df.elusion("result_sales").await?;
    filtered.display().await?;
//===============================================

    let sales_p = "C:\\Borivoj\\RUST\\Elusion\\SalesData2022.csv";
    let customer_p = "C:\\Borivoj\\RUST\\Elusion\\Customers.csv";
    let products_p = "C:\\Borivoj\\RUST\\Elusion\\Products.csv";
   
    let df_sales = CustomDataFrame::new(sales_p, "s").await?;
    let df_customers = CustomDataFrame::new(customer_p, "c").await?;
    let df_products = CustomDataFrame::new(products_p, "p").await?;

    let many_joins = df_sales.clone()
        .join_many([
            (df_customers.clone(), "s.CustomerKey = c.CustomerKey", "INNER"),
            (df_products, "s.ProductKey = p.ProductKey", "INNER"),
        ]) 
        .select([
            "c.CustomerKey","c.FirstName","c.LastName","p.ProductName",
        ]) 
        .agg([
            "SUM(s.OrderQuantity) AS total_quantity",
            "AVG(s.OrderQuantity) AS avg_quantity",
        ]) 
        .group_by(["c.CustomerKey", "c.FirstName", "c.LastName", "p.ProductName"]) 
        .having_many([("SUM(s.OrderQuantity) > 10"), ("AVG(s.OrderQuantity) < 100")]) 
        .order_by_many([
            ("total_quantity", true), 
            ("p.ProductName", false) // true is ascending, false is descending
        ])
        .limit(10); 

    let join_df3 = many_joins.elusion("df_joins").await?;
    join_df3.display().await?;

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

    let filter_df = sales_order_df.clone()
    .select(["customer_name", "order_date", "billable_value"])
    .filter_many([("order_date > '2021-07-04'"), ("billable_value > 100.0")]) //you can use fileter_many() as well
    .order_by(["order_date"], [true])
    .limit(10);

    let filtered = filter_df.elusion("result_sales").await?;
    filtered.display().await?;

    //==============================

    let result_sales = sales_order_df
        .select(["customer_name", "order_date", "ABS(billable_value) AS abs_billable_value","SQRT(billable_value) AS SQRT_billable_value"])
        .filter("billable_value > 100.0")
        .order_by(["order_date"], [true])
        .limit(10);

    let b = result_sales.elusion("result_sales").await?;
    b.display().await?;




    Ok(())
}

