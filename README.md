# Elusion DataFrame Library

**Elusion** is a high-performance, flexible library built on top of **DataFusion**for managing and querying data using a DataFrame-like interface. Designed for developers who need a powerful abstraction over data transformations, Elusion simplifies complex operations such as filtering, joining, aggregating, and more with an intuitive, chainable API.

---

## Key Features

- **CSV File Integration**: Load data directly from CSV files with customizable schemas.
- **SQL-Like Query Interface**: Use familiar operations such as `SELECT`, `WHERE`, `GROUP BY`, `JOIN`, and more.
- **Aggregation Builders**: Perform advanced aggregations, including `SUM`, `AVG`, `COUNT`, `APPROX_PERCENTILE`, and more.
- **Window Functions**: Define analytical functions with partitioning and ordering support.
- **Custom Table Aliases**: Manage complex multi-table operations with user-defined table aliases.
- **Join Support**: Perform various types of joins (`INNER`, `LEFT`, `RIGHT`, etc.) with intuitive syntax.
- **Data Preview**: Preview your data easily by displaying a subset of rows in the terminal.
- **Composable Queries**: Chain transformations seamlessly to build reusable and testable workflows.

---

## Installation

To add **Elusion** to your Rust project, include the following line in your `Cargo.toml` under `[dependencies]`:

```toml
elusion = "0.1.0"
```

---

## Dependencies that Elusion is build on top of:

```toml
[dependencies]
datafusion = "43.0.0"
arrow = "53.3.0"
tokio = { version = "1.0", features = ["rt-multi-thread"] }
futures = "0.3.31"
chrono = "0.4.38"
regex = "1.11.1"
encoding_rs = "0.8.35"
hex = "0.4.3"
```

## Usage examples:

`rust`

### Schema establishing

```
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
```
### CSV file paths

```
let sales_data = "C:\\Path\\To\\Your\\FIle.csv";
let customers_data = "C:\\Path\\To\\Your\\FIle.csv";
```
### Creating Custom data frame 
#### 3 arguemts needed:  Path, Schema, Table Alias

```
let df_sales = CustomDataFrame::new(sales_data, sales_columns, "sales").await; 
let df_customers = CustomDataFrame::new(customers_data, customers_columns, "customers").await;
```
### JOIN
```
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
```

### SELECT without Aggregation
```
let result_sales = sales_order_data.clone()
            .select(vec!["customer_name", "order_date", "billable_value"])
            .filter("billable_value > 100.0")
            .order_by(vec!["order_date"], vec![true])
            .limit(10);

    result_sales.display_query();   
    result_sales.display().await?;
```

### SELECT with Aggregation
```
let result_df = sales_order_data
    .aggregation(vec![
        AggregationBuilder::new("billable_value").sum().alias("total_sales"),
        AggregationBuilder::new("billable_value").avg().alias("avg_sales")
    ])
    .group_by(vec!["customer_name", "order_date"])
    .having("total_sales > 1000")
    .select(vec!["customer_name", "order_date", "total_sales", "avg_sales"]) // SELECT is used with Final columns after aggregation
    .order_by(vec!["total_sales"], vec![false])
    .limit(10);

    result_df.display_query();
    result_df.display().await?;
```
