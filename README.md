# Elusion 🦀 DataFrame Library for Everybody!

![Elusion Logo](images/elusion.png)

Elusion is a high-performance DataFrame library, for in-memory data formats (.csv, .json, .parquet). Built on top of DataFusion SQL query engine, for managing and querying data using a DataFrame-like interface. Designed for developers who need a powerful abstraction over data transformations, Elusion simplifies complex operations such as filtering, joining, aggregating, and more with an intuitive, chainable API.

SQL API is fully supported out of the gate, for writing Raw SQL Queries on in-memory data formats (.csv, .json).

# Motivation

DataFusion SQL engine has great potential in Data Engineering / Data Analytics world, but I believe that design choices for SQL and DataFrame API do not resemble popular DataFrame solutions out there, and I am here to narrow this gap, by rewriting, from scratch, all functions, readers, writers... and creating easily chainable constructs for anybody to use and understand. 

## Key Features

### 🚀 High-Performance DataFrame Operations
- Load and process data from CSV files with ease.
- Perform SQL-like transformations such as `SELECT`, `WHERE`, `GROUP BY`, and `JOIN`.

### 📊 Aggregations and Analytics
- Built-in support for functions like `SUM`, `AVG`, `MIN`, `MAX`, `COUNT`, and more.
- Advanced statistical functions like `CORR`, `STDDEV`, `VAR_POP`, and `PERCENTILE`.

### 🔗 Flexible Joins
- Join tables with various join types (`INNER`, `LEFT`, `RIGHT`, `FULL`, etc.).
- Intuitive syntax for specifying join conditions and aliases.

### 🪟 Window Functions
- Add analytical window functions like `RANK`, `DENSE_RANK`, `ROW_NUMBER`, and custom partition-based calculations.

### 🧹 Clean Query Construction
- Construct readable and reusable SQL queries.
- Support for Common Table Expressions (CTEs), subqueries, and set operations (`UNION`, `INTERSECT`, `EXCEPT`).

### 🛠️ Easy-to-Use API
- Chainable and intuitive API for building queries.
- Readable debug output of generated SQL for verification.

- **Data Preview**: Preview your data easily by displaying a subset of rows in the terminal.
- **Composable Queries**: Chain transformations seamlessly to build reusable and testable workflows.

---

## Installation

To add **Elusion** to your Rust project, include the following lines in your `Cargo.toml` under `[dependencies]`:

```toml
elusion = "0.2.5"
tokio = { version = "1.42.0", features = ["rt-multi-thread"] }
```
## Rust version needed
```toml
>= 1.81
```
---
# Usage examples:

### MAIN function

```rust
#[tokio::main]
async fn main() -> ElusionResult<()> {
    Ok(())
}
```

### MAIN function with small example

```rust
use elusion::prelude::*; // Import everything needed

#[tokio::main]
async fn main() -> ElusionResult<()> {

    let sales_data = "path\\to\\sales_data.csv";
    let df_sales = CustomDataFrame::new(sales_data, "sales").await?;

    let result = df_sales
        .select(vec!["OrderDate", "OrderNumber"])
        .limit(10);

    result.display().await?;

    Ok(())
}
```
### Schema establishing
#### SCHEMA IS AUTOMATICALLY INFERED from v0.3.0

### LOADing Files into CustomDAtaFrame
#### File extensions are automatically recognized 
#### All you have to do is to provide path to your file
#### Currently supported data files: CSV, PARQUET, JSON
```rust
let sales_data = "C:\\Path\\To\\Your\\sales_data.csv";
let parq_path = "C:\\Borivoj\\RUST\\Elusion\\prod_data.parquet";
let json_path = "C:\\Borivoj\\RUST\\Elusion\\db_data.json";
```
### Creating CustomDataFrame
#### 2 arguments needed:  *Path**, **Table Alias**

```rust
let df_sales = CustomDataFrame::new(sales_data, "sales").await; 
let df_customers = CustomDataFrame::new(customers_data, "customers").await;
```
## RULE of thumb: 
#### ALL Column names and Dataframe alias names, will be LOWERCASE(), TRIM(), REPLACE(" ", "_"), regardles of how you write it, or how they are writen in CSV file.
### Aggregation column Aliases will be LOWERCASE() regardles of how you write it.

### ALIAS column names in SELECT() function (AS is case insensitive)
```rust
let customers_alias = df_customers
    .select(vec!["CustomerKey AS customerkey_alias", "FirstName as first_name", "LastName", "EmailAddress"]);
```
### JOIN
#### currently implemented join types
```rust
"INNER" => JoinType::Inner,
"LEFT" => JoinType::Left,
"RIGHT" => JoinType::Right,
"FULL" => JoinType::Full,
"LEFT SEMI" => JoinType::LeftSemi,
"RIGHT SEMI" => JoinType::RightSemi,
"LEFT ANTI" => JoinType::LeftAnti,
"RIGHT ANTI" => JoinType::RightAnti,
"LEFT MARK" => JoinType::LeftMark,
```
#### JOIN example with 2 dataframes
```rust
// join with 2 dataframes
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
        
    join_df.display().await?;
```
### JOIN with 3 dataframes, AGGREGATION, GROUP BY, HAVING, SELECT, ORDER BY
```rust
let three_joins = df_sales
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
        "sales.orderdate",
        "customers.customerkey",     
        "customers.firstname",       
        "customers.lastname",       
        "products.productname" 
    ])
    .having("total_quantity > 10")
    .select(vec![
        "sales.orderdate",
        "customers.customerkey", 
        "customers.firstname",
        "customers.lastname",
        "products.productname",
        "total_quantity"  
    ])
    .order_by(vec!["total_quantity"], vec![false])
    .limit(10);

    three_joins.display().await?;
```
### SELECT without Aggregation
```rust
let result_sales = sales_order_data
    .select(vec!["customer_name", "order_date", "billable_value"])
    .filter("billable_value > 100.0")
    .order_by(vec!["order_date"], vec![true])
    .limit(10);

    result_sales.display().await?;
```

### SELECT with Mulitiple Aggregations
```rust
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

    result_df.display().await?;
```
### FILTER 
```rust
 let result_sales = sales_order_data
    .select(vec!["customer_name", "order_date", "billable_value"])
    .filter("billable_value > 100.0")
    .order_by(vec!["order_date"], vec![true])
    .limit(10);

    result_sales.display().await?;
```

# Raw SQL Querying
### FULL SQL SUPPORT is available
```rust

let sales_data = "C:\\Borivoj\\RUST\\Elusion\\SalesData2022.csv";
let customers_data = "C:\\Borivoj\\RUST\\Elusion\\Customers.csv";
let products_data = "C:\\Borivoj\\RUST\\Elusion\\Products.csv";

let df_sales = CustomDataFrame::new(sales_data, "sales").await; 
let df_customers = CustomDataFrame::new(customers_data, "customers").await; 
let df_products = CustomDataFrame::new(products_data, "products").await; 

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
    LIMIT 100;
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
    LIMIT 100;
    ";
    // we need to provide dataframe names in raw_sql that are included in query, as well as new alias ex:"sales_summary" for further use of dataframe if needed
    let result_three = df_sales.raw_sql(sql_three, "sales_summary", &[&df_customers, &df_products]).await?;
    result_three.display().await?;

```
# JSON files
### Currently supported files can include: Arrays, Objects. Best usage if you can make it flat ("key":"value") 
#### for JSON, all field types are infered to VARCHAR/TEXT/STRING
```rust
// example json structure
{
"name": "Adeel Solangi",
"language": "Sindhi",
"id": "V59OF92YF627HFY0",
"bio": "Donec lobortis eleifend condimentum. Cras dictum dolor lacinia lectus vehicula rutrum.",
"version": 6.1
}

let json_path = "C:\\Borivoj\\RUST\\Elusion\\test.json";
let json_df = CustomDataFrame::new(json_path, "test").await;

// example json structure
{
"someGUID": "e0bsg4d-d81c-4db6-8ad8-bc92cbcfsds06",
"someGUID2": "58asd1f6-c7ca-4c51-8ca0-37678csgd9c7",
"someName": "Some Name Here",
"someVersion": "Version 0232",
"emptyValue": null,
"verInd": {
    "$numberLong": "0"
    },
"elInd": {
    "$numberLong": "1"
    },
"qId": "question1",
"opId": {
    "$numberLong": "0"
    },
"label": "Some Label Here",
"labelValue": "45557",
"someGUID3": "5854ff6-c7ca-4c51-8ca0-3767sds4319c7|qId|7"
}

let json_path = "C:\\Borivoj\\RUST\\Elusion\\test2.json";
let json_df = CustomDataFrame::new(json_path, "test2").await;
```
#### Then you can do business as usual, either with DataFrame API or SQL API

```rust
    let json_sql = "
        SELECT * FROM test LIMIT 10
    ";
    // "labels" is set as new alias for result_json dataframe
    let result_json = json_df.raw_sql(json_sql, "labels", &[]).await?;
    result_json.display().await?;
```
# WRITERS

## Writing to Parquet File
#### We have 2 writing modes: Overwrite and Append
```rust
// overwrite existing file
result_df
    .write_to_parquet(
        "overwrite",
        "C:\\Path\\To\\Your\\test.parquet",
        None // I've set WriteOptions to default for writing Parquet files, so keep it None
    )
    .await
    .expect("Failed to write to Parquet");

//append to exisiting file
result_df
    .write_to_parquet(
        "append",
        "C:\\Path\\To\\Your\\test.parquet",
        None // I've set WriteOptions to default for writing Parquet files, so keep it None
    ) 
    .await
    .expect("Failed to append to Parquet");
```
## Writing to CSV File
#### CSV Writing options are mandatory
##### has_headers: TRUE is dynamically set for Overwrite mode, and FALSE for Append mode.
```rust
let custom_csv_options = CsvWriteOptions {
        delimiter: b',',
        escape: b'\\',
        quote: b'"',
        double_quote: false,
        null_value: "NULL".to_string(),
    };
```
#### We have 2 writing modes: Overwrite and Append
```rust
// overwrite existing file
result_df
    .write_to_csv(
        "overwrite", 
        "C:\\Borivoj\\RUST\\Elusion\\agg_sales.csv", 
        custom_csv_options
    )
    .await
    .expect("Failed to overwrite CSV file");

//append to exisiting file
result_df
    .write_to_csv(
        "append", 
        "C:\\Borivoj\\RUST\\Elusion\\agg_sales.csv", 
        custom_csv_options
    )
    .await
    .expect("Failed to append to CSV file");
```

---
### Current Clause functions (some still under development)

```rust
load(...)
select(...)
group_by(...)
order_by(...)
limit(...)
filter(...)
having(...)
join(...)
window(...)
aggregation(...)
from_subquery(...)
with_cte(...)
union(...)
intersect(...)
except(...)
display(...)
display_query(...)
display_query_plan(...)
```
### Current Aggregation functions (soon to be more)

```rust
sum(mut self)
avg(mut self)
min(mut self)
max(mut self)
stddev(mut self)
count(mut self)
count_distinct(mut self)
corr(mut self, other_column: &str)
grouping(mut self)
var_pop(mut self)
stddev_pop(mut self)
array_agg(mut self)
approx_percentile(mut self, percentile: f64)
first_value(mut self) 
nth_value(mut self, n: i64)

```

### License
Elusion is distributed under the [MIT License](https://opensource.org/licenses/MIT). 
However, since it builds upon [DataFusion](https://datafusion.apache.org/), which is distributed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0), some parts of this project are subject to the terms of the Apache License 2.0.
For full details, see the [LICENSE.txt file](LICENSE.txt).

### Acknowledgments
This library leverages the power of Rust's type system and libraries like [DataFusion](https://datafusion.apache.org/)
, Arrow for efficient query processing. Special thanks to the open-source community for making this project possible.


## 🚧 Disclaimer: Under Development 🚧

This crate is currently **under active development and testing**. It is not considered stable or ready for production use.

We are actively working to improve the features, performance, and reliability of this library. Breaking changes might occur between versions as we continue to refine the API and functionality.

If you want to contribute or experiment with the crate, feel free to do so, but please be aware of the current limitations and evolving nature of the project.

Thank you for your understanding and support!


## Where you can find me:

LindkedIn - [LinkedIn](https://www.linkedin.com/in/borivojgrujicic/ )
YouTube channel - [YouTube](https://www.youtube.com/@RustyBiz)
Udemy Instructor - [Udemy](https://www.udemy.com/user/borivoj-grujicic/)
