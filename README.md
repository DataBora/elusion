# Elusion 🦀 DataFrame Library for Everybody!

![Elusion Logo](images/elusion.png)

Elusion is a high-performance DataFrame library, for in-memory data formats (CSV, JSON, PARQUET, DELTA). Built on top of DataFusion SQL query engine, for managing and querying data using a DataFrame-like interface. Designed for developers who need a powerful abstraction over data transformations, Elusion simplifies complex operations such as filtering, joining, aggregating, and more with an intuitive, chainable API.

# Motivation

DataFusion SQL engine has great potential in Data Engineering / Data Analytics world, but I believe that design choices for SQL and DataFrame API do not resemble popular DataFrame solutions out there, and I am here to narrow this gap, and creating easily chainable constructs for anybody to use and understand. 

## Key Features

### 🚀 High-Performance DataFrame Operations
- Load and process data from CSV, PARQUET, JSON, DELTA table files with ease.
- Perform SQL-like transformations such as `SELECT`, `WHERE`, `GROUP BY`, and `JOIN`.

### 📊 Aggregations and Analytics
- Built-in support for functions like `SUM`, `AVG`, `MIN`, `MAX`, `COUNT`, and more.
- Advanced statistical functions like `CORR`, `STDDEV`, `VAR_POP`, `ApproxPercentile` and more.

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
elusion = "0.5.0"
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
use elusion::prelude::*; // Import everything needed

#[tokio::main]
async fn main() -> ElusionResult<()> {

    Ok(())
}
```
## Schema 
#### SCHEMA IS AUTOMATICALLY INFERED since v0.2.5

### LOADING Files into CustomDataFrame
#### File extensions are automatically recognized 
#### All you have to do is to provide path to your file
### Currently supported data files: CSV, PARQUET, JSON, DELTA
```rust
let sales_data = "C:\\Borivoj\\RUST\\Elusion\\sales_data.csv";
let parq_path = "C:\\Borivoj\\RUST\\Elusion\\prod_data.parquet";
let json_path = "C:\\Borivoj\\RUST\\Elusion\\db_data.json";
let delta_path = "C:\\Borivoj\\RUST\\Elusion\\agg_sales"; //you just specify folder name without extension
```
### Creating CustomDataFrame
#### 2 arguments needed:  **Path**, **Table Alias**

```rust
let df_sales = CustomDataFrame::new(sales_data, "sales").await?; 
let df_customers = CustomDataFrame::new(customers_data, "customers").await?;
```
## RULE of thumb: 
#### ALL Column names and Dataframe alias names, will be LOWERCASE(), TRIM(), REPLACE(" ", "_"), regardles of how you write it, or how they are writen in CSV file.
### Aggregation column Aliases will be LOWERCASE() regardles of how you write it.

### ALIAS column names in SELECT() function (AS is case insensitive)
```rust
let customers_alias = df_customers
    .select(["CustomerKey AS customerkey_alias", "FirstName as first_name", "LastName", "EmailAddress"]);
```
### JOIN
#### currently implemented join types
```rust
"INNER", "LEFT", "RIGHT", "FULL", 
"LEFT SEMI", "RIGHT SEMI", 
"LEFT ANTI", "RIGHT ANTI", "LEFT MARK" 
```
#### JOIN example with 2 dataframes
```rust
let single_join = df_sales
    .join(df_customers, "s.CustomerKey = c.CustomerKey", "INNER")
    .select(["s.OrderDate","c.FirstName", "c.LastName"])
    .agg([
        "SUM(s.OrderQuantity) AS total_quantity",
        "AVG(s.OrderQuantity) AS avg_quantity",
    ])
    .group_by(["s.OrderDate","c.FirstName","c.LastName"])
    .having("SUM(s.OrderQuantity) > 10") 
    .order_by(["total_quantity"], [false]) // true is ascending, false is descending
    .limit(10);

    let join_df1 = single_join.elusion("result_query").await?;
    join_df1.display().await?;
```
### JOIN with 3 dataframes, AGGREGATION, GROUP BY, HAVING, SELECT, ORDER BY
```rust
let many_joins = df_sales
    .join_many([
        (df_customers, "s.CustomerKey = c.CustomerKey", "INNER"),
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
    .having_many([
        ("SUM(s.OrderQuantity) > 10"), 
        ("AVG(s.OrderQuantity) < 100")]) 
    .order_by_many([
        ("total_quantity", true), 
        ("p.ProductName", false) // true is ascending, false is descending
    ])
    .limit(10); 

let join_df3 = many_joins.elusion("df_joins").await?;
join_df3.display().await?;
```
### FILTERING
```rust
let filter_df = sales_order_df
    .select(["customer_name", "order_date", "billable_value"])
    .filter("order_date > '2021-07-04'") //you can use filter_many() as well
    .order_by(["order_date"], [true])
    .limit(10);

let filtered = filter_df.elusion("result_sales").await?;
filtered.display().await?;
```
### WINDOW function
```rust
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
let json_df = CustomDataFrame::new(json_path, "test").await?;

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
let json_df = CustomDataFrame::new(json_path, "test2").await?;
```
# WRITERS

## Writing to Parquet File
#### We have 2 writing modes: **Overwrite** and **Append**
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

// append to exisiting file
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

#### CSV Writing options are **mandatory**
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

// append to exisiting file
result_df
    .write_to_csv(
        "append", 
        "C:\\Borivoj\\RUST\\Elusion\\agg_sales.csv", 
        custom_csv_options
    )
    .await
    .expect("Failed to append to CSV file");
```
## Writing to DELTA table / lake 
#### We can write to delta in 2 modes **Overwrite** and **Append**
#### Partitioning column is optional
#### DISCLAIMER: if you decide to use column for partitioning, make sure that you don't need that column as you wont be able to read it back to dataframe
#### DISCLAIMER 2: once you decide to use partitioning column for writing your delta table, if you want to APPEND to it, Append also need to have same column for partitioning
```rust
// Overwrite
result_df
    .write_to_delta_table(
        "overwrite",
        "C:\\Borivoj\\RUST\\Elusion\\agg_sales", 
        Some(vec!["order_date".into()]), // optional partitioning, you can use None
    )
    .await
    .expect("Failed to overwrite Delta table");
// Append
result_df
    .write_to_delta_table(
        "append",
        "C:\\Borivoj\\RUST\\Elusion\\agg_sales",
        Some(vec!["order_date".into()]),// optional partitioning, you can use None
    )
    .await
    .expect("Failed to append to Delta table");
```
---
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
