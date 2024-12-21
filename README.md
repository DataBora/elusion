# Elusion 🦀 DataFrame Library

Elusion is a high-performance, flexible library built on top of DataFusion SQL query engine, for managing and querying data using a DataFrame-like interface. Designed for developers who need a powerful abstraction over data transformations, Elusion simplifies complex operations such as filtering, joining, aggregating, and more with an intuitive, chainable API.

# Motivation

I believe that DataFusion has great potential in Data Engineering / Data Analytics world, but I think that design choices for SQL and DataFrame API do not resemble popular DataFrame soultions out there, and I am here to narrow this gap, by creating easily chainable constructs for anybody to use and uderstand.

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
- Construct readable and reusable SQL-like queries.
- Support for Common Table Expressions (CTEs), subqueries, and set operations (`UNION`, `INTERSECT`, `EXCEPT`).

### 🛠️ Easy-to-Use API
- Chainable and intuitive API for building queries.
- Readable debug output of generated SQL for verification.

- **Data Preview**: Preview your data easily by displaying a subset of rows in the terminal.
- **Composable Queries**: Chain transformations seamlessly to build reusable and testable workflows.

---

## Installation

To add **Elusion** to your Rust project, include the following line in your `Cargo.toml` under `[dependencies]`:

```toml
elusion = "0.1.1"
```

---

## Dependencies that you need in Cargo.toml to use Elusion:

```toml
[dependencies]
elusion = "0.1.1"
tokio = { version = "1.42.0", features = ["rt-multi-thread"] }

```

---
## Usage examples:

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
    let sales_columns = vec![
        ("OrderDate", "DATE", false),
        ("StockDate", "DATE", false),
        ("OrderNumber", "VARCHAR", false),
    ];

    let sales_data = "path\\to\\sales_data.csv";
    let df_sales = CustomDataFrame::new(sales_data, sales_columns, "sales").await?;

    let result = df_sales
        .select(vec!["OrderDate", "OrderNumber"])
        .limit(10);

    result.display().await?;

    Ok(())
}
```

### Schema establishing
#### **Column Name**, **SQL DataType** and If is **Null**-able (true, false) needs to be provided

```rust
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

```rust
let sales_data = "C:\\Path\\To\\Your\\FIle.csv";
let customers_data = "C:\\Path\\To\\Your\\FIle.csv";
```
### Creating Custom data frame 
#### 3 arguments needed:  **Path**, **Schema**, **Table Alias**

```rust
let df_sales = CustomDataFrame::new(sales_data, sales_columns, "sales").await; 
let df_customers = CustomDataFrame::new(customers_data, customers_columns, "customers").await;
```
### JOIN
```rust
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
        
    join_df.display_query(); // if you want to see generated sql query
    join_df.display().await?;
```

### SELECT without Aggregation
```rust
let result_sales = sales_order_data.clone()
    .select(vec!["customer_name", "order_date", "billable_value"])
    .filter("billable_value > 100.0")
    .order_by(vec!["order_date"], vec![true])
    .limit(10);

    result_sales.display_query(); // if you want to see generated sql query
    result_sales.display().await?;
```

### SELECT with Aggregation
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

    result_df.display_query(); // if you want to see generated sql query
    result_df.display().await?;
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

### Writing to Parquet File
#### We have 2 writing modes: Overwrite and Append
```rust
// overwrite existing file
result_df
    .write_to_parquet("overwrite","C:\\Path\\To\\Your\\test.parquet",None)
    .await
    .expect("Failed to write to Parquet");

//append to exisiting file
result_df
    .write_to_parquet("append","C:\\Path\\To\\Your\\test.parquet",None)
    .await
    .expect("Failed to append to Parquet");
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
