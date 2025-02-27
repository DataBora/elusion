# Elusion 🦀 DataFrame / Data Engineering / Data Analysis Library for Everybody!

![Elusion Logo](images/elusion.png)


Elusion is a high-performance DataFrame / Data Engineering / Data Analysis library designed for in-memory data formats such as CSV, JSON, PARQUET, DELTA, as well as for ODBC Database Connections for MySQL and PostgreSQL, as well as for Azure Blob Storage Connections, as well as for creating JSON files from REST API's which can be forwarded to DataFrame.

All of the DataFrame operations, Reading and Writing can be placed in PipelineScheduler for automated Data Engineering Pipelines.

DataFrame operations are built atop the DataFusion SQL query engine, Database operations are built atop Arrow ODBC, Azure BLOB HTTPS operations are built atop Azure Storage with BLOB and DFS (Data Lake Storage Gen2) endpoints available, Pipeline Scheduling is built atop Tokio Cron Scheduler, REST API is build atop Reqwest. Report Creation is built atop Plotly and AG GRID. (scroll down for examples)

Tailored for Data Engineers and Data Analysts seeking a powerful abstraction over data transformations. Elusion streamlines complex operations like filtering, joining, aggregating, and more with its intuitive, chainable DataFrame API, and provides a robust interface for managing and querying data efficiently. It also has Integrated Plotting and Interactive Dashboard features.

## Core Philosophy
Elusion wants you to be you!

Elusion offers flexibility in constructing queries without enforcing specific patterns or chaining orders, unlike SQL, PySpark, Polars, or Pandas. You can build your queries in any sequence that best fits your logic, writing functions in a manner that makes sense to you. Regardless of the order of function calls, Elusion ensures consistent results.

## Platform Compatibility
Tested for MacOS, Linux and Windows
![Platform comp](images/platformcom.png)

## Security
Codebase has Undergone Rigorous Auditing and Security Testing, ensuring that it is fully prepared for Production.

## Key Features

### 🔄 Job Scheduling (PipelineScheduler)
Flexible Intervals: From 1 minute to 30 days scheduling intervals.
Graceful Shutdown: Built-in Ctrl+C signal handling for clean termination.
Async Support: Built on tokio for non-blocking operations.

### 🌐 External Data Sources Integration
- Azure Blob Storage: Direct integration with Azure Blob Storage for Reading and Writing data files.
- Database Connectors: ODBC support for seamless data access from MySQL and PostgreSQL databases.
- REST API's: Create JSON files from REST API endpoints with Customizable Headers, Params, Date Ranges, Pagination...

### 🚀 High-Performance DataFrame Operations
Seamless Data Loading: Easily load and process data from CSV, PARQUET, JSON, and DELTA table files.
SQL-Like Transformations: Execute transformations such as SELECT, AGG, STRING FUNCTIONS, JOIN, FILTER, HAVING, GROUP BY, ORDER BY, DATETIME and WINDOW with ease.

### 📉 Aggregations and Analytics
Comprehensive Aggregations: Utilize built-in functions like SUM, AVG, MEAN, MEDIAN, MIN, COUNT, MAX, and more.
Advanced Scalar Math: Perform calculations using functions such as ABS, FLOOR, CEIL, SQRT, ISNAN, ISZERO, PI, POWER, and others.

### 🔗 Flexible Joins
Diverse Join Types: Perform joins using INNER, LEFT, RIGHT, FULL, and other join types.
Intuitive Syntax: Easily specify join conditions and aliases for clarity and simplicity.

### 🪟 Window Functions
Analytical Capabilities: Implement window functions like RANK, DENSE_RANK, ROW_NUMBER, and custom partition-based calculations to perform advanced analytics.

### 🔄 Pivot and Unpivot Functions
Data Reshaping: Transform your data structure using PIVOT and UNPIVOT functions to suit your analytical needs.

### 📊 Create REPORTS
Create HTML files with Interactive Dashboards with multiple interactive Plots and Tables.
Plots Available: TimeSeries, Bar, Pie, Donut, Histogram, Scatter, Box...
Tables can Paginate pages, Filter, Resize, Reorder columns...
Export Tables data to EXCEL and CSV

### 🧹 Clean Query Construction
Readable Queries: Construct SQL queries that are both readable and reusable.
Advanced Query Support: Utilize Common Table Expressions (CTEs), subqueries, and set operations such as APPEND, UNION, UNION ALL, INTERSECT, and EXCEPT. For multiple Dataframea operations: APPEND_MANY, UNION_MANY, UNION_ALL_MANY.

### 🛠️ Easy-to-Use API
Chainable Interface: Build queries using a chainable and intuitive API for streamlined development.
Debugging Support: Access readable debug outputs of the generated SQL for easy verification and troubleshooting.
**Data Preview**: Quickly preview your data by displaying a subset of rows in the terminal.
**Composable Queries**: Seamlessly chain transformations to create reusable and testable workflows.

---
## Installation

To add **Elusion** to your Rust project, include the following lines in your `Cargo.toml` under `[dependencies]`:

```toml
elusion = "3.3.1"
tokio = { version = "1.42.0", features = ["rt-multi-thread"] }
```
## Rust version needed
```toml
>= 1.81
```
---
## ODBC Support
Elusion now provides ODBC functionality behind an optional feature flag to keep the core library lightweight and provide flexibility for users.
### Enabling ODBC Support

To use ODBC-related features, you need to:

1. Add the ODBC feature when specifying the dependency:
```toml
[dependencies]
elusion = { version = "3.3.1", features = ["odbc"] }
```
2. Make sure to install ODBC Driver(unixodbc) on Ubuntu and macOS
Ubuntu/Debian: 
```toml
sudo apt-get install unixodbc-dev
```
macOS: 
```toml
brew install unixodbc
```
#### When building your project, use the ODBC feature:
```rust
cargo build --features odbc
```
```rust
cargo run --features odbc  
```
---
## NORMALIZATION
#### DataFrame (your files) Column Names will be normalized to LOWERCASE(), TRIM() and REPLACE(" ","_")
#### All DataFrame query expresions, functions, aliases and column names will be normalized to LOWERCASE(), TRIM() and REPLACE(" ","_")
---
## Schema 
#### SCHEMA IS DYNAMICALLY INFERED
---
# Usage examples:

### MAIN function 

```rust
// Import everything needed
use elusion::prelude::*; 

#[tokio::main]
async fn main() -> ElusionResult<()> {

    Ok(())
}

```
---
## LOADING
### - Loading data into CustomDataFrame can be from:
#### - In-Memory data formats: CSV, JSON, PARQUET, DELTA 
#### - Azure Blob Storage endpoints (BLOB, DFS)
#### - ODBC Connectors (databases)

#### -> NEXT is example for reading data from local files, 
#### down bellow are examples for Azure Blob Storage, ODBC
---
### LOADING data from Files into CustomDataFrame (in-memory data formats)
#### - File extensions are automatically recognized 
#### - All you have to do is to provide path to your file
```rust
let csv_data = "C:\\Borivoj\\RUST\\Elusion\\sales_data.csv";
let parquet_path = "C:\\Borivoj\\RUST\\Elusion\\prod_data.parquet";
let json_path = "C:\\Borivoj\\RUST\\Elusion\\db_data.json";
let delta_path = "C:\\Borivoj\\RUST\\Elusion\\agg_sales"; // for DELTA you just specify folder name without extension
```
### Creating CustomDataFrame
#### 2 arguments needed:  **Path**, **Table Alias**

```rust
let df_sales = CustomDataFrame::new(csv_data, "sales").await?; 
let df_customers = CustomDataFrame::new(parquet_path, "customers").await?;
```
### LOADING data from Databases into CustomDataFrame (scroll down for full example)
```rust
let pg_df = CustomDataFrame::from_db(pg_connection, sql_query).await?;
```
### LOADING data from Azure BLOB Storage into CustomDataFrame (scroll down for full example)
```rust
let df = CustomDataFrame::from_azure_with_sas_token(
        blob_url, 
        sas_token, 
        Some("folder-name/file-name"), // FILTERING is optional. Can be None if you want to take everything from Container
        "data" // alias for registering table
    ).await?;
```
---
## SELECT
### ALIAS column names in SELECT() function (AS is case insensitive)
```rust
let df_AS = select_df
    .select(["CustomerKey AS customerkey_alias", "FirstName as first_name", "LastName", "EmailAddress"]);

let df_select_all = select_df.select(["*"]);

let df_count_all = select_df.select(["COUNT(*)"]);

let df_distinct = select_df.select(["DISTINCT(column_name) as distinct_values"]);
```
---
## Where to use which Functions:
### Scalar and Operators -> in SELECT() function
### Aggregation Functions -> in AGG() function
### String Column Functions -> in STRING_FUNCTIONS() function
### DateTime Functions -> in DATETIME_FUNCTIONS() function
---
### Numerical Operators (supported +, -, * , / , %)
```rust
let num_ops_sales = sales_order_df
    .select([
        "customer_name",
        "order_date",
        "billable_value",
        "billable_value * 2 AS double_billable_value",  // Multiplication
        "billable_value / 100 AS percentage_billable"  // Division
    ])
    .filter("billable_value > 100.0")
    .order_by(["order_date"], [true])
    .limit(10);

let num_ops_res = num_ops_sales.elusion("scalar_df").await?;
num_ops_res.display().await?;
```
### FILTER  (used before aggregations)
```rust
let filter_df = sales_order_df
    .select(["customer_name", "order_date", "billable_value"])
    .filter_many([("order_date > '2021-07-04'"), ("billable_value > 100.0")])
    .order_by(["order_date"], [true])
    .limit(10);

let filtered = filter_df.elusion("result_sales").await?;
filtered.display().await?;

// exmple 2
const FILTER_CUSTOMER: &str = "customer_name == 'Customer IRRVL'";

let filter_query = sales_order_df
    .select([
        "customer_name",
        "order_date",
        "ABS(billable_value) AS abs_billable_value",
        "ROUND(SQRT(billable_value), 2) AS SQRT_billable_value",
        "billable_value * 2 AS double_billable_value",  // Multiplication
        "billable_value / 100 AS percentage_billable"  // Division
    ])
    .agg([
        "ROUND(AVG(ABS(billable_value)), 2) AS avg_abs_billable",
        "SUM(billable_value) AS total_billable",
        "MAX(ABS(billable_value)) AS max_abs_billable",
        "SUM(billable_value) * 2 AS double_total_billable",      // Operator-based aggregation
        "SUM(billable_value) / 100 AS percentage_total_billable" // Operator-based aggregation
    ])
    .filter(FILTER_CUSTOMER)
    .group_by_all()
    .order_by_many([
        ("total_billable", false),  // Order by total_billable descending
        ("max_abs_billable", true), // Then by max_abs_billable ascending
    ])
```
### HAVING (used after aggregations)
```rust
//Example 1 with aggregatied column names
 let example1 = sales_df
    .join_many([
        (customers_df, ["s.CustomerKey = c.CustomerKey"], "INNER"),
        (products_df, ["s.ProductKey = p.ProductKey"], "INNER"),
    ])
    .select(["c.CustomerKey", "c.FirstName", "c.LastName", "p.ProductName"])
    .agg([
        "SUM(s.OrderQuantity) AS total_quantity",
        "AVG(s.OrderQuantity) AS avg_quantity"
    ])
    .group_by(["c.CustomerKey", "c.FirstName", "c.LastName", "p.ProductName"])
    .having_many([
        ("total_quantity > 10"),
        ("avg_quantity < 100")
    ])
    .order_by_many([
        ("total_quantity", true ),
        ("p.ProductName", false)
    ]);

let result = example1.elusion("sales_res").await?;
result.display().await?;

//Example 2 with aggregation in having
let df_having= sales_df
    .join(customers_df, ["s.CustomerKey = c.CustomerKey"], 
        "INNER"
    )
    .select(["c.CustomerKey", "c.FirstName", "c.LastName"])
    .agg([
        "SUM(s.OrderQuantity) AS total_quantity",
        "AVG(s.OrderQuantity) AS avg_quantity"
    ])
    .group_by(["c.CustomerKey", "c.FirstName", "c.LastName"])
    .having_many([
        ("SUM(s.OrderQuantity) > 10"),
        ("AVG(s.OrderQuantity) < 100")
    ])
    .order_by(["total_quantity"], [true])
    .limit(5);

let result = df_having.elusion("sales_res").await?;
result.display().await?;
```
### SCALAR functions
```rust
let scalar_df = sales_order_df
    .select([
        "customer_name", 
        "order_date", 
        "ABS(billable_value) AS abs_billable_value",
        "ROUND(SQRT(billable_value), 2) AS SQRT_billable_value"])
    .filter("billable_value > 100.0")
    .order_by(["order_date"], [true])
    .limit(10);

let scalar_res = scalar_df.elusion("scalar_df").await?;
scalar_res.display().await?;
```
### AGGREGATE functions with nested Scalar functions 
```rust
let scalar_df = sales_order_df
    .select([
        "customer_name", 
        "order_date"
    ])
    .agg([
        "ROUND(AVG(ABS(billable_value)), 2) AS avg_abs_billable",
        "SUM(billable_value) AS total_billable",
        "MAX(ABS(billable_value)) AS max_abs_billable",
        "SUM(billable_value) * 2 AS double_total_billable",      // Operator-based aggregation
        "SUM(billable_value) / 100 AS percentage_total_billable" // Operator-based aggregation
    ])
    .group_by(["customer_name", "order_date"])
    .filter("billable_value > 100.0")
    .order_by(["order_date"], [true])
    .limit(10);

let scalar_res = scalar_df.elusion("scalar_df").await?;
scalar_res.display().await?;
```
### STRING functions
```rust
let df = sales_df
    .select(["FirstName", "LastName"])
    .string_functions([
        "'New' AS new_old_customer",
        "TRIM(c.EmailAddress) AS trimmed_email",
        "CONCAT(TRIM(c.FirstName), ' ', TRIM(c.LastName)) AS full_name",
    ]);

let result_df = df.elusion("df").await?;
result_df.display().await?;
```
### Numerical Operators, Scalar Functions, Aggregated Functions...
```rust
let mix_query = sales_order_df
    .select([
        "customer_name",
        "order_date",
        "ABS(billable_value) AS abs_billable_value",
        "ROUND(SQRT(billable_value), 2) AS SQRT_billable_value",
        "billable_value * 2 AS double_billable_value",  // Multiplication
        "billable_value / 100 AS percentage_billable"  // Division
    ])
    .agg([
        "ROUND(AVG(ABS(billable_value)), 2) AS avg_abs_billable",
        "SUM(billable_value) AS total_billable",
        "MAX(ABS(billable_value)) AS max_abs_billable",
        "SUM(billable_value) * 2 AS double_total_billable",      // Operator-based aggregation
        "SUM(billable_value) / 100 AS percentage_total_billable" // Operator-based aggregation
    ])
    .filter("billable_value > 50.0")
    .group_by_all()
    .order_by_many([
        ("total_billable", false),  // Order by total_billable descending
        ("max_abs_billable", true), // Then by max_abs_billable ascending
    ]);

let mix_res = mix_query.elusion("scalar_df").await?;
mix_res.display().await?;
```
---
### Supported Aggregation functions
```rust
SUM, AVG, MEAN, MEDIAN, MIN, COUNT, MAX,  
LAST_VALUE, FIRST_VALUE,  
GROUPING, STRING_AGG, ARRAY_AGG, VAR, VAR_POP,  
VAR_POPULATION, VAR_SAMP, VAR_SAMPLE,  
BIT_AND, BIT_OR, BIT_XOR, BOOL_AND, BOOL_OR 
```
### Supported Scalar Math Functions
```rust
ABS, FLOOR, CEIL, SQRT, ISNAN, ISZERO,  
PI, POW, POWER, RADIANS, RANDOM, ROUND,  
FACTORIAL, ACOS, ACOSH, ASIN, ASINH,  
COS, COSH, COT, DEGREES, EXP,  
SIN, SINH, TAN, TANH, TRUNC, CBRT,  
ATAN, ATAN2, ATANH, GCD, LCM, LN,  
LOG, LOG10, LOG2, NANVL, SIGNUM
```
---
## JOIN
#### JOIN examples with single condition and 2 dataframes, AGGREGATION, GROUP BY
```rust
let single_join = df_sales
    .join(df_customers, ["s.CustomerKey = c.CustomerKey"], "INNER")
    .select(["s.OrderDate","c.FirstName", "c.LastName"])
    .agg([
        "SUM(s.OrderQuantity) AS total_quantity",
        "AVG(s.OrderQuantity) AS avg_quantity",
    ])
    .group_by(["s.OrderDate","c.FirstName","c.LastName"])
    .having("total_quantity > 10") 
    .order_by(["total_quantity"], [false]) // true is ascending, false is descending
    .limit(10);

let join_df1 = single_join.elusion("result_query").await?;
join_df1.display().await?;
```
### JOIN with single conditions and 3 dataframes, AGGREGATION, GROUP BY, HAVING, SELECT, ORDER BY
```rust
let many_joins = df_sales
    .join_many([
        (df_customers, ["s.CustomerKey = c.CustomerKey"], "INNER"),
        (df_products, ["s.ProductKey = p.ProductKey"], "INNER"),
    ]) 
    .select([
        "c.CustomerKey","c.FirstName","c.LastName","p.ProductName",
    ]) 
    .agg([
        "SUM(s.OrderQuantity) AS total_quantity",
        "AVG(s.OrderQuantity) AS avg_quantity",
    ]) 
    .group_by(["c.CustomerKey", "c.FirstName", "c.LastName", "p.ProductName"]) 
    .having_many([("total_quantity > 10"), ("avg_quantity < 100")]) 
    .order_by_many([
        ("total_quantity", true), // true is ascending 
        ("p.ProductName", false)  // false is descending
    ])
    .limit(10); 

let join_df3 = many_joins.elusion("df_joins").await?;
join_df3.display().await?;
```
### JOIN with multiple conditions and 2 data frames
```rust
let result_join = orders_df
    .join(
        customers_df,
        ["o.CustomerID = c.CustomerID" , "o.RegionID = c.RegionID"],
        "INNER"
    )
    .select([
        "o.OrderID",
        "c.Name",
        "o.OrderDate"
    ])
    .string_functions([
        "CONCAT(TRIM(c.Name), ' (', c.Email, ')') AS customer_info",
        "UPPER(c.Status) AS customer_status",
        "LEFT(c.Email, POSITION('@' IN c.Email) - 1) AS username"
    ])
    .agg([
        "SUM(o.Amount) AS total_amount",
        "AVG(o.Quantity) AS avg_quantity",
        "COUNT(DISTINCT o.OrderID) AS order_count",
        "MAX(o.Amount) AS max_amount",
        "MIN(o.Amount) AS min_amount"
    ])
    .group_by([
        "o.OrderID",
        "c.Name",
        "o.OrderDate",
        "c.Email",   
        "c.Status"
    ]);

let res_joins = result_join.elusion("one_join").await?;
res_joins.display().await?;
```
### JOIN_MANY with multiple conditions and 3 data frames
```rust
let result_join_many = order_join_df
    .join_many([
        (customer_join_df,
            ["o.CustomerID = c.CustomerID" , "o.RegionID = c.RegionID"],
            "INNER"
        ),
        (regions_join_df,
            ["c.RegionID = r.RegionID" , "r.IsActive = true"],
            "INNER"
        )
    ])
    .select(["o.OrderID","c.Name","r.RegionName", "r.CountryID"])
    .string_functions([
    "CONCAT(r.RegionName, ' (', r.CountryID, ')') AS region_info",
 
    "CASE c.CreditLimit 
        WHEN 1000 THEN 'Basic'
        WHEN 2000 THEN 'Premium'
        ELSE 'Standard'
    END AS credit_tier",

    "CASE 
        WHEN c.CreditLimit > 2000 THEN 'High'
        WHEN c.CreditLimit > 1000 THEN 'Medium'
        ELSE 'Low'
    END AS credit_status",

    "CASE
        WHEN o.Amount > 1000 AND c.Status = 'active' THEN 'Priority'
        WHEN o.Amount > 500 THEN 'Regular'
        ELSE 'Standard'
    END AS order_priority",

    "CASE r.RegionName
        WHEN 'East Coast' THEN 'Eastern'
        WHEN 'West Coast' THEN 'Western'
        ELSE 'Other'
    END AS region_category",

    "CASE
        WHEN EXTRACT(DOW FROM o.OrderDate) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END AS order_day_type"
    ])
    .agg([
        "SUM(o.Amount) AS total_amount",                                  
        "COUNT(*) AS row_count",                                       
        "SUM(o.Amount * (1 - o.Discount/100)) AS net_amount",          
        "ROUND(SUM(o.Amount) / COUNT(*), 2) AS avg_order_value",       
        "SUM(o.Amount * r.TaxRate) AS total_tax"                      
    ])
    .group_by_all()
    .having("total_amount > 200")
    .order_by(["total_amount"], [false]); 

let res_joins_many = result_join_many.elusion("many_join").await?;
res_joins_many.display().await?;
```
### JOIN_MANY with single condition and 3 dataframes, STRING FUNCTIONS, AGGREGATION, GROUP BY, HAVING_MANY, ORDER BY
```rust

let str_func_joins = df_sales
    .join_many([
        (df_customers, ["s.CustomerKey = c.CustomerKey"], "INNER"),
        (df_products, ["s.ProductKey = p.ProductKey"], "INNER"),
    ]) 
    .select([
        "c.CustomerKey",
        "c.FirstName",
        "c.LastName",
        "c.EmailAddress",
        "p.ProductName",
    ])
    .string_functions([
        "TRIM(c.EmailAddress) AS trimmed_email_address",
        "CONCAT(TRIM(c.FirstName), ' ', TRIM(c.LastName)) AS full_name",
        "LEFT(p.ProductName, 15) AS short_product_name",
        "RIGHT(p.ProductName, 5) AS end_product_name",
    ])
    .agg([
        "COUNT(p.ProductKey) AS product_count",
        "SUM(s.OrderQuantity) AS total_order_quantity",
    ])
    .group_by_all()
    .having_many([("total_order_quantity > 10"),  ("product_count >= 1")])  
    .order_by_many([
        ("total_order_quantity", true), 
        ("p.ProductName", false) 
    ]); 

let join_str_df3 = str_func_joins.elusion("df_joins").await?;
join_str_df3.display().await?;
```
#### Currently implemented join types
```rust
"INNER", "LEFT", "RIGHT", "FULL", 
"LEFT SEMI", "RIGHT SEMI", 
"LEFT ANTI", "RIGHT ANTI", "LEFT MARK" 
```
---
### STRING FUNCTIONS
```rust
let string_functions_df = df_sales
    .join_many([
        (df_customers, ["s.CustomerKey = c.CustomerKey"], "INNER"),
        (df_products, ["s.ProductKey = p.ProductKey"], "INNER"),
    ]) 
    .select([
        "c.CustomerKey",
        "c.FirstName",
        "c.LastName",
        "c.EmailAddress",
        "p.ProductName"
    ])
    .string_functions([
    // Basic String Functions
    "TRIM(c.EmailAddress) AS trimmed_email",
    "LTRIM(c.EmailAddress) AS left_trimmed_email",
    "RTRIM(c.EmailAddress) AS right_trimmed_email",
    "UPPER(c.FirstName) AS upper_first_name",
    "LOWER(c.LastName) AS lower_last_name",
    "LENGTH(c.EmailAddress) AS email_length",
    "LEFT(p.ProductName, 10) AS product_start",
    "RIGHT(p.ProductName, 10) AS product_end",
    "SUBSTRING(p.ProductName, 1, 5) AS product_substr",
    // Concatenation
    "CONCAT(c.FirstName, ' ', c.LastName) AS full_name",
    "CONCAT_WS(' ', c.FirstName, c.LastName, c.EmailAddress) AS all_info",
    // Position and Search
    "POSITION('@' IN c.EmailAddress) AS at_symbol_pos",
    "STRPOS(c.EmailAddress, '@') AS email_at_pos",
    // Replacement and Modification
    "REPLACE(c.EmailAddress, '@adventure-works.com', '@newdomain.com') AS new_email",
    "TRANSLATE(c.FirstName, 'AEIOU', '12345') AS vowels_replaced",
    "REPEAT('*', 5) AS stars",
    "REVERSE(c.FirstName) AS reversed_name",
    // Padding
    "LPAD(c.CustomerKey::TEXT, 10, '0') AS padded_customer_id",
    "RPAD(c.FirstName, 20, '.') AS padded_name",
    // Case Formatting
    "INITCAP(LOWER(c.FirstName)) AS proper_case_name",
    // String Extraction
    "SPLIT_PART(c.EmailAddress, '@', 1) AS email_username",
    // Type Conversion
    "TO_CHAR(s.OrderDate, 'YYYY-MM-DD') AS formatted_date"
    ])
    .agg([
        "COUNT(*) AS total_records",
        "STRING_AGG(p.ProductName, ', ') AS all_products"
    ])
    .filter("c.EmailAddress IS NOT NULL")
    .group_by_all()
    .having("COUNT(*) > 1")
    .order_by(["c.CustomerKey"], [true]);   

let str_df = string_functions_df.elusion("df_joins").await?;
str_df.display().await?;    
```
#### Currently Available String functions
```rust
1.Basic String Functions:
TRIM() - Remove leading/trailing spaces
LTRIM() - Remove leading spaces
RTRIM() - Remove trailing spaces
UPPER() - Convert to uppercase
LOWER() - Convert to lowercase
LENGTH() or LEN() - Get string length
LEFT() - Extract leftmost characters
RIGHT() - Extract rightmost characters
SUBSTRING() - Extract part of string
2. String concatenation:
CONCAT() - Concatenate strings
CONCAT_WS() - Concatenate with separator
3. String Position and Search:
POSITION() - Find position of substring
STRPOS() - Find position of substring
INSTR() - Find position of substring
LOCATE() - Find position of substring
4. String Replacement and Modification:
REPLACE() - Replace all occurrences of substring
TRANSLATE() - Replace characters
OVERLAY() - Replace portion of string
REPEAT() - Repeat string
REVERSE() - Reverse string characters
5. String Pattern Matching:
LIKE() - Pattern matching with wildcards
REGEXP() or RLIKE() - Pattern matching with regular expressions
6. String Padding:
LPAD() - Pad string on left
RPAD() - Pad string on right
SPACE() - Generate spaces
7. String Case Formatting:
INITCAP() - Capitalize first letter of each word
8. String Extraction:
SPLIT_PART() - Split string and get nth part
SUBSTR() - Get substring
9. String Type Conversion:
TO_CHAR() - Convert to string
CAST() - Type conversion
CONVERT() - Type conversion
10. Control Flow:
CASE()
```
---
### DATETIME FUNCTIONS
#### Work best with YYYY-MM-DD format
```rust
let dt_query = sales_order_df
    .select([
        "customer_name",
        "order_date",
        "delivery_date"
    ])
    .datetime_functions([
    // Current date/time comparisons
    "CURRENT_DATE() AS today",
    "CURRENT_TIME() AS current_time",
    "CURRENT_TIMESTAMP() AS now",
    "NOW() AS now_timestamp",
    "TODAY() AS today_timestamp",
    
    // Date binning (for time-series analysis)
    "DATE_BIN('1 week', order_date, MAKE_DATE(2020, 1, 1)) AS weekly_bin",
    "DATE_BIN('1 month', order_date, MAKE_DATE(2020, 1, 1)) AS monthly_bin",
    
    // Date formatting
    "DATE_FORMAT(order_date, '%Y-%m-%d') AS formatted_date",
    "DATE_FORMAT(order_date, '%Y/%m/%d') AS formatted_date_alt",
    
    // Basic date components
    "DATE_PART('year', order_date) AS year",
    "DATE_PART('month', order_date) AS month",
    "DATE_PART('day', order_date) AS day",

    // Quarters and weeks
    "DATE_PART('quarter', order_date) AS order_quarter",
    "DATE_PART('week', order_date) AS order_week",

    // Day of week/year
    "DATE_PART('dow', order_date) AS day_of_week",
    "DATE_PART('doy', order_date) AS day_of_year",

    // Analysis
    "DATE_PART('day', delivery_date - order_date) AS delivery_days",
    "DATE_PART('day', CURRENT_DATE() - order_date) AS days_since_order",
    
    // Date truncation (alternative syntax)
    "DATE_TRUNC('week', order_date) AS week_start",
    "DATE_TRUNC('quarter', order_date) AS quarter_start",
    "DATE_TRUNC('month', order_date) AS month_start",
    "DATE_TRUNC('year', order_date) AS year_start",
    
    // Complex date calculations
    "CASE 
        WHEN DATE_PART('month', order_date) <= 3 THEN 'Q1'
        WHEN DATE_PART('month', order_date) <= 6 THEN 'Q2'
        WHEN DATE_PART('month', order_date) <= 9 THEN 'Q3'
        ELSE 'Q4'
        END AS fiscal_quarter",
    ])
    .order_by(["order_date"], [false])

let dt_res = dt_query.elusion("datetime_df").await?;
dt_res.display().await?;
```
#### Currently Available DateTime Functions
```rust
CURRENT_DATE()
CURRENT_TIME(),
CURRENT_TIMESTAMP()
NOW(),
TODAY(),
DATE_PART()
DATE_TRUNC()
DATE_BIN()
MAKE_DATE()
DATE_FORMAT()
```
---
### WINDOW functions
#### Aggregate, Ranking and Analytical functions
```rust
let window_query = df_sales
    .join(df_customers, ["s.CustomerKey = c.CustomerKey"], "INNER")
    .select(["s.OrderDate","c.FirstName","c.LastName","s.OrderQuantity"])
    //aggregated window functions
    .window("SUM(s.OrderQuantity) OVER (PARTITION BY c.CustomerKey ORDER BY s.OrderDate) as running_total")
    .window("AVG(s.OrderQuantity) OVER (PARTITION BY c.CustomerKey ORDER BY s.OrderDate) AS running_avg")
    .window("MIN(s.OrderQuantity) OVER (PARTITION BY c.CustomerKey ORDER BY s.OrderDate) AS running_min")
    .window("MAX(s.OrderQuantity) OVER (PARTITION BY c.CustomerKey ORDER BY s.OrderDate) AS running_max")
    .window("COUNT(s.OrderQuantity) OVER (PARTITION BY c.CustomerKey ORDER BY s.OrderDate) AS running_count")
    //ranking window functions
    .window("ROW_NUMBER() OVER (PARTITION BY c.CustomerKey ORDER BY s.OrderDate) as row_num")
    .window("DENSE_RANK() OVER (PARTITION BY c.CustomerKey ORDER BY s.OrderDate) AS dense_rnk")
    .window("PERCENT_RANK() OVER (PARTITION BY c.CustomerKey ORDER BY s.OrderDate) AS pct_rank")
    .window("CUME_DIST() OVER (PARTITION BY c.CustomerKey ORDER BY s.OrderDate) AS cume_dist")
    .window("NTILE(4) OVER (PARTITION BY c.CustomerKey ORDER BY s.OrderDate) AS quartile")
    // analytical window functions
    .window("FIRST_VALUE(s.OrderQuantity) OVER (PARTITION BY c.CustomerKey ORDER BY s.OrderDate) AS first_qty")
    .window("LAST_VALUE(s.OrderQuantity) OVER (PARTITION BY c.CustomerKey ORDER BY s.OrderDate) AS last_qty")
    .window("LAG(s.OrderQuantity, 1, 0) OVER (PARTITION BY c.CustomerKey ORDER BY s.OrderDate) AS prev_qty")
    .window("LEAD(s.OrderQuantity, 1, 0) OVER (PARTITION BY c.CustomerKey ORDER BY s.OrderDate) AS next_qty")
    .window("NTH_VALUE(s.OrderQuantity, 3) OVER (PARTITION BY c.CustomerKey ORDER BY s.OrderDate) AS third_qty");

let window_df = window_query.elusion("result_window").await?;
window_df.display().await?;
```
#### Rolling Window Functions
```rust
let rollin_query = df_sales
    .join(df_customers, ["s.CustomerKey = c.CustomerKey"], "INNER")
    .select(["s.OrderDate", "c.FirstName", "c.LastName", "s.OrderQuantity"])
        //aggregated rolling windows
    .window("SUM(s.OrderQuantity) OVER (PARTITION BY c.CustomerKey ORDER BY s.OrderDate
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total")
    .window("AVG(s.OrderQuantity) OVER (PARTITION BY c.CustomerKey ORDER BY s.OrderDate
             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS full_partition_avg");

let rollin_df = rollin_query.elusion("rollin_result").await?;
rollin_df.display().await?;
```
---
## APPEND, APPEND_MANY
#### APPEND: Combines rows from two dataframes, keeping all rows
#### APPEND_MANY: Combines rows from many dataframes, keeping all rows
```rust
let df1 = "C:\\Borivoj\\RUST\\Elusion\\API\\df1.json";
let df2 = "C:\\Borivoj\\RUST\\Elusion\\API\\df2.json";
let df3 = "C:\\Borivoj\\RUST\\Elusion\\API\\df3.json";
let df4 = "C:\\Borivoj\\RUST\\Elusion\\API\\df4.json";
let df5 = "C:\\Borivoj\\RUST\\Elusion\\API\\df5.json";

let df1 = CustomDataFrame::new(df1, "msales1").await?; 
let df2 = CustomDataFrame::new(df2, "msales2").await?; 
let df3 = CustomDataFrame::new(df3, "msales3").await?; 
let df4 = CustomDataFrame::new(df4, "msales4").await?; 
let df5 = CustomDataFrame::new(df5, "msales5").await?; 

let res_df1 = df1.select(["Month", "TotalSales"]).string_functions(["'site1' AS Restaurant"]);
let result_df1 = res_df1.elusion("el1").await?;

let res_df2 = df2.select(["Month", "TotalSales"]).string_functions(["'site2' AS Restaurant"]);
let result_df2 = res_df2.elusion("el2").await?;

let res_df3 = df3.select(["Month", "TotalSales"]).string_functions(["'site3' AS Restaurant"]);
let result_df3 = res_df3.elusion("el3").await?;

let res_df4 = df4.select(["Month", "TotalSales"]).string_functions(["'site4' AS Restaurant"]);
let result_df4 = res_df4.elusion("el4").await?;

let res_df5 = df5.select(["Month", "TotalSales"]).string_functions(["'site5' AS Restaurant"]);
let resuld_df5 = res_df5.elusion("el5").await?;

//APPEND
let append_df = result_df1.append(result_df2).await?;
//APPEND_MANY
let append_many_df = result_df1.append_many([result_df2, result_df3, result_df4, resuld_df5]).await?;
```
---
## UNION, UNION ALL, EXCEPT, INTERSECT
#### UNION: Combines rows from both, removing duplicates
#### UNION ALL: Combines rows from both, keeping duplicates
#### EXCEPT: Difference of two sets (only rows in left minus those in right).
#### INTERSECT: Intersection of two sets (only rows in both).
```rust
//UNION
let df1 = sales_df.clone()
.join(
    customers_df.clone(), ["s.CustomerKey = c.CustomerKey"], "INNER",
)
.select(["c.FirstName", "c.LastName"])
.string_functions([
    "TRIM(c.EmailAddress) AS trimmed_email",
    "CONCAT(TRIM(c.FirstName), ' ', TRIM(c.LastName)) AS full_name",
]);

let df2 = sales_df.clone()
.join(
    customers_df.clone(), ["s.CustomerKey = c.CustomerKey"], "INNER",
)
.select(["c.FirstName", "c.LastName"])
.string_functions([
    "TRIM(c.EmailAddress) AS trimmed_email",
    "CONCAT(TRIM(c.FirstName), ' ', TRIM(c.LastName)) AS full_name",
]);

let result_df1 = df1.elusion("df1").await?;
let result_df2 = df2.elusion("df2").await?;

let union_df = result_df1.union(result_df2).await?;

let union_df_final = union_df.limit(100).elusion("union_df").await?;
union_df_final.display().await?;

//UNION ALL
let union_all_df = result_df1.union_all(result_df2).await?;
//EXCEPT
let except_df = result_df1.except(result_df2).await?;
//INTERSECT
let intersect_df = result_df1.intersect(result_df2).await?;

```
## UNION_MANY, UNION_ALL_MANY
#### UNION_MANY: Combines rows from many dataframes, removing duplicates
#### UNION_ALL_MANY: Combines rows from many dataframes, keeping duplicates
```rust
let df1 = "C:\\Borivoj\\RUST\\Elusion\\API\\df1.json";
let df2 = "C:\\Borivoj\\RUST\\Elusion\\API\\df2.json";
let df3 = "C:\\Borivoj\\RUST\\Elusion\\API\\df3.json";
let df4 = "C:\\Borivoj\\RUST\\Elusion\\API\\df4.json";
let df5 = "C:\\Borivoj\\RUST\\Elusion\\API\\df5.json";

let df1 = CustomDataFrame::new(df1, "msales").await?; 
let df2 = CustomDataFrame::new(df2, "msales").await?; 
let df3 = CustomDataFrame::new(df3, "msales").await?; 
let df4 = CustomDataFrame::new(df4, "msales").await?; 
let df5 = CustomDataFrame::new(df5, "msales").await?; 

let res_df1 = df1.select(["Month", "TotalSales"]).string_functions(["'df1' AS Sitename"]);
let result_df1 = res_df1.elusion("el1").await?;

let res_df2 = df2.select(["Month", "TotalSales"]).string_functions(["'df2' AS Sitename"]);
let result_df2 = res_df2.elusion("el2").await?;

let res_df3 = df3.select(["Month", "TotalSales"]).string_functions(["'df3' AS Sitename"]);
let result_df3 = res_df3.elusion("el3").await?;

let res_df4 = df4.select(["Month", "TotalSales"]).string_functions(["'df4' AS Sitename"]);
let result_df4 = res_df4.elusion("el4").await?;

let res_df5 = df5.select(["Month", "TotalSales"]).string_functions(["'df5' AS Sitename"]);
let resuld_df5 = res_df5.elusion("el5").await?;

//UNION_MANY
let union_all_df = result_df1.union_many([result_df2, result_df3, result_df4, resuld_df5]).await?;
//UNION_ALL_MANY
let union_all_many_df = result_df1.union_all_many([result_df2, result_df3, result_df4, resuld_df5]).await?;
```
---
## PIVOT and UNPIVOT
#### Pivot and Unpivot functions are ASYNC function
#### They should be used separately from other functions: 1. directly on initial CustomDataFrame, 2. after .elusion() evaluation.
#### Future needs to be in final state so .await? must be used
```rust
// PIVOT
// directly on initial CustomDataFrame
let sales_p = "C:\\Borivoj\\RUST\\Elusion\\SalesData2022.csv";
let df_sales = CustomDataFrame::new(sales_p, "s").await?;

let pivoted = df_sales
    .pivot(
        ["StockDate"],     // Row identifiers
        "TerritoryKey",    // Column to pivot
        "OrderQuantity",   // Value to aggregate
        "SUM"              // Aggregation function
    ).await?;

let result_pivot = pivoted.elusion("pivoted_df").await?;
result_pivot.display().await?;

// after .elusion() evaluation
let sales_path = "C:\\Borivoj\\RUST\\Elusion\\sales_order_report.csv";
let sales_order_df = CustomDataFrame::new(sales_path, "sales").await?;

let scalar_df = sales_order_df
    .select([
        "customer_name", 
        "order_date", 
        "ABS(billable_value) AS abs_billable_value",
        "ROUND(SQRT(billable_value), 2) AS SQRT_billable_value"])
    .filter("billable_value > 100.0")
    .order_by(["order_date"], [true])
    .limit(10);
// elusion evaluation
let scalar_res = scalar_df.elusion("scalar_df").await?;

let pivoted_scalar = scalar_res
    .pivot(
        ["customer_name"],          // Row identifiers
        "order_date",               // Column to pivot
        "abs_billable_value",       // Value to aggregate
        "SUM"                       // Aggregation function
    ).await?;

let pitvoted_scalar = pivoted_scalar.elusion("pivoted_df").await?;
pitvoted_scalar.display().await?;

// UNPIVOT
let unpivoted = result_pivot
    .unpivot(
        ["StockDate"],                         // ID columns
        ["TerritoryKey_1", "TerritoryKey_2"],  // Value columns to unpivot
        "Territory",                           // New name column
        "Quantity"                             // New value column
    ).await?;

let result_unpivot = unpivoted.elusion("unpivoted_df").await?;
result_unpivot.display().await?;

// example 2
let unpivot_scalar = scalar_res
    .unpivot(
        ["customer_name", "order_date"],      // Keep these as identifiers
        ["abs_billable_value", "sqrt_billable_value"], // Columns to unpivot
        "measure_name",                       // Name for the measure column
        "measure_value"                       // Name for the value column
    ).await?;

let result_unpivot_scalar = unpivot_scalar.elusion("unpivoted_df2").await?;
result_unpivot_scalar.display().await?;
```
---
## Statistical Functions
#### These Functions can give you quick statistical overview of your DataFrame columns and correlations
#### Currently available: display_stats(), display_null_analysis(), display_correlation_matrix()
```rust
df.display_stats(&[
    "abs_billable_value",
    "sqrt_billable_value",
    "double_billable_value",
    "percentage_billable"
]).await?;

=== Column Statistics ===
--------------------------------------------------------------------------------
Column: abs_billable_value
------------------------------------------------------------------------------
| Metric               |           Value |             Min |             Max |
------------------------------------------------------------------------------
| Records              |              10 | -               | -               |
| Non-null Records     |              10 | -               | -               |
| Mean                 |         1025.71 | -               | -               |
| Standard Dev         |          761.34 | -               | -               |
| Value Range          |               - | 67.4            | 2505.23         |
------------------------------------------------------------------------------

Column: sqrt_billable_value
------------------------------------------------------------------------------
| Metric               |           Value |             Min |             Max |
------------------------------------------------------------------------------
| Records              |              10 | -               | -               |
| Non-null Records     |              10 | -               | -               |
| Mean                 |           29.48 | -               | -               |
| Standard Dev         |           13.20 | -               | -               |
| Value Range          |               - | 8.21            | 50.05           |
------------------------------------------------------------------------------
    
// Display null analysis
// Keep None if you want all columns to be analized
df.display_null_analysis(None).await?;

----------------------------------------------------------------------------------------
| Column                         |      Total Rows |      Null Count | Null Percentage |
----------------------------------------------------------------------------------------
| total_billable                 |              10 |               0 |           0.00% |
| order_count                    |              10 |               0 |           0.00% |
| customer_name                  |              10 |               0 |           0.00% |
| order_date                     |              10 |               0 |           0.00% |
| abs_billable_value             |              10 |               0 |           0.00% |
----------------------------------------------------------------------------------------

// Display correlation matrix
df.display_correlation_matrix(&[
    "abs_billable_value",
    "sqrt_billable_value",
    "double_billable_value",
    "percentage_billable"
]).await?;

=== Correlation Matrix ===
-------------------------------------------------------------------------------------------
|                 | abs_billable_va | sqrt_billable_v | double_billable | percentage_bill |
-------------------------------------------------------------------------------------------
| abs_billable_va |            1.00 |            0.98 |            1.00 |            1.00 |
| sqrt_billable_v |            0.98 |            1.00 |            0.98 |            0.98 |
| double_billable |            1.00 |            0.98 |            1.00 |            1.00 |
| percentage_bill |            1.00 |            0.98 |            1.00 |            1.00 |
-------------------------------------------------------------------------------------------
```
---
# DATABASE Connectors 
### ODBC connectors available for MySQL and PostgreSQL
#### Requirements: You need to install Driver for you database ODBC connector
##### For ODBC connectivity on Ubuntu and macOS you need to install unixodbc:
##### Ubuntu/Debian: sudo apt-get install unixodbc-dev
##### macOS: brew install unixodbc
##### Windows: ODBC drivers are typically included with the OS

#### Don't forget that you can always load tables from Database into DataFrames and work with DataFrame API, but for better performance you should aggregate data in SQL server than push it into dataframe. 

### MySQL example
```rust
let connection_string = "
    Driver={MySQL ODBC 9.1 Unicode Driver};\ 
    Server=127.0.0.1;\
    Port=3306;\
    Database=your_database_name;\
    User=your_user_name;\
    Password=your_password";
    
let sql_query = "
    SELECT 
        b.beer_style,
        b.location,
        c.color,
        AVG(b.fermentation_time) AS avg_fermentation_time,
        ROUND(AVG(b.temperature), 2) AS avg_temperature,
        ROUND(AVG(b.quality_score), 2) AS avg_quality,
        ROUND(AVG(b.brewhouse_efficiency), 2) AS avg_efficiency,
        SUM(b.volume_produced) AS total_volume,
        ROUND(AVG(b.loss_during_brewing), 2) AS avg_brewing_loss,
        ROUND(AVG(b.loss_during_fermentation), 2) AS avg_fermentation_loss,
        ROUND(SUM(b.total_sales), 2) AS total_sales,
        ROUND(AVG(b.brewhouse_efficiency - (b.loss_during_brewing + b.loss_during_fermentation)), 2) AS net_efficiency
    FROM brewery_data b
    JOIN colors c ON b.color = c.color_number
    WHERE volume_produced > 1000
    GROUP BY b.beer_style, b.location, c.color
    HAVING avg_quality > 8
    ORDER BY total_sales DESC, avg_quality DESC
    LIMIT 20
";

let mysql_df = CustomDataFrame::from_db(
    connection_string,
    sql_query
).await?;

let analysis_df = mysql_df.elusion("brewing_analysis").await?;
analysis_df.display().await?;
```
### PostgreSQL example
```rust
let pg_connection = "\
        Driver={PostgreSQL UNICODE};\
        Servername=127.0.0.1;\
        Port=5433;\
        Database=your_database_name;\
        UID=your_user_name;\
        PWD=your_password;\
    ";

let sql_query = "
    SELECT 
        c.name,
        c.email,
        SUM(s.quantity * s.price) as total_sales,
        COUNT(*) as number_of_purchases
    FROM sales s
    JOIN customers c ON s.customer_id = c.id
    GROUP BY c.id, c.name, c.email
    ORDER BY total_sales DESC
";

let pg_df = CustomDataFrame::from_db(pg_connection, sql_query).await?;

let pg_res = pg_df.elusion("pg_res").await?;
pg_res.display().await?;
```
---
# AZURE Blob Storage Connector 
## Storage connector available with BLOB and DFS url endpoints, along with SAS token provided
### Currently supported file types .JSON and .CSV
#### DFS endpoint is “Data Lake Storage Gen2” and behave more like a real file system. This makes reading operations more efficient—especially at large scale.

### BLOB endpoint example
```rust
let blob_url= "https://your_storage_account_name.blob.core.windows.net/your-container-name";
let sas_token = "your_sas_token";

let df = CustomDataFrame::from_azure_with_sas_token(
        blob_url, 
        sas_token, 
        Some("folder-name/file-name"), // FILTERING is optional. Can be None if you want to take everything from Container
        "data" // alias for registering table
    ).await?;

let data_df = df.select(["*"]);

let test_data = data_df.elusion("data_df").await?;
test_data.display().await?;
```
### DFS endpoint example

```rust
let dfs_url= "https://your_storage_account_name.dfs.core.windows.net/your-container-name";
let sas_token = "your_sas_token";

let df = CustomDataFrame::from_azure_with_sas_token(
        dfs_url, 
        sas_token, 
        Some("folder-name/file-name"), // FILTERING is optional. Can be None if you want to take everything from Container
        "data" // alias for registering table
    ).await?;

let data_df = df.select(["*"]);

let test_data = data_df.elusion("data_df").await?;
test_data.display().await?;
```
---
# Pipeline Scheduler
### Time is set according to UTC

#### Currently available job frequencies
```rust
"1min","2min","5min","10min","15min","30min" ,
"1h","2h","3h","4h","5h","6h","7h","8h","9h","10h","11h","12h","24h" 
"2days","3days","4days","5days","6days","7days","14days","30days" 
```
### PipelineScheduler Example (parsing data from Azure BLOB Stoarge, DataFrame operation and Writing to Parquet)
```rust
use elusion::prelude::*;

#[tokio::main]
async fn main() -> ElusionResult<()>{
    
// Create Pipeline Scheduler 
let scheduler = PipelineScheduler::new("5min", || async {

let dfs_url= "https://your_storage_account_name.dfs.core.windows.net/your-container-name";
let sas_token = "your_sas_token";
// Read from Azure
let header_df = CustomDataFrame::from_azure_with_sas_token(
    dfs_url,
    dfs_sas_token,
    Some("folder_name/"), // Optional: FILTERING can filter any part of string: file path, file name...
    "head"
).await?;

// DataFrame operation
let headers_payments = header_df
   .select(["Brand", "Id", "Name", "Item", "Bill", "Tax",
           "ServCharge", "Percentage", "Discount", "Date"])
   .agg([
       "SUM(Bill) AS total_bill",
       "SUM(Tax) AS total_tax", 
       "SUM(ServCharge) AS total_service",
       "AVG(Percentage) AS avg_percentage",
       "COUNT(*) AS transaction_count",
       "SUM(ServCharge) / SUM(Bill) * 100 AS service_ratio"
   ])
   .group_by(["Brand", "Date"])
   .filter("Bill > 0")
   .order_by(["total_bill"], [true])

let headers_data = headers_payments.elusion("headers_df").await?;

// Write output
headers_data
    .write_to_parquet(
        "overwrite",
        "C:\\Borivoj\\RUST\\Elusion\\Scheduler\\sales_data.parquet",
        None
    )
    .await?;
    
    Ok(())

}).await?;

scheduler.shutdown().await?;

Ok(())
}

```
---
# JSON files
### Currently supported files can include: Fileds, Arrays, Objects. 
#### Best performance with flat json ("key":"value") 
#### for JSON, all field types are infered to VARCHAR/TEXT/STRING
```rust
// example json structure with key:value pairs
{
"name": "Adeel Solangi",
"language": "Sindhi",
"id": "V59OF92YF627HFY0",
"bio": "Donec lobortis eleifend condimentum. Cras dictum dolor lacinia lectus vehicula rutrum.",
"version": 6.1
}

let json_path = "C:\\Borivoj\\RUST\\Elusion\\test.json";
let json_df = CustomDataFrame::new(json_path, "test").await?;

let df = json_df.select(["*"]).limit(10);

let result = df.elusion("df").await?;
result.display().await?;

// example json structure with Fields and Arrays
[
  {
    "id": "1",
    "name": "Form 1",
    "fields": [
      {"key": "first_name", "type": "text", "required": true},
      {"key": "age", "type": "number", "required": false},
      {"key": "email", "type": "email", "required": true}
    ]
  },
  {
    "id": "2",
    "name": "Form 2",
    "fields": [
      {"key": "address", "type": "text", "required": false},
      {"key": "phone", "type": "tel", "required": true}
    ]
  },
  {
    "id": "3",
    "name": "Form 3",
    "fields": [
      {"key": "notes", "type": "textarea", "required": false},
      {"key": "date", "type": "date", "required": true},
      {"key": "status", "type": "select", "required": true}
    ]
  }
]

let json_path = "C:\\Borivoj\\RUST\\Elusion\\test2.json";
let json_df = CustomDataFrame::new(json_path, "test2").await?;
```
---
# REST API
### Creating JSON files from REST API's
#### Customizable Headers, Params, Pagination, Date Ranges...
### FROM API
```rust
// example 1
let posts_df = ElusionApi::new();
posts_df
    .from_api(
        "https://jsonplaceholder.typicode.com/posts", // url
        "C:\\Borivoj\\RUST\\Elusion\\JSON\\posts_data.json" // path where json will be stored
    ).await?;

// example 2
let users_df = ElusionApi::new();
users_df.from_api(
    "https://jsonplaceholder.typicode.com/users",
    "C:\\Borivoj\\RUST\\Elusion\\JSON\\users_data.json",
).await?;

// example 3
let ceo = ElusionApi::new();
ceo.from_api(
    "https://dog.ceo/api/breeds/image/random/3",
    "C:\\Borivoj\\RUST\\Elusion\\JSON\\ceo_data.json"
).await?;
```
### FROM API WITH HEADERS
```rust
// example 1
let mut headers = HashMap::new();
headers.insert("Custom-Header".to_string(), "test-value".to_string());

let bin_df = ElusionApi::new();
bin_df.from_api_with_headers(
    "https://httpbin.org/headers",  // url
    headers,                        // headers
    "C:\\Borivoj\\RUST\\Elusion\\JSON\\bin_data.json",  // path where json will be stored
).await?;
    
// example 2
let mut headers = HashMap::new();
headers.insert("Accept".to_string(), "application/vnd.github.v3+json".to_string());
headers.insert("User-Agent".to_string(), "elusion-dataframe-test".to_string());

let git_hub = ElusionApi::new();
git_hub.from_api_with_headers(
    "https://api.github.com/search/repositories?q=rust+language:rust&sort=stars&order=desc",
    headers,
    "C:\\Borivoj\\RUST\\Elusion\\JSON\\git_hub_data.json"
).await?;

// example 3
let mut headers = HashMap::new();
headers.insert("Accept".to_string(), "application/json".to_string());
headers.insert("X-Version".to_string(), "1".to_string());

let pokemon_df = ElusionApi::new();
pokemon_df.from_api_with_headers(
    "https://pokeapi.co/api/v2/pokemon", 
    headers,
    "C:\\Borivoj\\RUST\\Elusion\\JSON\\pokemon_data.json"
).await?;
```
### FROM API WITH PARAMS
```rust
// Using OpenLibrary API with params
let mut params = HashMap::new();
params.insert("q", "rust programming");
params.insert("limit", "10");

let open_lib = ElusionApi::new();
open_lib.from_api_with_params(
    "https://openlibrary.org/search.json",           // url
    params,                                          // params
    "C:\\Borivoj\\RUST\\Elusion\\JSON\\open_lib_data.json",  // path where json will be stored
).await?;

// Random User Generator API with params
let mut params = HashMap::new();
params.insert("results", "10");
params.insert("nat", "us,gb");

let generator = ElusionApi::new(); 
generator.from_api_with_params(
    "https://randomuser.me/api",
    params,
    "C:\\Borivoj\\RUST\\Elusion\\JSON\\generator_data.json"
).await?;

// JSON Placeholder with multiple endpoints
let mut params = HashMap::new();
params.insert("userId", "1");
params.insert("_limit", "5");

let multi = ElusionApi::new(); 
multi.from_api_with_params(
    "https://jsonplaceholder.typicode.com/posts",
    params,
    "C:\\Borivoj\\RUST\\Elusion\\JSON\\multi_data.json"
).await?;

// NASA Astronomy Picture of the Day
let mut params = HashMap::new();
params.insert("count", "5");
params.insert("thumbs", "true");

let nasa = ElusionApi::new(); 
nasa.from_api_with_params(
    "https://api.nasa.gov/planetary/apod",
    params,
    "C:\\Borivoj\\RUST\\Elusion\\JSON\\nasa_pics_data.json"
).await?;

// example 5
let mut params = HashMap::new();
params.insert("brand", "elusion");
params.insert("password", "some_password");
params.insert("siteid", "993");
params.insert("Datefrom", "01 jan 2025 06:00");
params.insert("Dateto", "31 jan 2025 06:00");
params.insert("user", "borivoj");

let api = ElusionApi::new();
api.from_api_with_params(
    "https://salesapi.net.co.rs/SSPAPI/api/data",
    params,
    "C:\\Borivoj\\RUST\\Elusion\\JSON\\sales_jan_2025.json"
).await?;
```
### FROM API WITH PARAMS AND HEADERS
```rust
let mut params = HashMap::new();
params.insert("since", "2024-01-01T00:00:00Z");
params.insert("until", "2024-01-07T23:59:59Z");

let mut headers = HashMap::new();
headers.insert("Accept".to_string(), "application/vnd.github.v3+json".to_string());
headers.insert("User-Agent".to_string(), "elusion-dataframe-test".to_string());

let commits_df = ElusionApi::new();
commits_df.from_api_with_params_and_headers(
    "https://api.github.com/repos/rust-lang/rust/commits",    // url
    params,                                                   // params
    headers,                                                 // headers
    "C:\\Borivoj\\RUST\\Elusion\\JSON\\commits_data.json",  // path where json will be stored
).await?;
```
### FROM API WITH DATES
```rust
// example 1
let post_df = ElusionApi::new();
post_df.from_api_with_dates(
    "https://jsonplaceholder.typicode.com/posts",            // url
    "2024-01-01",                                           // date from
    "2024-01-07",                                           // date to
    "C:\\Borivoj\\RUST\\Elusion\\JSON\\post_data.json",  // path where json will be stored
).await?;

// Example 2: COVID-19 historical data
let covid_df = ElusionApi::new();
covid_df.from_api_with_dates(
    "https://disease.sh/v3/covid-19/historical/all",
    "2024-01-01",
    "2024-01-07",
    "C:\\Borivoj\\RUST\\Elusion\\JSON\\covid_data.json"
).await?;
```
### FROM API WITH PAGINATION
```rust
// example 1
let reqres = ElusionApi::new();
reqres.from_api_with_pagination(
    "https://reqres.in/api/users",
    1,      // page
    10,      // per_page
    "C:\\Borivoj\\RUST\\Elusion\\JSON\\reqres_data.json"
).await?;
```
### FROM API WITH SORT
```rust
let movie_db = ElusionApi::new();
movie_db.from_api_with_sort(
    "https://api.themoviedb.org/3/discover/movie", // base url
    "popularity",   // sort field
    "desc",         // order
    "C:\\Borivoj\\RUST\\Elusion\\JSON\\popular_movies.json"
).await?;
```
### FROM API WITH HEADERS AND SORT
```rust
let mut headers = HashMap::new();
headers.insert("Authorization".to_string(), "Bearer YOUR_TMDB_API_KEY".to_string());
headers.insert("accept".to_string(), "application/json".to_string());

let movie_db = ElusionApi::new();
movie_db.from_api_with_headers_and_sort(
    "https://api.themoviedb.org/3/discover/movie",  // base url
    headers,                                        // headers
    "popularity",                                   // sort field
    "desc",                                        // order
    "C:\\Borivoj\\RUST\\Elusion\\JSON\\popular_movies1.json"
).await?;
```
---
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
    .await?;

// append to exisiting file
result_df
    .write_to_parquet(
        "append",
        "C:\\Path\\To\\Your\\test.parquet",
        None // I've set WriteOptions to default for writing Parquet files, so keep it None
    ) 
    .await?;
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
    .await?;

// append to exisiting file
result_df
    .write_to_csv(
        "append", 
        "C:\\Borivoj\\RUST\\Elusion\\agg_sales.csv", 
        custom_csv_options
    )
    .await?;

```
## Writing to DELTA table / lake 
#### We can write to delta in 2 modes **Overwrite** and **Append**
#### Partitioning column is OPTIONAL and if you decide to use column for partitioning, make sure that you don't need that column as you won't be able to read it back to dataframe
#### Once you decide to use partitioning column for writing your delta table, if you want to APPEND to it, append also need to have same column for partitioning
```rust
// Overwrite
result_df
    .write_to_delta_table(
        "overwrite",
        "C:\\Borivoj\\RUST\\Elusion\\agg_sales", 
        Some(vec!["order_date".into()]), 
    )
    .await
    .expect("Failed to overwrite Delta table");
// Append
result_df
    .write_to_delta_table(
        "append",
        "C:\\Borivoj\\RUST\\Elusion\\agg_sales",
        Some(vec!["order_date".into()]),
    )
    .await
    .expect("Failed to append to Delta table");
```
## Writing Parquet to Azure BLOB Storage 
#### We have 2 writing options "overwrite" and "append"
#### Writing is set to Default, Compression: SNAPPY and Parquet 2.0
#### Threshold file size is 1GB
```rust
let df = CustomDataFrame::new(csv_data, "sales").await?; 

let query = df.select(["*"]);

let data = query.elusion("df_sales").await?;

let url_to_folder = "https://your_storage_account_name.dfs.core.windows.net/your-container-name/folder/sales.parquet";
let sas_write_token = "your_sas_token"; // make sure SAS token has writing permissions

data.write_parquet_to_azure_with_sas(
    "overwrite",
    url_to_folder,
    sas_write_token
).await?;

// append version
data.write_parquet_to_azure_with_sas(
    "append",
    url_to_folder,
    sas_write_token
).await?;
```
---
# REPORTING
### CREATING REPORT with Interactive Plots/Visuals and Tables
### Export Table data to EXCEL and CSV
#### Currently available Interactive Plots: TimeSeries, Box, Bar, Histogram, Pie, Donut, Scatter...
#### Interactive Tables can: Paginate pages, Filter, Reorder, Resize columns...
```rust
let ord = "C:\\Borivoj\\RUST\\Elusion\\sales_order_report.csv";
let sales_order_df = CustomDataFrame::new(ord, "ord").await?;

let mix_query = sales_order_df.clone()
.select([
    "customer_name",
    "order_date",
    "ABS(billable_value) AS abs_billable_value",
    "ROUND(SQRT(billable_value), 2) AS SQRT_billable_value",
    "billable_value * 2 AS double_billable_value",  // Multiplication
    "billable_value / 100 AS percentage_billable"  // Division
])
.agg([
    "ROUND(AVG(ABS(billable_value)), 2) AS avg_abs_billable",
    "SUM(billable_value) AS total_billable",
    "MAX(ABS(billable_value)) AS max_abs_billable",
    "SUM(billable_value) * 2 AS double_total_billable",      // Operator-based aggregation
    "SUM(billable_value) / 100 AS percentage_total_billable" // Operator-based aggregation
])
.filter("billable_value > 50.0")
.group_by_all()
.order_by_many([
    ("total_billable", false),  // Order by total_billable descending
    ("max_abs_billable", true), // Then by max_abs_billable ascending
]);

let mix_res = mix_query.elusion("scalar_df").await?;

//INTERACTIVE PLOTS
// Line plot showing sales over time
let line = mix_res.plot_line(
    "order_date", // - x_col: column name for x-axis (can be date or numeric)
    "double_billable_value", // - y_col: column name for y-axis
    true,  // - show_markers: true to show points, false for line only
    Some("Sales over time") // - title: optional custom title (can be None)
).await?;

// Bar plot showing aggregated values
let bars = mix_res
   .plot_bar(
       "customer_name",         // X-axis: Customer names
       "total_billable",        // Y-axis: Total billable amount
       Some("Customer Total Sales") // Title of the plot
   ).await?;

// Time series showing sales trend
let time_series = mix_res
   .plot_time_series(
       "order_date",           // X-axis: Date column (must be Date32 type)
       "total_billable",       // Y-axis: Total billable amount
       true,                   // Show markers on the line
       Some("Sales Trend Over Time") // Title of the plot
   ).await?;

// Histogram showing distribution of abs billable values
let histogram = mix_res
   .plot_histogram(
       "abs_billable_value",   // Data column for distribution analysis
       Some("Distribution of Sale Values") // Title of the plot
   ).await?;

// Box plot showing abs billable value distribution
let box_plot = mix_res
   .plot_box(
       "abs_billable_value",   // Value column for box plot
       Some("customer_name"),   // Optional grouping column
       Some("Sales Distribution by Customer") // Title of the plot
   ).await?;

// Scatter plot showing relationship between original and doubled values
let scatter = mix_res
   .plot_scatter(
       "abs_billable_value",   // X-axis: Original values
       "double_billable_value", // Y-axis: Doubled values
       Some(8)                 // Optional marker size
   ).await?;

// Pie chart showing sales distribution
let pie = mix_res
   .plot_pie(
       "customer_name",        // Labels for pie segments
       "total_billable",       // Values for pie segments
       Some("Sales Share by Customer") // Title of the plot
   ).await?;

// Donut chart alternative view
let donut = mix_res
   .plot_donut(
       "customer_name",        // Labels for donut segments
       "percentage_total_billable", // Values as percentages
       Some("Percentage Distribution") // Title of the plot
   ).await?;

 // Create Tables to add to report
let summary_table = mix_res.clone() //Clone for multiple usages
    .select([
        "customer_name",
        "total_billable",
        "avg_abs_billable",
        "max_abs_billable",
        "percentage_total_billable"
    ])
    .order_by_many([
        ("total_billable", false)
    ])
    .elusion("summary")
    .await?;

let transactions_table = mix_res
    .select([
        "customer_name",
        "order_date",
        "abs_billable_value",
        "double_billable_value",
        "percentage_billable"
    ])
    .order_by_many([
        ("order_date", false),
        ("abs_billable_value", false)
    ])
    .elusion("transactions")
    .await?;

// Create comprehensive dashboard with all plots
let plots = [
    (&line, "Sales Line"),                  // Line based analysis
    (&time_series, "Sales Timeline"),       // Time-based analysis
    (&bars, "Customer Sales"),              // Customer comparison
    (&histogram, "Sales Distribution"),      // Value distribution
    (&scatter, "Value Comparison"),         // Value relationships
    (&box_plot, "Customer Distributions"),   // Statistical distribution
    (&pie, "Sales Share"),                  // Share analysis
    (&donut, "Percentage View"),            // Percentage breakdown
];

// Add tables array
let tables = [
    (&summary_table, "Customer Summary"),
    (&transactions_table, "Transaction Details")
];

let layout = ReportLayout {
    grid_columns: 2, // Arrange plots in 2 columns
    grid_gap: 30, // 30px gap between plots
    max_width: 1600, // Maximum width of 1600px
    plot_height: 450, // Each plot 450px high
    table_height: 500,  // Height for tables
};
    
let table_options = TableOptions {
    pagination: true,       // Enable pagination for tables
    page_size: 15,         // Show 15 rows per page
    enable_sorting: true,   // Allow column sorting
    enable_filtering: true, // Allow column filtering
    enable_column_menu: true, // Show column menu (sort/filter/hide options)
    theme: "ag-theme-alpine".to_string(), // Use Alpine theme for modern look
};

// Generate the enhanced interactive report with all plots and tables
CustomDataFrame::create_report(
    Some(&plots),  // plots (Optional)
    Some(&tables),   // tables (Optional)
    "Interactive Sales Analysis Dashboard",  // report_title
    "C:\\Borivoj\\RUST\\Elusion\\Plots\\interactive_aggrid_dashboard.html", // filename
    Some(layout),      // layout_config (Optional)
    Some(table_options)  // table_options (Optional)
).await?;
```
### Dashboard Demo
![Dash](./images/interactivedash3.gif)
---
### License
Elusion is distributed under the [MIT License](https://opensource.org/licenses/MIT). 
However, since it builds upon [DataFusion](https://datafusion.apache.org/), which is distributed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0), some parts of this project are subject to the terms of the Apache License 2.0.
For full details, see the [LICENSE.txt file](LICENSE.txt).

### Acknowledgments
This library leverages the power of Rust's type system and libraries like [DataFusion](https://datafusion.apache.org/)
, Appache Arrow, Arrow ODBC, Tokio Cron Scheduler, Tokio... for efficient query processing. Special thanks to the open-source community for making this project possible.

## Where you can find me:

LindkedIn - [LinkedIn](https://www.linkedin.com/in/borivojgrujicic/ )
YouTube channel - [YouTube](https://www.youtube.com/@RustyBiz)
