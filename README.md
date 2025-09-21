# Elusion 🦎 DataFrame / Data Engineering Library

<div align="center">

[![Crates.io downloads](https://img.shields.io/crates/d/elusion?style=for-the-badge&color=orange&logo=rust)](https://crates.io/crates/elusion)
[![Crates.io version](https://img.shields.io/crates/v/elusion?style=for-the-badge&color=brightgreen&logo=rust)](https://crates.io/crates/elusion)
[![docs.rs](https://img.shields.io/docsrs/elusion?style=for-the-badge&color=blue&logo=docs.rs)](https://docs.rs/elusion)
[![GitHub license](https://img.shields.io/github/license/DataBora/elusion?style=for-the-badge&color=green&logo=github)](https://github.com/DataBora/elusion/blob/main/LICENSE)

</div>


![Elusion Logo](images/elusiongithub.png)
---

Elusion is a high-performance **DataFrame** / **Data Engineering** library designed for in-memory data formats such as **CSV**, **EXCEL**, **JSON**, **XML**, **PARQUET**, **DELTA**, as well as for **Microsoft Fabric - OneLake** connection, **Microsoft SharePoint** connection, **Microsoft Azure Blob Storage** connections, **Postgres Database** connection, **MySql Database** connection, and **REST API**'s for creating **JSON files** which can be forwarded to DataFrame, with advanced query results caching abilities with **Redis** and **Native cashing**.

This Library is mostly focused on **Microsoft Stack**, designed to be used for **Business Data Engineering** with reasonable file sizes, with focus on accuracy, user experience by auto-creating schema and simplified query usage. Elusion is NOT made for Data Science, nor Machine Learning, 1TB and 500 columns datasets. 

All of the DataFrame operations can be placed in PipelineScheduler for automated Data Engineering Pipelines.

Tailored for Data Engineers and Data Analysts seeking a powerful abstraction over data transformations. Elusion streamlines complex operations like filtering, joining, aggregating, and more with its intuitive, chainable DataFrame API, and provides a robust interface for managing and querying data efficiently, as well as Integrated Plotting and Interactive Dashboard features.

## Core Philosophy
Elusion wants you to be you!

Elusion offers flexibility in constructing queries without enforcing specific patterns or chaining orders, unlike SQL, PySpark, Polars, or Pandas. You can build your queries in ANY SEQUENCE THAT BEST FITS YOUR LOGIC, writing functions in ANY ORDER or a manner that makes sense to you. Regardless of the order of function calls, Elusion ensures consistent results.

## Platform Compatibility
Tested for MacOS, Linux and Windows
![Platform comp](images/platformcom.png)

## Fastest way to learn ELUSION
[Elusion UDEMY Course](https://www.udemy.com/course/rust-data-engineering-analytics-elusion/)

---
## Key Features and Feature Flags
Elusion uses Cargo feature flags to keep the library lightweight and modular. 
You can enable only the features you need, which helps reduce dependencies and compile time.

## 🚀 Available Features

<p align="center">
  <img src="https://upload.wikimedia.org/wikipedia/commons/4/44/Microsoft_logo.svg" alt="Fabric" height="32"/>
  &nbsp;&nbsp;&nbsp;
  <img src="https://static.cdnlogo.com/logos/m/58/microsoft-azure.svg" alt="Azure" height="32"/>
  &nbsp;&nbsp;&nbsp;
  <img src="https://static.cdnlogo.com/logos/s/57/sharepoint.svg" alt="SharePoint" height="32"/>
  &nbsp;&nbsp;&nbsp;
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/postgresql/postgresql-original.svg" alt="PostgreSQL" height="32"/>
  &nbsp;&nbsp;&nbsp;
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/mysql/mysql-original.svg" alt="MySQL" height="32"/>
  &nbsp;&nbsp;&nbsp;
  <img src="https://cdn-icons-png.flaticon.com/512/2659/2659360.png" alt="API" height="32"/>
  &nbsp;&nbsp;&nbsp;
  <img src="https://cdn.jsdelivr.net/npm/simple-icons@v11/icons/plotly.svg" alt="Dashboard" height="32"/>
  &nbsp;&nbsp;&nbsp;
  <img src="https://upload.wikimedia.org/wikipedia/commons/7/73/Microsoft_Excel_2013-2019_logo.svg" alt="Excel" height="32"/>
</p>

<p align="center">
  Fabric &nbsp; &nbsp; Azure &nbsp; &nbsp; SharePoint &nbsp; &nbsp; PostgreSQL &nbsp; &nbsp; MySQL &nbsp; &nbsp; API &nbsp; &nbsp; Dashboard &nbsp; &nbsp; Excel
</p>

| Feature | Description | Usage |
|---------|-------------|-------|
| **Fabric** | Microsoft Fabric - OneLake connectivity | `features = ["fabric"]` |
| **Azure** | Azure BLOB storage connectivity | `features = ["azure"]` |
| **SharePoint** | SharePoint connectivity | `features = ["sharepoint"]` |
| **PostgreSQL** | PostgreSQL Database connectivity | `features = ["postgres"]` |
| **MySQL** | MySQL Database connectivity | `features = ["mysql"]` |
| **API** | HTTP API integration | `features = ["api"]` |
| **Dashboard** | Data visualization and dashboards | `features = ["dashboard"]` |
| **Excel** | Excel file operations | `features = ["excel"]` |

---

### 🔄 Job Scheduling (PipelineScheduler)
- Flexible Intervals: From 1 minute to 30 days scheduling intervals.
- Graceful Shutdown: Built-in Ctrl+C signal handling for clean termination.
- Async Support: Built on tokio for non-blocking operations.

### 🌐 External Data Sources Integration
- Azure Blob Storage: Direct integration with Azure Blob Storage for Reading and Writing data files.
- REST API's: Create JSON files from REST API endpoints with Customizable Headers, Params, Date Ranges, Pagination...
- SharePoint: Elusion provides seamless integration with Microsoft SharePoint Online, allowing you to load data directly from SharePoint document libraries into DataFrames.

### 🚀 High-Performance DataFrame Query Operations
- Seamless Data Loading: Easily load and process data from CSV, EXCEL, PARQUET, JSON, and DELTA table files.
- SQL-Like Transformations: Execute transformations such as SELECT, AGG, STRING FUNCTIONS, JOIN, FILTER, HAVING, GROUP BY, ORDER BY, DATETIME and WINDOW with ease.

### 🏪 Caching and Views (Native)
- The caching and views functionality offer several significant advantages over regular querying:
#### Reduced Computation Time, Memory Management, Query Optimization, Interactive Analysis, Multiple visualizations for Dashboards and Reports, Resource Utilization, Concurrency

### 🏬 Redis Caching
**High-performance distributed caching** for production environments, multi-server deployments, and large-scale data processing. Redis caching provides:
- **Persistent cache** across application restarts
- **Distributed caching** for multiple application instances  
- **Production-ready** performance and reliability
- **Automatic TTL management** and expiration
- **6-10x performance improvements** for repeated queries

### When to Use Redis vs Native Cache:
- **Native Cache**: Development, single-instance apps, temporary caching
- **Redis Cache**: Production, distributed systems, persistent caching, large datasets

### 📉 Aggregations and Analytics
- Comprehensive Aggregations: Utilize built-in functions like SUM, AVG, MEAN, MEDIAN, MIN, COUNT, MAX, and more.
- Advanced Scalar Math: Perform calculations using functions such as ABS, FLOOR, CEIL, SQRT, ISNAN, ISZERO, PI, POWER, and others.

### 🔗 Flexible Joins
- Diverse Join Types: Perform joins using INNER, LEFT, RIGHT, FULL, and other join types.
- Intuitive Syntax: Easily specify join conditions and aliases for clarity and simplicity.

### 🪟 Window Functions
- Analytical Capabilities: Implement window functions like RANK, DENSE_RANK, ROW_NUMBER, and custom partition-based calculations to - perform advanced analytics.

### 🔄 Pivot and Unpivot Functions
- Data Reshaping: Transform your data structure using PIVOT and UNPIVOT functions to suit your analytical needs.

### 📊 Create REPORTS
- Create HTML files with Interactive Dashboards with multiple interactive Plots and Tables.
- Plots Available: TimeSeries, Bar, Pie, Donut, Histogram, Scatter, Box...
- Tables can Paginate pages, Filter, Resize, Reorder columns...
- Export Tables data to EXCEL and CSV

### 🧹 Clean Query Construction
- Readable Queries: Construct SQL queries that are both readable and reusable.
- Advanced Query Support: Utilize operations such as APPEND, UNION, UNION ALL, INTERSECT, and EXCEPT. For multiple Dataframea operations: APPEND_MANY, UNION_MANY, UNION_ALL_MANY.

## 🎨 **Developer Experience That Delights**

### 🔗 **Fluent, Chainable API**
Write data transformations that read like natural language:
```rust
sales_df
    .join_many([
        (customers_df, ["s.CustomerKey = c.CustomerKey"], "INNER"),
        (products_df, ["s.ProductKey = p.ProductKey"], "INNER"),
    ])
    .select(["c.name", "p.category", "s.amount"])
    .filter("s.amount > 1000")
    .agg(["SUM(s.amount) AS total_revenue"])
    .group_by(["c.region", "p.category"]) 
    .order_by(["total_revenue"], ["DESC"])
    .elusion("quarterly_report")
    .await?
```
---
**Ready to transform your data engineering workflow?** 
Elusion combines the **performance of Rust**, the **flexibility of modern DataFrames**, and the **reliability of enterprise software** into one powerful library.

*Join thousands of developers building the future of data engineering with Elusion.*
---
## INSTALLATION

To add 🚀 Latest and the Greatest 🚀 version of **Elusion** to your Rust project, include the following lines in your `Cargo.toml` under `[dependencies]`:

```toml
elusion = "6.3.0"
tokio = { version = "1.45.0", features = ["rt-multi-thread"] }
```
## Rust version needed
```toml
>= 1.89.0
```

Usage:
- In your Cargo.toml, specify which features you want to enable:

- Add the POSTGRES feature when specifying the dependency:
```toml
[dependencies]
elusion = { version = "6.3.0", features = ["fabric"] }
```

- Using NO Features (minimal dependencies):
```rust
[dependencies]
elusion = "6.3.0"
```

- Using multiple specific features:
```rust
[dependencies]
elusion = { version = "6.3.0", features = ["dashboard", "api", "fabric"] }
```

- Using all features:
```rust
[dependencies]
elusion = { version = "6.3.0", features = ["all"] }
```

### Feature Implications
#### When a feature is not enabled, You'll receive an error:
#### Error: ***Warning***: API feature not enabled. Add feature under [dependencies]
---
## NORMALIZATION
#### DataFrame (your files) Column Names will be normalized to LOWERCASE(), TRIM() and REPLACE(" ","_")
#### All DataFrame query expresions, functions, aliases and column names will be normalized to LOWERCASE(), TRIM() and REPLACE(" ","_")
## BREAKAGE
#### If your column names have special characters like: / * + - ...or any special characters that can be part of sql operation keywords, group_by_all() can brake as I am unable to handle special characters in column names, during automatical expansion from select(["*"]) or select(["alias.*"]). For best usage and performance use snake_case style column names.
---
## SCHEMA
#### SCHEMA IS DYNAMICALLY INFERED
---
# Usage examples:
## Most DataFrame OPERATIONS AND EXAMPLES, that you will need, are bellow.

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
# CREATING DATA FRAMES
---
### - Loading data into CustomDataFrame can be from:
#### - Empty() DataFrames
#### - In-Memory data formats: CSV, EXCEL, JSON, XML, PARQUET, DELTA 
#### - SharePoint
#### - Azure Blob Storage endpoints (BLOB, DFS)
#### - Postgres Database SQL Queries
#### - MySQL Database Queries
#### - REST API -> json -> DataFrame

#### -> NEXT is example for reading data from local files, 
#### at the end are examples for Azure Blob Storage, Postgres and MySQL Databases
---
### LOADING data from Files into CustomDataFrame (in-memory data formats)
#### - File extensions are automatically recognized 
#### - All you have to do is to provide path to your file

## Creating CustomDataFrame
#### 2 arguments needed:  **Path**, **Table Alias**
#### File extensions are automatically recognized (csv, excel, json, parquet, delta)
---
## REGULAR LOADING
---
### LOADING data from CSV into CustomDataFrame
#### Delimiters are auto-detected: b'\t' => "tab (TSV)", b',' => "comma (CSV)", b';' => "semicolon", b'|' => "pipe"
```rust
let csv_path = "C:\\BorivojGrujicic\\RUST\\Elusion\\csv_data.csv";
let df = CustomDataFrame::new(csv_path, "csv_data").await?; 
```
### LOADING data from EXCEL into CustomDataFrame
```rust
let excel_path = "C:\\BorivojGrujicic\\RUST\\Elusion\\excel_data.xlsx";
let df = CustomDataFrame::new(excel_path, "xlsx_data").await?;
```
### LOADING data from PARQUET into CustomDataFrame
```rust
let parquet_path = "C:\\BorivojGrujicic\\RUST\\Elusion\\prod_data.parquet";
let df = CustomDataFrame::new(parquet_path, "parq_data").await?;
```
### LOADING data from JSON into CustomDataFrame
```rust
let json_path = "C:\\BorivojGrujicic\\RUST\\Elusion\\mongo_data.json";
let df = CustomDataFrame::new(json_path, "json_data").await?;
```
### LOADING data from XML into CustomDataFrame
#### Automatically analyzes XML file structure and chooses the optimal processing strategy and data types for your data
```rust
let xml_path = "C:\\BorivojGrujicic\\RUST\\Elusion\\sales.xml";
let df = CustomDataFrame::new(xml_path, "xml_data").await?;
```
### LOADING data from DELTA table into CustomDataFrame
```rust
let delta_path = "C:\\BorivojGrujicic\\RUST\\Elusion\\agg_sales"; // for DELTA you just specify folder name without extension
let df = CustomDataFrame::new(delta_path, "delta_data").await?;
```
---
## STREAMING
---
### LOADING data from CSV into CustomDataFrame
#### Example for Stream processing (Process and display results)
```rust
let big_file_path = "C:\\Borivoj\\RUST\\Elusion\\bigdata\\customers-2000000.csv"; 
let big_file_path_df = CustomDataFrame::new(big_file_path, "raw22").await?;

big_file_path_df
    .select(["first_name", "last_name","company", "city" ,"country"])
    .string_functions(["CAST(subscription_date AS DATE) as date"])
    .limit(10)
    .elusion_streaming("logentries1").await?;
```
#### Example for Stream writing (Writes DataFrame result into file extension choosen withing file path)
```rust
let big_file_path = "C:\\Borivoj\\RUST\\Elusion\\bigdata\\customers-2000000.csv"; 
let big_file_path_df = CustomDataFrame::new_with_stream(big_file_path, "raw22").await?;

big_file_path_df
    .select(["first_name", "last_name","company", "city" ,"country"])
    .string_functions(["CAST(subscription_date AS DATE) as date"])
    .limit(10)
    .elusion_streaming_write("data", "C:\\output\\results.csv", "overwrite").await?; // you can also use "append"

SAME USAGE IS FOR .json and .parquet
.elusion_streaming_write("data", "C:\\output\\results.json", "overwrite").await?; // you can also use "append"
.elusion_streaming_write("data", "C:\\output\\results.parquet", "overwrite").await?; // you can also use "append"
```
---
### LOADING data from LOCAL FOLDER into CustomDataFrame
#### - Automatically loads and combines multiple files from a folder
#### - Supports CSV, EXCEL, JSON, XML and PARQUET files
#### - Handles schema compatibility and column reordering automatically
#### - Uses UNION ALL to combine all files

## Loading All Files from Folder
#### 3 arguments needed: **Folder Path**, **File Extensions Filter (Optional)**, **Result Alias**
#### Loads all supported files and combines them into a single DataFrame

```rust
// Load all supported files from folder
let combined_data = CustomDataFrame::load_folder(
   "C:\\BorivojGrujicic\\RUST\\Elusion\\SalesReports",
   None, // Load all supported file types (csv, xlsx, json, parquet)
   "combined_sales_data"
).await?;

// Load only specific file types
let csv_excel_data = CustomDataFrame::load_folder(
   "C:\\BorivojGrujicic\\RUST\\Elusion\\SalesReports", 
   Some(vec!["csv", "xlsx"]), // Only load CSV and Excel files
   "filtered_data"
).await?;
```
### LOADING data from LOCAL FOLDER with FILENAME TRACKING into CustomDataFrame
#### - Same as load_folder but adds "filename_added" column to track source files
#### - Perfect for time-series data where filename contains date information
#### - Automatically loads and combines multiple files from a folder
#### - Supports CSV, EXCEL, JSON, XML and PARQUET files

## Loading Files from Folder with Filename Column
#### 3 arguments needed: **Folder Path**, **File Extensions Filter (Optional)**, **Result Alias**
#### Adds "filename_added" column and combines all files into a single DataFrame

```rust
// Load files with filename tracking
let data_with_source = CustomDataFrame::load_folder_with_filename_column(
   "C:\\BorivojGrujicic\\RUST\\Elusion\\DailyReports",
   None, // Load all supported file types
   "daily_data_with_source"
).await?;

// Load only specific file types with filename tracking
let excel_files_with_source = CustomDataFrame::load_folder_with_filename_column(
   "C:\\BorivojGrujicic\\RUST\\Elusion\\MonthlySales", 
   Some(vec!["xlsx", "xls"]), // Only Excel files
   "monthly_excel_data"
).await?;
```
---
# SharePoint connector
### You can load single EXCEL, CSV, JSON and PARQUET files OR All files from a FOLDER into Single DataFrame
### To connect to SharePoint you need AzureCLI installed and to be logged in 
### 1. Install Azure CLI
- Download and install Azure CLI from: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli
- Microsoft users can download here: https://learn.microsoft.com/en-us/cli/azure/install-azure-cli-windows?view=azure-cli-latest&pivots=msi 
- 🍎 macOS: brew install azure-cli
- 🐧 Linux: 
### Ubuntu/Debian
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

### CentOS/RHEL/Fedora
sudo rpm --import https://packages.microsoft.com/keys/microsoft.asc
sudo dnf install azure-cli

### Arch Linux
sudo pacman -S azure-cli

# For other distributions, visit:
- https://docs.microsoft.com/en-us/cli/azure/install-azure-cli-linux

### 2. Login to Azure
Open Command Prompt and write:
```rust
"az login"
```
This will open a browser window for authentication. Sign in with your Microsoft account that has access to your SharePoint site.
### 3. Verify Login
```rust
"az account show"
```
This should display your account information and confirm you're logged in.

### Grant necessary SharePoint permissions:
- Sites.Read.All or Sites.ReadWrite.All
- Files.Read.All or Files.ReadWrite.All

#### Single file loading auto-recognize file extension (csv, excel, parquet, json):
```rust
//Example:
let df = CustomDataFrame::load_from_sharepoint(
    "your-tenant-id", //tenant id
    "your-client-id", //clientid
    "https://contoso.sharepoint.com/sites/MySite", //siteid
    "Shared Documents/Data/customer_data.csv", //file path
    "combined_data" //dataframe alias
).await?;

let sales_data = df
    .select(["Column_1","Column_2","Column_3"])
    .elusion("my_sales_data").await?;

sales_data.display().await?;
```
#### Reading ALL Files from a folder into single DataFrame example:
```rust
let dataframes = CustomDataFrame::load_folder_from_sharepoint(
    "your-tenant-id",//tenant id
    "your-client-id", //client id
    "http://companyname.sharepoint.com/sites/SiteName", //site id
    "Shared Documents/MainFolder/SubFolder",//folder path
    None, // None will read any file type, or you can filter by extension: Some(vec!["xlsx", "csv"])
    "combined_data" //dataframe alias
).await?;

dataframes.display().await?;
```
#### Reading ALL Files from a folder into single DataFrame with Adding filename column automatically:
```rust
let dataframes = CustomDataFrame::load_folder_from_sharepoint_with_filename_column(
    "your-tenant-id",
    "your-client-id", 
    "http://companyname.sharepoint.com/sites/SiteName", 
    "Shared Documents/MainFolder/SubFolder",
    None, // None will read any file type, or you can filter by extension: Some(vec!["xlsx", "csv"])
    "combined_data" //dataframe alias
).await?;

dataframes.display().await?;
```
---
# Fabric Connector
### Also require Azure CLI login as SharePoint
#### For reading you need abfss path, folder/file name and alias. Currectly supported file extensions: csv, json, excel, xml, parquet
```rust
let df = CustomDataFrame::from_fabric(
        "abfss://here-goes-workspaceid@onelake.dfs.fabric.microsoft.com/here-goes-lakehouseid/Files", //abfss path
        "artikli/mapping.csv", // folder/file name
        "map_data" // alias
    ).await?;
```
#### Writing to Fabric requires only abfss path and folder/file name. Currectly supported file extension: parquet
```rust
   df.write_parquet_to_fabric(
        "abfss://here-goes-workspaceid@onelake.dfs.fabric.microsoft.com/here-goes-lakehouseid/Files", //abfss path
        "artikli/mapping_results.parquet" // folder/file name
    ).await?;
```
---

### LOADING data from Azure BLOB Storage into CustomDataFrame (**scroll till the end for FULL example**)
```rust
let df = CustomDataFrame::from_azure_with_sas_token(
        blob_url, 
        sas_token, 
        Some("folder-name/file-name"), // FILTERING is optional. Can be None if you want to take everything from Container
        "data" // alias for registering table
    ).await?;
```
### LOADING data from POSTGRES into CustomDataFrame (**scroll till the end for FULL example with config, conn and query**)
```rust
let df = CustomDataFrame::from_postgres(&conn, query, "df_alias").await?;
```
### LOADING data from MySQL into CustomDataFrame (**scroll till the end for FULL example with config, conn and query**)
```rust
let df = CustomDataFrame::from_mysql(&conn, query, "df_alias").await?;
```
---
## CREATE EMPTY DataFrame
#### Create empty() DataFrame and populate it with data
```rust
 let temp_df = CustomDataFrame::empty().await?;
    
let date_table = temp_df
    .datetime_functions([
        "CURRENT_DATE() as current_date", 
        "DATE_TRUNC('week', CURRENT_DATE()) AS week_start",
        "DATE_TRUNC('week', CURRENT_DATE()) + INTERVAL '1 week' AS next_week_start",
        "DATE_PART('year', CURRENT_DATE()) AS current_year",
        "DATE_PART('week', CURRENT_DATE()) AS current_week_num",
    ])
    .elusion("date_table").await?;

date_table.display().await?;

RESULT:
+--------------+---------------------+---------------------+--------------+------------------+
| current_date | week_start          | next_week_start     | current_year | current_week_num |
+--------------+---------------------+---------------------+--------------+------------------+
| 2025-03-07   | 2025-03-03T00:00:00 | 2025-03-10T00:00:00 | 2025.0       | 10.0             |
+--------------+---------------------+---------------------+--------------+------------------+
```
---
## CREATE DATE TABLE DataFrame
#### Create Date Table from Range of Dates
```rust
let date_table = CustomDataFrame::create_date_range_table(
    "2025-01-01", // start date
    "2025-12-31", // end date
    "calendar_2025" // table alias
).await?;

date_table.display().await?;

RESULT:
+------------+------+-------+-----+---------+----------+-------------+------------------+-------------+------------+-------------+---------------+------------+------------+
| date       | year | month | day | quarter | week_num | day_of_week | day_of_week_name | day_of_year | week_start | month_start | quarter_start | year_start | is_weekend |
+------------+------+-------+-----+---------+----------+-------------+------------------+-------------+------------+-------------+---------------+------------+------------+
| 2025-01-01 | 2025 | 1     | 1   | 1       | 1        | 3           | Wednesday        | 1           | 2024-12-29 | 2025-01-01  | 2025-01-01    | 2025-01-01 | false      |
| 2025-01-02 | 2025 | 1     | 2   | 1       | 1        | 4           | Thursday         | 2           | 2024-12-29 | 2025-01-01  | 2025-01-01    | 2025-01-01 | false      |
| 2025-01-03 | 2025 | 1     | 3   | 1       | 1        | 5           | Friday           | 3           | 2024-12-29 | 2025-01-01  | 2025-01-01    | 2025-01-01 | false      |
| 2025-01-04 | 2025 | 1     | 4   | 1       | 1        | 6           | Saturday         | 4           | 2024-12-29 | 2025-01-01  | 2025-01-01    | 2025-01-01 | true       |
| 2025-01-05 | 2025 | 1     | 5   | 1       | 1        | 0           | Sunday           | 5           | 2025-01-05 | 2025-01-01  | 2025-01-01    | 2025-01-01 | true       |
| 2025-01-06 | 2025 | 1     | 6   | 1       | 2        | 1           | Monday           | 6           | 2025-01-05 | 2025-01-01  | 2025-01-01    | 2025-01-01 | false      |
| 2025-01-07 | 2025 | 1     | 7   | 1       | 2        | 2           | Tuesday          | 7           | 2025-01-05 | 2025-01-01  | 2025-01-01    | 2025-01-01 | false      |
| 2025-01-08 | 2025 | 1     | 8   | 1       | 2        | 3           | Wednesday        | 8           | 2025-01-05 | 2025-01-01  | 2025-01-01    | 2025-01-01 | false      |
| 2025-01-09 | 2025 | 1     | 9   | 1       | 2        | 4           | Thursday         | 9           | 2025-01-05 | 2025-01-01  | 2025-01-01    | 2025-01-01 | false      |
| .......... | .... | .     | .   | .       | .        | .           | ................ | ..........  | .......... | ..........  | ............. | ...........| .......... |
+------------+------+-------+-----+---------+----------+-------------+------------------+-------------+------------+-------------+---------------+------------+------------+
```
---
## CREATE DATE TABLE DataFrame WITH CUSTOM FORMATS
#### You can create Date Table with Custom formats (ISO, Compact, Human Readable...) and week, month, quarter, year Ranges (start-end)
```rust
let date_table = CustomDataFrame::create_formatted_date_range_table(
    "2025-01-01", // date start
    "2025-12-31", // date end
    "calendar_2025", // table alias
    "date".to_string(), // first column name
    DateFormat::HumanReadable, // 1 Jan 2025
    true,  // Include period ranges (start - end)
    Weekday::Mon  // Week starts on Monday
).await?;

date_table.display().await?;

RESULT:
+-------------+------+-------+-----+---------+----------+-------------+------------------+-------------+------------+-------------+-------------+-------------+-------------+---------------+-------------+-------------+-------------+
| date        | year | month | day | quarter | week_num | day_of_week | day_of_week_name | day_of_year | is_weekend | week_start  | week_end    | month_start | month_end   | quarter_start | quarter_end | year_start  | year_end    |
+-------------+------+-------+-----+---------+----------+-------------+------------------+-------------+------------+-------------+-------------+-------------+-------------+---------------+-------------+-------------+-------------+
|  1 Jan 2025 | 2025 | 1     | 1   | 1       | 1        | 2           | Wednesday        | 1           | false      | 30 Dec 2024 |  5 Jan 2025 |  1 Jan 2025 | 31 Jan 2025 |  1 Jan 2025   | 31 Mar 2025 |  1 Jan 2025 | 31 Dec 2025 |
|  2 Jan 2025 | 2025 | 1     | 2   | 1       | 1        | 3           | Thursday         | 2           | false      | 30 Dec 2024 |  5 Jan 2025 |  1 Jan 2025 | 31 Jan 2025 |  1 Jan 2025   | 31 Mar 2025 |  1 Jan 2025 | 31 Dec 2025 |
|  3 Jan 2025 | 2025 | 1     | 3   | 1       | 1        | 4           | Friday           | 3           | false      | 30 Dec 2024 |  5 Jan 2025 |  1 Jan 2025 | 31 Jan 2025 |  1 Jan 2025   | 31 Mar 2025 |  1 Jan 2025 | 31 Dec 2025 |
|  4 Jan 2025 | 2025 | 1     | 4   | 1       | 1        | 5           | Saturday         | 4           | true       | 30 Dec 2024 |  5 Jan 2025 |  1 Jan 2025 | 31 Jan 2025 |  1 Jan 2025   | 31 Mar 2025 |  1 Jan 2025 | 31 Dec 2025 |
|  5 Jan 2025 | 2025 | 1     | 5   | 1       | 1        | 6           | Sunday           | 5           | true       | 30 Dec 2024 |  5 Jan 2025 |  1 Jan 2025 | 31 Jan 2025 |  1 Jan 2025   | 31 Mar 2025 |  1 Jan 2025 | 31 Dec 2025 |
|  6 Jan 2025 | 2025 | 1     | 6   | 1       | 2        | 0           | Monday           | 6           | false      |  6 Jan 2025 | 12 Jan 2025 |  1 Jan 2025 | 31 Jan 2025 |  1 Jan 2025   | 31 Mar 2025 |  1 Jan 2025 | 31 Dec 2025 |
|  7 Jan 2025 | 2025 | 1     | 7   | 1       | 2        | 1           | Tuesday          | 7           | false      |  6 Jan 2025 | 12 Jan 2025 |  1 Jan 2025 | 31 Jan 2025 |  1 Jan 2025   | 31 Mar 2025 |  1 Jan 2025 | 31 Dec 2025 |
|  8 Jan 2025 | 2025 | 1     | 8   | 1       | 2        | 2           | Wednesday        | 8           | false      |  6 Jan 2025 | 12 Jan 2025 |  1 Jan 2025 | 31 Jan 2025 |  1 Jan 2025   | 31 Mar 2025 |  1 Jan 2025 | 31 Dec 2025 |
|  9 Jan 2025 | 2025 | 1     | 9   | 1       | 2        | 3           | Thursday         | 9           | false      |  6 Jan 2025 | 12 Jan 2025 |  1 Jan 2025 | 31 Jan 2025 |  1 Jan 2025   | 31 Mar 2025 |  1 Jan 2025 | 31 Dec 2025 |
| ........... | .... | ..    | ..  | .       | .        | .           | .........        | ...         | .....      | ........... |  .......... |  .......... | ........... |  ..........   | ........... |  .......... | ........... |
+-------------+------+-------+-----+---------+----------+-------------+------------------+-------------+------------+-------------+-------------+-------------+-------------+---------------+-------------+-------------+-------------+
```
### ALL AVAILABLE DATE FORMATS
```rust
IsoDate,            // YYYY-MM-DD
IsoDateTime,        // YYYY-MM-DD HH:MM:SS
UsDate,             // MM/DD/YYYY
EuropeanDate,       // DD.MM.YYYY
EuropeanDateDash,   // DD-MM-YYYY
BritishDate,        // DD/MM/YYYY
HumanReadable,      // 1 Jan 2025
HumanReadableTime,  // 1 Jan 2025 00:00
SlashYMD,           // YYYY/MM/DD
DotYMD,             // YYYY.MM.DD
CompactDate,        // YYYYMMDD
YearMonth,          // YYYY-MM
MonthYear,          // MM-YYYY
MonthNameYear,      // January 2025
Custom(String)      // Custom format string

For Custom Date formats some of the common format specifiers:
%Y - Full year (2025)
%y - Short year (25)
%m - Month as number (01-12)
%b - Abbreviated month name (Jan)
%B - Full month name (January)
%d - Day of month (01-31)
%e - Day of month, space-padded ( 1-31)
%a - Abbreviated weekday name (Mon)
%A - Full weekday name (Monday)
%H - Hour (00-23)
%I - Hour (01-12)
%M - Minute (00-59)
%S - Second (00-59)
%p - AM/PM

EXAMPLES:
DateFormat::Custom("%d %b %Y %H:%M".to_string()),  // "01 Jan 2025 00:00"
// ISO 8601 with T separator and timezone
DateFormat::Custom("%Y-%m-%dT%H:%M:%S%z".to_string())
// US date with 12-hour time
DateFormat::Custom("%m/%d/%Y %I:%M %p".to_string())
// Custom format with weekday
DateFormat::Custom("%A, %B %e, %Y".to_string())  // "Monday, January 1, 2025"
```
---
# DATA INSPECTION, SCHEMA INSPECTION, SQL GENERATED INFO, PREVIEW FUNCTIONS AND STATISTICAL FUNCTIONS
---
### Quickly preview your data with SHOW_HEAD(), SHOW_TAIL(), and PEEK() functions
#### Display the first n rows of your DataFrame for quick data inspection
#### 1 argument needed: Number of Rows
```rust
let csv_path = "C:\\BorivojGrujicic\\RUST\\Elusion\\sales_data.csv";
let df = CustomDataFrame::new(csv_path, "sales").await?;

// Show first 5 rows
df.show_head(5).await?;

// Show last 10 rows
df.show_tail(10).await?;

// Show first 3 and last 3 rows
df.peek(3).await?;

// Show Column names and their types
df_arhiva.df_schema();
```
---
### SQL GENERATED INFO (for debuging purposes)
#### THIS CAN BE INACCURATE if analyzer can't figure out overly complex generated query
#### It works accurate in most cases
```rust
let complex_result = df_arhiva
    .filter_many([("mesec = 'Januar'"), ("neto_vrednost > 1000")])
    .select([
        "veledrogerija as pharm",
        "region AS refionale" , 
        "kolicina",
        "neto_vrednost",
        "mesto"
    ])
    .window("ROW_NUMBER() OVER (PARTITION BY region ORDER BY mesto DESC) as region_rank")
    .agg([
        "COUNT(*) as broj_transakcija",
        "SUM(kolicina) AS ukupna_kolicina", 
        "SUM(neto_vrednost) AS ukupna_vrednost"
    ])
    .group_by(["pharm",
        "regionale" , 
        "kolicina",
        "neto_vrednost",
        "mesto"])
    .order_by(["ukupna_vrednost"], ["DESC"])
    .limit(10);

complex_result.display_query();
complex_result.display_query_with_info();

let res =   complex_result.elusion("analysis1").await?;
    
res.display().await?;
```
### YOU WILL GET RESULT:
#### 📋 Generated SQL Query:
============================================================
```sql
SELECT count( * ) as "broj_transakcija", sum("analysis"."kolicina") as "ukupna_kolicina", sum("analysis"."neto_vrednost") as "ukupna_vrednost", "veledrogerija" AS "pharm", "region" AS "regionale", "kolicina", "neto_vrednost", "mesto", row_number() over (partition by region order by mesto desc) as region_rank
FROM "analysis" AS analysis
WHERE "mesec" = 'Januar' AND "neto_vrednost" > 1000
GROUP BY "veledrogerija", "region", "kolicina", "neto_vrednost", "mesto"
ORDER BY "ukupna_vrednost" DESC
LIMIT 10
```
#### ============================================================
#### 📋 Query Analysis:
#### ==================================================
#### 🔍 SQL Query:
```sql
SELECT count( * ) as "broj_transakcija", sum("analysis"."kolicina") as "ukupna_kolicina", sum("analysis"."neto_vrednost") as "ukupna_vrednost", "veledrogerija" AS "pharm", "region" AS "regionale", "kolicina", "neto_vrednost", "mesto", row_number() over (partition by region order by mesto desc) as region_rank
FROM "analysis" AS analysis
WHERE "mesec" = 'Januar' AND "neto_vrednost" > 1000
GROUP BY "veledrogerija", "region", "kolicina", "neto_vrednost", "mesto"
ORDER BY "ukupna_vrednost" DESC
LIMIT 10
```
#### 📊 Query Info:
#####    • Has CTEs: false
#####    • Has JOINs: false
#####    • Has WHERE: true
#####    • Has GROUP BY: true
#####    • Has HAVING: false
#####    • Has ORDER BY: true
#####    • Has LIMIT: true
#####    • Has UNION: false
#####    • CTE count: 0
#####    • Join count: 0
#####    • Union count: 0
#####    • Function calls: ~5
#####   • Complexity: Moderate
---
### STATISTICAL FUNCTIONS
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
# NULL VALUE HANDLING
---
### FILL NULL VALUES
#### Handle missing data with advanced null detection and cleaning functions
#### These functions detect: NULL, empty strings (''), 'null'/'NULL', 'na'/'NA', 'n/a'/'N/A', 'none'/'NONE', '-', '?', 'NaN'/'nan'
#### Replace null-like values in specific columns with a replacement value
#### 2 arguments needed: Array of Column Names, Fill Value
```rust
let csv_path = "C:\\BorivojGrujicic\\RUST\\Elusion\\customer_data.csv";
let df = CustomDataFrame::new(csv_path, "customers").await?;

// Fill nulls in single column
let cleaned_df = df
    .fill_null(["age"], "0")
    .elusion("cleaned_customers").await?;

// Fill nulls in multiple columns
let cleaned_df = df
    .fill_null(["age", "salary", "phone"], "Unknown")
    .elusion("cleaned_customers").await?;

// Chain with other operations
let processed_df = df
    .fill_null(["age"], "0")
    .fill_null(["name"], "Anonymous")
    .filter("age > 18")
    .select(["name", "age", "salary"])
    .elusion("processed_data").await?;
```
---
### DROP NULL VALUES
#### Remove rows that contain null-like values in specified columns
#### 1 argument needed: Array of Column Names
```rust
let csv_path = "C:\\BorivojGrujicic\\RUST\\Elusion\\customer_data.csv";
let df = CustomDataFrame::new(csv_path, "customers").await?;

// Drop rows with nulls in single column
let cleaned_df = df
    .drop_null(["email"])
    .elusion("customers_with_email").await?;

// Drop rows with nulls in multiple columns
let cleaned_df = df
    .drop_null(["email", "phone", "address"])
    .elusion("complete_customers").await?;

// Chain with other operations
let processed_df = df
    .drop_null(["customer_id"])
    .fill_null(["age"], "0")
    .filter("age > 21")
    .elusion("adult_customers").await?;
```
---
### FILL_DOWN function - fill_down() - that fills down null values in column with firs non null values above
#### Imagine you have DataFrame like bellow with lots of null values.
```rust
+---------------------+---------------+----------------+----------+----------+
| site                | location      | centre         | net      | gross    |
+---------------------+---------------+----------------+----------+----------+
| null                | null          | null           | null     | null     |
| null                | null          | null           | null     | null     |
|                     |               |                | Dinner   | null     |
| Site Name           | Location Name | Revenue Centre | Net      | Gross    |
| Babaluga            | Bar           | Beer           | 95.24    | 110      |
| null                | null          | Food           | 1080.04  | 1247.4   |
| null                | null          | Liquor         | 0        | 0        |
| null                | null          | Non Alc. Bev   | 51.08    | 59       |
| null                | null          | Wine           | 64.94    | 75       |
| null                | Terrace       | Beer           | 2642.89  | 3052.5   |
| null                | null          | Champagne      | 450.2    | 520      |
| null                | null          | Food           | 77974.82 | 90060.93 |
| null                | null          | Liquor         | 21258.71 | 24554    |
| null                | null          | Non Alc. Bev   | 15560.95 | 17973.5  |
| null                | null          | Tobacco        | 19939.11 | 23030    |
| null                | null          | Wine           | 18774.9  | 21685    |
+---------------------+---------------+----------------+----------+----------+
```
#### Now to remove null rows, empty value rows and to fill down this Dataframe we can write this:
```rust
let sales_data = df
    .select(["Site","Location","Centre","Net","Gross"])
    .filter("Centre != 'Revenue Centre'")
    .drop_null(["gross"])
    .fill_down(["Site", "Location"])
    .elusion("my_sales_data").await?;

sales_data.display().await?;

//THEN WE GET THIS RESULT
+---------------------+----------+--------------+----------+----------+
| site                | location | centre       | net      | gross    |
+---------------------+----------+--------------+----------+----------+
| Babaluga            | Bar      | Beer         | 95.24    | 110      |
| Babaluga            | Bar      | Food         | 1080.04  | 1247.4   |
| Babaluga            | Bar      | Liquor       | 0        | 0        |
| Babaluga            | Bar      | Non Alc. Bev | 51.08    | 59       |
| Babaluga            | Bar      | Wine         | 64.94    | 75       |
| Babaluga            | Terrace  | Beer         | 2642.89  | 3052.5   |
| Babaluga            | Terrace  | Champagne    | 450.2    | 520      |
| Babaluga            | Terrace  | Food         | 77974.82 | 90060.93 |
| Babaluga            | Terrace  | Liquor       | 21258.71 | 24554    |
| Babaluga            | Terrace  | Non Alc. Bev | 15560.95 | 17973.5  |
| Babaluga            | Terrace  | Tobacco      | 19939.11 | 23030    |
| Babaluga            | Terrace  | Wine         | 18774.9  | 21685    |
+---------------------+----------+--------------+----------+----------+
```
---
### ROW SKIPPING AND DATA EXTRACTION
#### Skip unwanted rows
#### 1 argument needed: Number of Rows to Skip
```rust
let excel_path = "C:\\BorivojGrujicic\\RUST\\Elusion\\report.xlsx";
let df = CustomDataFrame::new(excel_path, "report").await?;

// Skip first 3 rows (common for Excel reports with titles)
let data_df = df
    .skip_rows(3)
    .elusion("clean_report").await?;

// Chain with other operations
let processed_df = df
    .skip_rows(2)                    // Skip title and empty row
    .filter("amount > 0")            // Filter valid amounts
    .fill_null(["category"], "Other") // Fill missing categories
    .elusion("processed_report").await?;
```
---
# DATAFRAME WRANGLING
---
## SELECT
### ALIAS column names in SELECT() function (AS is case insensitive)
```rust
let df_AS = select_df
    .select(["CustomerKey AS customerkey_alias", "FirstName as first_name", "LastName", "EmailAddress"]);

let df_select_all = select_df.select(["*"]);

let df_count_all = select_df.select(["COUNT(*)"]);

let df_distinct = select_df.select(["DISTINCT(column_name) as distinct_values"]);

// example usage
let join_result = sales_df
    .join_many([
        (customers_df, ["s.CustomerKey = c.CustomerKey"], "RIGHT"),
        (products_df, ["s.ProductKey = p.ProductKey"], "LEFT OUTER"),
    ])
    .select(["c.*","p.*"])
    .elusion("sales_join") .await?;

join_result.display().await?;

// example usage 2
 aggregate_result
    .filter_many([("mesec = 'Januar'"), ("neto_vrednost > 1000")])
    .select(["*"])  // Full star selection on large dataset
    .agg([
        "COUNT(*) AS transaction_count",
        "SUM(neto_vrednost) AS total_value",
        "AVG(kolicina) AS avg_quantity"
    ])
    .group_by_all()
    .order_by(["total_value"], ["DESC"])
    .limit(200)
    .elusion("archive_star_full").await?;

aggregate_result.display().await?
```
---
### IMPORTANT: Star Selection Duplicate Column Behavior

#### Overview
When using star selections 
```rust
select(["*"]) or select(["alias.*"])
``` 
with joined tables, **duplicate column names are automatically removed** to prevent SQL errors and schema conflicts. This behavior ensures your queries work reliably while following intuitive rules.

## 🔄 Automatic Duplicate Removal with Star Selections

```rust
// When you use star selections:
.select(["s.*", "c.*", "p.*"])
```

**What happens:**
- `s.*` expands to: `s.customerkey`, `s.productkey`, `s.orderdate`, etc.
- `c.*` expands to: `c.customerkey`, `c.firstname`, `c.lastname`, etc.  
- `p.*` expands to: `p.productkey`, `p.productname`, `p.productcolor`, etc.

**Duplicate Detection:**
- ✅ **KEEPS:** `s.customerkey` (first occurrence - main table priority)
- ❌ **REMOVES:** `c.customerkey` (duplicate of customerkey)
- ✅ **KEEPS:** `s.productkey` (first occurrence - main table priority)  
- ❌ **REMOVES:** `p.productkey` (duplicate of productkey)

**Priority Order:** Main table → Joined tables (in join order)

## ✅ Explicit Column Selection Preserves Duplicates

```rust
// When you explicitly specify columns:
.select(["s.CustomerKey", "c.CustomerKey", "p.ProductName"])
```

**What happens:**
- ✅ **KEEPS:** `s.CustomerKey` (explicitly requested)
- ✅ **KEEPS:** `c.CustomerKey` (explicitly requested - different qualified name)
- ✅ **KEEPS:** `p.ProductName` (explicitly requested)

**No duplicate removal** - you get exactly what you specify.

## Mixed Selections Work Too

```rust
// Mix star and explicit columns:
.select(["c.*", "s.OrderDate", "p.ProductName as product"])
```

**Behavior:**
- `c.*` expands with duplicate removal applied
- Explicit columns (`s.OrderDate`, `p.ProductName as product`) are always preserved
- Final result combines both approaches

## 🏷️ Aliases Work with Both Approaches

### Star Selection with Aliases
```rust
.select(["s.CustomerKey AS sales_customer", "c.*", "p.*"])
// Result: sales_customer + all c.* columns + all p.* columns (duplicates removed)
```

### Explicit Selection with Aliases  
```rust
.select([
    "s.CustomerKey AS sales_key", 
    "c.CustomerKey AS master_key",
    "p.ProductName AS product"
])
// Result: All three columns preserved with their aliases
```

### Multiple Aliases for Same Base Column
```rust
.select([
    "c.CustomerKey as customer_master",
    "s.CustomerKey as sales_fk", 
    "p.ProductName"
])
// Result: Both customerkey columns kept with different aliases
```
## ⚠️ When This Matters

### ✅ Use Star Selections When:
- You want "all relevant columns" without conflicts
- You don't need to see duplicate foreign key values  
- You want simple, predictable behavior
- You're doing exploratory data analysis

```rust
// Simple approach - no conflicts, works reliably
.select(["s.*", "c.*", "p.*"])
.group_by_all()  // Just works!
```

### ✅ Use Explicit Columns When:
- You need both foreign key values for comparison
- You want specific control over which columns appear
- You need different aliases for duplicate column names
- You're building production reports with exact specifications

```rust
// Advanced approach - full control
.select([
    "s.CustomerKey AS sales_fk",
    "c.CustomerKey AS customer_pk", 
    "c.FirstName",
    "p.ProductName"
])
.group_by_all()  // Will include both customerkey columns
```
## 🔧 Working with .elusion() and Duplicate Columns

When using `.elusion()` to register query results, the system automatically handles duplicate column scenarios:

### ✅ This Works
```rust
.select([
    "s.CustomerKey AS sales_key",
    "c.CustomerKey AS customer_key",  // Different aliases
    "p.ProductName"
])
.group_by_all()
.elusion("my_result")  // ✅ Works - unique aliases
```
## 💡 Best Practices

1. **Start with star selections** for quick analysis and exploration
2. **Use explicit columns** when you need duplicate keys or precise control
3. **Use descriptive aliases** to rename duplicate columns when needed
4. **Test your queries** to ensure you get expected columns
5. **Mix approaches** when appropriate (star + explicit)

## What Gets Considered Duplicates

Only columns with **identical base names** are considered duplicates:
- `s.customerkey` vs `c.customerkey` → **Duplicate** (same base: "customerkey")
- `s.orderdate` vs `c.birthdate` → **Not duplicate** (different base names)
- `s.productkey` vs `p.productkey` → **Duplicate** (same base: "productkey")
- `s.CustomerId` vs `s.CustomerKey` → **Not duplicate** (different column names)

### Example 1: Star Selection (Automatic Deduplication)
```rust
let star_query = sales_df
    .join_many([
        (customers_df, ["s.CustomerKey = c.CustomerKey"], "LEFT"),
        (products_df, ["s.ProductKey = p.ProductKey"], "LEFT"),
    ])
    .select(["s.*", "c.*", "p.*"])  // Duplicates removed automatically
    .agg(["SUM(s.OrderQuantity) AS total_qty"])
    .group_by_all()                 // Just works!
    .limit(100);
```

### Example 2: Explicit Selection (Full Control)
```rust
let explicit_query = sales_df
    .join_many([
        (customers_df, ["s.CustomerKey = c.CustomerKey"], "LEFT"),
        (products_df, ["s.ProductKey = p.ProductKey"], "LEFT"),
    ])
    .select([
        "s.CustomerKey AS sales_customer_key",
        "c.CustomerKey AS customer_master_key",  // Both kept
        "c.FirstName",
        "p.ProductName",
        "s.OrderQuantity"
    ])
    .agg(["SUM(s.OrderQuantity) AS total_qty"])
    .group_by_all()                              // Handles both customerkey columns
    .limit(100);
```
### Example 3: Mixed Approach
```rust
let mixed_query = sales_df
    .join_many([
        (customers_df, ["s.CustomerKey = c.CustomerKey"], "LEFT"),
        (products_df, ["s.ProductKey = p.ProductKey"], "LEFT"),
    ])
    .select([
        "c.*",                          // All customer columns (deduplicated)
        "s.OrderDate",                  // Specific sales column
        "s.OrderQuantity",              // Another specific column
        "p.ProductName AS product",     // Aliased product column
        "p.ProductPrice"                // Product price
    ])
    .agg(["COUNT(*) AS order_count"])
    .group_by_all()
    .limit(100);
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
    .order_by(["order_date"], ["ASC"])
    .limit(10);

let num_ops_res = num_ops_sales.elusion("scalar_df").await?;
num_ops_res.display().await?;
```
---
### FILTER (evaluated before aggregations)
```rust
let filter_df = sales_order_df
    .select(["customer_name", "order_date", "billable_value"])
    .filter_many([("order_date > '2021-07-04'"), ("billable_value > 100.0")])
    .order_by(["order_date"], ["ASC"])
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
        ("total_billable", "DESC"),  
        ("max_abs_billable", "ASC"), 
    ])
```
---
### HAVING (evaluated after aggregations)
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
        ("total_quantity", "ASC"),
        ("p.ProductName", "DESC")
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
    .order_by(["total_quantity"], ["ASC"])
    .limit(5);

let result = df_having.elusion("sales_res").await?;
result.display().await?;
```
---
### SCALAR functions
```rust
let scalar_df = sales_order_df
    .select([
        "customer_name", 
        "order_date", 
        "ABS(billable_value) AS abs_billable_value",
        "ROUND(SQRT(billable_value), 2) AS SQRT_billable_value"])
    .filter("billable_value > 100.0")
    .order_by(["order_date"], ["ASC"])
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
    .order_by(["order_date"], ["ASC"])
    .limit(10);

let scalar_res = scalar_df.elusion("scalar_df").await?;
scalar_res.display().await?;
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
        ("total_billable", "DESC"),  
        ("max_abs_billable", "ASC"), 
    ]);

let mix_res = mix_query.elusion("scalar_df").await?;
mix_res.display().await?;
```
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
### STRING functions (basic)
```rust
let df = sales_df
    .select(["FirstName", "LastName"])
    .string_functions([
        "'New' AS new_old_customer",
        "TRIM(c.EmailAddress) AS trimmed_email",
        "CONCAT(TRIM(c.FirstName), ' ', TRIM(c.LastName)) AS full_name",
        "CONCAT(region, ' - Rank ', CAST(region_rank AS TEXT)) AS region_rank_label",
        "CASE WHEN region_rank <= 5 THEN 'TOP_5' ELSE 'OTHER' END AS performance_tier"
    ]);

let result_df = df.elusion("df").await?;
result_df.display().await?;
```
### STRING functions (extended)
```rust
let string_functions_df = df_sales
    .join_many([
        (df_customers, ["s.CustomerKey = c.CustomerKey"], "INNER"),
        (df_products, ["s.ProductKey = p.ProductKey"], "INNER"),
    ]) 
    .select([
        "c.CustomerKey as custmer_code"
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
    .agg([
        "COUNT(*) AS total_records",
        "STRING_AGG(p.ProductName, ', ') AS all_products"
        ])
    .filter("c.emailaddress IS NOT NULL")
    //.group_by_all() YOU CAN USE GROUP BY ALL to group on all non-aggregated columns
    .group_by([
    "c.CustomerKey",
    "c.FirstName", 
    "c.LastName",
    "c.EmailAddress",
    "p.ProductName"
    ]) 
    .having("COUNT(*) > 1")
    .order_by(["c.CustomerKey"], ["ASC"]);   

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

    // Extract Day
    "DATE_PART('day', CAST(delivery_date AS DATE) - CAST(order_date AS DATE)) AS delivery_days",
    "DATE_PART('day', CAST(CURRENT_DATE() AS DATE) - CAST(order_date AS DATE)) AS days_since_order",
    
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
    
    // Date comparisons with current date - FIX: Cast for arithmetic
    "CASE 
        WHEN CAST(order_date AS DATE) = CAST(CURRENT_DATE() AS DATE) THEN 'Today'
        WHEN DATE_PART('day', CAST(CURRENT_DATE() AS DATE) - CAST(order_date AS DATE)) <= 7 THEN 'Last Week'
        WHEN DATE_PART('day', CAST(CURRENT_DATE() AS DATE) - CAST(order_date AS DATE)) <= 30 THEN 'Last Month'
        ELSE 'Older'
        END AS order_recency",

    // Time windows
    "CASE 
        WHEN DATE_BIN('1 week', order_date, CURRENT_DATE()) = DATE_BIN('1 week', CURRENT_DATE(), CURRENT_DATE()) 
        THEN 'This Week'
        ELSE 'Previous Weeks'
    END AS week_window",

    // Fiscal year calculations
    "CASE 
        WHEN DATE_PART('month', order_date) >= 7 
        THEN DATE_PART('year', order_date) + 1 
        ELSE DATE_PART('year', order_date) 
    END AS fiscal_year",

    // Complex date logic -
    "CASE 
        WHEN CAST(order_date AS DATE) < CAST(MAKE_DATE(2024, 1, 1) AS DATE) THEN 'Past'
        ELSE 'Present'
    END AS temporal_status",
    
    "CASE 
        WHEN DATE_PART('hour', CURRENT_TIMESTAMP()) < 12 THEN 'Morning'
        ELSE 'Afternoon'
    END AS time_of_day",
    ])
    .agg([
        "SUM(order_value) AS total_order"
    ])
    .group_by([
        "customer_name",
        "order_date", 
        "delivery_date"
    ])
    // .group_by_all() OR YOU CAN USE grouping by all columns
    .order_by(["order_date"], ["DESC"]);

let dt_res = dt_query.elusion("datetime_df").await?;
dt_res.display().await?;
```
#### Currently Available DateTime Functions
```rust
CURRENT_DATE()
CURRENT_TIME()
CURRENT_TIMESTAMP()
NOW()
TODAY()
DATE_PART()
DATE_TRUNC()
DATE_BIN()
MAKE_DATE()
DATE_FORMAT()
```
---

## IMPORTANT
### GROUP BY with Functions specifics

When using `.string_functions()` or `.datetime_functions()` with aggregations, you have two options:

### ✅ Option 1: Include base columns in `.select()` + `.group_by_all()`
```rust
df.select([
    "customer_name",     // ← Include all columns your functions will use
    "email",            // ← TRIM(email) needs this
    "first_name",       // ← UPPER(first_name) needs this  
    "order_date"        // ← DATE_PART('month', order_date) needs this
])
.string_functions([
    "TRIM(email) AS clean_email",
    "UPPER(first_name) AS upper_name"
])
.datetime_functions([
    "DATE_PART('month', order_date) AS month"
])
.agg(["COUNT(*) AS total"])
.group_by_all()         // ✅ Automatically groups by all SELECT columns
```

### ✅ Option 2: Manual GROUP BY
```rust
df.select(["customer_name"]) 
.string_functions(["TRIM(email) AS clean_email"])
.agg(["COUNT(*) AS total"])
.group_by([
    "customer_name",
    "email"              // ← Manually specify function dependencies
])
```

### ❌ This won't work:
```rust
df.select(["customer_name"])           // ← Only customer_name
.string_functions(["TRIM(email)"])     // ← Function uses 'email' but it's not in SELECT
.group_by_all()                        // ❌ Error: email missing from GROUP BY
```

**Rule:** If your function uses a column, that column must be in `.select()` for `group_by_all()` to work.
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
    .order_by(["total_quantity"], ["DESC"]) 
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
        ("total_quantity", "ASC"), 
        ("p.ProductName", "DESC") 
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
    .order_by(["total_amount"], ["DESC"]); 

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
        ("total_order_quantity", "ASC"), 
        ("p.ProductName", "DESC") 
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
    .window("ROW_NUMBER() OVER (ORDER BY c.CustomerKey) AS customer_index")
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
> **📝 Note:** Window functions require any columns used in `PARTITION BY` or `ORDER BY` clauses to be included in your `.select([...])` statement. For example, if your window function uses `PARTITION BY region`, make sure `"region"` is in your select list.
---
## JSON functions
### .json() 
#### function works with Columns that only have simple JSON values
#### example json structure: 
```rust
[{"Key1":"Value1","Key2":"Value2","Key3":"Value3"}]
```
#### example usage
```rust
let path = "C:\\Borivoj\\RUST\\Elusion\\jsonFile.csv";
let json_df = CustomDataFrame::new(path, "j").await?;

let df_extracted = json_df.json([
    "ColumnName.'$Key1' AS column_name_1",
    "ColumnName.'$Key2' AS column_name_2",
    "ColumnName.'$Key3' AS column_name_3"
])
.select(["some_column1", "some_column2"])
.elusion("json_extract").await?;

df_extracted.display().await?;
```
```rust
RESULT:
+---------------+---------------+---------------+---------------+---------------+
| column_name_1 | column_name_2 | column_name_3 | some_column1  | some_column2  |
+---------------+---------------+---------------+---------------+---------------+
| registrations | 2022-09-15    | CustomerCode  | 779-0009E3370 | 646443D134762 |
| registrations | 2023-09-11    | CustomerCode  | 770-00009ED61 | 463497C334762 |
| registrations | 2017-10-01    | CustomerCode  | 889-000049C9E | 634697C134762 |
| registrations | 2019-03-26    | CustomerCode  | 000-00006C4D5 | 446397D134762 |
| registrations | 2021-08-31    | CustomerCode  | 779-0009E3370 | 463643D134762 |
| registrations | 2019-05-09    | CustomerCode  | 770-00009ED61 | 634697C934762 |
| registrations | 2005-10-24    | CustomerCode  | 889-000049C9E | 123397C334762 |
| registrations | 2023-02-14    | CustomerCode  | 000-00006C4D5 | 932393D134762 |
| registrations | 2021-01-20    | CustomerCode  | 779-0009E3370 | 323297C334762 |
| registrations | 2018-07-17    | CustomerCode  | 000-00006C4D5 | 322097C921462 |
+---------------+---------------+---------------+---------------+---------------+
```
### .json_array() 
#### function works with Columns that has Array of objects with pathern "column.'$ValueField:IdField=IdValue' AS column_alias"
The function parameters:
column: The column containing the JSON array
ValueField: The field to extract from matching objects
IdField: The field to use as identifier
IdValue: The value to match on the identifier field
column_alias: The output column name

#### example json structure
```rust
[
  {"Id":"Date","Value":"2022-09-15","ValueKind":"Date"},
  {"Id":"MadeBy","Value":"Borivoj Grujicic","ValueKind":"Text"},
  {"Id":"Timeline","Value":1.0,"ValueKind":"Number"},
  {"Id":"ETR_1","Value":1.0,"ValueKind":"Number"}
]
```
#### example usage
```rust
let multiple_values = df_json.json_array([
    "Value.'$Value:Id=Date' AS date",
    "Value.'$Value:Id=MadeBy' AS made_by",
    "Value.'$Value:Id=Timeline' AS timeline",
    "Value.'$Value:Id=ETR_1' AS etr_1",
    "Value.'$Value:Id=ETR_2' AS etr_2", 
    "Value.'$Value:Id=ETR_3' AS etr_3"
    ])
.select(["Id"])
.elusion("multiple_values")
.await?;

multiple_values.display().await?;

RESULT:
+-----------------+-------------------+----------+-------+-------+-------+--------+
| date            | made_by           | timeline | etr_1 | etr_2 | etr_3 | id     |
+-----------------+-------------------+----------+-------+-------+-------+--------+
| 2022-09-15      | Borivoj Grujicic  | 1.0      | 1.0   | 1.0   | 1.0   | 77E10C |
| 2023-09-11      |                   | 5.0      |       |       |       | 770C24 |
| 2017-10-01      |                   |          |       |       |       | 7795FA |
| 2019-03-26      |                   | 1.0      |       |       |       | 77F2E6 |
| 2021-08-31      |                   | 5.0      |       |       |       | 77926E |
| 2019-05-09      |                   |          |       |       |       | 77CC0F |
| 2005-10-24      |                   |          |       |       |       | 7728BA |
| 2023-02-14      |                   |          |       |       |       | 77F7F8 |
| 2021-01-20      |                   |          |       |       |       | 7731F6 |
| 2018-07-17      |                   | 3.0      |       |       |       | 77FB18 |
+-----------------+-------------------+----------+-------+-------+-------+--------+
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
let df1 = sales_df
.join(
    customers_df, ["s.CustomerKey = c.CustomerKey"], "INNER",
)
.select(["c.FirstName", "c.LastName"])
.string_functions([
    "TRIM(c.EmailAddress) AS trimmed_email",
    "CONCAT(TRIM(c.FirstName), ' ', TRIM(c.LastName)) AS full_name",
]);

let df2 = sales_df
.join(
    customers_df, ["s.CustomerKey = c.CustomerKey"], "INNER",
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
    .order_by(["order_date"], ["ASC"])
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
## EXTRACTING VALUES: extract_value_from_df()
#### Example how you can extract values from DataFrame and use it within REST API
```rust
//create calendar dataframe
 let date_calendar = CustomDataFrame::create_formatted_date_range_table(
    "2025-01-01", 
    "2025-12-31", 
    "dt", 
    "date".to_string(),
    DateFormat::HumanReadableTime, 
    true, 
    Weekday::Mon
).await?;

// take columns from Calendar
let week_range_2025 = date_calendar
    .select(["DISTINCT(week_start)","week_end", "week_num"])
    .order_by(["week_num"], ["ASC"])
    .elusion("wr")
    .await?;

// create empty dataframe
let temp_df = CustomDataFrame::empty().await?;

//populate empty dataframe with current week number
let current_week = temp_df
    .datetime_functions([
        "CAST(DATE_PART('week', CURRENT_DATE()) as INT) AS current_week_num",
    ])
    .elusion("cd").await?;

// join data frames to get range for current week
let week_for_api = week_range_2025
    .join(current_week,["wr.week_num == cd.current_week_num"], "INNER")
    .select(["TRIM(wr.week_start) AS datefrom", "TRIM(wr.week_end) AS dateto"])
    .elusion("api_week")
    .await?;

// Extract Date Value from DataFrame based on column name and Row Index
let date_from = extract_value_from_df(&week_for_api, "datefrom", 0).await?;
let date_to = extract_value_from_df(&week_for_api, "dateto", 0).await?;

//PRINT results for preview
week_for_api.display().await?;

println!("Date from: {}", date_from);
println!("Date to: {}", date_to);

RESULT:
+------------------+------------------+
| datefrom         | dateto           |
+------------------+------------------+
| 3 Mar 2025 00:00 | 9 Mar 2025 00:00 |
+------------------+------------------+

Date from: 3 Mar 2025 00:00
Date to: 9 Mar 2025 00:00

NOW WE CAN USE THESE EXTRACTED VALUES:

let post_df = ElusionApi::new();
post_df.from_api_with_dates(
    "https://jsonplaceholder.typicode.com/posts",  // url
    &date_from,  // date from
    &date_to,  // date to
    "C:\\Borivoj\\RUST\\Elusion\\JSON\\rest_api_data.json",  // path where json will be stored
).await?;
```
## EXTRACTING ROWS: extract_row_from_df()
#### Example how you can extract Row from DataFrame and use it within REST API.
```rust
//create calendar dataframe
 let date_calendar = CustomDataFrame::create_formatted_date_range_table(
    "2025-01-01", 
    "2025-12-31", 
    "dt", 
    "date".to_string(),
    DateFormat::IsoDate, 
    true, 
    Weekday::Mon
).await?;
//take columns from calendar
let week_range_2025 = date_calendar
    .select(["DISTINCT(week_start)","week_end", "week_num"])
    .order_by(["week_num"], ["ASC"])
    .elusion("wr")
    .await?;

// create empty dataframe
let temp_df = CustomDataFrame::empty().await?;

//populate empty dataframe with current week number
let current_week = temp_df
    .datetime_functions([
        "CAST(DATE_PART('week', CURRENT_DATE()) as INT) AS current_week_num",
    ])
    .elusion("cd").await?;

// join data frames to ge range for current week
let week_for_api = week_range_2025
    .join(current_week,["wr.week_num == cd.current_week_num"], "INNER")
    .select(["TRIM(wr.week_start) AS datefrom", "TRIM(wr.week_end) AS dateto"])
    .elusion("api_week")
    .await?;

// Extract Row Values from DataFrame based on Row Index
let row_values = extract_row_from_df(&week_for_api, 0).await?;

// PRINT row for preview
println!("DataFrame row: {:?}", row_values);

RESULT:
DataFrame row: {"datefrom": "2025-03-03", "dateto": "2025-03-09"}

NOW WE CAN USE THESE EXTRACTED ROW:

let post_df = ElusionApi::new();
post_df.from_api_with_dates(
    "https://jsonplaceholder.typicode.com/posts", // url
    row_values.get("datefrom").unwrap_or(&String::new()), // date from
    row_values.get("dateto").unwrap_or(&String::new()), // date to
    "C:\\Borivoj\\RUST\\Elusion\\JSON\\extraction_df2.json",  // path where json will be stored
).await?;
```
---
# CREATE VIEWS and CACHING (Native)
---
## Materialized Views:
For long-term storage of complex query results. When results need to be referenced by name. For data that changes infrequently.  Example: Monthly sales summaries, customer metrics, product analytics
### Query Caching:
For transparent performance optimization. When the same query might be run multiple times in a session. For interactive analysis scenarios. Example: Dashboard queries, repeated data exploration.
```rust
let sales = "C:\\Borivoj\\RUST\\Elusion\\SalesData2022.csv";
let products = "C:\\Borivoj\\RUST\\Elusion\\Products.csv";
let customers = "C:\\Borivoj\\RUST\\Elusion\\Customers.csv";

let sales_df = CustomDataFrame::new(sales, "s").await?;
let customers_df = CustomDataFrame::new(customers, "c").await?;
let products_df = CustomDataFrame::new(products, "p").await?;

//  Using materialized view for customer count
// The TTL parameter (3600) specifies how long the view remains valid in seconds (1 hour)
customers_df
    .select(["COUNT(*) as count"])
    .limit(10)
    .create_view("customer_count_view", Some(3600)) 
    .await?;

// Access the view by name - no recomputation needed
let customer_count = CustomDataFrame::from_view("customer_count_view").await?;
customer_count.display().await?;
// Example 2: Using query caching with complex joins and aggregations
// First execution computes and stores the result
let join_result = sales_df
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
        ("total_quantity", "ASC"),
        ("p.ProductName", "TRUE")
    ])
    .elusion_with_cache("sales_join") // caching query with DataFrame alias 
    .await?;

join_result.display().await?;

// Other useful cache/view management functions:
CustomDataFrame::invalidate_cache(&["table_name".to_string()]); // Clear cache for specific tables
CustomDataFrame::clear_cache(); // Clear entire cache
CustomDataFrame::refresh_view("view_name").await?; // Refresh a materialized view
CustomDataFrame::drop_view("view_name").await?; // Remove a materialized view
CustomDataFrame::list_views().await; // Get info about all views
```
---
# REDIS CACHING
### Redis Setup:
```bash
# Install Redis (Windows)
# Download from: https://github.com/tporadowski/redis/releases

# Install Redis (macOS)
brew install redis
brew services start redis

# Install Redis (Linux)
sudo apt install redis-server
sudo systemctl start redis

# Docker (All platforms)
docker run --name redis-cache -p 6379:6379 -d redis:latest

# Test connection
redis-cli ping  # Should return: PONG
```
### EXAMPLE USAGE:
```rust
let sales = "C:\\Borivoj\\RUST\\Elusion\\SalesData2022.csv";
let products = "C:\\Borivoj\\RUST\\Elusion\\Products.csv";
let customers = "C:\\Borivoj\\RUST\\Elusion\\Customers.csv";

let sales_df = CustomDataFrame::new(sales, "s").await?;
let customers_df = CustomDataFrame::new(customers, "c").await?;
let products_df = CustomDataFrame::new(products, "p").await?;

// Connect to Redis (requires Redis server running)
let redis_conn = CustomDataFrame::create_redis_cache_connection().await?;

// Use Redis caching for high-performance distributed caching
let redis_cached_result = sales_df
    .join_many([
        (customers_df, ["s.CustomerKey = c.CustomerKey"], "RIGHT"),
        (products_df, ["s.ProductKey = p.ProductKey"], "LEFT OUTER"),
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
        ("total_quantity", "ASC"),
        ("p.ProductName", "DESC")
    ])
    .elusion_with_redis_cache(&redis_conn, "sales_join_redis", Some(3600)) // Redis caching with 1-hour TTL
    .await?;

redis_cached_result.display().await?;
```
### Another option to connect to Redis is with Config
```rust
// Custom Redis connection with authentication
let redis_conn = CustomDataFrame::create_redis_cache_connection_with_config(
    "localhost",         // host
    6379,               // port  
    Some("password"),   // password (optional)
    Some(1)             // database (optional)
).await?;
```
### Clearing cache
```rust
// Clear Redis cache
CustomDataFrame::clear_redis_cache(&redis_conn, None).await?;

// Invalidate cache for specific tables
CustomDataFrame::invalidate_redis_cache(&redis_conn, &["sales", "customers"]).await?;
```
### Checking stats
```rust
    println!("📊 Getting Redis cache statistics...");

    let stats = CustomDataFrame::redis_cache_stats(&redis_conn).await?;

    println!("🔹 Cache Statistics:");
    println!("   📈 Total cached keys: {}", stats.total_keys);
    println!("   ✅ Cache hits: {}", stats.cache_hits);
    println!("   ❌ Cache misses: {}", stats.cache_misses);
    println!("   📊 Hit rate: {:.2}%", stats.hit_rate);
    println!("   💾 Memory used: {}", stats.total_memory_used);
    println!("   ⏱️  Average query time: {:.2}ms", stats.avg_query_time_ms);
    println!();
```
---
# Postgres Database Connector 
### Create Config, Conn and Query, and pass it to from_postgres() function.
```rust
 let pg_config = PostgresConfig {
        host: "localhost".to_string(),
        port: 5432,
        user: "borivoj".to_string(),
        password: "pass123".to_string(),
        database: "db_test".to_string(),
        pool_size: Some(5), 
    };

let conn = PostgresConnection::new(pg_config).await?;

Option2: You can use map_err()
let conn = PostgresConnection::new(pg_config).await
    .map_err(|e| ElusionError::Custom(format!("PostgreSQL connection error: {}", e)))?;

let query = "
    SELECT 
        c.id, 
        c.name, 
        s.product_name,
        SUM(s.quantity * s.price) as total_revenue
    FROM customers c
    LEFT JOIN sales s ON c.id = s.customer_id
    GROUP BY c.id, c.name, s.product_name
    ORDER BY total_revenue DESC
";

let sales_by_customer_df = CustomDataFrame::from_postgres(&conn, query, "postgres_df").await?;

sales_by_customer_df.display().await?;
```
---
# MySQL Database Connector 
### Create Config, Conn and Query, and pass it to from_mysql() function.
```rust
let mysql_config = MySqlConfig {
    host: "localhost".to_string(),
    port: 3306,
    user: "borivoj".to_string(),
    password: "pass123".to_string(),
    database: "db_test".to_string(),
    pool_size: Some(5),
};

let conn = MySqlConnection::new(mysql_config).await?;

let mysql_query = "
    WITH ranked_sales AS (
        SELECT 
            c.color AS brew_color, 
            bd.beer_style, 
            bd.location, 
            SUM(bd.total_sales) AS total_sales
        FROM 
            brewery_data bd
        JOIN 
            colors c ON bd.Color = c.color_number
        WHERE 
            bd.brew_date >= '2020-01-01' AND bd.brew_date <= '2020-03-01'
        GROUP BY 
            c.color, bd.beer_style, bd.location
    )
    SELECT 
        brew_color, 
        beer_style, 
        location, 
        total_sales,
        ROW_NUMBER() OVER (PARTITION BY brew_color ORDER BY total_sales DESC) AS ranked
    FROM 
        ranked_sales
    ORDER BY 
    brew_color, total_sales DESC";

let df = CustomDataFrame::from_mysql(&conn, mysql_query, "mysql_df").await?;

df.display().await?;
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
        Some("folder-name/file-name.csv"), // FILTERING is optional. Can be None if you want to take everything from Container
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
   .order_by(["total_bill"], ["ASC"]);

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
# REST API connectors
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

## Writing to EXCEL File ***needs excel feature enabled

#### EXCEL writer can only write or overwrite, so only 2 arguments needed
#### 1. Path, 2. Optional Sheet name. (default is Sheet1)
```rust
 df.write_to_excel(
    "C:\\Borivoj\\RUST\\Elusion\\Excel\\sales2.xlsx", //path
    Some("string_interop") // Optional sheet name. Can be None
).await?;
```
---
## Writing to Parquet File
#### We have 2 writing modes: **Overwrite** and **Append**
```rust
// overwrite existing file
df.write_to_parquet(
    "overwrite",
    "C:\\Path\\To\\Your\\test.parquet",
    None // I've set WriteOptions to default for writing Parquet files, so keep it None
)
.await?;

// append to exisiting file
df.write_to_parquet(
    "append",
    "C:\\Path\\To\\Your\\test.parquet",
    None // I've set WriteOptions to default for writing Parquet files, so keep it None
) 
.await?;
```
---
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
df.write_to_csv(
    "overwrite", 
    "C:\\Borivoj\\RUST\\Elusion\\agg_sales.csv", 
    custom_csv_options
)
.await?;

// append to exisiting file
df.write_to_csv(
    "append", 
    "C:\\Borivoj\\RUST\\Elusion\\agg_sales.csv", 
    custom_csv_options
)
.await?;

```
---
## Writing to JSON File

#### JSON writer can only overwrite, so only 2 arguments needed
#### 1. Path, 2. If you want pretty-printed JSON or not (true or false)
```rust
df.write_to_json(
    "C:\\Borivoj\\RUST\\Elusion\\date_table.json", // path
    true // pretty-printed JSON, false for compact JSON
).await?;
```
---
## Writing to DELTA table / lake 
#### We can write to delta in 2 modes **Overwrite** and **Append**
#### Partitioning column is OPTIONAL and if you decide to use column for partitioning, make sure that you don't need that column as you won't be able to read it back to dataframe
#### Once you decide to use partitioning column for writing your delta table, if you want to APPEND to it, append also need to have same column for partitioning
```rust
// Overwrite
df.write_to_delta_table(
    "overwrite",
    "C:\\Borivoj\\RUST\\Elusion\\agg_sales", 
    Some(vec!["order_date".into()]), 
)
.await
.expect("Failed to overwrite Delta table");
// Append
df.write_to_delta_table(
    "append",
    "C:\\Borivoj\\RUST\\Elusion\\agg_sales",
    Some(vec!["order_date".into()]),
)
.await
.expect("Failed to append to Delta table");
```
---
## Writing Parquet to Azure BLOB Storage 
#### We have 2 writing options "overwrite" and "append"
#### Writing is set to Default, Compression: SNAPPY and Parquet 2.0
#### Threshold file size is 1GB
```rust
let df = CustomDataFrame::new(csv_data, "sales").await?; 

let query = df.select(["*"]);

let data = query.elusion("df_sales").await?;

let url_to_folder_and_file_name = "https://your_storage_account_name.dfs.core.windows.net/your-container-name/folder/sales.parquet";
let sas_write_token = "your_sas_token"; // make sure SAS token has writing permissions

data.write_parquet_to_azure_with_sas(
    "overwrite",
    url_to_folder_and_file_name,
    sas_write_token
).await?;

// append version
data.write_parquet_to_azure_with_sas(
    "append",
    url_to_folder_and_file_name,
    sas_write_token
).await?;
```
---
## Writing JSON to Azure BLOB Storage 
#### Only can create new or overwrite exisitng file
#### Threshold file size is 1GB
```rust
let df = CustomDataFrame::new(csv_data, "sales").await?; 

let query = df.select(["*"]);

let data = query.elusion("df_sales").await?;

let url_to_folder_and_file_name = "https://your_storage_account_name.dfs.core.windows.net/your-container-name/folder/data.json";
let sas_write_token = "your_sas_token"; // make sure SAS token has writing permissions

data.write_json_to_azure_with_sas(
    url_to_folder_and_file_name,
    sas_write_token,
    true  // Set to true for pretty-printed JSON, false for compact JSON
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
    ("total_billable", "DESC"),  
    ("max_abs_billable", "ASC"), 
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
let summary_table = mix_res //Clone for multiple usages
    .select([
        "customer_name",
        "total_billable",
        "avg_abs_billable",
        "max_abs_billable",
        "percentage_total_billable"
    ])
    .order_by_many([
        ("total_billable", "DESC")
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
        ("order_date", "DESC"),
        ("abs_billable_value", "DESC")
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

# Contributing
I appreciate the interest in contributing to Elusion! However, I'm not currently accepting contributions.
- **Feature requests**: Feel free to message me if you need any new features - if possible, I'll be happy to implement them
- **Modifications**: You're welcome to fork the repository for your own changes
- **Issues**: Bug reports are always appreciated

Thanks for understanding!

---
### License
Elusion is distributed under the [MIT License](https://opensource.org/licenses/MIT). 
However, since it builds upon [DataFusion](https://datafusion.apache.org/), which is distributed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0), some parts of this project are subject to the terms of the Apache License 2.0.
For full details, see the [LICENSE.txt file](LICENSE.txt).

### Acknowledgments
This library leverages the power of Rust's type system and libraries like [DataFusion](https://datafusion.apache.org/)
,Appache Arrow, Tokio Cron Scheduler, Tokio... for efficient query processing. Special thanks to the open-source community for making this project possible.

## Where you can find me:
borivoj.grujicic@gmail.com
