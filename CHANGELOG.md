## [6.0.0] - 2025-08-26
## BREAKING CHANGE
- Removed new_with_stream() function for "streaming loading" which was not actual streaming, because I couldnt overcome DataFusion file read for query planning for aggregations. Will se what i can do in the future..for now Elusion will work in-memory constraints.

## [5.4.1] - 2025-08-24
## Fixed
- Logic for deciding on different float formats:EU , US...

## [5.4.0] - 2025-08-23
## Fixed
- Excluded DataFusion loading into memory for STREAMING
## Added
- Streaming writers for .json, .csv and .parquet

## [5.3.4] - 2025-08-23
## Fixed
- Enhanced string types conversion for different qurencies and edge cases and formats for Streaming loading - new_with_stream() on different systems

## [5.3.3] - 2025-08-23
## Fixed
- Enhanced string and int types conversion for different qurencies and edge cases and formats for Streaming loading - new_with_stream() on different systems

## [5.3.2] - 2025-08-23
## Fixed
- Enhanced float types conversion for different qurencies and edge cases and formats for Streaming loading - new_with_stream() on different systems

## [5.3.1] - 2025-08-22
## Fixed
- Enhanced float types conversion for US and EU format for Streaming loading - new_with_stream() on different systems

## [5.3.0] - 2025-08-22
## Fixed
- Streaming loading - new_with_stream() - to read schema directly from file not from DataFrame, to avoid loading into memory.

## [5.2.0] - 2025-08-20
## Added / Enhanced
- Auto CSV file delimiter detection:   b'\t' => "tab (TSV)", b',' => "comma (CSV)", b';' => "semicolon", b'|' => "pipe"

## [5.1.0] - 2025-08-17
## Added
- Redis caching

## [5.0.0] - 2025-08-16
## BREAKING CHANGES
- Added `uses_group_by_all` field to `CustomDataFrame` struct
- Added new error variants: `GroupByAllWindowError`, `GroupByAllDependencyError`, `GroupByAllCompatibilityError`

### Features
- 🎉 Enhanced `group_by_all()` error messages with context-aware suggestions
- 🎉 Dynamic error messages using actual column names instead of hardcoded examples
- 🎉 Smart detection of window function dependency issues

### Improvements
- Better error messages for JOIN duplicate column issues
- Contextual suggestions based on actual query structure

## [4.1.0] - 2025-08-14
## Added
- Automatic column name expension while using select(["*"]) or select(["alias.*"]) for group_by_all() function.
- Error messages enhanced
- Enhanced base column and alias parsing 
- SQL Query generation inspection with display_query() and display_query_with_info()

## [4.0.1] - 2025-08-12
## Fixed
- Parsing aliases from string_functions() and datetime_functions(), into group_by() and group_by_all()

## [4.0.0] - 2025-08-10
## BREAKING CHANGES
- **CustomDataFrame struct**: Added 6 new fields for state tracking
- **order_by() and order_by_many()*: Now accept "ASC" and "DESC" instead of true, false

### BE AWARE
- Existing compiled code must be recompiled due to internal struct changes
- All order_by() and order_by_many() need to comply with new implementation

## New Features
- **Stream laoding for CSV files loading with CustomDataFrame::new_with_stream() function
- **Smart Type Detection**: Email, version number, and edge case handling
- **Enhanced Error Messages**: Better debugging information
- **State Tracking**: Raw field tracking for optimization

## Performance Improvements
- **Regex Optimization**: Lazy compilation reduces startup time
- **Async Performance**: CPU-intensive operations moved to thread pool
- **Memory Efficiency**: Optimized cloning and buffer allocation
- **SQL Building**: Reduced string allocations in query construction

## New Functions
- **df_schema()**: Print Column names and their types

## Bug Fixes
- Fixed edge cases in date parsing
- Fixed complex parsing forneset string functions like CONCAT with nested TRIM, CAST etc.
- Improved handling of malformed numeric data
- Better validation for thousand separators for CSV loading

## Dependencies
- once_cell = "1.21.3"
- Enhanced Arrow compatibility

## [3.14.0] - 2025-08-08
### Added
FUNCTIONS:
- show_head(n) that Show first n rows
- show_tail(n) that Show last n rows
- peek(n) that Show first and last n rows 
- fill_null() that fill nulls in specified column
- drop_null() that drop rows with null values in specified column
- skip_rows() that skip number of specified rows

## [3.13.3] - 2025-08-07
### Bug Fixed
- Postgres SQL timestamp conversion
### Added
- Azure Blob Storage timeout for controlled reading times
### Enhanced
- Azure blob storage direct file reading vs full path reading
- Skiping empty/small json files < 3kb for blob reading
- Writers extension check

## [3.13.2] - 2025-08-02
### Bug Fixed
- Fixed unpivot() column name normalization - Column names now properly converted to lowercase

## [3.13.1] - 2025-08-02
### Fixed
- Features dependency clash

## [3.13.0] - 2025-08-02
### BREAKING CHANGE
- REMOVED: Individul Sharepoint functions: load_csv_from_sharepoint(),load_excel_from_sharepoint(),load_json_from_sharepoint(),load_parquet_from_sharepoint(),
### Added
- Single loading Sharepoint function that auto-reconizes file extension: load_from_sharepoint()
- Readling multiple files with same schema/columns from local folder (csv, json, excel, parquet): load_folder(), load_folder_with_filename_column()

## [3.12.5] - 2025-07-23
### Added
- Sharepoint connector early checkout after path is found

## [3.12.4] - 2025-07-23
### Fixed
- Sharepoint connector for users with probelms for Unicode encoding error with the character \u010d (which is č)

## [3.12.3] - 2025-07-22
### Added
- load_folder_from_sharepoint_with_filename_column() function for SharePoint connector that read all files from folder, adds filename column and UNION ALL to single dataframe.

## [3.12.2] - 2025-07-22
### Added
- load_folder_from_sharepoint() function for SharePoint connector that read all files from folder and UNION ALL to single dataframe.

## [3.12.1] - 2025-07-21
### Added
- FILL DOWN function fill_down() that fills down null values in column with firs non null values above

## [3.12.0] - 2025-07-20
### Added
- SharePoint connection with AzureCLI
- load_excel_from_sharepoint(), load_csv_from_sharepoint(), load_json_from_sharepoint(), load_parquet_from_sharepoint()

## [3.11.0] - 2025-05-17
### Added
- EXCEL reader/loader

## [3.10.0] - 2025-05-17
### Added
- EXCEL Feature. write_to_excel() function 

## [3.9.0] - 2025-05-08
### Added
- MYSQL Database connectivity
## Fixed
- Filter lowercasing column name
- AS case insensitivity for .json() and .json_array()

## [3.8.1] - 2025-05-07
### Fixed
- Client import clash from API (reqwest::Client) and POSTGRES (tokio_postgres::Client) features
### Updated
- tokio from `1.41.1` to `1.45.0`

## [3.8.0] - 2025-05-07
### Added
- POSTGRES Database connectivity

## [3.7.5] - 2025-05-06
### Fixed
- Fixing features behind the Flag

## [3.7.4] - 2025-05-06
### Added
- json_array() function for parsing json values from DataFrame columns

## [3.7.3] - 2025-05-06
### BREAKING CHANGE
- Removed ODBC features
### Added
- json() function for parsing json values from DataFrame columns
- Moved Azure Blob Storage behind feature flag


## [3.7.2] - 2025-05-04
### Added
- Moved Dashboards/Reporting and APIs behind feature flag
### Dependency update
- Tokio `1.42.0` to `1.42.1`

## [3.7.1] - 2025-03-09
### Added
- extract_value_from_df() that extracts Value from a DataFrame based on column name and row index
- extract_row_from_df() that extracts Row from a DataFrame as a HashMap based on row index

## [3.7.0] - 2025-03-09
### Added
- write_to_json() for writing to local JSON files
- write_json_to_azure_with_sas() for writing JSON to Azure Blob Storage with SAS token

## [3.5.1] - 2025-03-08
### Added
- create_formatted_date_range_table() function that will create Date Table with custom Formats (ISO, European, HumanReadanle...) and week, month, quarter and year ranges (start -end)

## [3.5.0] - 2025-03-02
### Added
- empty() function that will crate empty dataframe taht can be populated
- create_date_range_table() function that will create Date Table

## [3.4.1] - 2025-03-02
### Renamed
- Renamed function from_materialized_view() to from_view()

## [3.4.0] - 2025-03-02
### Added
- Caching and Materialized Views

## [3.3.1] - 2025-02-26
### Fixed build 
- Update to arrow crate and arrow-arith crate made build conflict with chrono crate. DOwngraded datafusion to `43.0.0`
arrow fixed to `=53.2.0`, chrono fixed to `=0.4.38`

## [3.3.0] - 2025-02-21
### Fixed
- Select() AS not to get _ in between column name and AS

## [3.2.0] - 2025-02-20
### Fixed
- Query flexible query ordering for select, string, agg, filter and datetime

## [3.1.0] - 2025-02-17
### Fixed
- filter() condition Case Sensitivity

## [3.0.0] - 2025-02-17
### BREAKING CHANGE
- ALL Query Expresions, DataFrame Columns and Functions are normalized LOWERCASE(), TRIM(), REPLACE(" ", "_")

## [2.8.0] - 2025-02-16
### Added
- DateTime Functions within datetime_functions()
### FeatureFlag
- Moved ODBC behind feature flag

## [2.7.0] - 2025-02-11
### Added
- Interactive tables for Reporting

## [2.5.1] - 2025-02-09
### Added
- Interactive Dashboarding

## [2.5.0] - 2025-02-09
### REAKING CHANGE
- Now write_parquet_to_azure_with_sas() function needs specified writing mode: overwrite or append
### Fixed
- CSV writer
### Added
- Dependency tempfile `3.16.0`

## [2.4.3] - 2025-02-09
### Fixed
- Appending data for Parquet Writer

## [2.4.2] - 2025-02-08
### Added
- Custom ERROR handling to UNION, APPEND...

## [2.4.1] - 2025-02-07
### Added
- Custom ERROR handling

## [2.4.0] - 2025-02-06
### Fixed
- Fixed UNION, UNION_ALL, EXCEPT, INTESECT now they return proper results
### BREAKING CHANGE
- UNION, UNION_ALL, EXCEPT, INTESECT are now async and also need to be evaluated with elusion() - check readme.md for examples
### Added
- Fixed APPEND, APPEND_ALL

## [2.3.0] - 2025-02-04
### Fixed
- Fixed case sensitivity within statistical functions

## [2.2.0] - 2025-02-03
### Fixed
- Improved parsing for single dataframes, for all functions, to avoid using aliases on single dataframes

## [2.1.0] - 2025-02-02
### BREAKING CHANGE
- REST API now must use file path + json file name as argument. ex: "C:\\Borivoj\\RUST\\Elusion\\sales_jan_2025.json"

## [2.0.0] - 2025-01-31
### Added
- REST API to JSON files
### BREAKING CHANGES
- REST API is now detached from CustomDataFrame (check readme for examples)

## [1.7.2] - 2025-01-31
### Added
- Improved Reading JSON files performance by 50%

## [1.7.1] - 2025-01-29
### Added
- Wriring Parquet to Azure Blob Storage

## [1.7.0] - 2025-01-28
### Removed
- REST API (until I fix it)

## [1.5.1] - 2025-01-28
### Added
- URL Encoding for REST API params and headers 
### Added
- Dependencies: `urlencoding` `2.1.3`

## [1.5.0] - 2025-01-27
### Added
- Reading Data From API into CustomDataFrame
### Added
- Dependencies: `reqwest` `0.12`

## [1.4.0] - 2025-01-26
### Added
- Pipeline Scheduler
### Added
- Dependencies: `tokio-cron-scheduler` `0.13.0`

## [1.3.0] - 2025-01-25
### Added
- Azure Blob Connection. You can connect and download .json or .csv files with from_azure_with_sas_token() function 
### Added
- Dependencies: `azure_storage_blobs` `0.21.0`, `azure_storage` `0.21.0`, `csv` `1.1`

## [1.2.0] - 2025-01-24
### Added
- ODBC Database connections for MySQL and PostgreSQL
### Added
- Dependencies: `lazy_static` `1.5.0`, `arrow-odbc` `14.1.0`

## [1.1.1] - 2025-01-21
### Added
- Statistical Functions: display_stats(), display_null_analysis(), display_correlation_matrix()

## [1.1.0] - 2025-01-21
### Added
- Dependencies: `plotly` `0.12.1` with Plots: Line, TimeSeries, Bar, Pie, Donut, Histogram, Box

## [1.0.1] - 2025-01-20
### Updated
- Platform Compatibility (MacOS, Linux, Microsoft) and Code/Dependencies Audit

## [1.0.0] - 2025-01-19
### BREAKING CHANGE
- JOIN and JOIN_MANY functions now can receive multiple arguments
### Updated
- Handling conditions within String Functions and Aggregate functions
### MAJOR RELEASE
- Library fully tested and ready for production

## [0.5.8] - 2025-01-18
### Added
- PIVOT and UNPIVOT functions
### Updated
- Dependencies: `datafusion` to `44.0.0`

## [0.5.7] - 2025-01-12
### Fixed
- Window function to proprely parse multiple arguments within aggregation, analytics and ranking

## [0.5.5] - 2025-01-12
### Added
- except() and intersect()

## [0.5.4] - 2025-01-12
### Added
- union() and union_all()

## [0.5.3] - 2025-01-09
### Fixed
- Multiple nested functions in SELECT()
### Added
- group_by_all() function that Takes all non-aggregated columns from SELECT

## [0.5.2] - 2025-01-10
### Added
- `String Functions` that can be applied on string columns

## [0.5.1] - 2025-01-09
### Fixed
- Scalar and Aggregation function parsing, for single and nested functions

## [0.5.0] - 2025-01-07
### BREAKING CHANGE
- Removed AggegationBuilder now we can use agg() for aggregations
- Removed SQL Support as DataFrame API considerably developed and there is not much need of raw SQL moving forward. If there is a demmand for Raw SQL i will bring it back in v1.0.0

## [0.4.0] - 2025-01-06
### BREAKING CHANGE
- No more use of vec![] in DataFrame API Query Functions

## [0.3.0] - 2025-01-05
### Added
- DELTA table Writer and Reader

## [0.2.5] - 2025-01-02
### Added
- PARQUET reader
- Removed manual SCHEMA declaration, now CustomDataFrame::new() only need file path and alias 

## [0.2.4] - 2025-01-01
### Fixed
- JOIN for multiple dataframes
- HAVING and FILTER functions fixed

## [0.2.3] - 2024-12-29
### Added
- CSV writer

## [0.2.2] - 2024-12-28
### Added
- Dependencies: `serde`  `1.0.216`, `serde_json` `1.0.134`
- Support for JSON files: Reading and Loading to CustomDataFrame
### Fixed
- Improved display() function for better formating.

## [0.2.0] - 2024-12-24
### Added
- Full Raw SQL Querying support

## [0.1.3] - 2024-12-23
### Added
- Aliasing column names directly in select() function

## [0.1.1] - 2024-12-21
### Added
- Added support for `prelude` to simplify imports for users.

### Fixed
- Improved error handling and clarified documentation.

### Updated
- Dependencies: `chrono` to `0.4.39` , `tokio` to `1.42.0`
