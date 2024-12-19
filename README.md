# Elusion DataFrame Library

**Elusion** is a high-performance, flexible library for managing and querying data using a DataFrame-like interface. Designed for developers who need a powerful abstraction over data transformations, Elusion simplifies complex operations such as filtering, joining, aggregating, and more with an intuitive, chainable API.

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
