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
