use crate::prelude::*;

pub enum DateFormat {
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
}

impl DateFormat {
    pub fn format_str(&self) -> &str {
        match self {
            DateFormat::IsoDate => "%Y-%m-%d",
            DateFormat::IsoDateTime => "%Y-%m-%d %H:%M:%S",
            DateFormat::UsDate => "%m/%d/%Y",
            DateFormat::EuropeanDate => "%d.%m.%Y",
            DateFormat::EuropeanDateDash => "%d-%m-%Y",
            DateFormat::BritishDate => "%d/%m/%Y",
            DateFormat::HumanReadable => "%e %b %Y",
            DateFormat::HumanReadableTime => "%e %b %Y %H:%M",
            DateFormat::SlashYMD => "%Y/%m/%d",
            DateFormat::DotYMD => "%Y.%m.%d",
            DateFormat::CompactDate => "%Y%m%d",
            DateFormat::YearMonth => "%Y-%m",
            DateFormat::MonthYear => "%m-%Y",
            DateFormat::MonthNameYear => "%B %Y",
            DateFormat::Custom(fmt) => fmt,
        }
    }
}

    /// Create a date range table with multiple date formats and period ranges
    pub async fn create_formatted_date_range_table_impl(
        start_date: &str,
        end_date: &str,
        alias: &str,
        format_name: String,
        format: DateFormat,
        include_period_ranges: bool,
        week_start_day: Weekday
    ) -> ElusionResult<CustomDataFrame> {
    
        let start = NaiveDate::parse_from_str(start_date, "%Y-%m-%d")
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Date Parsing".to_string(),
                reason: format!("Failed to parse start_date '{}': {}", start_date, e),
                suggestion: "💡 Ensure date format is YYYY-MM-DD".to_string(),
            })?;
            
        let end = NaiveDate::parse_from_str(end_date, "%Y-%m-%d")
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Date Parsing".to_string(),
                reason: format!("Failed to parse end_date '{}': {}", end_date, e),
                suggestion: "💡 Ensure date format is YYYY-MM-DD".to_string(),
            })?;
            
        if end < start {
            return Err(ElusionError::InvalidOperation {
                operation: "Date Range Validation".to_string(),
                reason: format!("End date '{}' is before start date '{}'", end_date, start_date),
                suggestion: "💡 Ensure end_date is after or equal to start_date".to_string(),
            });
        }
        
        let days = end.signed_duration_since(start).num_days() as usize + 1;
        
        // Check if format includes time components
        let format_str = format.format_str();
        let has_time_components = format_str.contains("%H") || 
                                 format_str.contains("%M") || 
                                 format_str.contains("%S") ||
                                 format_str.contains("%I") ||
                                 format_str.contains("%p") ||
                                 format_str.contains("%P");
        
        // create builder for the date column
        let mut date_builder = StringBuilder::new();
        
        let mut year_builder = Int32Builder::new();
        let mut month_builder = Int32Builder::new();
        let mut day_builder = Int32Builder::new();
        let mut quarter_builder = Int32Builder::new();
        let mut week_num_builder = Int32Builder::new();
        let mut day_of_week_builder = Int32Builder::new();
        let mut day_of_week_name_builder = StringBuilder::new();
        let mut day_of_year_builder = Int32Builder::new();
        let mut is_weekend_builder = BooleanBuilder::new();
        
        let mut week_start_builder = StringBuilder::new();
        let mut week_end_builder = StringBuilder::new();
        let mut month_start_builder = StringBuilder::new();
        let mut month_end_builder = StringBuilder::new();
        let mut quarter_start_builder = StringBuilder::new();
        let mut quarter_end_builder = StringBuilder::new();
        let mut year_start_builder = StringBuilder::new();
        let mut year_end_builder = StringBuilder::new();
    
        for day_offset in 0..days {
            let current_date = start + chrono::Duration::days(day_offset as i64);
            
            // Format date with the specified format, handling time components if present
            let formatted_date = if has_time_components {
                // Convert date to datetime at midnight for formats with time components
                let datetime = NaiveDateTime::new(
                    current_date, 
                    NaiveTime::from_hms_opt(0, 0, 0).unwrap()
                );
                datetime.format(format_str).to_string()
            } else {
                current_date.format(format_str).to_string()
            };
            
            date_builder.append_value(formatted_date);
            
            // year
            year_builder.append_value(current_date.year());
            
            // month
            month_builder.append_value(current_date.month() as i32);
            
            // day of month
            day_builder.append_value(current_date.day() as i32);
            
            // quarter
            let quarter = ((current_date.month() - 1) / 3 + 1) as i32;
            quarter_builder.append_value(quarter);
            
            // week number
            let week_num = current_date.iso_week().week() as i32;
            week_num_builder.append_value(week_num);
            
            // day of week (0 = Monday, 6 = Sunday if week_start_day is Monday)
            // or (0 = Sunday, 6 = Saturday if week_start_day is Sunday)
            let adjusted_weekday = match week_start_day {
                Weekday::Mon => (current_date.weekday().num_days_from_monday()) as i32,
                Weekday::Sun => (current_date.weekday().num_days_from_sunday()) as i32,
                _ => ((current_date.weekday().num_days_from_monday() + 
                     7 - week_start_day.num_days_from_monday()) % 7) as i32,
            };
            day_of_week_builder.append_value(adjusted_weekday);
            
            let day_name = match current_date.weekday() {
                Weekday::Mon => "Monday",
                Weekday::Tue => "Tuesday",
                Weekday::Wed => "Wednesday",
                Weekday::Thu => "Thursday",
                Weekday::Fri => "Friday",
                Weekday::Sat => "Saturday",
                Weekday::Sun => "Sunday",
            };
            day_of_week_name_builder.append_value(day_name);
            
            // day of year
            day_of_year_builder.append_value(current_date.ordinal() as i32);
            
            // weekend flag (Saturday and Sunday)
            is_weekend_builder.append_value(current_date.weekday() == Weekday::Sat || 
                                           current_date.weekday() == Weekday::Sun);
            
            if include_period_ranges {
                // week start and end
                let days_since_week_start = current_date.weekday().num_days_from_monday() as i64;
                let adjusted_days = match week_start_day {
                    Weekday::Mon => days_since_week_start,
                    // if week starts on Sunday and today is Sunday, then it's 0 days from week start
                    Weekday::Sun => if current_date.weekday() == Weekday::Sun { 0 } 
                                            else { (days_since_week_start + 1) % 7 },
                    // for other start days, calculating the offset
                    _ => (days_since_week_start + 7 - 
                         week_start_day.num_days_from_monday() as i64) % 7,
                };
                
                let week_start = current_date - Duration::days(adjusted_days);
                let week_end = week_start + Duration::days(6); // Week end is 6 days after start
                
                // month start (first day of month)
                let month_start = NaiveDate::from_ymd_opt(
                    current_date.year(), current_date.month(), 1).unwrap();
                
                // month end (last day of month)
                let month_end = if current_date.month() == 12 {
                    NaiveDate::from_ymd_opt(current_date.year() + 1, 1, 1).unwrap() - Duration::days(1)
                } else {
                    NaiveDate::from_ymd_opt(current_date.year(), current_date.month() + 1, 1).unwrap() - Duration::days(1)
                };
                
                // quarter start
                let quarter_start_month = ((quarter - 1) * 3 + 1) as u32;
                let quarter_start = chrono::NaiveDate::from_ymd_opt(
                    current_date.year(), quarter_start_month, 1).unwrap();
                
                // quarter end
                let quarter_end_month = quarter_start_month + 2;
                let quarter_end_year = if quarter_end_month > 12 {
                    current_date.year() + 1
                } else {
                    current_date.year()
                };
                let quarter_end_month_adj = if quarter_end_month > 12 {
                    quarter_end_month - 12
                } else {
                    quarter_end_month
                };
                
                let quarter_end = if quarter_end_month_adj == 12 {
                    chrono::NaiveDate::from_ymd_opt(quarter_end_year + 1, 1, 1).unwrap() - chrono::Duration::days(1)
                } else {
                    chrono::NaiveDate::from_ymd_opt(quarter_end_year, quarter_end_month_adj + 1, 1).unwrap() - chrono::Duration::days(1)
                };
                
                // year start and end
                let year_start = NaiveDate::from_ymd_opt(current_date.year(), 1, 1).unwrap();
                let year_end = NaiveDate::from_ymd_opt(current_date.year(), 12, 31).unwrap();
                
                // Format period dates, handling time components if present
                let format_period_date = |date: NaiveDate| -> String {
                    if has_time_components {
                        // Add midnight time for formats with time components
                        let datetime = NaiveDateTime::new(
                            date, 
                            NaiveTime::from_hms_opt(0, 0, 0).unwrap()
                        );
                        datetime.format(format_str).to_string()
                    } else {
                        date.format(format_str).to_string()
                    }
                };
                // Apply formatting to all period dates
                week_start_builder.append_value(format_period_date(week_start));
                week_end_builder.append_value(format_period_date(week_end));
                month_start_builder.append_value(format_period_date(month_start));
                month_end_builder.append_value(format_period_date(month_end));
                quarter_start_builder.append_value(format_period_date(quarter_start));
                quarter_end_builder.append_value(format_period_date(quarter_end));
                year_start_builder.append_value(format_period_date(year_start));
                year_end_builder.append_value(format_period_date(year_end));
            }
        }
        
        let mut fields = Vec::new();
        let mut columns = Vec::new();
        
        // date format column
        fields.push(Field::new(&format_name, ArrowDataType::Utf8, false));
        columns.push(Arc::new(date_builder.finish()) as Arc<dyn Array>);
        
        // date parts columns
        fields.push(Field::new("year", ArrowDataType::Int32, false));
        columns.push(Arc::new(year_builder.finish()) as Arc<dyn Array>);
        
        fields.push(Field::new("month", ArrowDataType::Int32, false));
        columns.push(Arc::new(month_builder.finish()) as Arc<dyn Array>);
        
        fields.push(Field::new("day", ArrowDataType::Int32, false));
        columns.push(Arc::new(day_builder.finish()) as Arc<dyn Array>);
        
        fields.push(Field::new("quarter", ArrowDataType::Int32, false));
        columns.push(Arc::new(quarter_builder.finish()) as Arc<dyn Array>);
        
        fields.push(Field::new("week_num", ArrowDataType::Int32, false));
        columns.push(Arc::new(week_num_builder.finish()) as Arc<dyn Array>);
        
        fields.push(Field::new("day_of_week", ArrowDataType::Int32, false));
        columns.push(Arc::new(day_of_week_builder.finish()) as Arc<dyn Array>);
        
        fields.push(Field::new("day_of_week_name", ArrowDataType::Utf8, false));
        columns.push(Arc::new(day_of_week_name_builder.finish()) as Arc<dyn Array>);
        
        fields.push(Field::new("day_of_year", ArrowDataType::Int32, false));
        columns.push(Arc::new(day_of_year_builder.finish()) as Arc<dyn Array>);
        
        fields.push(Field::new("is_weekend", ArrowDataType::Boolean, false));
        columns.push(Arc::new(is_weekend_builder.finish()) as Arc<dyn Array>);
        
        // period range columns
        if include_period_ranges {
            fields.push(Field::new("week_start", ArrowDataType::Utf8, false));
            columns.push(Arc::new(week_start_builder.finish()) as Arc<dyn Array>);
            
            fields.push(Field::new("week_end", ArrowDataType::Utf8, false));
            columns.push(Arc::new(week_end_builder.finish()) as Arc<dyn Array>);
            
            fields.push(Field::new("month_start", ArrowDataType::Utf8, false));
            columns.push(Arc::new(month_start_builder.finish()) as Arc<dyn Array>);
            
            fields.push(Field::new("month_end", ArrowDataType::Utf8, false));
            columns.push(Arc::new(month_end_builder.finish()) as Arc<dyn Array>);
            
            fields.push(Field::new("quarter_start", ArrowDataType::Utf8, false));
            columns.push(Arc::new(quarter_start_builder.finish()) as Arc<dyn Array>);
            
            fields.push(Field::new("quarter_end", ArrowDataType::Utf8, false));
            columns.push(Arc::new(quarter_end_builder.finish()) as Arc<dyn Array>);
            
            fields.push(Field::new("year_start", ArrowDataType::Utf8, false));
            columns.push(Arc::new(year_start_builder.finish()) as Arc<dyn Array>);
            
            fields.push(Field::new("year_end", ArrowDataType::Utf8, false));
            columns.push(Arc::new(year_end_builder.finish()) as Arc<dyn Array>);
        }
        
        let schema = Arc::new(Schema::new(fields));
        
        let record_batch = RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| ElusionError::Custom(
                format!("Failed to create record batch: {}", e)
            ))?;
        
        let ctx = SessionContext::new();
        
        let mem_table = MemTable::try_new(schema.clone().into(), vec![vec![record_batch]])
            .map_err(|e| ElusionError::SchemaError {
                message: format!("Failed to create date table: {}", e),
                schema: Some(schema.to_string()),
                suggestion: "💡 This is likely an internal error".to_string(),
            })?;
        
        ctx.register_table(alias, Arc::new(mem_table))
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Table Registration".to_string(),
                reason: format!("Failed to register date table: {}", e),
                suggestion: "💡 Try a different alias name".to_string(),
            })?;
        
        let df = ctx.table(alias).await
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "DataFrame Creation".to_string(),
                reason: format!("Failed to create date table DataFrame: {}", e),
                suggestion: "💡 Verify table registration succeeded".to_string(),
            })?;
        
        Ok(CustomDataFrame {
            df,
            table_alias: alias.to_string(),
            from_table: alias.to_string(),
            selected_columns: Vec::new(),
            alias_map: Vec::new(),
            aggregations: Vec::new(),
            group_by_columns: Vec::new(),
            where_conditions: Vec::new(),
            having_conditions: Vec::new(),
            order_by_columns: Vec::new(),
            limit_count: None,
            joins: Vec::new(),
            window_functions: Vec::new(),
            ctes: Vec::new(),
            subquery_source: None,
            set_operations: Vec::new(),
            query: String::new(),
            aggregated_df: None,
            union_tables: None,
            original_expressions: Vec::new(),
            needs_normalization: false,
            raw_selected_columns: Vec::new(),
            raw_group_by_columns: Vec::new(),
            raw_where_conditions: Vec::new(),
            raw_having_conditions: Vec::new(),
            raw_join_conditions: Vec::new(),
            raw_aggregations: Vec::new(),
        })
    }

    /// A CustomDataFrame containing a date table with one row per day in the range
    pub async fn create_date_range_table_impl(
        start_date: &str,
        end_date: &str,
        alias: &str
    ) -> ElusionResult<CustomDataFrame> {

        let start = NaiveDate::parse_from_str(start_date, "%Y-%m-%d")
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Date Parsing".to_string(),
                reason: format!("Failed to parse start_date '{}': {}", start_date, e),
                suggestion: "💡 Ensure date format is YYYY-MM-DD".to_string(),
            })?;
            
        let end = NaiveDate::parse_from_str(end_date, "%Y-%m-%d")
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Date Parsing".to_string(),
                reason: format!("Failed to parse end_date '{}': {}", end_date, e),
                suggestion: "💡 Ensure date format is YYYY-MM-DD".to_string(),
            })?;
            
        if end < start {
            return Err(ElusionError::InvalidOperation {
                operation: "Date Range Validation".to_string(),
                reason: format!("End date '{}' is before start date '{}'", end_date, start_date),
                suggestion: "💡 Ensure end_date is after or equal to start_date".to_string(),
            });
        }
        
        let duration = end.signed_duration_since(start);
        let days = duration.num_days() as usize + 1; // Include the end date

        let mut date_array = StringBuilder::new();
        let mut year_array = Int32Builder::new();
        let mut month_array = Int32Builder::new();
        let mut day_array = Int32Builder::new();
        let mut quarter_array = Int32Builder::new();
        let mut week_num_array = Int32Builder::new();
        let mut day_of_week_array = Int32Builder::new();
        let mut day_of_week_name_array = StringBuilder::new(); 
        let mut day_of_year_array = Int32Builder::new();
        let mut week_start_array = StringBuilder::new();
        let mut month_start_array = StringBuilder::new();
        let mut quarter_start_array = StringBuilder::new();
        let mut year_start_array = StringBuilder::new();
        let mut is_weekend_array = BooleanBuilder::new();
        
        // Generate data for each day in the range
        for day_offset in 0..days {
            let current_date = start + chrono::Duration::days(day_offset as i64);
            
            // Date string (YYYY-MM-DD)
            date_array.append_value(current_date.format("%Y-%m-%d").to_string());
            
            // Year
            year_array.append_value(current_date.year());
            
            // Month
            month_array.append_value(current_date.month() as i32);
            
            // Day of month
            day_array.append_value(current_date.day() as i32);
            
            // Quarter
            let quarter = ((current_date.month() - 1) / 3 + 1) as i32;
            quarter_array.append_value(quarter);
            
            // Week number
            let week_num = current_date.iso_week().week() as i32;
            week_num_array.append_value(week_num);
            
            // Day of week (0 = Sunday, 6 = Saturday)
            let day_of_week = current_date.weekday().number_from_sunday() - 1;
            day_of_week_array.append_value(day_of_week as i32);
            
            // Day of week name (Monday, Tuesday, etc.)
            let day_name = match current_date.weekday() {
                Weekday::Mon => "Monday",
                Weekday::Tue => "Tuesday",
                Weekday::Wed => "Wednesday",
                Weekday::Thu => "Thursday",
                Weekday::Fri => "Friday",
                Weekday::Sat => "Saturday",
                Weekday::Sun => "Sunday",
            };
            day_of_week_name_array.append_value(day_name);
            
            // Day of year
            let day_of_year = current_date.ordinal() as i32;
            day_of_year_array.append_value(day_of_year);
            
            // Week start (first day of the week) is sunday
            let week_start = current_date - chrono::Duration::days(current_date.weekday().number_from_sunday() as i64 - 1);
            week_start_array.append_value(week_start.format("%Y-%m-%d").to_string());
            
            // Month start
            let month_start = chrono::NaiveDate::from_ymd_opt(current_date.year(), current_date.month(), 1)
                .unwrap_or(current_date);
            month_start_array.append_value(month_start.format("%Y-%m-%d").to_string());
            
            // Quarter start
            let quarter_start = chrono::NaiveDate::from_ymd_opt(
                current_date.year(), 
                ((quarter - 1) * 3 + 1) as u32, 
                1
            ).unwrap_or(current_date);
            quarter_start_array.append_value(quarter_start.format("%Y-%m-%d").to_string());
            
            // Year start
            let year_start = chrono::NaiveDate::from_ymd_opt(current_date.year(), 1, 1)
                .unwrap_or(current_date);
            year_start_array.append_value(year_start.format("%Y-%m-%d").to_string());
            
            // Weekend flag (Saturday = 6, Sunday = 0)
            // Changed to correctly flag Saturday and Sunday as weekend
            is_weekend_array.append_value(current_date.weekday() == chrono::Weekday::Sat || 
                                          current_date.weekday() == chrono::Weekday::Sun);
        }
        
        // Create a comprehensive schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("date", ArrowDataType::Utf8, false),
            Field::new("year", ArrowDataType::Int32, false),
            Field::new("month", ArrowDataType::Int32, false),
            Field::new("day", ArrowDataType::Int32, false),
            Field::new("quarter", ArrowDataType::Int32, false),
            Field::new("week_num", ArrowDataType::Int32, false),
            Field::new("day_of_week", ArrowDataType::Int32, false),
            Field::new("day_of_week_name", ArrowDataType::Utf8, false), 
            Field::new("day_of_year", ArrowDataType::Int32, false),
            Field::new("week_start", ArrowDataType::Utf8, false),
            Field::new("month_start", ArrowDataType::Utf8, false),
            Field::new("quarter_start", ArrowDataType::Utf8, false),
            Field::new("year_start", ArrowDataType::Utf8, false),
            Field::new("is_weekend", ArrowDataType::Boolean, false),
        ]));
        
        // Create the record batch with all columns
        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(date_array.finish()),
                Arc::new(year_array.finish()),
                Arc::new(month_array.finish()),
                Arc::new(day_array.finish()),
                Arc::new(quarter_array.finish()),
                Arc::new(week_num_array.finish()),
                Arc::new(day_of_week_array.finish()),
                Arc::new(day_of_week_name_array.finish()), 
                Arc::new(day_of_year_array.finish()),
                Arc::new(week_start_array.finish()),
                Arc::new(month_start_array.finish()),
                Arc::new(quarter_start_array.finish()),
                Arc::new(year_start_array.finish()),
                Arc::new(is_weekend_array.finish()),
            ]
        ).map_err(|e| ElusionError::Custom(
            format!("Failed to create record batch: {}", e)
        ))?;
        
        // Create a session context for SQL execution
        let ctx = SessionContext::new();
        
        // Create a memory table with the date range data
        let mem_table = MemTable::try_new(schema.clone().into(), vec![vec![record_batch]])
            .map_err(|e| ElusionError::SchemaError {
                message: format!("Failed to create date table: {}", e),
                schema: Some(schema.to_string()),
                suggestion: "💡 This is likely an internal error".to_string(),
            })?;
        
        // Register the date table
        ctx.register_table(alias, Arc::new(mem_table))
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "Table Registration".to_string(),
                reason: format!("Failed to register date table: {}", e),
                suggestion: "💡 Try a different alias name".to_string(),
            })?;
        
        // Create DataFrame
        let df = ctx.table(alias).await
            .map_err(|e| ElusionError::InvalidOperation {
                operation: "DataFrame Creation".to_string(),
                reason: format!("Failed to create date table DataFrame: {}", e),
                suggestion: "💡 Verify table registration succeeded".to_string(),
            })?;
        
        // Return new CustomDataFrame
        Ok(CustomDataFrame {
            df,
            table_alias: alias.to_string(),
            from_table: alias.to_string(),
            selected_columns: Vec::new(),
            alias_map: Vec::new(),
            aggregations: Vec::new(),
            group_by_columns: Vec::new(),
            where_conditions: Vec::new(),
            having_conditions: Vec::new(),
            order_by_columns: Vec::new(),
            limit_count: None,
            joins: Vec::new(),
            window_functions: Vec::new(),
            ctes: Vec::new(),
            subquery_source: None,
            set_operations: Vec::new(),
            query: String::new(),
            aggregated_df: None,
            union_tables: None,
            original_expressions: Vec::new(),
            needs_normalization: false,
            raw_selected_columns: Vec::new(),
            raw_group_by_columns: Vec::new(),
            raw_where_conditions: Vec::new(),
            raw_having_conditions: Vec::new(),
            raw_join_conditions: Vec::new(),
            raw_aggregations: Vec::new(),
        })
    }