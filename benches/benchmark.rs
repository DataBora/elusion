use criterion::{criterion_group, criterion_main, Criterion};
use elusion::prelude::*;

// Helper function to set up test DataFrames
async fn setup_test_dataframes() -> ElusionResult<(CustomDataFrame, CustomDataFrame, CustomDataFrame, CustomDataFrame)> {
    let sales_path = "C:\\Borivoj\\RUST\\Elusion\\SalesData2022.csv";
    let customer_path = "C:\\Borivoj\\RUST\\Elusion\\Customers.csv";
    let products_path = "C:\\Borivoj\\RUST\\Elusion\\Products.csv";
    let sales_order_path = "C:\\Borivoj\\RUST\\Elusion\\sales_order_report.csv";
   
    let sales_df = CustomDataFrame::new(sales_path, "se").await?;
    let customers_df = CustomDataFrame::new(customer_path, "c").await?;
    let products_df = CustomDataFrame::new(products_path, "p").await?;
    let order_df = CustomDataFrame::new(sales_order_path, "o").await?;

    Ok((sales_df, customers_df, products_df, order_df))
}

fn benchmark_joins(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (sales_df, customers_df, products_df, _) = rt.block_on(setup_test_dataframes()).unwrap();

    let mut group = c.benchmark_group("Joins");

    // Single Join Benchmark
    group.bench_function("single_join", |b| b.iter(|| {
        rt.block_on(async {
            sales_df.clone()
                .join(
                    customers_df.clone(),
                    ["se.CustomerKey = c.CustomerKey"],
                    "INNER"
                )
                .select([
                    "se.OrderDate",
                    "c.FirstName",
                    "c.LastName",
                    "se.OrderQuantity"
                ])
                .elusion("bench_join")
                .await
                .unwrap()
        })
    }));

    // Multiple Joins Benchmark
    group.bench_function("multiple_joins", |b| b.iter(|| {
        rt.block_on(async {
            sales_df.clone()
                .join_many([
                    (customers_df.clone(), ["se.CustomerKey = c.CustomerKey"], "INNER"),
                    (products_df.clone(), ["se.ProductKey = p.ProductKey"], "INNER"),
                ])
                .select([
                    "c.CustomerKey",
                    "c.FirstName",
                    "c.LastName",
                    "p.ProductName",
                ])
                .elusion("bench_many_joins")
                .await
                .unwrap()
        })
    }));

    group.finish();
}

fn benchmark_aggregations(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (sales_df, customers_df, _, _) = rt.block_on(setup_test_dataframes()).unwrap();

    let mut group = c.benchmark_group("Aggregations");

    group.bench_function("simple_agg", |b| b.iter(|| {
        rt.block_on(async {
            sales_df.clone()
                .agg([
                    "SUM(se.OrderQuantity) AS total_quantity",
                    "AVG(se.OrderQuantity) AS avg_quantity",
                ])
                .elusion("bench_agg")
                .await
                .unwrap();
        })
    }));

    group.bench_function("complex_agg_with_join", |b| b.iter(|| {
        rt.block_on(async {
            sales_df.clone()
                .join(
                    customers_df.clone(), // Use the destructured customers_df
                    ["se.CustomerKey = c.CustomerKey"],
                    "INNER"
                )
                .select([
                    "c.FirstName",
                    "c.LastName"
                ])
                .agg([
                    "SUM(se.OrderQuantity) AS total_quantity",
                    "AVG(se.OrderQuantity) AS avg_quantity"
                ])
                .group_by(["c.FirstName", "c.LastName"])
                .elusion("bench_complex_agg")
                .await
                .unwrap();
        })
    }));

    group.finish();
}

fn benchmark_multiple_groupings(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let ( _, _, _,order_df) = rt.block_on(setup_test_dataframes()).unwrap();

    let mut group = c.benchmark_group("Multiple_Groupings");

    group.bench_function("agg_multiple_groupings", |b| b.iter(|| {
        rt.block_on(async {
            order_df.clone()
                .agg([
                    "ROUND(AVG(ABS(billable_value)), 2) AS avg_abs_billable",
                    "SUM(billable_value) AS total_billable",
                    "MAX(ABS(billable_value)) AS max_abs_billable",
                    "SUM(billable_value) * 2 AS double_total_billable",
                    "SUM(billable_value) / 100 AS percentage_total_billable"
                ])
                .group_by(["customer_name", "order_date"])
                .filter("billable_value > 100.0")
                .order_by(["order_date"], [true])
                .limit(10)
                .elusion("agg_multiple_groupings")
                .await
                .unwrap();
        })
    }));

    group.finish();
}



fn benchmark_window_functions(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (sales_df, customers_df, _, _) = rt.block_on(setup_test_dataframes()).unwrap();

    let mut group = c.benchmark_group("Window_Functions");

    group.bench_function("basic_window_functions", |b| b.iter(|| {
        rt.block_on(async {
            sales_df.clone()
                .join(customers_df.clone(), ["se.CustomerKey = c.CustomerKey"], "INNER")
                .select([
                    "se.OrderDate",
                    "c.FirstName",
                    "c.LastName",
                    "se.OrderQuantity"
                ])
                // Aggregated window functions
                .window("SUM(se.OrderQuantity) OVER (PARTITION BY c.CustomerKey ORDER BY se.OrderDate) AS running_total")
                .window("AVG(se.OrderQuantity) OVER (PARTITION BY c.CustomerKey ORDER BY se.OrderDate) AS running_avg")
                // Ranking window functions
                .window("ROW_NUMBER() OVER (PARTITION BY c.CustomerKey ORDER BY se.OrderDate) AS row_num")
                .window("DENSE_RANK() OVER (PARTITION BY c.CustomerKey ORDER BY se.OrderDate) AS dense_rnk")
                .limit(10)
                .elusion("bench_window_functions")
                .await
                .unwrap();
        })
    }));

    group.bench_function("advanced_window_functions", |b| b.iter(|| {
        rt.block_on(async {
            sales_df.clone()
                .join(customers_df.clone(), ["se.CustomerKey = c.CustomerKey"], "INNER")
                .select([
                    "se.OrderDate",
                    "c.FirstName",
                    "c.LastName",
                    "se.OrderQuantity"
                ])
                // Analytical window functions
                .window("FIRST_VALUE(se.OrderQuantity) OVER (PARTITION BY c.CustomerKey ORDER BY se.OrderDate) AS first_qty")
                .window("LAST_VALUE(se.OrderQuantity) OVER (PARTITION BY c.CustomerKey ORDER BY se.OrderDate) AS last_qty")
                .window("LAG(se.OrderQuantity, 1, 0) OVER (PARTITION BY c.CustomerKey ORDER BY se.OrderDate) AS prev_qty")
                .window("LEAD(se.OrderQuantity, 1, 0) OVER (PARTITION BY c.CustomerKey ORDER BY se.OrderDate) AS next_qty")
                .limit(10)
                .elusion("bench_advanced_window_functions")
                .await
                .unwrap();
        })
    }));

    group.finish();
}

fn benchmark_window_functions_with_frames(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (sales_df, customers_df, _, _) = rt.block_on(setup_test_dataframes()).unwrap();

    let mut group = c.benchmark_group("Window_Functions_With_Frames");

    group.bench_function("aggregated_rolling_windows", |b| b.iter(|| {
        rt.block_on(async {
            sales_df.clone()
                .join(customers_df.clone(), ["se.CustomerKey = c.CustomerKey"], "INNER")
                .select(["se.OrderDate", "c.FirstName", "c.LastName", "se.OrderQuantity"])
                // Aggregated rolling windows
                .window("SUM(se.OrderQuantity) OVER (PARTITION BY c.CustomerKey ORDER BY se.OrderDate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total")
                .window("AVG(se.OrderQuantity) OVER (PARTITION BY c.CustomerKey ORDER BY se.OrderDate ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS full_partition_avg")
                .limit(10)
                .elusion("bench_rolling_windows")
                .await
                .unwrap();
        })
    }));

    group.finish();
}

fn benchmark_pivot(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (sales_df, _, _, _) = rt.block_on(setup_test_dataframes()).unwrap();

    let mut group = c.benchmark_group("Pivot");

    group.bench_function("pivot_operation", |b| b.iter(|| {
        rt.block_on(async {
            sales_df.clone()
                .pivot(
                    ["StockDate"],         // Row identifiers
                    "TerritoryKey",        // Column to pivot
                    "OrderQuantity",       // Value to aggregate
                    "SUM"                   // Aggregation function
                )
                .await
                .unwrap()
                .elusion("bench_pivot")
                .await
                .unwrap();
        })
    }));

    group.finish();
}

fn benchmark_unpivot(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (pivoted_df, _) = rt.block_on(async {
        let (sales_df, _, _, _) = setup_test_dataframes().await.unwrap();
        let pivoted = sales_df.clone()
            .pivot(
                ["StockDate"],
                "TerritoryKey",
                "OrderQuantity",
                "SUM"
            )
            .await
            .unwrap()
            .elusion("pivoted_df")
            .await
            .unwrap();
        (pivoted, ())
    });

    let mut group = c.benchmark_group("Unpivot");

    group.bench_function("unpivot_operation", |b| b.iter(|| {
        rt.block_on(async {
            pivoted_df.clone()
                .unpivot(
                    ["StockDate"],                         // ID columns
                    ["TerritoryKey_1", "TerritoryKey_2"],  // Value columns to unpivot
                    "Territory",                           // New name column
                    "Quantity"                             // New value column
                )
                .await
                .unwrap()
                .elusion("bench_unpivot")
                .await
                .unwrap();
        })
    }));

    group.finish();
}

fn benchmark_string_functions(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (sales_df, customers_df, products_df, _) = rt.block_on(setup_test_dataframes()).unwrap();

    let mut group = c.benchmark_group("String_Functions");

    group.bench_function("string_functions_query", |b| b.iter(|| {
        rt.block_on(async {
            sales_df.clone()
                .join_many([
                    (customers_df.clone(), ["se.CustomerKey = c.CustomerKey"], "INNER"),
                    (products_df.clone(), ["se.ProductKey = p.ProductKey"], "INNER"),
                ]) 
                .select([
                    "c.CustomerKey",
                    "c.FirstName",
                    "c.LastName",
                    "c.EmailAddress",
                    "p.ProductName"
                ])
                .string_functions([
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
                ])
                .agg([
                    "COUNT(*) AS total_records",
                    "STRING_AGG(p.ProductName, ', ') AS all_products"
                ])
                .filter("c.EmailAddress IS NOT NULL")
                .group_by_all()
                .having("COUNT(*) > 1")
                .order_by(["c.CustomerKey"], [true])
                .limit(10)
                .elusion("bench_string_functions")
                .await
                .unwrap();
        })
    }));

    group.finish();
}

fn benchmark_union_intersect(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (sales_df, customers_df, _,order_df) = rt.block_on(setup_test_dataframes()).unwrap();

    let mut group = c.benchmark_group("Union_Intersect");

    // Benchmark for Intersect Operation
    group.bench_function("intersect_operation", |b| b.iter(|| {
        rt.block_on(async {
            let df1 = order_df.clone()
                .select([
                    "customer_name",
                    "order_date",
                    "billable_value",
                    "billable_value * 2 AS double_billable_value",
                    "billable_value / 100 AS percentage_billable"
                ])
                .filter("billable_value > 100.0")
                .order_by(["order_date"], [true])
                .limit(10);

            let df2 = order_df.clone()
                .select([
                    "customer_name",
                    "order_date",
                    "billable_value",
                    "billable_value * 2 AS double_billable_value", 
                    "billable_value / 100 AS percentage_billable"  
                ])
                .filter("billable_value > 100.0")
                .order_by(["order_date"], [true])
                .limit(10);

            df1.clone()
                .intersect(df2.clone())
                .elusion("bench_intersect")
                .await
                .unwrap();
        })
    }));

    // Benchmark for Aggregation Intersect
    group.bench_function("agg_intersect", |b| b.iter(|| {
        rt.block_on(async {
            let agg_df1 = order_df.clone()
                .select(["customer_name"])
                .agg([
                    "SUM(billable_value) AS total_billable",
                    "COUNT(*) AS order_count"
                ])
                .group_by_all()
                .limit(5);

            let agg_df2 = order_df.clone()
                .select(["customer_name"])
                .agg([
                    "SUM(billable_value) AS total_billable",
                    "COUNT(*) AS order_count"
                ])
                .group_by_all()
                .limit(5);

            agg_df1.clone()
                .intersect(agg_df2.clone())
                .elusion("bench_agg_intersect")
                .await
                .unwrap();
        })
    }));

    // Benchmark for String Functions Intersect
    group.bench_function("string_functions_intersect", |b| b.iter(|| {
        rt.block_on(async {
            let string_df1 = sales_df.clone()
                .join(
                    customers_df.clone(),
                    ["se.CustomerKey = c.CustomerKey"],
                    "INNER"
                )
                .select(["c.FirstName", "c.LastName"])
                .string_functions([
                    "TRIM(c.EmailAddress) AS trimmed_email",
                    "CONCAT(TRIM(c.FirstName), ' ', TRIM(c.LastName)) AS full_name",
                ])
                .limit(5);
    
            let string_df2 = sales_df.clone()
                .join(
                    customers_df.clone(),
                    ["se.CustomerKey = c.CustomerKey"],
                    "INNER"
                )
                .select(["c.FirstName", "c.LastName"])
                .string_functions([
                    "TRIM(c.EmailAddress) AS trimmed_email",
                    "CONCAT(TRIM(c.FirstName), ' ', TRIM(c.LastName)) AS full_name",
                ])
                .limit(5);
    
            string_df1
                .intersect(string_df2)
                .elusion("bench_string_intersect")
                .await
                .unwrap()
        })
    }));

    group.finish();
}


criterion_group!(
    benches,
    benchmark_joins,
    benchmark_multiple_groupings,
    benchmark_aggregations,
    benchmark_window_functions,
    benchmark_window_functions_with_frames,
    benchmark_pivot,
    benchmark_unpivot,
    benchmark_string_functions,
    benchmark_union_intersect
);
criterion_main!(benches);
