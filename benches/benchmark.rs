// benches/benchmark.rs
use criterion::{criterion_group, criterion_main, Criterion};
use elusion::prelude::*;

// Helper function to set up test DataFrames
async fn setup_test_dataframes() -> ElusionResult<(CustomDataFrame, CustomDataFrame, CustomDataFrame)> {
    let sales_path = "C:\\Borivoj\\RUST\\Elusion\\SalesData2022.csv";
    let customer_path = "C:\\Borivoj\\RUST\\Elusion\\Customers.csv";
    let products_path = "C:\\Borivoj\\RUST\\Elusion\\Products.csv";
   
    let sales_df = CustomDataFrame::new(sales_path, "s").await?;
    let customers_df = CustomDataFrame::new(customer_path, "c").await?;
    let products_df = CustomDataFrame::new(products_path, "p").await?;

    Ok((sales_df, customers_df, products_df))
}

fn benchmark_joins(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (sales_df, customers_df, products_df) = rt.block_on(setup_test_dataframes()).unwrap();

    let mut group = c.benchmark_group("Joins");

    // Single Join Benchmark
    group.bench_function("single_join", |b| b.iter(|| {
        rt.block_on(async {
            sales_df.clone()
                .join(
                    customers_df.clone(),
                    ["s.CustomerKey = c.CustomerKey"],
                    "INNER"
                )
                .select([
                    "s.OrderDate",
                    "c.FirstName",
                    "c.LastName",
                    "s.OrderQuantity"
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
                    (customers_df.clone(), ["s.CustomerKey = c.CustomerKey"], "INNER"),
                    (products_df.clone(), ["s.ProductKey = p.ProductKey"], "INNER"),
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
    let (sales_df, customers_df, _) = rt.block_on(setup_test_dataframes()).unwrap();

    let mut group = c.benchmark_group("Aggregations");

    // Simple Aggregation
    group.bench_function("simple_agg", |b| b.iter(|| {
        rt.block_on(async {
            sales_df.clone()
                .agg([
                    "SUM(OrderQuantity) AS total_quantity",
                    "AVG(OrderQuantity) AS avg_quantity",
                ])
                .elusion("bench_agg")
                .await
                .unwrap()
        })
    }));

    // Complex Aggregation with Joins
    group.bench_function("complex_agg_with_join", |b| b.iter(|| {
        rt.block_on(async {
            sales_df.clone()
                .join(
                    customers_df.clone(),
                    ["s.CustomerKey = c.CustomerKey"],
                    "INNER"
                )
                .select(["c.FirstName", "c.LastName"])
                .agg([
                    "SUM(s.OrderQuantity) AS total_quantity",
                    "AVG(s.OrderQuantity) AS avg_quantity",
                    "COUNT(DISTINCT s.OrderID) AS order_count"
                ])
                .group_by(["c.FirstName", "c.LastName"])
                .elusion("bench_complex_agg")
                .await
                .unwrap()
        })
    }));

    group.finish();
}

fn benchmark_window_functions(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (sales_df, customers_df, _) = rt.block_on(setup_test_dataframes()).unwrap();

    let mut group = c.benchmark_group("Window_Functions");

    group.bench_function("window_functions", |b| b.iter(|| {
        rt.block_on(async {
            sales_df.clone()
                .join(customers_df.clone(), ["s.CustomerKey = c.CustomerKey"], "INNER")
                .select([
                    "s.OrderDate",
                    "c.FirstName",
                    "s.OrderQuantity"
                ])
                .window("SUM(s.OrderQuantity) OVER (PARTITION BY c.CustomerKey ORDER BY s.OrderDate) as running_total")
                .window("ROW_NUMBER() OVER (PARTITION BY c.CustomerKey ORDER BY s.OrderDate) as row_num")
                .elusion("bench_window")
                .await
                .unwrap()
        })
    }));

    group.finish();
}

criterion_group!(
    benches,
    benchmark_joins,
    benchmark_aggregations,
    benchmark_window_functions
);
criterion_main!(benches);