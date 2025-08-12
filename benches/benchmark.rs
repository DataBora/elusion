use criterion::{criterion_group, criterion_main, Criterion};
use elusion::prelude::*;

async fn setup_test_dataframes() -> ElusionResult<(CustomDataFrame, CustomDataFrame, CustomDataFrame, CustomDataFrame)> {
    let sales_path = "C:\\Borivoj\\RUST\\Elusion\\SalesData2022.csv"; // 200k rows 13 columns
    let customer_path = "C:\\Borivoj\\RUST\\Elusion\\Customers.csv";
    let products_path = "C:\\Borivoj\\RUST\\Elusion\\Products.csv";
    let sales_order_path = "C:\\Borivoj\\RUST\\Elusion\\sales_order_report2.csv";
   
    let sales_df = CustomDataFrame::new(sales_path, "se").await?;
    let customers_df = CustomDataFrame::new(customer_path, "c").await?;
    let products_df = CustomDataFrame::new(products_path, "p").await?;
    let order_df = CustomDataFrame::new(sales_order_path, "o").await?;

    Ok((sales_df, customers_df, products_df, order_df))
}

async fn setup_large_archive() -> ElusionResult<CustomDataFrame> {
    let archive_path = "C:\\Borivoj\\RUST\\Elusion\\arhiva_2024.csv"; // 897k rows and 21 columns
    CustomDataFrame::new(archive_path, "arch").await
}

fn benchmark_groupby_alias_scenarios(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let archive_df = rt.block_on(setup_large_archive()).unwrap();
    
    let mut group = c.benchmark_group("GroupBy_Alias_Scenarios");
    group.sample_size(100);
    group.measurement_time(std::time::Duration::from_secs(120));
    
    group.bench_function("group_by_all_mixed_aliases", |b| b.iter(|| {
        rt.block_on(async {
            archive_df.clone()
                .filter_many([("mesec = 'Januar'"), ("neto_vrednost > 1000")])
                .select([
                    "veledrogerija as pharmacy",     
                    "region AS territory",           
                    "grupa As category",            
                    "kolicina",                      
                    "neto_vrednost as net_value"     
                ])
                .agg([
                    "COUNT(*) AS transaction_count",
                    "SUM(kolicina) AS total_quantity", 
                    "AVG(neto_vrednost) AS avg_value"
                ])
                .group_by_all()  // Should handle all alias styles
                .order_by(["transaction_count"], ["DESC"])
                .limit(100)
                .elusion("mixed_aliases_test")
                .await
                .unwrap()
        })
    }));

    group.bench_function("explicit_group_by_with_aliases", |b| b.iter(|| {
        rt.block_on(async {
            archive_df.clone()
                .select([
                    "veledrogerija as pharm",
                    "region AS reg", 
                    "grupa as cat",
                    "mesec",
                    "neto_vrednost"
                ])
                .agg([
                    "SUM(neto_vrednost) AS total_value",
                    "COUNT(*) AS count"
                ])
                .group_by(["pharm", "reg", "cat", "mesec", "neto_vrednost"]) 
                .order_by(["total_value"], ["DESC"])
                .limit(100)
                .elusion("explicit_alias_groupby")
                .await
                .unwrap()
        })
    }));

    group.bench_function("complex_expressions_with_aliases", |b| b.iter(|| {
        rt.block_on(async {
            archive_df.clone()
                .select([
                    "region",
                    "neto_vrednost"
                ])
                .string_functions([
                    "UPPER(TRIM(veledrogerija)) as clean_pharmacy",
                    "CONCAT(godina, '-', mesec) AS year_month",
                    "CASE WHEN neto_vrednost > 5000 THEN 'HIGH' ELSE 'LOW' END as value_tier",])
                .agg([
                    "COUNT(*) AS record_count",
                    "SUM(neto_vrednost) AS total_sales"
                ])
                .group_by(["clean_pharmacy", "year_month", "value_tier", "region", "neto_vrednost"])
                .having("COUNT(*) > 5")
                .order_by(["total_sales"], ["DESC"])
                .limit(50)
                .elusion("complex_expr_aliases")
                .await
                .unwrap()
        })
    }));

   
    group.finish();
}

fn benchmark_groupby_performance_comparison(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let archive_df = rt.block_on(setup_large_archive()).unwrap();
    
    let mut group = c.benchmark_group("GroupBy_Performance_Comparison");
    group.sample_size(100);
    group.measurement_time(std::time::Duration::from_secs(150));

    group.bench_function("simple_columns_group_by_all", |b| b.iter(|| {
        rt.block_on(async {
            archive_df.clone()
                .select([
                    "region",
                    "veledrogerija", 
                    "grupa",
                    "mesec"
                ])
                .agg([
                    "COUNT(*) AS count",
                    "SUM(neto_vrednost) AS total"
                ])
                .group_by_all()
                .limit(100)
                .elusion("simple_columns_perf")
                .await
                .unwrap()
        })
    }));

    group.bench_function("aliased_columns_group_by_all", |b| b.iter(|| {
        rt.block_on(async {
            archive_df.clone()
                .select([
                    "region as territory",
                    "veledrogerija as pharmacy", 
                    "grupa as category",
                    "mesec as month"
                ])
                .agg([
                    "COUNT(*) AS count",
                    "SUM(neto_vrednost) AS total"
                ])
                .group_by_all()
                .limit(100)
                .elusion("aliased_columns_perf")
                .await
                .unwrap()
        })
    }));

    group.bench_function("large_dataset_alias_stress", |b| b.iter(|| {
        rt.block_on(async {
            archive_df.clone()
                .select([
                    "grupa as product_group"
                ])
                .string_functions([
                    "TRIM(veledrogerija) as clean_pharmacy",
                    "UPPER(region) AS upper_region",
                    "CONCAT(godina, '-', mesec) as period",
                    "CASE WHEN neto_vrednost > 1000 THEN 'HIGH' ELSE 'LOW' END as value_segment",
                ])
                .agg([
                    "COUNT(*) AS transaction_count",
                    "SUM(neto_vrednost) AS total_value",
                    "AVG(neto_vrednost) AS avg_value",
                    "MIN(neto_vrednost) AS min_value",
                    "MAX(neto_vrednost) AS max_value"
                ])
                .group_by_all()
                .having("COUNT(*) > 50")
                .order_by(["total_value"], ["DESC"])
                .limit(200)
                .elusion("large_alias_stress")
                .await
                .unwrap()
        })
    }));

    group.finish();
}

fn benchmark_joins(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (sales_df, customers_df, products_df, _) = rt.block_on(setup_test_dataframes()).unwrap();

    let mut group = c.benchmark_group("Joins");

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
    let (sales_df, _customers_df, _, _) = rt.block_on(setup_test_dataframes()).unwrap();

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
    
    group.finish();
}

fn benchmark_large_window_functions(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let archive_df = rt.block_on(setup_large_archive()).unwrap();
    
    let mut group = c.benchmark_group("Large_Window_Functions");
    group.sample_size(100);
    group.measurement_time(std::time::Duration::from_secs(180));
    
    group.bench_function("running_totals_900k", |b| b.iter(|| {
        rt.block_on(async {
            archive_df.clone()
                .select([
                    "region",
                    "mesec",
                    "neto_vrednost",
                    "veledrogerija"
                ])
                .window("SUM(neto_vrednost) OVER (PARTITION BY region ORDER BY mesec) AS running_total")
                .window("AVG(neto_vrednost) OVER (PARTITION BY region ORDER BY mesec ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg_3m")
                .window("ROW_NUMBER() OVER (PARTITION BY region ORDER BY neto_vrednost DESC) AS value_rank")
                .filter("region IS NOT NULL")
                .limit(1000)
                .elusion("large_window_running")
                .await
                .unwrap();
        })
    }));
    
    group.bench_function("ranking_functions900k", |b| b.iter(|| {
        rt.block_on(async {
            archive_df.clone()
                .select([
                    "veledrogerija",
                    "grupa",
                    "neto_vrednost",
                    "region"
                ])
                .window("DENSE_RANK() OVER (PARTITION BY grupa ORDER BY neto_vrednost DESC) AS group_rank")
                .window("PERCENT_RANK() OVER (PARTITION BY region ORDER BY neto_vrednost) AS percentile_rank")
                .window("NTILE(10) OVER (PARTITION BY veledrogerija ORDER BY neto_vrednost) AS decile")
                .filter("neto_vrednost > 0")
                .limit(1000)
                .elusion("large_window_ranking")
                .await
                .unwrap();
        })
    }));
    
    group.finish();
}

fn benchmark_large_string_operations(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let archive_df = rt.block_on(setup_large_archive()).unwrap();
    
    let mut group = c.benchmark_group("Large_String_Operations");
    group.sample_size(100);
    group.measurement_time(std::time::Duration::from_secs(90));
    
    group.bench_function("string_processing_900k", |b| b.iter(|| {
        rt.block_on(async {
            archive_df.clone()
                .select([
                    "naziv_proizvoda",
                    "naziv_ustanove", 
                    "naziv_centrale",
                    "naziv_terena",
                    "mesec"
                ])
                .string_functions([
                    "UPPER(naziv_proizvoda) AS product_upper",
                    "LOWER(naziv_ustanove) AS institution_lower", 
                    "LENGTH(naziv_proizvoda) AS product_name_length",
                    "TRIM(naziv_centrale) AS clean_central_name",
                    "SUBSTRING(naziv_proizvoda, 1, 20) AS product_short",
                    "CONCAT(naziv_terena, ' - ', mesec) AS territory_month",
                    "REPLACE(naziv_ustanove, 'APOTEKA', 'PHARMACY') AS eng_institution",
                    "INITCAP(LOWER(naziv_proizvoda)) AS product_title_case"
                ])
                .filter("naziv_proizvoda IS NOT NULL")
                .limit(5000)
                .elusion("large_string_ops")
                .await
                .unwrap();
        })
    }));
  
    group.bench_function("text_search_filtering_900k", |b| b.iter(|| {
        rt.block_on(async {
            archive_df.clone()
                .select([
                    "naziv_proizvoda",
                    "naziv_ustanove",
                    "neto_vrednost",
                    "region"
                ])
                .filter("naziv_proizvoda LIKE '%SENI%' OR naziv_ustanove LIKE '%APOTEKA%'")
                .string_functions([
                    "POSITION('APOTEKA' IN naziv_ustanove) AS apoteka_pos",
                    "CASE WHEN naziv_proizvoda LIKE '%SENI%' THEN 'SENI_PRODUCT' ELSE 'OTHER' END AS product_category"
                ])
                .agg([
                    "COUNT(*) AS matching_records",
                    "SUM(neto_vrednost) AS total_value"
                ])
                .group_by_all()
                .elusion("large_text_search")
                .await
                .unwrap();
        })
    }));
    
    group.finish();
}

fn benchmark_memory_optimizations_v4(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let archive_df = rt.block_on(setup_large_archive()).unwrap();
    
    let mut group = c.benchmark_group("Memory_Optimizations_v4");
    group.sample_size(100);
    group.measurement_time(std::time::Duration::from_secs(120));
    
    group.bench_function("efficient_cloning_900k", |b| b.iter(|| {
        rt.block_on(async {
            let _clone1 = archive_df.clone();
            let _clone2 = archive_df.clone();
            let _clone3 = archive_df.clone();
            
            let result1 = _clone1.select(["region", "neto_vrednost"]).agg(["SUM(neto_vrednost) AS total"]).group_by_all();
            let result2 = _clone2.select(["veledrogerija", "kolicina"]).agg(["AVG(kolicina) AS avg_qty"]).group_by_all();
            let result3 = _clone3.select(["grupa", "neto_cena"]).agg(["MAX(neto_cena) AS max_price"]).group_by_all();
            
            let _r1 = result1.elusion("mem_opt_1").await.unwrap();
            let _r2 = result2.elusion("mem_opt_2").await.unwrap();
            let _r3 = result3.elusion("mem_opt_3").await.unwrap();
        })
    }));
    
    group.bench_function("async_task_efficiency_900k", |b| b.iter(|| {
        rt.block_on(async {
            archive_df.clone()
                .select([
                    "godina",
                    "mesec", 
                    "region",
                    "veledrogerija",
                    "neto_vrednost",
                    "bruto_vrednost_prometa"
                ])
                .agg([
                    "SUM(neto_vrednost) AS net_total",
                    "SUM(bruto_vrednost_prometa) AS gross_total",
                    "COUNT(*) AS record_count",
                    "AVG(neto_vrednost) AS avg_net"
                ])
                .group_by_all()
                .having("COUNT(*) > 10")
                .order_by(["net_total"],["DESC"] )
                .limit(500)
                .elusion("async_efficiency")
                .await
                .unwrap();
        })
    }));
    
    group.finish();
}

fn benchmark_complex_pipelines_900k(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let archive_df = rt.block_on(setup_large_archive()).unwrap();
    
    let mut group = c.benchmark_group("Complex_Pipelines_900k");
    group.sample_size(100);
    group.measurement_time(std::time::Duration::from_secs(240));
    
    group.bench_function("analytical_pipeline_900k", |b| b.iter(|| {
        rt.block_on(async {

            let base_agg = archive_df.clone()
                .select([
                    "region",
                    "veledrogerija", 
                    "grupa",
                    "mesec",
                    "neto_vrednost",
                    "kolicina"
                ])
                .agg([
                    "SUM(neto_vrednost) AS total_value",
                    "SUM(kolicina) AS total_quantity",
                    "COUNT(*) AS transaction_count"
                ])
                .group_by_all()
                .elusion("pipeline_base")
                .await
                .unwrap();
            
            let with_rankings = base_agg
                .select([ 
                    "region",
                    "veledrogerija",
                    "grupa",
                    "mesec",
                    "total_value",
                    "total_quantity",
                    "transaction_count"
                ])
                .window("ROW_NUMBER() OVER (PARTITION BY region ORDER BY total_value DESC) AS region_rank")
                .window("SUM(total_value) OVER (PARTITION BY region) AS region_total")
                .window("PERCENT_RANK() OVER (ORDER BY total_value) AS overall_percentile")
                .elusion("pipeline_ranked")
                .await
                .unwrap();
            
            let _final_result = with_rankings
                .select([ 
                    "region",
                    "region_rank",
                    "total_value"
                ])
                .string_functions([
              //  "CONCAT(TRIM(region), ' - Rank ', TRIM(region_rank)) AS region_rank_label1",
                "CONCAT(region, ' - Rank ', CAST(region_rank AS TEXT)) AS region_rank_label",
                "CASE WHEN region_rank <= 5 THEN 'TOP_5' ELSE 'OTHER' END AS performance_tier"
                ])
                .filter("region_rank <= 10")
                .order_by(["region", "region_rank"], ["ASC", "ASC"])
                .limit(100)
                .elusion("pipeline_final")
                .await
                .unwrap();
        })
    }));

    group.bench_function("etl_pipeline_900k", |b| b.iter(|| {
        rt.block_on(async {
            // Let's simplify this to isolate the issue
            let result = archive_df.clone()
                // Extract and clean
                .select([
                    "godina",
                    "mesec",
                    "veledrogerija",
                    "ustanova", 
                    "proizvod",
                    "grupa",
                    "neto_vrednost",
                    "neto_cena",
                    "kolicina",
                    "region"
                ])
                .filter("neto_vrednost > 0 AND kolicina > 0 AND region IS NOT NULL");
            
            let with_strings = result
                .string_functions([
                    "UPPER(TRIM(veledrogerija)) AS clean_pharmacy",
                    "CONCAT(godina, '-', mesec) AS year_month"  
                ]);
                
            let _simple_result = with_strings
                .agg([
                    "COUNT(*) AS record_count",
                    "SUM(neto_vrednost) AS total_value"
                ])
                .group_by_all()
                .limit(100)
                .elusion("etl_simple_test")
                .await
                .unwrap();
        })
    }));
    
    group.finish();
}

fn benchmark_type_inference_v4(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("Type_Inference_v4");
    group.sample_size(100);
    group.measurement_time(std::time::Duration::from_secs(60));
    
    // Test type inference on various file sizes
    group.bench_function("type_inference_small", |b| b.iter(|| {
        rt.block_on(async {
            let _df = CustomDataFrame::new("C:\\Borivoj\\RUST\\Elusion\\Customers.csv", "type_test_small").await.unwrap();
        })
    }));
    
    group.bench_function("type_inference_medium", |b| b.iter(|| {
        rt.block_on(async {
            let _df = CustomDataFrame::new("C:\\Borivoj\\RUST\\Elusion\\SalesData2022.csv", "type_test_medium").await.unwrap();
        })
    }));
    
    group.bench_function("type_inference_large_900k", |b| b.iter(|| {
        rt.block_on(async {
            let _df = CustomDataFrame::new("C:\\Borivoj\\RUST\\Elusion\\arhiva_2024.csv", "type_test_large").await.unwrap();
        })
    }));
    
    group.finish();
}

fn benchmark_streaming_vs_regular_900k(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let archive_path = "C:\\Borivoj\\RUST\\Elusion\\arhiva_2024.csv";
    
    let mut group = c.benchmark_group("Streaming_vs_Regular_900k");
    group.sample_size(50);  
    group.measurement_time(std::time::Duration::from_secs(300)); 
    
    group.bench_function("regular_new_complex_query", |b| b.iter(|| {
        rt.block_on(async {
            let df_arhiva = CustomDataFrame::new(archive_path, "arch_regular").await.unwrap();
            
            let complex_result = df_arhiva
                .filter_many([("mesec = 'Januar'"), ("neto_vrednost > 1000")])
                .select([
                    "veledrogerija",
                    "region", 
                    "kolicina",
                    "neto_vrednost",
                    "mesto"
                ])
                .window("ROW_NUMBER() OVER (PARTITION BY region ORDER BY mesto DESC) AS region_rank")
                .agg([
                    "COUNT(*) AS broj_transakcija",
                    "SUM(kolicina) AS ukupna_kolicina", 
                    "SUM(neto_vrednost) AS ukupna_vrednost"
                ])
                .group_by_all()
                .order_by(["ukupna_vrednost"], ["DESC"])
                .elusion("analysis_regular")
                .await
                .unwrap();
                
            complex_result
        })
    }));

    group.bench_function("streaming_new_complex_query", |b| b.iter(|| {
        rt.block_on(async {
            let df_arhiva = CustomDataFrame::new_with_stream(archive_path, "arch_streaming").await.unwrap();
            
            let complex_result = df_arhiva
                .filter_many([("mesec = 'Januar'"), ("neto_vrednost > 1000")])
                .select([
                    "veledrogerija as pharmacy",
                    "region", 
                    "kolicina",
                    "neto_vrednost",
                    "mesto"
                ])
                .window("ROW_NUMBER() OVER (PARTITION BY region ORDER BY mesto DESC) AS region_rank")
                .agg([
                    "COUNT(*) AS broj_transakcija",
                    "SUM(kolicina) AS ukupna_kolicina", 
                    "SUM(neto_vrednost) AS ukupna_vrednost"
                ])
                .group_by_all()
                .order_by(["ukupna_vrednost"], ["DESC"])
                .elusion("analysis_streaming")
                .await
                .unwrap();
                
            complex_result
        })
    }));

    // Bonus: Test just the loading performance without the query
    group.bench_function("regular_new_load_only", |b| b.iter(|| {
        rt.block_on(async {
            let _df_arhiva = CustomDataFrame::new(archive_path, "arch_load_regular").await.unwrap();
        })
    }));

    group.bench_function("streaming_new_load_only", |b| b.iter(|| {
        rt.block_on(async {
            let _df_arhiva = CustomDataFrame::new_with_stream(archive_path, "arch_load_streaming").await.unwrap();
        })
    }));
    
    group.finish();
}

criterion_group!(
    benches, 
    benchmark_joins,
    benchmark_aggregations,
    benchmark_large_window_functions, 
    benchmark_large_string_operations,
    benchmark_memory_optimizations_v4,
    benchmark_complex_pipelines_900k,
    benchmark_type_inference_v4,
    benchmark_streaming_vs_regular_900k,
    benchmark_groupby_alias_scenarios,
    benchmark_groupby_performance_comparison
);

criterion_main!(benches);  