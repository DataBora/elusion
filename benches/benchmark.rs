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

fn benchmark_star_selection_patterns(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (sales_df, customers_df, products_df, _) = rt.block_on(setup_test_dataframes()).unwrap();
    
    let mut group = c.benchmark_group("Star_Selection_Patterns");
    group.sample_size(100);
    group.measurement_time(std::time::Duration::from_secs(120));
    
    group.bench_function("star_selection_simple_no_agg", |b| b.iter(|| {
        rt.block_on(async {
            sales_df.clone()
                .join_many([
                    (customers_df.clone(), ["se.CustomerKey = c.CustomerKey"], "RIGHT"),
                    (products_df.clone(), ["se.ProductKey = p.ProductKey"], "LEFT OUTER"),
                ])
                .select(["c.*", "p.*"])
                .limit(1000)  // Limit for performance
                .elusion("star_simple_no_agg")
                .await
                .unwrap()
        })
    }));

    group.bench_function("star_selection_with_aggregations", |b| b.iter(|| {
        rt.block_on(async {
            sales_df.clone()
                .join_many([
                    (customers_df.clone(), ["se.CustomerKey = c.CustomerKey"], "RIGHT"),
                    (products_df.clone(), ["se.ProductKey = p.ProductKey"], "LEFT OUTER"),
                ])
                .select(["c.*", "p.*"])
                .agg([
                    "SUM(se.OrderQuantity) AS total_quantity",
                    "AVG(se.OrderQuantity) AS avg_quantity",
                    "COUNT(*) AS order_count"
                ])
                .group_by_all()
                .having_many([
                    ("total_quantity > 10"),
                    ("avg_quantity < 100")
                ])
                .order_by_many([
                    ("total_quantity", "ASC"),
                    ("p.ProductName", "DESC")
                ])
                .limit(100)
                .elusion("star_with_agg")
                .await
                .unwrap()
        })
    }));

    group.bench_function("mixed_star_and_explicit_columns", |b| b.iter(|| {
        rt.block_on(async {
            sales_df.clone()
                .join_many([
                    (customers_df.clone(), ["se.CustomerKey = c.CustomerKey"], "INNER"),
                    (products_df.clone(), ["se.ProductKey = p.ProductKey"], "INNER"),
                ])
                .select([
                    "c.*",
                    "se.OrderDate",
                    "se.OrderQuantity", 
                    "p.ProductName as proizvod",
                    "p.ProductPrice"
                ])
                .agg([
                    "SUM(se.OrderQuantity) AS total_qty",
                    "COUNT(DISTINCT se.OrderDate) AS unique_order_days"
                ])
                .group_by_all()
                .having("COUNT(*) > 5")
                .order_by(["total_qty"], ["DESC"])
                .limit(50)
                .elusion("mixed_star_explicit")
                .await
                .unwrap()
        })
    }));

    group.bench_function("single_table_star_vs_explicit", |b| b.iter(|| {
        rt.block_on(async {
            customers_df.clone()
                .select(["c.*"])  
                .agg([
                    "COUNT(*) AS customer_count",
                    "AVG(c.AnnualIncome) AS avg_income"
                ])
                .group_by_all()
                .having("COUNT(*) > 0")
                .limit(100)
                .elusion("single_table_star")
                .await
                .unwrap()
        })
    }));

    group.bench_function("archive_star_selection", |b| b.iter(|| {
        rt.block_on(async {
            let archive_df = setup_large_archive().await.unwrap();
            
            archive_df
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
                .elusion("archive_star_full")
                .await
                .unwrap()
        })
    }));

    group.bench_function("triple_join_star_selection", |b| b.iter(|| {
        rt.block_on(async {
            sales_df.clone()
                .join_many([
                    (customers_df.clone(), ["se.CustomerKey = c.CustomerKey"], "INNER"),
                    (products_df.clone(), ["se.ProductKey = p.ProductKey"], "INNER"),
                ])
                .select([
                    "se.*", 
                    "c.*", 
                    "p.*" 
                ])
                .filter("se.OrderQuantity > 1")
                .agg([
                    "SUM(se.OrderQuantity) AS total_orders",
                    "COUNT(DISTINCT c.CustomerKey) AS unique_customers",
                    "COUNT(DISTINCT p.ProductKey) AS unique_products",
                    "AVG(p.ProductPrice) AS avg_product_price"
                ])
                .group_by_all()
                .having("COUNT(*) > 3")
                .order_by_many([
                    ("total_orders", "DESC"),
                    ("unique_customers", "DESC")
                ])
                .limit(75)
                .elusion("triple_join_star")
                .await
                .unwrap()
        })
    }));

    group.bench_function("explicit_columns_equivalent", |b| b.iter(|| {
        rt.block_on(async {
            sales_df.clone()
                .join_many([
                    (customers_df.clone(), ["se.CustomerKey = c.CustomerKey"], "RIGHT"),
                    (products_df.clone(), ["se.ProductKey = p.ProductKey"], "LEFT OUTER"),
                ])
                .select([
                    "c.CustomerKey as blob",
                    "se.CustomerKey as cvc",
                    "c.Prefix", 
                    "c.FirstName",
                    "c.LastName",
                    "c.BirthDate",
                    "c.MaritalStatus",
                    "c.Gender",
                    "c.EmailAddress",
                    "c.AnnualIncome",
                    "c.TotalChildren",
                    "c.EducationLevel",
                    "c.Occupation",
                    "c.HomeOwner",
                    "p.ProductKey",
                    "p.ProductSubcategoryKey",
                    "p.ProductSKU",
                    "p.ProductName",
                    "p.ModelName", 
                    "p.ProductDescription",
                    "p.ProductColor",
                    "p.ProductSize",
                    "p.ProductStyle",
                    "p.ProductCost",
                    "p.ProductPrice"
                ])
                .agg([
                    "SUM(se.OrderQuantity) AS total_quantity",
                    "AVG(se.OrderQuantity) AS avg_quantity"
                ])
                .group_by_all()
                .having_many([
                    ("total_quantity > 10"),
                    ("avg_quantity < 100")
                ])
                .order_by_many([
                    ("total_quantity", "ASC"),
                    ("p.ProductName", "DESC")
                ])
                .limit(100)
                .elusion("explicit_equivalent")
                .await
                .unwrap()
        })
    }));

    group.finish();
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
                    "grupa as product_group",
                    "veledrogerija",          
                    "region",                
                    "godina",                 
                    "mesec",      
                    "neto_vrednost" 
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

fn benchmark_cache_comparison(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (sales_df, customers_df, products_df, _) = rt.block_on(setup_test_dataframes()).unwrap();
    
    let redis_conn = match rt.block_on(CustomDataFrame::create_redis_cache_connection()) {
        Ok(conn) => Some(conn),
        Err(_) => {
            println!("⚠️ Redis not available - skipping Redis cache benchmarks");
            None
        }
    };

    let mut group = c.benchmark_group("Cache_Comparison");
    group.sample_size(50); // Reduced for cache tests
    group.measurement_time(std::time::Duration::from_secs(180));

    group.bench_function("native_cache_complex_join_miss", |b| b.iter(|| {
        rt.block_on(async {
            sales_df.clone()
                .join_many([
                    (customers_df.clone(), ["se.CustomerKey = c.CustomerKey"], "RIGHT"),
                    (products_df.clone(), ["se.ProductKey = p.ProductKey"], "LEFT OUTER"),
                ])
                .select(["c.CustomerKey", "c.FirstName", "c.LastName", "p.ProductName"])
                .agg([
                    "SUM(se.OrderQuantity) AS total_quantity",
                    "AVG(se.OrderQuantity) AS avg_quantity"
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
                .elusion_with_cache("native_cache_benchmark") // Native caching
                .await
                .unwrap()
        })
    }));

    group.bench_function("native_cache_complex_join_hit", |b| b.iter(|| {
        rt.block_on(async {
            sales_df.clone()
                .join_many([
                    (customers_df.clone(), ["se.CustomerKey = c.CustomerKey"], "RIGHT"),
                    (products_df.clone(), ["se.ProductKey = p.ProductKey"], "LEFT OUTER"),
                ])
                .select(["c.CustomerKey", "c.FirstName", "c.LastName", "p.ProductName"])
                .agg([
                    "SUM(se.OrderQuantity) AS total_quantity",
                    "AVG(se.OrderQuantity) AS avg_quantity"
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
                .elusion_with_cache("native_cache_benchmark") // Same cache key - should hit
                .await
                .unwrap()
        })
    }));

    if let Some(ref redis_connection) = redis_conn {
        let _ = rt.block_on(CustomDataFrame::clear_redis_cache(redis_connection, None));

    group.finish();
}
}

fn benchmark_platform_specific_performance(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let archive_df = rt.block_on(setup_large_archive()).unwrap();
    
    let mut group = c.benchmark_group("Platform_Specific_Performance");
    group.sample_size(30);
    group.measurement_time(std::time::Duration::from_secs(90));
    
    group.bench_function("memory_intensive_groupby", |b| b.iter(|| {
        rt.block_on(async {
            archive_df.clone()
                .select([
                    "veledrogerija",
                    "ustanova", 
                    "proizvod",
                    "grupa",
                    "region",
                    "mesto",
                    "mesec",
                    "godina",
                    "neto_vrednost"
                ])
                .agg([
                    "COUNT(*) AS count",
                    "SUM(neto_vrednost) AS total",
                    "AVG(neto_vrednost) AS avg",
                    "MIN(neto_vrednost) AS min_val",
                    "MAX(neto_vrednost) AS max_val"
                ])
                .group_by_all() 
                .limit(1000)
                .elusion("memory_intensive_test")
                .await
                .unwrap()
        })
    }));
   
    group.bench_function("floating_point_calculations", |b| b.iter(|| {
        rt.block_on(async {
            archive_df.clone()
                .select([
                    "neto_vrednost",
                    "bruto_vrednost_prometa",
                    "neto_cena",
                    "kolicina"
                ])
                .string_functions([
                    "CAST(neto_vrednost AS DOUBLE) * 1.21 AS with_tax",
                    "ROUND(neto_cena * kolicina, 2) AS calculated_total",
                    "SQRT(neto_vrednost) AS sqrt_value",
                    "LOG(neto_vrednost + 1) AS log_value"  // +1 to avoid log(0)
                ])
                .filter("neto_vrednost > 0")
                .limit(10000)
                .elusion("floating_point_test")
                .await
                .unwrap()
        })
    }));
    
    group.finish();
}



fn benchmark_concurrency_performance(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (sales_df, customers_df, products_df, _) = rt.block_on(setup_test_dataframes()).unwrap();
    
    let mut group = c.benchmark_group("Concurrency_Performance");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_secs(120));
    
    group.bench_function("parallel_joins", |b| b.iter(|| {
        rt.block_on(async {

            let join1 = sales_df.clone()
                .join(customers_df.clone(), ["se.CustomerKey = c.CustomerKey"], "INNER")
                .agg(["COUNT(*) AS count1"])
                .group_by_all();
                
            let join2 = sales_df.clone()
                .join(products_df.clone(), ["se.ProductKey = p.ProductKey"], "INNER")
                .agg(["SUM(se.OrderQuantity) AS total_qty"])
                .group_by_all();
            
            let _result1 = join1.elusion("parallel_test1").await.unwrap();
            let _result2 = join2.elusion("parallel_test2").await.unwrap();
        })
    }));
    
    group.finish();
}

// Test system-specific data type handling
fn benchmark_data_type_performance(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let archive_df = rt.block_on(setup_large_archive()).unwrap();
    
    let mut group = c.benchmark_group("Data_Type_Performance");
    group.sample_size(40);
    group.measurement_time(std::time::Duration::from_secs(80));
    
    // 32-bit vs 64-bit
    group.bench_function("integer_operations", |b| b.iter(|| {
        rt.block_on(async {
            archive_df.clone()
                .select([
                    "kolicina",
                    "godina"
                ])
                .agg([
                    "SUM(kolicina) AS total_qty",
                    "COUNT(*) AS count",
                    "MAX(kolicina) AS max_qty",
                    "MIN(godina) AS min_year"
                ])
                .group_by_all()
                .elusion("integer_ops")
                .await
                .unwrap()
        })
    }));
    
    group.bench_function("string_operations_unicode", |b| b.iter(|| {
        rt.block_on(async {
            archive_df.clone()
                .select([
                    "naziv_proizvoda",
                    "naziv_ustanove"
                ])
                .string_functions([
                    "UPPER(naziv_proizvoda) AS upper_product",
                    "LENGTH(naziv_ustanove) AS institution_length",
                    "SUBSTRING(naziv_proizvoda, 1, 10) AS product_short"
                ])
                .filter("naziv_proizvoda IS NOT NULL")
                .limit(5000)
                .elusion("string_unicode_ops")
                .await
                .unwrap()
        })
    }));
    
    group.finish();
}

fn benchmark_csv_type_inference_cross_platform(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    
    let mut group = c.benchmark_group("CSV_Type_Inference_Cross_Platform");
    group.sample_size(30);
    group.measurement_time(std::time::Duration::from_secs(90));
    
    group.bench_function("empty_string_handling_performance", |b| b.iter(|| {
        rt.block_on(async {
            let df = CustomDataFrame::new_with_stream("C:\\Borivoj\\RUST\\Elusion\\arhiva_2024.csv", "empty_string_test").await.unwrap();
            
            // Query that specifically tests empty string columns (like mesto)
            df.select([
                    "mesto", 
                    "veledrogerija",
                    "region",
                    "neto_vrednost"
                ])
                .filter("neto_vrednost > 0") 
                .agg([
                    "COUNT(*) AS total_records",
                    "COUNT(mesto) AS non_empty_mesto" 
                ])
                .group_by_all()
                .elusion("empty_string_perf")
                .await
                .unwrap()
        })
    }));
    
    // Test number format parsing across different locales/systems
    group.bench_function("number_format_parsing", |b| b.iter(|| {
        rt.block_on(async {
            let df = CustomDataFrame::new_with_stream("C:\\Borivoj\\RUST\\Elusion\\arhiva_2024.csv", "number_format_test").await.unwrap();
            
            df.select([
                    "neto_vrednost",
                    "neto_cena",
                    "bruto_vrednost_prometa"
                ])
                .agg([
                    "SUM(neto_vrednost) AS total_net_value",
                    "AVG(neto_cena) AS avg_net_price",
                    "MAX(bruto_vrednost_prometa) AS max_gross"
                ])
                .group_by_all()
                .elusion("number_parsing_perf")
                .await
                .unwrap()
        })
    }));
    
    group.finish();
}

criterion_group!(
    benches, 
    benchmark_platform_specific_performance,                
    benchmark_concurrency_performance,     
    benchmark_data_type_performance,    
    benchmark_csv_type_inference_cross_platform,
    benchmark_cache_comparison,
    benchmark_star_selection_patterns,
    benchmark_groupby_alias_scenarios,
    benchmark_groupby_performance_comparison,
    benchmark_joins,
    benchmark_aggregations,
    benchmark_large_window_functions, 
    benchmark_large_string_operations,
    benchmark_memory_optimizations_v4,
    benchmark_complex_pipelines_900k,
    benchmark_type_inference_v4,
    benchmark_streaming_vs_regular_900k,
  
);

criterion_main!(benches);  