use elusion::prelude::*;

#[tokio::main]
async fn main() -> ElusionResult<()> {

    //============================= POSTGRES =======================

    let pg_config = PostgresConfig {
        host: "localhost".to_string(),
        port: 5433,
        user: "postgres".to_string(),
        password: "!Djavolak1".to_string(),
        database: "elusiontest".to_string(),
        pool_size: Some(5), 
    };

    let conn = PostgresConnection::new(pg_config).await?;

    let query = "
        SELECT 
            c.customer_id,
            c.first_name || ' ' || c.last_name AS customer_name,
            s.product_name,
            SUM(s.quantity * s.total_amount) as total_sales
        FROM 
            customers c
        JOIN 
            sales s ON c.customer_id = s.customer_id
        GROUP BY
            c.customer_id,
            c.first_name || ' ' || c.last_name,
            s.product_name
        ORDER BY 
            c.customer_id
    ";

    let sales_by_customer_df = CustomDataFrame::from_postgres(&conn, query, "sales_by_customer").await?;

    sales_by_customer_df.display().await?;

    //======================== AZURE i API =============================

    const API_URL_ITEMS: &str = "https://customerapi.quadranet.co.uk/SQLAPI/api/masterdata_items";

    const AZURE_SAS_TOKEN: &str ="sv=2020-02-10&st=2025-02-21T11%3A09%3A23Z&se=2027-02-22T11%3A09%3A00Z&sr=c&sp=racwdlm&sig=ZFfjBq983%2Bzh2KDFUi60x2u%2BNge6irden5Pb%2FCnD7oU%3D";
    const AZURE_STORAGE_URL: &str = "https://expertgroupdatalake.dfs.core.windows.net/rikas/test-1/data/itemstotal.parquet";

    //PASSWORD=85829c32-e55d-413f-9d55-5153d897gug
    const LACANTINE_PASSWORD: &str = "2a0ffef8-35a7-4cb9-842d-be3b825876e7";
    const LACANTINE_BRAND: &str = "306";
    const LACANTINE_SITE: &str = "871";
    const LACANTNE_USER: &str ="LaCantine";

  let date_calendar = CustomDataFrame::create_formatted_date_range_table(
        "2025-01-01", 
        "2025-12-31", 
        "dt", 
        "date".to_string(),
        DateFormat::HumanReadableTime, 
        true, Weekday::Mon
    ).await?;

    let week_range_2025 = date_calendar
        .select(["DISTINCT(week_start)","week_end", "week_num"])
        .order_by(["week_num"], [true])
        .elusion("wr")
        .await?;

    let temp_df = CustomDataFrame::empty().await?;
    
    let current_week = temp_df
        .datetime_functions([
           "CAST(DATE_PART('week', CURRENT_DATE()) as INT) AS current_week_num",
        ])
        .elusion("cd").await?;

    let week_for_api = week_range_2025
        .join(current_week,["wr.week_num == cd.current_week_num"], "INNER")
        .select(["TRIM(wr.week_start) AS datefrom", "TRIM(wr.week_end) AS dateto"])
        .elusion("api_week")
        .await?;

    week_for_api.display().await?;

    let date_from = extract_value_from_df(&week_for_api, "datefrom", 0).await?;
    let date_to= extract_value_from_df(&week_for_api, "dateto", 0).await?;
    
    // let data_dir = get_env_or_default("DATA_DIR", "/usr/src/app/data");

    let lacantine_password = LACANTINE_PASSWORD;
    let lacantine_brand = LACANTINE_BRAND;
    let lacantine_site = LACANTINE_SITE;
    let lacantine_user = LACANTNE_USER;

    let mut params = HashMap::new();
    params.insert("brandid", &*lacantine_brand);
    params.insert("password", &lacantine_password);
    params.insert("siteid", &lacantine_site);
    params.insert("Datefrom", &date_from);
    params.insert("Dateto", &date_to); 
    params.insert("user", &lacantine_user);

    // println!("Params: {:?}", params);

    let api_url = API_URL_ITEMS;
    let json_path = "C:\\Borivoj\\RUST\\Elusion\\TestRezultatiFajlovi\\test.json";


    let api = ElusionApi::new();
    api.from_api_with_params(
        &api_url,
        params,
        &json_path
    ).await?;

    let df = CustomDataFrame::new(&json_path, "sales").await?;

    let save_sales = df
        .select(["*"])
        .limit(10)
        .elusion_with_cache("res").await?;

    save_sales.display().await?;
    
    let sas_write = AZURE_SAS_TOKEN;
    let url_to_folder = AZURE_STORAGE_URL;

    save_sales.write_parquet_to_azure_with_sas(
                "overwrite",
                &url_to_folder,
                &sas_write
            ).await?;

    // ============ DATAFRAME Ops ========================

    let sales = "C:\\Borivoj\\RUST\\Elusion\\SalesData2022.csv";
    let products = "C:\\Borivoj\\RUST\\Elusion\\Products.csv";
    let customers = "C:\\Borivoj\\RUST\\Elusion\\Customers.csv";

    let sales_df = CustomDataFrame::new(sales, "s").await?;
    let customers_df = CustomDataFrame::new(customers, "c").await?;
    let products_df = CustomDataFrame::new(products, "p").await?;

    let join_result = sales_df.clone()
        .join_many([
            (customers_df.clone(), ["s.CustomerKey = c.CustomerKey"], "INNER"),
            (products_df.clone(), ["s.ProductKey = p.ProductKey"], "INNER"),
        ])
        .select(["c.EmailAddress", "c.CustomerKey", "c.FirstName", "c.LastName", "p.ProductName"])
        .agg([
            "SUM(s.OrderQuantity) AS total_quantity",
            "AVG(s.OrderQuantity) AS avg_quantity"
        ])
        .group_by(["c.EmailAddress", "c.CustomerKey", "c.FirstName", "c.LastName", "p.ProductName"])
        .having_many([
            ("total_quantity > 10"),
            ("avg_quantity < 100")
        ])
        .order_by_many([
            ("total_quantity", true),
            ("p.ProductName", false)
        ])
        .elusion("sales_join") 
    .await?;

    join_result.display().await?;


    //=========== PRIMER 2

    let result = sales_df.clone().join_many([
        (customers_df.clone(), ["s.CustomerKey = c.CustomerKey"], "INNER"),
        (products_df.clone(), ["s.ProductKey = p.ProductKey"], "INNER"),
    ]) 
    .select([
        "c.CustomerKey",
        "c.FirstName",
        "c.LastName",
        "c.EmailAddress",
        "p.ProductName"
    ])
    .string_functions([
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
   .filter("c.emailaddress IS NOT NULL")
    .group_by_all()
    .having("COUNT(*) > 1")
    .order_by(["c.CustomerKey"], [true])
    .limit(10)
    .elusion("bench_string_functions")
    .await?;

    result.display().await?;


    // ================== DASHBOARD =====================================

    let ord = "C:\\Borivoj\\RUST\\Elusion\\sales_order_report2.csv";
    let sales_order_df = CustomDataFrame::new(ord, "ord").await?;

    let mix_query = sales_order_df.clone()
    .select([
        "customer_name",
        "order_date",
        "ABS(billable_value) AS abs_billable_value",
        "ROUND(SQRT(billable_value), 2) AS SQRT_billable_value",
        "billable_value * 2 AS double_billable_value",  
        "billable_value / 100 AS percentage_billable"  
    ])
    .agg([
        "ROUND(AVG(ABS(billable_value)), 2) AS avg_abs_billable",
        "SUM(billable_value) AS total_billable",
        "MAX(ABS(billable_value)) AS max_abs_billable",
        "SUM(billable_value) * 2 AS double_total_billable",
        "SUM(billable_value) / 100 AS percentage_total_billable" 
    ])
    .filter("billable_value > 50.0")
    .group_by_all()
    .order_by_many([
        ("total_billable", false), 
        ("max_abs_billable", true),
    ]);

    let mix_res = mix_query.elusion("scalar_df").await?;

    let line = mix_res.plot_line(
        "order_date", 
        "double_billable_value", 
        true, 
        Some("Sales over time")
    ).await?;

    let bars = mix_res
    .plot_bar(
        "customer_name",  
        "total_billable",  
        Some("Customer Total Sales")
    ).await?;

    let time_series = mix_res
    .plot_time_series(
        "order_date",  
        "total_billable",  
        true,                
        Some("Sales Trend Over Time") 
    ).await?;

    let histogram = mix_res
    .plot_histogram(
        "abs_billable_value",  
        Some("Distribution of Sale Values") 
    ).await?;

     let box_plot = mix_res
    .plot_box(
        "abs_billable_value",   
        Some("customer_name"),  
        Some("Sales Distribution by Customer")
    ).await?;

    let scatter = mix_res
    .plot_scatter(
        "abs_billable_value",  
        "double_billable_value", 
        Some(8) 
    ).await?;

    let pie = mix_res
    .plot_pie(
        "customer_name",      
        "total_billable",      
        Some("Sales Share by Customer") 
    ).await?;

    let donut = mix_res
    .plot_donut(
        "customer_name",      
        "percentage_total_billable", 
        Some("Percentage Distribution") 
    ).await?;

    let summary_table = mix_res.clone() 
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

    let plots = [
        (&line, "Sales Line"),                
        (&time_series, "Sales Timeline"),     
        (&bars, "Customer Sales"),              
        (&histogram, "Sales Distribution"),      
        (&scatter, "Value Comparison"),        
        (&box_plot, "Customer Distributions"),  
        (&pie, "Sales Share"),                  
        (&donut, "Percentage View"),       
    ];

    let tables = [
        (&summary_table, "Customer Summary"),
        (&transactions_table, "Transaction Details")
    ];

    let layout = ReportLayout {
        grid_columns: 2, 
        grid_gap: 30, 
        max_width: 1600, 
        plot_height: 450, 
        table_height: 500,  
    };
        
    let table_options = TableOptions {
        pagination: true,      
        page_size: 15,         
        enable_sorting: true,  
        enable_filtering: true, 
        enable_column_menu: true,
        theme: "ag-theme-alpine".to_string(), 
    };

    CustomDataFrame::create_report(
        Some(&plots), 
        Some(&tables),  
        "Interactive Sales Analysis Dashboard",  
        "C:\\Borivoj\\RUST\\Elusion\\TestRezultatiFajlovi\\test_dash.html", 
        Some(layout),    
        Some(table_options)  
    ).await?;


    // ==================== MY SQL =======================

    let mysql_config = MySqlConfig {
        host: "localhost".to_string(),
        port: 3306,
        user: "databora".to_string(),
        password: "!Djavolak1".to_string(),
        database: "brewery".to_string(),
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

    let df = CustomDataFrame::from_mysql(&conn, mysql_query, "mysql_data").await?;

    let result = df.limit(100).elusion("res").await?;

    result.display().await?;

    Ok(())
}

