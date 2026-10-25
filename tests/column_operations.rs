// Tests for column operations (with_columns, drop_columns, rename, cast),
// DataFrame info (columns, count_rows, shape, value_counts) and partitioned Parquet
use elusion::prelude::*;
use std::fs::File;
use std::io::Write;
use tempfile::TempDir;

const SALES_CSV: &str = "OrderDate,Region,Product,Quantity,Price,Comment
1.1.2022,North,Apple,2,1.5,first
15.1.2022,South,Pear,1,2.25,second
3.2.2022,North,Apple,5,1.5,third
20.2.2022,East,Plum,3,3.75,fourth
28.2.2022,North,Pear,4,2.25,fifth
";

fn create_test_csv(content: &str, filename: &str, temp_dir: &TempDir) -> String {
    let file_path = temp_dir.path().join(filename);
    let mut file = File::create(&file_path).unwrap();
    file.write_all(content.as_bytes()).unwrap();
    file_path.to_str().unwrap().to_string()
}

async fn load_sales(temp_dir: &TempDir, alias: &str) -> CustomDataFrame {
    let path = create_test_csv(SALES_CSV, &format!("{}.csv", alias), temp_dir);
    CustomDataFrame::new(&path, alias).await.unwrap()
}

fn column_type(df: &CustomDataFrame, column: &str) -> ArrowDataType {
    df.schema().field_with_name(column).unwrap().data_type().clone()
}

#[tokio::test]
async fn test_columns_count_rows_shape() {
    let temp_dir = TempDir::new().unwrap();
    let sales = load_sales(&temp_dir, "info_sales").await;

    assert_eq!(sales.columns(), vec!["orderdate", "region", "product", "quantity", "price", "comment"]);
    assert_eq!(sales.count_rows().await.unwrap(), 5);
    assert_eq!(sales.shape().await.unwrap(), (5, 6));
}

#[tokio::test]
async fn test_with_columns_adds_new_and_replaces_in_place() {
    let temp_dir = TempDir::new().unwrap();
    let sales = load_sales(&temp_dir, "wc_sales").await;

    let result = sales
        .with_columns([
            "quantity * price AS revenue",
            "quantity * 10 AS quantity",
            "UPPER(region) AS Region_Upper",
        ])
        .order_by(["orderdate"], ["ASC"])
        .elusion("wc_result").await.unwrap();

    // existing column keeps its position, new columns go to the end, names are lowercased
    assert_eq!(
        result.columns(),
        vec!["orderdate", "region", "product", "quantity", "price", "comment", "revenue", "region_upper"]
    );
    assert_eq!(result.count_rows().await.unwrap(), 5);

    let quantities = result.clone().select(["quantity"]).order_by(["quantity"], ["ASC"]).elusion("wc_q").await.unwrap();
    assert_eq!(extract_value_from_df(&quantities, "quantity", 0).await.unwrap(), "10");
    assert_eq!(extract_value_from_df(&quantities, "quantity", 4).await.unwrap(), "50");

    let upper = result.clone().filter("product = 'Plum'").elusion("wc_plum").await.unwrap();
    assert_eq!(extract_value_from_df(&upper, "region_upper", 0).await.unwrap(), "EAST");
    assert_eq!(extract_value_from_df(&upper, "revenue", 0).await.unwrap(), "11.25");
}

#[tokio::test]
async fn test_drop_rename_cast_chain_in_call_order() {
    let temp_dir = TempDir::new().unwrap();
    let sales = load_sales(&temp_dir, "chain_sales").await;

    let result = sales
        .with_columns(["SPLIT_PART(orderdate, '.', 3) AS order_year"])
        .drop_columns(["comment", "PRICE"]) // case-insensitive
        .rename([("region", "area")])
        .cast([("order_year", "INT"), ("quantity", "DOUBLE")]) // cast a column created earlier in the chain
        .elusion("chain_result").await.unwrap();

    assert_eq!(result.columns(), vec!["orderdate", "area", "product", "quantity", "order_year"]);
    assert_eq!(column_type(&result, "order_year"), ArrowDataType::Int32);
    assert_eq!(column_type(&result, "quantity"), ArrowDataType::Float64);
    assert_eq!(extract_value_from_df(&result, "order_year", 0).await.unwrap(), "2022");
}

#[tokio::test]
async fn test_column_operations_with_select_and_filter() {
    let temp_dir = TempDir::new().unwrap();
    let sales = load_sales(&temp_dir, "sf_sales").await;

    let result = sales
        .select(["region", "product", "quantity"])
        .filter("region = 'North'")
        .with_columns(["quantity * 2 AS double_qty"])
        .rename([("product", "item")])
        .elusion("sf_result").await.unwrap();

    assert_eq!(result.columns(), vec!["region", "item", "quantity", "double_qty"]);
    assert_eq!(result.count_rows().await.unwrap(), 3);
}

#[tokio::test]
async fn test_vec_variants() {
    let temp_dir = TempDir::new().unwrap();
    let sales = load_sales(&temp_dir, "vec_sales").await;

    let to_drop = vec!["comment"];
    let result = sales
        .with_columns_vec(vec!["quantity + 1 AS qty_plus"])
        .drop_columns_vec(to_drop)
        .rename_vec(vec![("qty_plus", "qty_next")])
        .cast_vec(vec![("qty_next", "BIGINT")])
        .elusion("vec_result").await.unwrap();

    assert_eq!(result.columns(), vec!["orderdate", "region", "product", "quantity", "price", "qty_next"]);
    assert_eq!(column_type(&result, "qty_next"), ArrowDataType::Int64);
}

#[tokio::test]
async fn test_column_operation_errors() {
    let temp_dir = TempDir::new().unwrap();
    let sales = load_sales(&temp_dir, "err_sales").await;

    let missing = sales.clone().drop_columns(["not_a_column"]).elusion("err1").await;
    match missing {
        Err(ElusionError::MissingColumn { column, available_columns }) => {
            assert_eq!(column, "not_a_column");
            assert!(available_columns.contains(&"quantity".to_string()));
        }
        other => panic!("expected MissingColumn, got {:?}", other.map(|_| ())),
    }

    let missing_msg = sales.clone().rename([("nope", "x")]).elusion("err2").await.unwrap_err().to_string();
    assert!(missing_msg.contains("df_schema()"), "error should point to df_schema(): {}", missing_msg);

    let no_alias = sales.clone().with_columns(["quantity * 2"]).elusion("err3").await;
    assert!(matches!(no_alias, Err(ElusionError::InvalidOperation { .. })));

    let bad_type = sales.clone().cast([("quantity", "NOT_A_TYPE")]).elusion("err4").await;
    assert!(matches!(bad_type, Err(ElusionError::InvalidOperation { .. })));

    let empty_name = sales.clone().rename([("quantity", " ")]).elusion("err5").await;
    assert!(matches!(empty_name, Err(ElusionError::InvalidOperation { .. })));
}

#[tokio::test]
async fn test_value_counts() {
    let temp_dir = TempDir::new().unwrap();
    let sales = load_sales(&temp_dir, "vc_sales").await;

    // column name lookup is case-insensitive
    let counts = sales.value_counts("Region", "region_counts").await.unwrap();

    assert_eq!(counts.columns(), vec!["region", "count"]);
    assert_eq!(counts.count_rows().await.unwrap(), 3);
    // most frequent first
    assert_eq!(extract_value_from_df(&counts, "region", 0).await.unwrap(), "North");
    assert_eq!(extract_value_from_df(&counts, "count", 0).await.unwrap(), "3");

    assert!(sales.value_counts("missing", "x").await.is_err());
}

#[tokio::test]
async fn test_cache_keys_differ_for_column_operations() {
    let temp_dir = TempDir::new().unwrap();
    let sales = load_sales(&temp_dir, "cache_sales").await;

    let doubled = sales.clone()
        .select(["quantity"])
        .filter("quantity = 5")
        .with_columns(["quantity * 2 AS cache_test_x"])
        .elusion_with_cache("cache_a").await.unwrap();
    let tripled = sales.clone()
        .select(["quantity"])
        .filter("quantity = 5")
        .with_columns(["quantity * 3 AS cache_test_x"])
        .elusion_with_cache("cache_b").await.unwrap();

    assert_eq!(extract_value_from_df(&doubled, "cache_test_x", 0).await.unwrap(), "10");
    assert_eq!(extract_value_from_df(&tripled, "cache_test_x", 0).await.unwrap(), "15");
}

#[tokio::test]
async fn test_create_view_rejects_column_operations() {
    let temp_dir = TempDir::new().unwrap();
    let sales = load_sales(&temp_dir, "view_sales").await;

    let result = sales
        .with_columns(["quantity * 2 AS x"])
        .create_view("column_ops_view_test", None).await;
    assert!(matches!(result, Err(ElusionError::InvalidOperation { .. })));
}

#[tokio::test]
async fn test_partitioned_parquet_round_trip() {
    let temp_dir = TempDir::new().unwrap();
    let sales = load_sales(&temp_dir, "part_sales").await
        .with_columns(["LPAD(SPLIT_PART(orderdate, '.', 2), 2, '0') AS order_month"])
        .elusion("part_source").await.unwrap();

    let out = temp_dir.path().join("sales_partitioned");
    let out = out.to_str().unwrap();

    sales.write_to_parquet_partitioned("overwrite", out, &["region", "order_month"]).await.unwrap();

    // Hive layout: region=North/order_month=01/...
    let north_jan = temp_dir.path().join("sales_partitioned").join("region=North").join("order_month=01");
    assert!(north_jan.is_dir(), "expected partition folder {:?}", north_jan);

    let back = CustomDataFrame::load_parquet_partitioned(out, "part_back").await.unwrap();
    assert_eq!(back.count_rows().await.unwrap(), 5);
    let columns = back.columns();
    assert!(columns.contains(&"region".to_string()));
    assert!(columns.contains(&"order_month".to_string()));
    assert_eq!(column_type(&back, "region"), ArrowDataType::Utf8);

    let north = back.clone().filter("region = 'North'").elusion("part_north").await.unwrap();
    assert_eq!(north.count_rows().await.unwrap(), 3);

    // append adds files, overwrite replaces the folder
    sales.write_to_parquet_partitioned("append", out, &["region", "order_month"]).await.unwrap();
    let appended = CustomDataFrame::load_parquet_partitioned(out, "part_appended").await.unwrap();
    assert_eq!(appended.count_rows().await.unwrap(), 10);

    sales.write_to_parquet_partitioned("overwrite", out, &["region", "order_month"]).await.unwrap();
    let overwritten = CustomDataFrame::load_parquet_partitioned(out, "part_overwritten").await.unwrap();
    assert_eq!(overwritten.count_rows().await.unwrap(), 5);
}

#[tokio::test]
async fn test_partitioned_parquet_errors() {
    let temp_dir = TempDir::new().unwrap();
    let sales = load_sales(&temp_dir, "perr_sales").await;
    let out = temp_dir.path().join("out");
    let out = out.to_str().unwrap();

    assert!(sales.write_to_parquet_partitioned("overwrite", "C:/data/file.parquet", &["region"]).await.is_err());
    assert!(sales.write_to_parquet_partitioned("overwrite", out, &[]).await.is_err());
    assert!(matches!(
        sales.write_to_parquet_partitioned("overwrite", out, &["missing"]).await,
        Err(ElusionError::MissingColumn { .. })
    ));
    assert!(sales.write_to_parquet_partitioned("merge", out, &["region"]).await.is_err());

    let missing_folder = temp_dir.path().join("does_not_exist");
    assert!(CustomDataFrame::load_parquet_partitioned(missing_folder.to_str().unwrap(), "x").await.is_err());
}
