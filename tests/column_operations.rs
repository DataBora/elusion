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

// ---------------- fill_null_strategy, with_row_index, sample, NDJSON, JSON nulls, COUNT(DISTINCT)

const GAPS_CSV: &str = "Seq,Site,Amount
1,Alpha,10
2,,20
3,,
4,Beta,40
5,,
";

async fn load_gaps(temp_dir: &TempDir, alias: &str) -> CustomDataFrame {
    let path = create_test_csv(GAPS_CSV, &format!("{}.csv", alias), temp_dir);
    CustomDataFrame::new(&path, alias).await.unwrap()
}

async fn sorted_values(df: &CustomDataFrame, column: &str, alias: &str) -> Vec<String> {
    let sorted = df.clone().order_by(["seq"], ["ASC"]).elusion(alias).await.unwrap();
    let mut values = Vec::new();
    for i in 0..sorted.count_rows().await.unwrap() {
        values.push(extract_value_from_df(&sorted, column, i).await.unwrap());
    }
    values
}

#[tokio::test]
async fn test_fill_null_strategy_forward_backward() {
    let temp_dir = TempDir::new().unwrap();
    let gaps = load_gaps(&temp_dir, "ffb_gaps").await;

    let forward = gaps.clone().order_by(["seq"], ["ASC"]).elusion("ffb_sorted").await.unwrap()
        .fill_null_strategy(["site"], "forward").elusion("ffb_fwd").await.unwrap();
    assert_eq!(sorted_values(&forward, "site", "ffb_fwd_s").await, vec!["Alpha", "Alpha", "Alpha", "Beta", "Beta"]);

    let backward = gaps.clone().order_by(["seq"], ["ASC"]).elusion("ffb_sorted2").await.unwrap()
        .fill_null_strategy(["site"], "backward").elusion("ffb_bwd").await.unwrap();
    // nothing after the last row, so it stays empty
    assert_eq!(sorted_values(&backward, "site", "ffb_bwd_s").await, vec!["Alpha", "Beta", "Beta", "Beta", ""]);

    // no helper window column is left behind
    assert_eq!(forward.columns(), vec!["seq", "site", "amount"]);
}

#[tokio::test]
async fn test_fill_null_strategy_numeric() {
    let temp_dir = TempDir::new().unwrap();
    let gaps = load_gaps(&temp_dir, "fnum_gaps").await;

    let expected = [
        ("mean", "23.333333333333332"),
        ("median", "20"),
        ("min", "10"),
        ("max", "40"),
        ("zero", "0"),
    ];
    for (strategy, value) in expected {
        let filled = gaps.clone().fill_null_strategy(["amount"], strategy).elusion(&format!("fnum_{}", strategy)).await.unwrap();
        let nulls = filled.clone().filter("amount IS NULL").elusion(&format!("fnum_n_{}", strategy)).await.unwrap();
        assert_eq!(nulls.count_rows().await.unwrap(), 0, "strategy {}", strategy);
        let row3 = filled.clone().filter("seq = 3").elusion(&format!("fnum_r_{}", strategy)).await.unwrap();
        assert_eq!(extract_value_from_df(&row3, "amount", 0).await.unwrap(), value, "strategy {}", strategy);
    }

    let zero_on_text = gaps.clone().fill_null_strategy(["site"], "zero").elusion("fnum_e1").await;
    assert!(matches!(zero_on_text, Err(ElusionError::InvalidOperation { .. })));
    let unknown = gaps.clone().fill_null_strategy(["amount"], "average").elusion("fnum_e2").await;
    assert!(matches!(unknown, Err(ElusionError::InvalidOperation { .. })));
    let missing = gaps.clone().fill_null_strategy(["nope"], "mean").elusion("fnum_e3").await;
    assert!(matches!(missing, Err(ElusionError::MissingColumn { .. })));
}

#[tokio::test]
async fn test_with_row_index() {
    let temp_dir = TempDir::new().unwrap();
    let sales = load_sales(&temp_dir, "ri_sales").await;

    let indexed = sales.clone().with_row_index("row_nr", 0).elusion("ri_0").await.unwrap();
    assert_eq!(indexed.columns().first().unwrap(), "row_nr");
    assert_eq!(indexed.columns().len(), 7);
    let stats = indexed.select(["MIN(row_nr) AS lo", "MAX(row_nr) AS hi", "COUNT(DISTINCT row_nr) AS n"]).elusion("ri_stats").await.unwrap();
    assert_eq!(extract_value_from_df(&stats, "lo", 0).await.unwrap(), "0");
    assert_eq!(extract_value_from_df(&stats, "hi", 0).await.unwrap(), "4");
    assert_eq!(extract_value_from_df(&stats, "n", 0).await.unwrap(), "5");

    let from_one = sales.clone().with_row_index("nr", 1).elusion("ri_1").await.unwrap();
    let stats1 = from_one.select(["MIN(nr) AS lo", "MAX(nr) AS hi"]).elusion("ri_stats1").await.unwrap();
    assert_eq!(extract_value_from_df(&stats1, "lo", 0).await.unwrap(), "1");
    assert_eq!(extract_value_from_df(&stats1, "hi", 0).await.unwrap(), "5");

    assert!(sales.clone().with_row_index(" ", 0).elusion("ri_err").await.is_err());
}

#[tokio::test]
async fn test_sample() {
    let temp_dir = TempDir::new().unwrap();
    let sales = load_sales(&temp_dir, "smp_sales").await;

    let three = sales.clone().sample(3).elusion("smp_3").await.unwrap();
    assert_eq!(three.count_rows().await.unwrap(), 3);
    assert_eq!(three.columns().len(), 6);

    let more_than_rows = sales.clone().sample(100).elusion("smp_100").await.unwrap();
    assert_eq!(more_than_rows.count_rows().await.unwrap(), 5);

    let all = sales.clone().sample_fraction(1.0).elusion("smp_all").await.unwrap();
    assert_eq!(all.count_rows().await.unwrap(), 5);

    assert!(sales.clone().sample(0).elusion("smp_e1").await.is_err());
    assert!(sales.clone().sample_fraction(0.0).elusion("smp_e2").await.is_err());
    assert!(sales.clone().sample_fraction(1.5).elusion("smp_e3").await.is_err());
}

#[tokio::test]
async fn test_ndjson_round_trip_keeps_nulls_and_integers() {
    let temp_dir = TempDir::new().unwrap();
    let gaps = load_gaps(&temp_dir, "nd_gaps").await;

    let path = temp_dir.path().join("gaps.jsonl");
    let path = path.to_str().unwrap();
    gaps.write_to_ndjson(path).await.unwrap();

    let text = std::fs::read_to_string(path).unwrap();
    assert_eq!(text.lines().count(), 5);
    assert!(text.contains("\"seq\":1,"), "integers must stay integers: {}", text);
    assert!(text.contains("\"amount\":null"), "nulls must be written as null: {}", text);

    let back = CustomDataFrame::new(path, "nd_back").await.unwrap();
    assert_eq!(back.columns(), vec!["seq", "site", "amount"]);
    assert_eq!(back.count_rows().await.unwrap(), 5);
    let nulls = back.clone().filter("amount IS NULL").elusion("nd_nulls").await.unwrap();
    assert_eq!(nulls.count_rows().await.unwrap(), 2);
    assert_eq!(column_type(&back, "amount"), ArrowDataType::Int64);

    let ndjson_ext = temp_dir.path().join("gaps.ndjson");
    gaps.write_to_ndjson(ndjson_ext.to_str().unwrap()).await.unwrap();
    assert_eq!(CustomDataFrame::new(ndjson_ext.to_str().unwrap(), "nd_back2").await.unwrap().count_rows().await.unwrap(), 5);

    assert!(gaps.write_to_ndjson(temp_dir.path().join("x.json").to_str().unwrap()).await.is_err());
}

#[tokio::test]
async fn test_json_nulls_and_mixed_numbers() {
    let temp_dir = TempDir::new().unwrap();
    let path = create_test_csv(
        r#"[{"id": 1, "price": 1, "note": null}, {"id": 2, "price": 1.5, "note": "ok"}, {"id": 3, "price": null, "note": null}]"#,
        "mixed.json",
        &temp_dir,
    );
    let df = CustomDataFrame::new(&path, "mixed").await.unwrap();

    assert_eq!(column_type(&df, "id"), ArrowDataType::Int64);
    // whole and decimal numbers in one column -> Float64, nulls don't turn it into text
    assert_eq!(column_type(&df, "price"), ArrowDataType::Float64);
    let null_notes = df.clone().filter("note IS NULL").elusion("mixed_nn").await.unwrap();
    assert_eq!(null_notes.count_rows().await.unwrap(), 2);
    let null_prices = df.clone().filter("price IS NULL").elusion("mixed_np").await.unwrap();
    assert_eq!(null_prices.count_rows().await.unwrap(), 1);
}

#[tokio::test]
async fn test_count_distinct_in_select_and_agg() {
    let temp_dir = TempDir::new().unwrap();
    let sales = load_sales(&temp_dir, "cd_sales").await;

    let in_select = sales.clone().select(["COUNT(DISTINCT region) AS regions"]).elusion("cd_1").await.unwrap();
    assert_eq!(extract_value_from_df(&in_select, "regions", 0).await.unwrap(), "3");

    let in_agg = sales.clone().agg(["count(distinct product) AS products"]).elusion("cd_2").await.unwrap();
    assert_eq!(extract_value_from_df(&in_agg, "products", 0).await.unwrap(), "3");

    let grouped = sales.clone()
        .select(["region"])
        .agg(["COUNT(DISTINCT product) AS products"])
        .group_by(["region"])
        .filter("region = 'North'")
        .elusion("cd_3").await.unwrap();
    assert_eq!(extract_value_from_df(&grouped, "products", 0).await.unwrap(), "2");
}

// ---------------- explode, explode_split, to_dummies, interpolate

#[tokio::test]
async fn test_explode_json_arrays() {
    let temp_dir = TempDir::new().unwrap();
    let path = create_test_csv(
        r#"[{"id": 1, "tags": ["red", "blue"]}, {"id": 2, "tags": []}, {"id": 3, "tags": null}, {"id": 4, "tags": [7, "green", null]}]"#,
        "tags.json",
        &temp_dir,
    );
    let df = CustomDataFrame::new(&path, "tags_src").await.unwrap();

    let exploded = df.explode("tags").order_by(["id"], ["ASC"]).elusion("tags_out").await.unwrap();
    // 2 + 1 (empty array) + 1 (null) + 3
    assert_eq!(exploded.count_rows().await.unwrap(), 7);
    assert_eq!(exploded.columns(), vec!["id", "tags"]);
    assert_eq!(column_type(&exploded, "tags"), ArrowDataType::Utf8);

    let id1 = exploded.clone().filter("id = 1").elusion("tags_1").await.unwrap();
    assert_eq!(id1.count_rows().await.unwrap(), 2);
    let empty = exploded.clone().filter("id = 2 AND tags IS NULL").elusion("tags_2").await.unwrap();
    assert_eq!(empty.count_rows().await.unwrap(), 1);
    let numbers = exploded.clone().filter("tags = '7'").elusion("tags_7").await.unwrap();
    assert_eq!(numbers.count_rows().await.unwrap(), 1);
}

#[tokio::test]
async fn test_explode_split() {
    let temp_dir = TempDir::new().unwrap();
    let path = create_test_csv(
        "Order,Items\nA,\"apple, pear , plum\"\nB,banana\nC,\n",
        "items.csv",
        &temp_dir,
    );
    let df = CustomDataFrame::new(&path, "items_src").await.unwrap();

    let exploded = df.clone().explode_split("items", ",").elusion("items_out").await.unwrap();
    assert_eq!(exploded.count_rows().await.unwrap(), 5);
    let trimmed = exploded.clone().filter("items = 'pear'").elusion("items_pear").await.unwrap();
    assert_eq!(trimmed.count_rows().await.unwrap(), 1);

    assert!(df.explode_split("items", "").elusion("items_err").await.is_err());
}

#[tokio::test]
async fn test_to_dummies() {
    let temp_dir = TempDir::new().unwrap();
    let sales = load_sales(&temp_dir, "dum_sales").await;

    let dummies = sales.clone()
        .select(["product", "region", "quantity"])
        .to_dummies(["region"])
        .elusion("dum_out").await.unwrap();

    // dummy columns replace the original column, in sorted order
    assert_eq!(
        dummies.columns(),
        vec!["product", "region_east", "region_north", "region_south", "quantity"]
    );
    let sums = dummies.select(["SUM(region_north) AS n", "SUM(region_east) AS e"]).elusion("dum_sums").await.unwrap();
    assert_eq!(extract_value_from_df(&sums, "n", 0).await.unwrap(), "3");
    assert_eq!(extract_value_from_df(&sums, "e", 0).await.unwrap(), "1");

    // numeric categories are ordered as numbers
    let numeric = sales.clone()
        .select(["quantity"])
        .to_dummies(["quantity"])
        .elusion("dum_num").await.unwrap();
    assert_eq!(numeric.columns(), vec!["quantity_1", "quantity_2", "quantity_3", "quantity_4", "quantity_5"]);
}

#[tokio::test]
async fn test_interpolate() {
    let temp_dir = TempDir::new().unwrap();
    let path = create_test_csv("Seq,Value\n1,\n2,10\n3,\n4,\n5,40\n6,\n", "series.csv", &temp_dir);
    let df = CustomDataFrame::new(&path, "series_src").await.unwrap()
        .order_by(["seq"], ["ASC"])
        .elusion("series_sorted").await.unwrap();

    let interpolated = df.clone().interpolate(["value"]).elusion("series_out").await.unwrap();
    // helper column is removed
    assert_eq!(interpolated.columns(), vec!["seq", "value"]);
    // nulls before the first and after the last value stay empty
    assert_eq!(sorted_values(&interpolated, "value", "series_vals").await, vec!["", "10.0", "20.0", "30.0", "40.0", ""]);

    let text_column = df.clone().with_columns(["CAST(seq AS VARCHAR) AS label"]).elusion("series_text").await.unwrap();
    assert!(text_column.interpolate(["label"]).elusion("series_err").await.is_err());
}
