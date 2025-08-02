use elusion::prelude::*;

#[tokio::main]
async fn main() -> ElusionResult<()> {

    println!("HELLO, ELUSION!!!");

     let dataframes = CustomDataFrame::load_folder(
        "C:\\Users\\BorivojGrujičić\\RUST\\TestLoadFolder",
            None, // None will check all types, or you can filter by extension vec!["xlsx", "csv"]
            "combined_data"
        ).await?;

        //dataframes.display().await?;

    let df = dataframes.select([
        "column_1 AS Site",
        "column_2 AS Location",
        "column_3 AS Centre",
        "column_4 as Breafast_Net",
        "column_5 AS Breafast_Gross",
        "column_6 AS Lunch_Net",
        "column_7 AS Lunch_Gross", 
        "column_8 AS Dinner_Net",
        "column_9 AS Dinner_Gross"])
    .filter_many([("column_3 != 'null'"), ("column_3 != ''"),("column_3 != 'Revenue Centre'")])
    .fill_down(["Site", "Location"])
    .elusion("my_sales_data").await?;

   // df.display().await?;

   let unpivoted_net = df.unpivot(
        ["Site", "Location", "Centre"],    
        ["Breafast_Net", "Lunch_Net", "Dinner_Net"], 
        "Meal_Period",                             
        "Net_Amount"                               
        ).await?
        .elusion("unpivoted_net").await?;

    unpivoted_net.display().await?;

    //========== PIVOT


    let sales_p = "C:\\Users\\BorivojGrujičić\\RUST\\SalesData2022.csv";
    let df_sales = CustomDataFrame::new(sales_p, "s").await?;

    let pivoted = df_sales
        .pivot(
            ["StockDate"],     // Row identifiers
            "TerritoryKey",    // Column to pivot
            "OrderQuantity",   // Value to aggregate
            "SUM"              // Aggregation function
        ).await?;

    let result_pivot = pivoted.elusion("pivoted_df").await?;
    result_pivot.display().await?;

    Ok(())
}
 