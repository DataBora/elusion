
use crate::prelude::*;

//======= Ploting Helper functions
#[cfg(feature = "dashboard")]
fn convert_to_f64_vec(array: &dyn Array) -> ElusionResult<Vec<f64>> {
    match array.data_type() {
        ArrowDataType::Float64 => {
            let float_array = array.as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| ElusionError::Custom("Failed to downcast to Float64Array".to_string()))?;
            Ok(float_array.values().to_vec())
        },
        ArrowDataType::Int64 => {
            let int_array = array.as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| ElusionError::Custom("Failed to downcast to Int64Array".to_string()))?;
            Ok(int_array.values().iter().map(|&x| x as f64).collect())
        },
        ArrowDataType::Date32 => {
            let date_array = array.as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| ElusionError::Custom("Failed to downcast to Date32Array".to_string()))?;
            Ok(convert_date32_to_timestamps(date_array))
        },
        ArrowDataType::Utf8 => {
            let string_array = array.as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| ElusionError::Custom("Failed to downcast to StringArray".to_string()))?;
            let mut values = Vec::with_capacity(array.len());
            for i in 0..array.len() {
                let value = string_array.value(i).parse::<f64>().unwrap_or(0.0);
                values.push(value);
            }
            Ok(values)
        },
        other_type => {
            Err(ElusionError::Custom(format!("Unsupported data type for plotting: {:?}", other_type)))
        }
    }
}

#[cfg(feature = "dashboard")]
fn convert_to_string_vec(array: &dyn Array) -> ElusionResult<Vec<String>> {
    match array.data_type() {
        ArrowDataType::Utf8 => {
            let string_array = array.as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| ElusionError::Custom("Failed to downcast to StringArray".to_string()))?;
            
            let mut values = Vec::with_capacity(array.len());
            for i in 0..array.len() {
                values.push(string_array.value(i).to_string());
            }
            Ok(values)
        },
        other_type => {
            Err(ElusionError::Custom(format!("Expected string type but got: {:?}", other_type)))
        }
    }
}

#[cfg(feature = "dashboard")]
fn convert_date32_to_timestamps(array: &Date32Array) -> Vec<f64> {
    array.values()
        .iter()
        .map(|&days| {
            // Convert days since epoch to timestamp
            let date = NaiveDate::from_num_days_from_ce_opt(days + 719163)
                .unwrap_or(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
            let datetime = date.and_hms_opt(0, 0, 0).unwrap();
            datetime.and_utc().timestamp() as f64 * 1000.0 // Convert to milliseconds for plotly
        })
        .collect()
}

// Helper function to sort date-value pairs
#[cfg(feature = "dashboard")]
fn sort_by_date(x_values: &[f64], y_values: &[f64]) -> (Vec<f64>, Vec<f64>) {
    let mut pairs: Vec<(f64, f64)> = x_values.iter()
        .cloned()
        .zip(y_values.iter().cloned())
        .collect();
    
    // Sort by date (x values)
    pairs.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
    
    // Unzip back into separate vectors
    pairs.into_iter().unzip()
}

//helper funciton for converting dates for dashboard
#[cfg(feature = "dashboard")]
fn parse_date_string(date_str: &str) -> Option<chrono::NaiveDateTime> {
    // Try different date formats
    let formats = [
        // Standard formats
        "%Y-%m-%d",           // 2024-03-12
        "%d.%m.%Y",           // 1.2.2024
        "%d/%m/%Y",           // 1/2/2024
        "%Y/%m/%d",           // 2024/03/12
        
        // Formats with month names
        "%d %b %Y",           // 1 Jan 2024
        "%d %B %Y",           // 1 January 2024
        "%b %d %Y",           // Jan 1 2024
        "%B %d %Y",           // January 1 2024
        
        // Formats with time
        "%Y-%m-%d %H:%M:%S",  // 2024-03-12 15:30:00
        "%d.%m.%Y %H:%M:%S",  // 1.2.2024 15:30:00
        
        // Additional regional formats
        "%m/%d/%Y",           // US format: 3/12/2024
        "%Y.%m.%d",           // 2024.03.12
    ];

    for format in formats {
        if let Ok(date) = chrono::NaiveDateTime::parse_from_str(date_str, format) {
            return Some(date);
        } else if let Ok(date) = chrono::NaiveDate::parse_from_str(date_str, format) {
            return Some(date.and_hms_opt(0, 0, 0).unwrap_or_default());
        }
    }

    None
}


// ================================== PLOTING====================================== //
    ///Create line plot
    #[cfg(feature = "dashboard")]
    pub async fn plot_linee(
        df: &CustomDataFrame,
        x_col: &str, 
        y_col: &str,
        show_markers: bool,
        title: Option<&str>
    ) -> ElusionResult<PlotlyPlot> {
        let batches = df.df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        let batch = &batches[0];
        
        let x_idx = batch.schema().index_of(x_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", x_col, e)))?;
        let y_idx = batch.schema().index_of(y_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", y_col, e)))?;

        // Get arrays and convert to vectors
        let x_values: Vec<f64> = convert_to_f64_vec(batch.column(x_idx))?;
        let y_values: Vec<f64> = convert_to_f64_vec(batch.column(y_idx))?;

        // Sort values chronologically
        let (sorted_x, sorted_y) = sort_by_date(&x_values, &y_values);

        // Create trace with appropriate mode
        let trace = if show_markers {
            Scatter::new(sorted_x, sorted_y)
                .mode(Mode::LinesMarkers)
                .name(&format!("{} vs {}", y_col, x_col))
                .line(Line::new()
                    .color(Rgb::new(55, 128, 191))
                    .width(2.0))
                .marker(Marker::new()
                    .color(Rgb::new(55, 128, 191))
                    .size(8))
        } else {
            Scatter::new(sorted_x, sorted_y)
                .mode(Mode::Lines)
                .name(&format!("{} vs {}", y_col, x_col))
                .line(Line::new()
                    .color(Rgb::new(55, 128, 191))
                    .width(2.0))
        };
            
        let mut plot = PlotlyPlot::new();
        plot.add_trace(trace);
        
        // Check if x column is a date type and set axis accordingly
        let x_axis = if matches!(batch.column(x_idx).data_type(), ArrowDataType::Date32) {
            Axis::new()
                .title(x_col.to_string())
                .grid_color(Rgb::new(229, 229, 229))
                .show_grid(true)
                .type_(plotly::layout::AxisType::Date)
        } else {
            Axis::new()
                .title(x_col.to_string())
                .grid_color(Rgb::new(229, 229, 229))
                .show_grid(true)
        };

        let layout = Layout::new()
            .title(title.unwrap_or(&format!("{} vs {}", y_col, x_col))) 
            .x_axis(x_axis)
            .y_axis(Axis::new()
                .title(y_col.to_string())     
                .grid_color(Rgb::new(229, 229, 229))
                .show_grid(true));

        plot.set_layout(layout);
        Ok(plot)
    }


    /// Create time series Plot
    #[cfg(feature = "dashboard")]
    pub async fn plot_time_seriess(
        df: &CustomDataFrame,
        date_col: &str,
        value_col: &str,
        show_markers: bool,
        title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        let batches = df.df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        let batch = &batches[0];
        
        let x_idx = batch.schema().index_of(date_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", date_col, e)))?;
        let y_idx = batch.schema().index_of(value_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", value_col, e)))?;

        // Check if x column is a date type
        if !matches!(batch.column(x_idx).data_type(), ArrowDataType::Date32) {
            return Err(ElusionError::Custom(
                format!("Column {} must be a Date32 type for time series plot", date_col)
            ));
        }

        let x_values = convert_to_f64_vec(batch.column(x_idx))?;
        let y_values = convert_to_f64_vec(batch.column(y_idx))?;

        // Sort values chronologically
        let (sorted_x, sorted_y) = sort_by_date(&x_values, &y_values);

        let trace = if show_markers {
            Scatter::new(sorted_x, sorted_y)
                .mode(Mode::LinesMarkers)
                .name(value_col)
                .line(Line::new()
                    .color(Rgb::new(55, 128, 191))
                    .width(2.0))
                .marker(Marker::new()
                    .color(Rgb::new(55, 128, 191))
                    .size(8))
        } else {
            Scatter::new(sorted_x, sorted_y)
                .mode(Mode::Lines)
                .name(value_col)
                .line(Line::new()
                    .color(Rgb::new(55, 128, 191))
                    .width(2.0))
        };

        let mut plot = PlotlyPlot::new();
        plot.add_trace(trace);

        let layout = Layout::new()
            .title(title.unwrap_or(&format!("{} over Time", value_col)))
            .x_axis(Axis::new()
                .title(date_col.to_string())
                .grid_color(Rgb::new(229, 229, 229))
                .show_grid(true)
                .type_(plotly::layout::AxisType::Date))
            .y_axis(Axis::new()
                .title(value_col.to_string())
                .grid_color(Rgb::new(229, 229, 229))
                .show_grid(true));

        plot.set_layout(layout);
        Ok(plot)
    }

    /// Create a scatter plot from two columns
    #[cfg(feature = "dashboard")]
    pub async fn plot_scatterr(
        df: &CustomDataFrame,
        x_col: &str,
        y_col: &str,
        marker_size: Option<usize>,
    ) -> ElusionResult<PlotlyPlot> {
        let batches = df.df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        let batch = &batches[0];
        
        let x_idx = batch.schema().index_of(x_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", x_col, e)))?;
        let y_idx = batch.schema().index_of(y_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", y_col, e)))?;

        let x_values: Vec<f64> = convert_to_f64_vec(batch.column(x_idx))?;
        let y_values: Vec<f64> = convert_to_f64_vec(batch.column(y_idx))?;

        let trace = Scatter::new(x_values, y_values)
            .mode(Mode::Markers)
            .name(&format!("{} vs {}", y_col, x_col))
            .marker(Marker::new()
                .color(Rgb::new(55, 128, 191))
                .size(marker_size.unwrap_or(8)));

        let mut plot = PlotlyPlot::new();
        plot.add_trace(trace);
        
        let layout = Layout::new()
            .title(format!("Scatter Plot: {} vs {}", y_col, x_col))
            .x_axis(Axis::new().title(x_col.to_string()))
            .y_axis(Axis::new().title(y_col.to_string()));

        plot.set_layout(layout);
        Ok(plot)
    }

    /// Create a bar chart from two columns
    #[cfg(feature = "dashboard")]
    pub async fn plot_barr(
        df: &CustomDataFrame, 
        x_col: &str,
        y_col: &str,
        orientation: Option<&str>, 
        title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        let batches = df.df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        let batch = &batches[0];

        let x_idx = batch.schema().index_of(x_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", x_col, e)))?;
        let y_idx = batch.schema().index_of(y_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", y_col, e)))?;

        let (x_values, y_values) = if batch.column(x_idx).data_type() == &ArrowDataType::Utf8 {
            (convert_to_string_vec(batch.column(x_idx))?, convert_to_f64_vec(batch.column(y_idx))?)
        } else {
            (convert_to_string_vec(batch.column(y_idx))?, convert_to_f64_vec(batch.column(x_idx))?)
        };

        let trace = match orientation.unwrap_or("v") {
            "h" => {
                Bar::new(x_values.clone(), y_values.clone())
                    .orientation(Orientation::Horizontal)
                    .name(&format!("{} by {}", y_col, x_col))
            },
            _ => {
                Bar::new(x_values, y_values)
                    .orientation(Orientation::Vertical)
                    .name(&format!("{} by {}", y_col, x_col))
            }
        };

        let mut plot = PlotlyPlot::new();
        plot.add_trace(trace);

        let layout = Layout::new()
            .title(title.unwrap_or(&format!("Bar Chart: {} by {}", y_col, x_col)))
            .x_axis(Axis::new().title(x_col.to_string()))
            .y_axis(Axis::new().title(y_col.to_string()));

        plot.set_layout(layout);
        Ok(plot)
    }

    /// Create a histogram from a single column
    #[cfg(feature = "dashboard")]
    pub async fn plot_histogramm(
        df: &CustomDataFrame,
        col: &str,
        bins: Option<usize>,
        title: Option<&str>
    ) -> ElusionResult<PlotlyPlot> {
        let batches = df.df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        let batch = &batches[0];

        let idx = batch.schema().index_of(col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", col, e)))?;

        let values = convert_to_f64_vec(batch.column(idx))?;

        let trace = Histogram::new(values)
            .name(col)
            .n_bins_x(bins.unwrap_or(30));

        let mut plot = PlotlyPlot::new();
        plot.add_trace(trace);

        let layout = Layout::new()
            .title(title.unwrap_or(&format!("Histogram of {}", col)))
            .x_axis(Axis::new().title(col.to_string()))
            .y_axis(Axis::new().title("Count".to_string()));

        plot.set_layout(layout);
        Ok(plot)
    }

    /// Create a box plot from a column
    #[cfg(feature = "dashboard")]
    pub async fn plot_boxx(
        df: &CustomDataFrame,
        value_col: &str,
        group_by_col: Option<&str>,
        title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        let batches = df.df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        let batch = &batches[0];
        
        // Get value column index
        let value_idx = batch.schema().index_of(value_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", value_col, e)))?;

        // Convert values column
        let values = convert_to_f64_vec(batch.column(value_idx))?;

        let trace = if let Some(group_col) = group_by_col {
            // Get group column index
            let group_idx = batch.schema().index_of(group_col)
                .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", group_col, e)))?;

            // Convert group column to strings
            let groups = convert_to_f64_vec(batch.column(group_idx))?;

            BoxPlot::new(values)
                .x(groups) // Groups on x-axis
                .name(value_col)
        } else {
            BoxPlot::new(values)
                .name(value_col)
        };

        let mut plot = PlotlyPlot::new();
        plot.add_trace(trace);

        let layout = Layout::new()
            .title(title.unwrap_or(&format!("Distribution of {}", value_col)))
            .y_axis(Axis::new()
                .title(value_col.to_string())
                .grid_color(Rgb::new(229, 229, 229))
                .show_grid(true))
            .x_axis(Axis::new()
                .title(group_by_col.unwrap_or("").to_string())
                .grid_color(Rgb::new(229, 229, 229))
                .show_grid(true));

        plot.set_layout(layout);
        Ok(plot)
    }

     /// Create a pie chart from two columns: labels and values
     #[cfg(feature = "dashboard")]
     pub async fn plot_piee(
        df: &CustomDataFrame,
        label_col: &str,
        value_col: &str,
        title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        let batches = df.df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        let batch = &batches[0];

        // Get column indices
        let label_idx = batch.schema().index_of(label_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", label_col, e)))?;
        let value_idx = batch.schema().index_of(value_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", value_col, e)))?;

        // Convert columns to appropriate types
        let labels = convert_to_string_vec(batch.column(label_idx))?;
        let values = convert_to_f64_vec(batch.column(value_idx))?;

        // Create the pie chart trace
        let trace = Pie::new(values)
            .labels(labels)
            .name(value_col)
            .hole(0.0);

        let mut plot = PlotlyPlot::new();
        plot.add_trace(trace);

        // Create layout
        let layout = Layout::new()
            .title(title.unwrap_or(&format!("Distribution of {}", value_col)))
            .show_legend(true);

        plot.set_layout(layout);
        Ok(plot)
    }

    /// Create a donut chart (pie chart with a hole)
    #[cfg(feature = "dashboard")]
    pub async fn plot_donutt(
        df: &CustomDataFrame, 
        label_col: &str,
        value_col: &str,
        title: Option<&str>,
        hole_size: Option<f64>, // Value between 0 and 1
    ) -> ElusionResult<PlotlyPlot> {
        let batches = df.df.clone().collect().await.map_err(ElusionError::DataFusion)?;
        let batch = &batches[0];

        let label_idx = batch.schema().index_of(label_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", label_col, e)))?;
        let value_idx = batch.schema().index_of(value_col)
            .map_err(|e| ElusionError::Custom(format!("Column {} not found: {}", value_col, e)))?;

        let labels = convert_to_string_vec(batch.column(label_idx))?;
        let values = convert_to_f64_vec(batch.column(value_idx))?;

        // Ensure hole size is between 0 and 1
        let hole_size = hole_size.unwrap_or(0.5).max(0.0).min(1.0);

        let trace = Pie::new(values)
            .labels(labels)
            .name(value_col)
            .hole(hole_size); 

        let mut plot = PlotlyPlot::new();
        plot.add_trace(trace);

        let layout = Layout::new()
            .title(title.unwrap_or(&format!("Distribution of {}", value_col)))
            .show_legend(true);

        plot.set_layout(layout);
        Ok(plot)
    }

     // -------------Interactive Charts
    #[cfg(feature = "dashboard")]
    pub async fn plot_line_impl(
        df: &CustomDataFrame, 
        date_col: &str,
        value_col: &str,
        show_markers: bool,
        title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        let mut plot = plot_linee(df, date_col, value_col, show_markers, title).await?;

        // Create range selector buttons
        let buttons = vec![
            Button::new()
                .name("1m")
                .args(json!({
                    "xaxis.range": ["now-1month", "now"]
                }))
                .label("1m"),
            Button::new()
                .name("6m")
                .args(json!({
                    "xaxis.range": ["now-6months", "now"]
                }))
                .label("6m"),
            Button::new()
                .name("1y")
                .args(json!({
                    "xaxis.range": ["now-1year", "now"]
                }))
                .label("1y"),
            Button::new()
                .name("YTD")
                .args(json!({
                    "xaxis.range": ["now-ytd", "now"]
                }))
                .label("YTD"),
            Button::new()
                .name("all")
                .args(json!({
                    "xaxis.autorange": true
                }))
                .label("All")
        ];

        // Update layout with range selector and slider
        let layout = plot.layout().clone()
            .x_axis(Axis::new()
                .title(date_col.to_string())
                .grid_color(Rgb::new(229, 229, 229))
                .show_grid(true)
                .type_(plotly::layout::AxisType::Date)
                .range_slider(RangeSlider::new().visible(true)))
            .update_menus(vec![
                UpdateMenu::new()
                    .buttons(buttons)
                    .direction(UpdateMenuDirection::Down)
                    .show_active(true)
            ])
            .drag_mode(DragMode::Zoom);

        plot.set_layout(layout);
        Ok(plot)
    }

     /// Create an enhanced time series plot with range selector buttons
     #[cfg(feature = "dashboard")]
     pub async fn plot_time_series_impl(
        df: &CustomDataFrame, 
        date_col: &str,
        value_col: &str,
        show_markers: bool,
        title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        let mut plot = plot_time_seriess(df, date_col, value_col, show_markers, title).await?;

        // Create range selector buttons
        let buttons = vec![
            Button::new()
                .name("1m")
                .args(json!({
                    "xaxis.range": ["now-1month", "now"]
                }))
                .label("1m"),
            Button::new()
                .name("6m")
                .args(json!({
                    "xaxis.range": ["now-6months", "now"]
                }))
                .label("6m"),
            Button::new()
                .name("1y")
                .args(json!({
                    "xaxis.range": ["now-1year", "now"]
                }))
                .label("1y"),
            Button::new()
                .name("YTD")
                .args(json!({
                    "xaxis.range": ["now-ytd", "now"]
                }))
                .label("YTD"),
            Button::new()
                .name("all")
                .args(json!({
                    "xaxis.autorange": true
                }))
                .label("All")
        ];

        // Update layout with range selector and slider
        let layout = plot.layout().clone()
            .x_axis(Axis::new()
                .title(date_col.to_string())
                .grid_color(Rgb::new(229, 229, 229))
                .show_grid(true)
                .type_(plotly::layout::AxisType::Date)
                .range_slider(RangeSlider::new().visible(true)))
            .update_menus(vec![
                UpdateMenu::new()
                    .buttons(buttons)
                    .direction(UpdateMenuDirection::Down)
                    .show_active(true)
            ])
            .drag_mode(DragMode::Zoom);

        plot.set_layout(layout);
        Ok(plot)
    }


    /// Create an enhanced bar chart with sort buttons
    #[cfg(feature = "dashboard")]
    pub async fn plot_bar_impl(
        df: &CustomDataFrame,
        x_col: &str,
        y_col: &str,
        title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        let mut plot = plot_barr(df, x_col, y_col, None, title).await?;

        // Create sort buttons
        let update_menu_buttons = vec![
            Button::new()
                .name("reset")
                .args(json!({
                    "xaxis.type": "category",
                    "xaxis.categoryorder": "trace"
                }))
                .label("Reset"),
            Button::new()
                .name("ascending")
                .args(json!({
                    "xaxis.type": "category",
                    "xaxis.categoryorder": "total ascending"
                }))
                .label("Sort Ascending"),
            Button::new()
                .name("descending")
                .args(json!({
                    "xaxis.type": "category",
                    "xaxis.categoryorder": "total descending"
                }))
                .label("Sort Descending")
        ];

        // Update layout with buttons
        let layout = plot.layout().clone()
            .show_legend(true)
            .update_menus(vec![
                UpdateMenu::new()
                    .buttons(update_menu_buttons)
                    .direction(UpdateMenuDirection::Down)
                    .show_active(true)
            ]);

        plot.set_layout(layout);
        Ok(plot)
    }

    /// Create an enhanced scatter plot with zoom and selection modes
    #[cfg(feature = "dashboard")]
    pub async fn plot_scatter_impl(
        df: &CustomDataFrame,
        x_col: &str,
        y_col: &str,
        marker_size: Option<usize>,
    ) -> ElusionResult<PlotlyPlot> {
        let mut plot = plot_scatterr(df, x_col, y_col, marker_size).await?;

        // Create mode buttons
        let mode_buttons = vec![
            Button::new()
                .name("zoom")
                .args(json!({
                    "dragmode": "zoom"
                }))
                .label("Zoom"),
            Button::new()
                .name("select")
                .args(json!({
                    "dragmode": "select"
                }))
                .label("Select"),
            Button::new()
                .name("pan")
                .args(json!({
                    "dragmode": "pan"
                }))
                .label("Pan")
        ];

        // Update layout with buttons
        let layout = plot.layout().clone()
            .show_legend(true)
            .drag_mode(DragMode::Zoom)
            .update_menus(vec![
                UpdateMenu::new()
                    .buttons(mode_buttons)
                    .direction(UpdateMenuDirection::Down)
                    .show_active(true)
            ]);
            
        plot.set_layout(layout);
        Ok(plot)
    }

    ///Interactive histogram plot
    #[cfg(feature = "dashboard")]
    pub async fn plot_histogram_impl(
        df: &CustomDataFrame,
        col: &str,
        title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        let mut plot = plot_histogramm(df, col, None, title).await?;
    
        // Create binning control buttons
        let bin_buttons = vec![
            Button::new()
                .name("bins10")
                .args(json!({
                    "xbins.size": 10
                }))
                .label("10 Bins"),
            Button::new()
                .name("bins20")
                .args(json!({
                    "xbins.size": 20
                }))
                .label("20 Bins"),
            Button::new()
                .name("bins30")
                .args(json!({
                    "xbins.size": 30
                }))
                .label("30 Bins")
        ];
    
        let layout = plot.layout().clone()
            .show_legend(true)
            .update_menus(vec![
                UpdateMenu::new()
                    .buttons(bin_buttons)
                    .direction(UpdateMenuDirection::Down)
                    .show_active(true)
            ]);
    
        plot.set_layout(layout);
        Ok(plot)
    }


    ///Interactive Box plot
    #[cfg(feature = "dashboard")]
    pub async fn plot_box_impl(
        df: &CustomDataFrame,
        value_col: &str,
        group_by_col: Option<&str>,
        title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        let mut plot = plot_boxx(df, value_col, group_by_col, title).await?;
    
        // Create outlier control buttons
        let outlier_buttons = vec![
            Button::new()
                .name("show_outliers")
                .args(json!({
                    "boxpoints": "outliers"
                }))
                .label("Show Outliers"),
            Button::new()
                .name("hide_outliers")
                .args(json!({
                    "boxpoints": false
                }))
                .label("Hide Outliers")
        ];
    
        let layout = plot.layout().clone()
            .show_legend(true)
            .update_menus(vec![
                UpdateMenu::new()
                    .buttons(outlier_buttons)
                    .direction(UpdateMenuDirection::Down)
                    .show_active(true)
            ]);
    
        plot.set_layout(layout);
        Ok(plot)
    }

    ///Interactive Pie Plot
    #[cfg(feature = "dashboard")]
    pub async fn plot_pie_impl(
        df: &CustomDataFrame,
        label_col: &str,
        value_col: &str,
        title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        let mut plot = plot_piee(df, label_col, value_col, title).await?;
    
        // Create display mode buttons
        let display_buttons = vec![
            Button::new()
                .name("percentage")
                .args(json!({
                    "textinfo": "percent"
                }))
                .label("Show Percentages"),
            Button::new()
                .name("values")
                .args(json!({
                    "textinfo": "value"
                }))
                .label("Show Values"),
            Button::new()
                .name("both")
                .args(json!({
                    "textinfo": "value+percent"
                }))
                .label("Show Both")
        ];
    
        let layout = plot.layout().clone()
            .show_legend(true)
            .update_menus(vec![
                UpdateMenu::new()
                    .buttons(display_buttons)
                    .direction(UpdateMenuDirection::Down)
                    .show_active(true)
            ]);
    
        plot.set_layout(layout);
        Ok(plot)
    }


    ///Interactive Donut Plot
    #[cfg(feature = "dashboard")]
    pub async fn plot_donut_impl(
        df: &CustomDataFrame,
        label_col: &str,
        value_col: &str,
        title: Option<&str>,
    ) -> ElusionResult<PlotlyPlot> {
        let mut plot = plot_donutt(df, label_col, value_col, title, Some(0.5)).await?;
    
        // Create hole size control buttons
        let hole_buttons = vec![
            Button::new()
                .name("small")
                .args(json!({
                    "hole": 0.3
                }))
                .label("Small Hole"),
            Button::new()
                .name("medium")
                .args(json!({
                    "hole": 0.5
                }))
                .label("Medium Hole"),
            Button::new()
                .name("large")
                .args(json!({
                    "hole": 0.7
                }))
                .label("Large Hole")
        ];
    
        let layout = plot.layout().clone()
            .show_legend(true)
            .update_menus(vec![
                UpdateMenu::new()
                    .buttons(hole_buttons)
                    .direction(UpdateMenuDirection::Down)
                    .show_active(true)
            ]);
    
        plot.set_layout(layout);
        Ok(plot)
    }

    //=========== REPORTING =============

    /// Create an enhanced report with interactive features
    #[cfg(feature = "dashboard")]
    pub async fn create_report_impl(
        plots: Option<&[(&PlotlyPlot, &str)]>,
        tables: Option<&[(&CustomDataFrame, &str)]>,
        report_title: &str,
        filename: &str,  // Full path including filename
        layout_config: Option<ReportLayout>,
        table_options: Option<TableOptions>, 
    ) -> ElusionResult<()> {
        
        if let Some(parent) = LocalPath::new(filename).parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)?;
            }
        }
    
        let file_path_str = LocalPath::new(filename).to_str()
            .ok_or_else(|| ElusionError::Custom("Invalid path".to_string()))?;
    
        // Get layout configuration
        let layout = layout_config.unwrap_or_default();
    
        // Create plot containers HTML if plots are provided
        let plot_containers = plots.map(|plots| {
            plots.iter().enumerate()
                .map(|(i, (plot, title))| format!(
                    r#"<div class="plot-container" 
                        data-plot-data='{}'
                        data-plot-layout='{}'>
                        <div class="plot-title">{}</div>
                        <div id="plot_{}" style="width:100%;height:{}px;"></div>
                    </div>"#,
                    serde_json::to_string(plot.data()).unwrap(),
                    serde_json::to_string(plot.layout()).unwrap(),
                    title,
                    i,
                    layout.plot_height
                ))
                .collect::<Vec<_>>()
                .join("\n")
        }).unwrap_or_default();
    
        // Create table containers HTML if tables are provided
        let table_containers = if let Some(tables) = tables {

            let table_op = TableOptions::default();
            let table_opts = table_options.as_ref().unwrap_or(&table_op);

            let mut containers = Vec::new();
            for (i, (df, title)) in tables.iter().enumerate() {
                // Access the inner DataFrame with df.df
                let batches = df.df.clone().collect().await?;
                let schema = df.df.schema();
                let columns = schema.fields().iter()
                    .map(|f| {
                        let base_def = format!(
                            r#"{{
                                field: "{}",
                                headerName: "{}",
                                sortable: true,
                                filter: true,
                                resizable: true"#,
                            f.name(),
                            f.name()
                        );

                        // Add date-specific formatting for date columns and potential string dates
                        let column_def = match f.data_type() {
                            ArrowDataType::Date32 | ArrowDataType::Date64 | ArrowDataType::Timestamp(_, _) => {
                                format!(
                                    r#"{},
                                    filter: 'agDateColumnFilter',
                                    filterParams: {{
                                        browserDatePicker: true,
                                        minValidYear: 1000,
                                        maxValidYear: 9999
                                    }}"#,
                                    base_def
                                )
                            },
                            ArrowDataType::Utf8 if f.name().to_lowercase().contains("date") || 
                                                f.name().to_lowercase().contains("time") => {
                                format!(
                                    r#"{},
                                    filter: 'agDateColumnFilter',
                                    filterParams: {{
                                        browserDatePicker: true,
                                        minValidYear: 1000,
                                        maxValidYear: 9999,
                                        comparator: (filterValue, cellValue) => {{
                                            try {{
                                                const filterDate = new Date(filterValue);
                                                const cellDate = new Date(cellValue);
                                                if (!isNaN(filterDate) && !isNaN(cellDate)) {{
                                                    return cellDate - filterDate;
                                                }}
                                            }} catch (e) {{}}
                                            return 0;
                                        }}
                                    }}"#,
                                    base_def
                                )
                            },
                            _ => base_def,
                        };

                        // Close the column definition object
                        format!("{}}}", column_def)
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                
                // Convert batches to rows
                let mut rows = Vec::new();
                for batch in &batches {
                    for row_idx in 0..batch.num_rows() {
                        let mut row = serde_json::Map::new();
                        for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                            let col = batch.column(col_idx);
                            let value = match col.data_type() {
                                ArrowDataType::Int32 => {
                                    let array = col.as_any().downcast_ref::<Int32Array>().unwrap();
                                    if array.is_null(row_idx) {
                                        serde_json::Value::Null
                                    } else {
                                        serde_json::Value::Number(array.value(row_idx).into())
                                    }
                                },
                                ArrowDataType::Int64 => {
                                    let array = col.as_any().downcast_ref::<Int64Array>().unwrap();
                                    if array.is_null(row_idx) {
                                        serde_json::Value::Null
                                    } else {
                                        serde_json::Value::Number(array.value(row_idx).into())
                                    }
                                },
                                ArrowDataType::Float64 => {
                                    let array = col.as_any().downcast_ref::<Float64Array>().unwrap();
                                    if array.is_null(row_idx) {
                                        serde_json::Value::Null
                                    } else {
                                        let num = array.value(row_idx);
                                        if num.is_finite() {
                                            serde_json::Number::from_f64(num)
                                                .map(serde_json::Value::Number)
                                                .unwrap_or(serde_json::Value::Null)
                                        } else {
                                            serde_json::Value::Null
                                        }
                                    }
                                },
                                ArrowDataType::Date32 => {
                                    let array = col.as_any().downcast_ref::<Date32Array>().unwrap();
                                    if array.is_null(row_idx) {
                                        serde_json::Value::Null
                                    } else {
                                        let days = array.value(row_idx);
                                        // Using your convert_date32_to_timestamps logic but adapting for a single value
                                        let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163)
                                            .unwrap_or(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());
                                        let datetime = date.and_hms_opt(0, 0, 0).unwrap();
                                        serde_json::Value::String(datetime.format("%Y-%m-%d").to_string())
                                    }
                                },
                                ArrowDataType::Date64 => {
                                    let array = col.as_any().downcast_ref::<Date64Array>().unwrap();
                                    if array.is_null(row_idx) {
                                        serde_json::Value::Null
                                    } else {
                                        let ms = array.value(row_idx);
                                        let datetime = chrono::DateTime::from_timestamp_millis(ms)
                                            .unwrap_or_default()
                                            .naive_utc();
                                        serde_json::Value::String(datetime.format("%Y-%m-%d %H:%M:%S").to_string())
                                    }
                                },
                                ArrowDataType::Timestamp(time_unit, None) => {
                                    let array = col.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
                                    if array.is_null(row_idx) {
                                        serde_json::Value::Null
                                    } else {
                                        let ts = array.value(row_idx);
                                        let datetime = match time_unit {
                                            TimeUnit::Second => chrono::DateTime::from_timestamp(ts, 0),
                                            TimeUnit::Millisecond => chrono::DateTime::from_timestamp_millis(ts),
                                            TimeUnit::Microsecond => chrono::DateTime::from_timestamp_micros(ts),
                                            TimeUnit::Nanosecond => chrono::DateTime::from_timestamp(
                                                ts / 1_000_000_000,
                                                (ts % 1_000_000_000) as u32
                                            ),
                                        }.unwrap_or_default().naive_utc();
                                        serde_json::Value::String(datetime.format("%Y-%m-%d %H:%M:%S").to_string())
                                    }
                                },
                                ArrowDataType::Utf8 => {
                                    let array = col.as_any().downcast_ref::<StringArray>().unwrap();
                                    if array.is_null(row_idx) {
                                        serde_json::Value::Null
                                    } else {
                                        let value = array.value(row_idx);
                                        // Try to parse as date if the column name suggests it might be a date
                                        if field.name().to_lowercase().contains("date") || 
                                           field.name().to_lowercase().contains("time") {
                                            if let Some(datetime) = parse_date_string(value) {
                                                serde_json::Value::String(
                                                    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
                                                )
                                            } else {
                                                serde_json::Value::String(value.to_string())
                                            }
                                        } else {
                                            serde_json::Value::String(value.to_string())
                                        }
                                    }
                                },
                                _ => serde_json::Value::Null,
                            };
                            row.insert(field.name().clone(), value);
                        }
                        rows.push(serde_json::Value::Object(row));
                    }
                }

                let container = format!(
                    r#"<div class="table-container">
                        <div class="table-title">{0}</div>
                        <div id="grid_{1}" class="{2}" style="width:100%;height:{3}px;">
                            <!-- AG Grid will be rendered here -->
                        </div>
                        <script>
                            (function() {{
                                console.log('Initializing grid_{1}');
                                
                                // Column definitions with more detailed configuration
                                const columnDefs = [{4}];
                                console.log('Column definitions:', columnDefs);
                
                                const rowData = {5};
                                console.log('Row data:', rowData);
                
                                // Grid options with more features
                                const gridOptions = {{
                                    columnDefs: columnDefs,
                                    rowData: rowData,
                                    pagination: {6},
                                    paginationPageSize: {7},
                                    defaultColDef: {{
                                        flex: 1,
                                        minWidth: 100,
                                        sortable: {8},
                                        filter: {9},
                                        floatingFilter: true,
                                        resizable: true,
                                        cellClass: 'ag-cell-font-size'
                                    }},
                                    onGridReady: function(params) {{
                                        console.log('Grid Ready event fired for grid_{1}');
                                        params.api.sizeColumnsToFit();
                                        const event = new CustomEvent('gridReady');
                                        gridDiv.dispatchEvent(event);
                                    }},
                                    enableRangeSelection: true,
                                    enableCharts: true,
                                    popupParent: document.body,
                                    // Add styling options
                                    headerClass: "ag-header-cell",
                                    rowClass: "ag-row-font-size",
                                    sideBar: {{
                                        toolPanels: ['columns', 'filters'],
                                        defaultToolPanel: '',
                                        hiddenByDefault: {10}
                                    }}
                                }};
                
                                // Initialize AG Grid
                                const gridDiv = document.querySelector('#grid_{1}');
                                console.log('Grid container:', gridDiv);
                                console.log('AG Grid loaded:', typeof agGrid !== 'undefined');
                                
                                if (!gridDiv) {{
                                    console.error('Grid container not found for grid_{1}');
                                    return;
                                }}
                                
                                try {{
                                    new agGrid.Grid(gridDiv, gridOptions);
                                    gridDiv.gridOptions = gridOptions;
                                }} catch (error) {{
                                    console.error('Error initializing AG Grid:', error);
                                }}
                            }})();
                        </script>
                    </div>"#,
                    title,                   // {0}
                    i,                      // {1}
                    table_opts.theme,       // {2}
                    layout.table_height,    // {3}
                    columns,               // {4} Column definitions
                    serde_json::to_string(&rows).unwrap_or_default(),  // {5}
                    table_opts.pagination,  // {6}
                    table_opts.page_size,   // {7}
                    table_opts.enable_sorting,  // {8}
                    table_opts.enable_filtering,  // {9}
                    !table_opts.enable_column_menu  // {10}
                );
                containers.push(container);
            }
            containers.join("\n")
        } else {
            String::new()
        };
    
        let html_content = format!(
            r#"<!DOCTYPE html>
            <html>
            <head>
                <title>{0}</title>
                {1}
                {2}
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                        margin: 0;
                        padding: 20px;
                        background-color: #f5f5f5;
                    }}
                    .container {{
                        max-width: {3}px;
                        margin: 0 auto;
                        background-color: white;
                        padding: 20px;
                        border-radius: 8px;
                        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                    }}
                    h1 {{
                        color: #333;
                        text-align: center;
                        margin-bottom: 30px;
                    }}
                    .controls {{
                        margin-bottom: 20px;
                        padding: 15px;
                        background: #f8f9fa;
                        border-radius: 8px;
                        display: flex;
                        gap: 10px;
                        justify-content: center;
                    }}
                    .controls button {{
                        padding: 8px 16px;
                        border: none;
                        border-radius: 4px;
                        background: #007bff;
                        color: white;
                        cursor: pointer;
                        transition: background 0.2s;
                    }}
                    .controls button:hover {{
                        background: #0056b3;
                    }}
                    .controls button {{
                        padding: 8px 16px;
                        border: none;
                        border-radius: 4px;
                        background: #007bff;
                        color: white;
                        cursor: pointer;
                        transition: background 0.2s;
                    }}
                    .controls button:hover {{
                        background: #0056b3;
                    }}
                    .controls button.export-button {{
                        background: #28a745; 
                    }}
                    .controls button.export-button:hover {{
                        background: #218838; 
                    }}
                    .grid {{
                        display: grid;
                        grid-template-columns: repeat({4}, 1fr);
                        gap: {5}px;
                    }}
                    .plot-container, .table-container {{
                        background: white;
                        padding: 15px;
                        border-radius: 8px;
                        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
                    }}
                    .plot-title, .table-title {{
                        font-size: 18px;
                        font-weight: bold;
                        margin-bottom: 10px;
                        color: #444;
                    }}
                    @media (max-width: 768px) {{
                        .grid {{
                            grid-template-columns: 1fr;
                        }}
                    }}
                    .loading {{
                        display: none;
                        position: fixed;
                        top: 50%;
                        left: 50%;
                        transform: translate(-50%, -50%);
                        background: rgba(255,255,255,0.9);
                        padding: 20px;
                        border-radius: 8px;
                        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                    }}
                    .ag-cell-font-size {{
                        font-size: 17px;
                    }}

                    .ag-cell-bold {{
                        font-weight: bold;
                    }}

                    .ag-header-cell {{
                        font-weight: bold;
                        border-bottom: 1px solid #3fdb59;
                    }}

                    .align-right {{
                        text-align: left;
                    }}

                    .ag-theme-alpine {{
                        --ag-font-size: 17px;
                        --ag-header-height: 40px;
                    }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>{0}</h1>
                    <div class="controls">
                        {6}
                    </div>
                    <div id="loading" class="loading">Processing...</div>
                    {7}
                    {8}
                </div>
                <script>
                    {9}
                </script>
            </body>
            </html>"#,
            report_title,                // {0}
            if plots.is_some() {         // {1}
                r#"<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>"#
            } else { "" },
            if tables.is_some() {        // {2}
                r#"
                <script>
                    // Check if AG Grid is already loaded
                    console.log('AG Grid script loading status:', typeof agGrid !== 'undefined');
                </script>
                <script src="https://cdn.jsdelivr.net/npm/ag-grid-community@31.0.1/dist/ag-grid-community.min.js"></script>
                <script src="https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js"></script>
                <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/ag-grid-community@31.0.1/styles/ag-grid.css">
                <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/ag-grid-community@31.0.1/styles/ag-theme-alpine.css">
                <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/ag-grid-community@31.0.1/styles/ag-theme-quartz.css">
                <script>
                    // Verify AG Grid loaded correctly
                    document.addEventListener('DOMContentLoaded', function() {
                        console.log('AG Grid loaded check:', typeof agGrid !== 'undefined');
                        console.log('XLSX loaded check:', typeof XLSX !== 'undefined');
                    });
                </script>
                "#
            } else { "" },
            layout.max_width,            // {3}
            layout.grid_columns,         // {4}
            layout.grid_gap,            // {5}
            generate_controls(plots.is_some(), tables.is_some()),  // {6}
            if !plot_containers.is_empty() {  // {7}
                format!(r#"<div class="grid">{}</div>"#, plot_containers)
            } else { String::new() },
            if !table_containers.is_empty() {  // {8}
                format!(r#"<div class="tables">{}</div>"#, table_containers)
            } else { String::new() },
            generate_javascript(plots.is_some(), tables.is_some(), layout.grid_columns)  // {9}
        );
    
        std::fs::write(file_path_str, html_content)?;
        println!("✅ Interactive Dashboard created at {}", file_path_str);
        Ok(())
    }

    // ReportLayout with all fields when dashboard feature is enabled
    #[cfg(feature = "dashboard")]
    #[derive(Debug, Clone)]
    pub struct ReportLayout {
        pub grid_columns: usize,     // Number of columns in the grid
        pub grid_gap: usize,         // Gap between plots in pixels
        pub max_width: usize,        // Maximum width of the container
        pub plot_height: usize,      // Height of each plot
        pub table_height: usize,     // Height of each table
    }

    #[cfg(feature = "dashboard")]
    impl Default for ReportLayout {
        fn default() -> Self {
            Self {
                grid_columns: 2,
                grid_gap: 20,
                max_width: 1200,
                plot_height: 400,
                table_height: 400,
            }
        }
    }

    // ReportLayout stub with all the same fields when dashboard feature is not enabled
    #[cfg(not(feature = "dashboard"))]
    #[derive(Debug, Clone)]
    pub struct ReportLayout {
        pub grid_columns: usize,
        pub grid_gap: usize, 
        pub max_width: usize,
        pub plot_height: usize,
        pub table_height: usize,
    }


    #[cfg(feature = "dashboard")]
    #[derive(Debug, Clone)]
    pub struct TableOptions {
        pub pagination: bool,
        pub page_size: usize,
        pub enable_sorting: bool,
        pub enable_filtering: bool,
        pub enable_column_menu: bool,
        pub theme: String,           // "ag-theme-alpine", "ag-theme-balham", etc.
    }

    #[cfg(feature = "dashboard")]
    impl Default for TableOptions {
        fn default() -> Self {
            Self {
                pagination: true,
                page_size: 10,
                enable_sorting: true,
                enable_filtering: true,
                enable_column_menu: true,
                theme: "ag-theme-alpine".to_string(),
            }
        }
    }

    #[cfg(not(feature = "dashboard"))]
    #[derive(Debug, Clone)]
    pub struct TableOptions {
        pub pagination: bool,
        pub page_size: usize,
        pub enable_sorting: bool,
        pub enable_filtering: bool,
        pub enable_column_menu: bool,
        pub theme: String,
    }

    #[cfg(feature = "dashboard")]
    fn generate_controls(has_plots: bool, has_tables: bool) -> String {
        let mut controls = Vec::new();
        
        if has_plots {
            controls.extend_from_slice(&[
                r#"<button onclick="toggleGrid()">Toggle Layout</button>"#
            ]);
        }
        
        if has_tables {
            controls.extend_from_slice(&[
                r#"<button onclick="exportAllTables()" class="export-button">Export to CSV</button>"#,
                r#"<button onclick="exportToExcel()" class="export-button">Export to Excel</button>"#
            ]);
        }
        
        controls.join("\n")
    }

    #[cfg(feature = "dashboard")]
    fn generate_javascript(has_plots: bool, has_tables: bool, grid_columns: usize) -> String {
        let mut js = String::new();
        
        // Common utilities
        js.push_str(r#"
        document.addEventListener('DOMContentLoaded', function() {
            console.log('DOMContentLoaded event fired');
            showLoading();
            
            const promises = [];
            
            // Wait for plots if they exist
            const plotContainers = document.querySelectorAll('.plot-container');
            console.log('Found plot containers:', plotContainers.length);
            
            if (plotContainers.length > 0) {
                promises.push(...Array.from(plotContainers).map(container => 
                    new Promise(resolve => {
                        const observer = new MutationObserver((mutations, obs) => {
                            if (container.querySelector('.js-plotly-plot')) {
                                obs.disconnect();
                                resolve();
                            }
                        });
                        observer.observe(container, { childList: true, subtree: true });
                    })
                ));
            }
            
            // Wait for grids if they exist
            const gridContainers = document.querySelectorAll('[id^="grid_"]');
            console.log('Found grid containers:', gridContainers.length);
            
            if (gridContainers.length > 0) {
                promises.push(...Array.from(gridContainers).map(container => 
                    new Promise(resolve => {
                        container.addEventListener('gridReady', () => {
                            console.log('Grid ready event received for:', container.id);
                            resolve();
                        }, { once: true });
                        // Add a timeout to prevent infinite waiting
                        setTimeout(() => {
                            console.log('Grid timeout for:', container.id);
                            resolve();
                        }, 5000);
                    })
                ));
            }
            
            // If no async content to wait for, hide loading immediately
            if (promises.length === 0) {
                console.log('No async content to wait for');
                hideLoading();
                return;
            }
            
            // Wait for all content to load or timeout
            Promise.all(promises)
                .then(() => {
                    console.log('All content loaded successfully');
                    hideLoading();
                    showNotification('Report loaded successfully', 'info');
                })
                .catch(error => {
                    console.error('Error loading report:', error);
                    hideLoading();
                    showNotification('Error loading some components', 'error');
                });
            });
        "#);

        // Plots JavaScript
        if has_plots {
            js.push_str(&format!(r#"
                const plots = [];
                let currentGridColumns = {};
                
                document.querySelectorAll('.plot-container').forEach((container, index) => {{
                    const plotDiv = container.querySelector(`#plot_${{index}}`);
                    const data = JSON.parse(container.dataset.plotData);
                    const layout = JSON.parse(container.dataset.plotLayout);
                    
                    layout.autosize = true;
                    
                    Plotly.newPlot(plotDiv, data, layout, {{
                        responsive: true,
                        scrollZoom: true,
                        modeBarButtonsToAdd: [
                            'hoverClosestCartesian',
                            'hoverCompareCartesian'
                        ],
                        displaylogo: false
                    }}).then(gd => {{
                        plots.push(gd);
                        gd.on('plotly_click', function(data) {{
                            highlightPoint(data, index);
                        }});
                    }}).catch(error => {{
                        console.error('Error creating plot:', error);
                        showNotification('Error creating plot', 'error');
                    }});
                }});


                function toggleGrid() {{
                    const grid = document.querySelector('.grid');
                    currentGridColumns = currentGridColumns === {0} ? 1 : {0};
                    grid.style.gridTemplateColumns = `repeat(${{currentGridColumns}}, 1fr)`;
                    showNotification(`Layout changed to ${{currentGridColumns}} column(s)`);
                }}

                function highlightPoint(data, plotIndex) {{
                    if (!data.points || !data.points[0]) return;
                    
                    const point = data.points[0];
                    const pointColor = 'red';
                    
                    plots.forEach((plot, idx) => {{
                        if (idx !== plotIndex) {{
                            const trace = plot.data[0];
                            if (trace.x && trace.y) {{
                                const matchingPoints = trace.x.map((x, i) => {{
                                    return {{x, y: trace.y[i]}};
                                }}).filter(p => p.x === point.x && p.y === point.y);
                                
                                if (matchingPoints.length > 0) {{
                                    Plotly.restyle(plot, {{'marker.color': pointColor}}, [0]);
                                }}
                            }}
                        }}
                    }});
                }}
            "#, grid_columns));
        }

        // Tables JavaScript
        if has_tables {
            js.push_str(r#"
                // Table utility functions
                function exportAllTables() {
                    try {
                        document.querySelectorAll('.ag-theme-alpine').forEach((container, index) => {
                            const gridApi = container.gridOptions.api;
                            const csvContent = gridApi.getDataAsCsv({
                                skipHeader: false,
                                skipFooters: true,
                                skipGroups: true,
                                fileName: `table_${index}.csv`
                            });
                            
                            const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
                            const link = document.createElement('a');
                            link.href = URL.createObjectURL(blob);
                            link.download = `table_${index}.csv`;
                            link.click();
                        });
                        showNotification('Exported all tables');
                    } catch (error) {
                        console.error('Error exporting tables:', error);
                        showNotification('Error exporting tables', 'error');
                    }
                }

                function exportToExcel() {
                try {
                    document.querySelectorAll('.ag-theme-alpine').forEach((container, index) => {
                        const gridApi = container.gridOptions.api;
                        const columnApi = container.gridOptions.columnApi;
                        
                        // Get displayed columns
                        const columns = columnApi.getAllDisplayedColumns();
                        const columnDefs = columns.map(col => ({
                            header: col.colDef.headerName || col.colDef.field,
                            field: col.colDef.field
                        }));
                        
                        // Get all rows data
                        const rowData = [];
                        gridApi.forEachNode(node => {
                            const row = {};
                            columnDefs.forEach(col => {
                                row[col.header] = node.data[col.field];
                            });
                            rowData.push(row);
                        });
                        
                        // Create workbook and worksheet
                        const worksheet = XLSX.utils.json_to_sheet(rowData);
                        const workbook = XLSX.utils.book_new();
                        XLSX.utils.book_append_sheet(workbook, worksheet, `Table_${index}`);
                        
                        // Generate Excel file
                        const excelBuffer = XLSX.write(workbook, { bookType: 'xlsx', type: 'array' });
                        const blob = new Blob([excelBuffer], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
                        const url = window.URL.createObjectURL(blob);
                        const link = document.createElement('a');
                        link.href = url;
                        link.download = `table_${index}.xlsx`;
                        document.body.appendChild(link);
                        link.click();
                        
                        // Cleanup
                        setTimeout(() => {
                            document.body.removeChild(link);
                            window.URL.revokeObjectURL(url);
                        }, 0);
                    });
                    showNotification('Exported tables to Excel');
                } catch (error) {
                    console.error('Error exporting to Excel:', error);
                    showNotification('Error exporting to Excel', 'error');
                }
            }
        
                // Initialize AG Grid Quick Filter after DOM is loaded
                document.addEventListener('DOMContentLoaded', function() {
                    document.querySelectorAll('.ag-theme-alpine').forEach(container => {
                        const gridOptions = container.gridOptions;
                        if (gridOptions) {
                            console.log('Initializing quick filter for container:', container.id);
                            const quickFilterInput = document.createElement('input');
                            quickFilterInput.type = 'text';
                            quickFilterInput.placeholder = 'Quick Filter...';
                            quickFilterInput.className = 'quick-filter';
                            quickFilterInput.style.cssText = 'margin: 10px 0; padding: 5px; width: 200px;';
                            
                            quickFilterInput.addEventListener('input', function(e) {
                                gridOptions.api.setQuickFilter(e.target.value);
                            });
                            
                            container.parentNode.insertBefore(quickFilterInput, container);
                        }
                    });
                });
            "#);
        }

        // Add notification styles
        js.push_str(r#"
            const style = document.createElement('style');
            style.textContent = `
                .notification {
                    position: fixed;
                    bottom: 20px;
                    right: 20px;
                    padding: 10px 20px;
                    border-radius: 4px;
                    color: white;
                    font-weight: bold;
                    z-index: 1000;
                    animation: slideIn 0.5s ease-out;
                }
                .notification.info {
                    background-color: #007bff;
                }
                .notification.error {
                    background-color: #dc3545;
                }
                @keyframes slideIn {
                    from { transform: translateX(100%); }
                    to { transform: translateX(0); }
                }
            `;
            document.head.appendChild(style);
        "#);

        js
    }

