use datafusion::logical_expr::{Expr, col};
use datafusion::functions_aggregate::expr_fn::{
    sum, min, max, avg, stddev, count, count_distinct, corr, first_value, grouping,
    var_pop, stddev_pop, array_agg,
};

pub struct AggregationBuilder {
    column: String,
    pub agg_alias: Option<String>,
    agg_fn: Option<Box<dyn Fn(Expr) -> Expr>>, // Store aggregation function
}

impl AggregationBuilder {
    pub fn new(column: &str) -> Self {
        Self {
            column: column.to_string(),
            agg_alias: None,
            agg_fn: None, // No aggregation function initially
        }
    }

    pub fn alias(mut self, alias: &str) -> Self {
        self.agg_alias = Some(alias.to_string());
        self
    }

    pub fn sum(mut self) -> Self {
        self.agg_fn = Some(Box::new(sum)); // Store the sum function
        self
    }

    pub fn avg(mut self) -> Self {
        self.agg_fn = Some(Box::new(avg)); // Store the avg function
        self
    }

    pub fn min(mut self) -> Self {
        self.agg_fn = Some(Box::new(min)); // Store the min function
        self
    }

    pub fn max(mut self) -> Self {
        self.agg_fn = Some(Box::new(max)); // Store the max function
        self
    }

    pub fn build_expr(&self, table_alias: &str) -> Expr {
        let qualified_column = col_with_relation(table_alias, &self.column);

        // Apply aggregation function if present
        let base_expr = if let Some(agg_fn) = &self.agg_fn {
            agg_fn(qualified_column) // Apply the stored aggregation function
        } else {
            qualified_column
        };

        // Apply alias if present
        if let Some(alias) = &self.agg_alias {
            base_expr.alias(alias.clone())
        } else {
            base_expr
        }
    }
}

// Helper function to qualify column names
fn col_with_relation(relation: &str, column: &str) -> Expr {
    if column.contains('.') {
        col(column) // Already qualified
    } else {
        col(&format!("{}.{}", relation, column)) // Add table alias
    }
}


impl From<&str> for AggregationBuilder {
    fn from(column: &str) -> Self {
        AggregationBuilder::new(column)
    }
}

impl From<AggregationBuilder> for Expr {
    fn from(builder: AggregationBuilder) -> Self {
        builder.build_expr("default_alias") // Replace "default_alias" if context requires
    }
}
