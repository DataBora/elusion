use datafusion::logical_expr::{Expr, col};
use datafusion::functions_aggregate::expr_fn::{
    sum, min, max, avg, stddev, count, count_distinct, corr, first_value, grouping,
    var_pop, stddev_pop, array_agg,approx_percentile_cont, nth_value
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

    pub fn build_expr(&self, table_alias: &str) -> Expr {
        let qualified_column = col_with_relation(table_alias, &self.column);

        let base_expr = if let Some(agg_fn) = &self.agg_fn {
            agg_fn(qualified_column) 
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

    pub fn alias(mut self, alias: &str) -> Self {
        self.agg_alias = Some(alias.to_string());
        self
    }

    ////////// --- FUNKCIJE --------------\\\\\\\\\\

    pub fn sum(mut self) -> Self {
        self.agg_fn = Some(Box::new(sum)); 
        self
    }

    pub fn avg(mut self) -> Self {
        self.agg_fn = Some(Box::new(avg)); 
        self
    }

    pub fn min(mut self) -> Self {
        self.agg_fn = Some(Box::new(min)); 
        self
    }

    pub fn max(mut self) -> Self {
        self.agg_fn = Some(Box::new(max)); 
        self
    }

    pub fn stddev(mut self) -> Self {
        self.agg_fn = Some(Box::new(stddev)); 
        self
    }

    pub fn count(mut self) -> Self {
        self.agg_fn = Some(Box::new(count)); 
        self
    }

    pub fn count_distinct(mut self) -> Self {
        self.agg_fn = Some(Box::new(count_distinct)); 
        self
    }

    pub fn corr(mut self, other_column: &str) -> Self {
        let other_column = other_column.to_string(); 
        self.agg_fn = Some(Box::new(move |expr| {
            corr(expr, col_with_relation("", &other_column))
        })); 
        self
    }
    


    pub fn grouping(mut self) -> Self {
        self.agg_fn = Some(Box::new(grouping)); // Store the grouping function
        self
    }

    pub fn var_pop(mut self) -> Self {
        self.agg_fn = Some(Box::new(var_pop)); // Store the population variance function
        self
    }

    pub fn stddev_pop(mut self) -> Self {
        self.agg_fn = Some(Box::new(stddev_pop)); // Store the population standard deviation function
        self
    }

    pub fn array_agg(mut self) -> Self {
        self.agg_fn = Some(Box::new(array_agg)); // Store the array aggregation function
        self
    }

    pub fn approx_percentile(mut self, percentile: f64) -> Self {
        println!("Building approx_percentile for column: {}, percentile: {}", self.column, percentile); // Example log
        self.agg_fn = Some(Box::new(move |expr| {
            approx_percentile_cont(expr, Expr::Literal(percentile.into()), None)
        }));
        self
    }
    

    pub fn first_value(mut self) -> Self {
        self.agg_fn = Some(Box::new(|expr| first_value(expr, None))); // First value function
        self
    }

    pub fn nth_value(mut self, n: i64) -> Self {
        self.agg_fn = Some(Box::new(move |expr| nth_value(expr, n, vec![]))); 
        self
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
// fn col_with_relation(relation: &str, column: &str) -> Expr {
//     if column.contains('.') {
//         col(column) // Already qualified
//     } else if !relation.is_empty() {
//         col(&format!("{}.{}", relation, column)) // Add table alias
//     } else {
//         col(column) // Use column name as is
//     }
// }


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
