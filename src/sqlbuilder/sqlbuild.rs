use std::fmt::Write;
use crate::Join;

/// Optimized SQL Builder with pre-allocated capacity and efficient string building
pub struct SqlBuilder {
    pub buffer: String,
}

impl SqlBuilder {
 
    pub fn with_capacity(capacity: usize) -> Self {
        Self { 
            buffer: String::with_capacity(capacity)
        }
    }
    
    pub fn with_ctes(&mut self, ctes: &[String]) -> &mut Self {
        if !ctes.is_empty() {
            self.buffer.push_str("WITH ");
            self.buffer.push_str(&ctes.join(", "));
            self.buffer.push(' ');
        }
        self
    }
    
    pub fn select(&mut self, parts: &[String]) -> &mut Self {
        self.buffer.push_str("SELECT ");
        if parts.is_empty() {
            self.buffer.push('*');
        } else {
            for (i, part) in parts.iter().enumerate() {
                if i > 0 { 
                    self.buffer.push_str(", "); 
                }
                self.buffer.push_str(part);
            }
        }
        self
    }
    
    pub fn from_table(&mut self, table: &str, alias: Option<&str>) -> &mut Self {
        self.buffer.push_str(" FROM ");
        
        if table.starts_with('(') && table.ends_with(')') {
            // Subquery - no alias formatting
            self.buffer.push_str(table);
        } else {
            // Regular table with alias
            write!(self.buffer, "\"{}\"", table.trim()).unwrap();
            if let Some(alias) = alias {
                write!(self.buffer, " AS {}", alias).unwrap();
            }
        }
        self
    }
    
    pub fn joins(&mut self, joins: &[Join]) -> &mut Self {
        for join in joins {
            write!(self.buffer, " {} JOIN \"{}\" AS {} ON {}",
                join.join_type,
                join.dataframe.from_table,
                join.dataframe.table_alias,
                join.condition
            ).unwrap();
        }
        self
    }
    
    pub fn where_clause(&mut self, conditions: &[String]) -> &mut Self {
        if !conditions.is_empty() {
            self.buffer.push_str(" WHERE ");
            self.join_conditions(conditions, " AND ");
        }
        self
    }
    
    pub fn group_by(&mut self, columns: &[String]) -> &mut Self {
        if !columns.is_empty() {
            self.buffer.push_str(" GROUP BY ");
            self.join_conditions(columns, ", ");
        }
        self
    }
    
    pub fn having(&mut self, conditions: &[String]) -> &mut Self {
        if !conditions.is_empty() {
            self.buffer.push_str(" HAVING ");
            self.join_conditions(conditions, " AND ");
        }
        self
    }
    
    pub fn order_by(&mut self, orders: &[(String, bool)]) -> &mut Self {
        if !orders.is_empty() {
            self.buffer.push_str(" ORDER BY ");
            for (i, (col, asc)) in orders.iter().enumerate() {
                if i > 0 { 
                    self.buffer.push_str(", "); 
                }
                self.buffer.push_str(col);
                self.buffer.push_str(if *asc { " ASC" } else { " DESC" });
            }
        }
        self
    }
    
    pub fn limit(&mut self, limit: Option<u64>) -> &mut Self {
        if let Some(limit) = limit {
            write!(self.buffer, " LIMIT {}", limit).unwrap();
        }
        self
    }
    
    pub fn join_conditions(&mut self, items: &[String], separator: &str) {
        for (i, item) in items.iter().enumerate() {
            if i > 0 { 
                self.buffer.push_str(separator); 
            }
            self.buffer.push_str(item);
        }
    }
    
    pub fn build(self) -> String {
        self.buffer
    }
}