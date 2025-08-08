use crate::prelude::*;

pub struct MaterializedView {
    // Name of the materialized view
    pub(crate) name: String,
    // The SQL query that defines this view
    definition: String,
    // The actual data stored as batches
    data: Vec<RecordBatch>,
    // Time when this view was created/refreshed
    refresh_time: DateTime<Utc>,
    // Optional time-to-live in seconds
    ttl: Option<u64>,
}

impl MaterializedView {
    fn is_valid(&self) -> bool {
        if let Some(ttl) = self.ttl {
            let now = Utc::now();
            let age = now.signed_duration_since(self.refresh_time).num_seconds();
            return age < ttl as i64;
        }
        true
    }

    fn display_info(&self) -> String {
        format!(
            "View '{}' - Created: {}, TTL: {} seconds",
            self.name,
            self.refresh_time.format("%Y-%m-%d %H:%M:%S"),
            self.ttl.map_or("None".to_string(), |ttl| ttl.to_string())
        )
    }
}

pub struct QueryCache {
    pub cached_queries: HashMap<u64, (Vec<RecordBatch>, DateTime<Utc>)>,
    max_cache_size: usize,
    ttl_seconds: Option<u64>,
}

impl QueryCache {
    pub fn new(max_cache_size: usize, ttl_seconds: Option<u64>) -> Self {
        Self {
            cached_queries: HashMap::new(),
            max_cache_size,
            ttl_seconds,
        }
    }

    pub fn cache_query(&mut self, query: &str, result: Vec<RecordBatch>) {
        if self.cached_queries.len() >= self.max_cache_size {
            // Simple LRU eviction - remove the oldest entry
            if let Some(oldest) = self.cached_queries
                .iter()
                .min_by_key(|(_, (_, time))| time) {
                let key = *oldest.0;
                self.cached_queries.remove(&key);
            }
        }

        let mut hasher = DefaultHasher::new();
        query.hash(&mut hasher);
        let query_hash = hasher.finish();
        self.cached_queries.insert(query_hash, (result, Utc::now()));
    }

    pub fn get_cached_result(&mut self, query: &str) -> Option<Vec<RecordBatch>> {
        let mut hasher = DefaultHasher::new();
        query.hash(&mut hasher);
        let query_hash = hasher.finish();

        if let Some((result, timestamp)) = self.cached_queries.get(&query_hash) {
            // Check TTL if set
            if let Some(ttl) = self.ttl_seconds {
                let now = Utc::now();
                let age = now.signed_duration_since(*timestamp).num_seconds();
                if age > ttl as i64 {
                    // Cache entry expired, remove it
                    self.cached_queries.remove(&query_hash);
                    return None;
                }
            }
            return Some(result.clone());
        }
        None
    }

    pub fn clear(&mut self) {
        self.cached_queries.clear();
    }

    pub fn invalidate(&mut self, table_names: &[String]) {
        // if any tables are modified, clear entire cache
        if !table_names.is_empty() {
            println!("Invalidating cache due to changes in tables: {:?}", table_names);
            self.clear();
        }
    }
}

pub struct MaterializedViewManager {
    views: HashMap<String, MaterializedView>,
    max_views: usize,
}

impl MaterializedViewManager {
    pub fn new(max_views: usize) -> Self {
        Self {
            views: HashMap::new(),
            max_views,
        }
    }

    pub async fn create_view(
        &mut self,
        ctx: &SessionContext,
        name: &str,
        query: &str,
        ttl: Option<u64>,
    ) -> ElusionResult<()> {
        // Check if we've hit the max number of views
        if self.views.len() >= self.max_views && !self.views.contains_key(name) {
            return Err(ElusionError::Custom(
                format!("Maximum number of materialized views ({}) reached", self.max_views)
            ));
        }

        // Execute the query
        let df = ctx.sql(query).await.map_err(|e| ElusionError::Custom(
            format!("Failed to execute query for materialized view: {}", e)
        ))?;

        let batches = df.collect().await.map_err(|e| ElusionError::Custom(
            format!("Failed to collect results for materialized view: {}", e)
        ))?;

        // Create or update the materialized view
        let view = MaterializedView {
            name: name.to_string(),
            definition: query.to_string(),
            data: batches,
            refresh_time: Utc::now(),
            ttl,
        };

        self.views.insert(name.to_string(), view);
        Ok(())
    }

    pub async fn refresh_view(
        &mut self,
        ctx: &SessionContext,
        name: &str,
    ) -> ElusionResult<()> {
        if let Some(view) = self.views.get(name) {
            let query = view.definition.clone();
            let ttl = view.ttl;
            return self.create_view(ctx, name, &query, ttl).await;
        }
        Err(ElusionError::Custom(format!("View '{}' not found", name)))
    }

    pub async fn get_view_as_dataframe(
        &self,
        ctx: &SessionContext,
        name: &str,
    ) -> ElusionResult<DataFrame> {
        if let Some(view) = self.views.get(name) {
            if !view.is_valid() {
                return Err(ElusionError::Custom(
                    format!("View '{}' has expired", name)
                ));
            }

            let schema = match view.data.first() {
                Some(batch) => batch.schema(),
                None => return Err(ElusionError::Custom(
                    format!("View '{}' contains no data", name)
                )),
            };

            let mem_table = MemTable::try_new(schema.clone(), vec![view.data.clone()])
                .map_err(|e| ElusionError::Custom(
                    format!("Failed to create memory table from view: {}", e)
                ))?;

            let table_name = format!("view_{}", name);
            ctx.register_table(&table_name, Arc::new(mem_table))
                .map_err(|e| ElusionError::Custom(
                    format!("Failed to register table from view: {}", e)
                ))?;

            let df = ctx.table(&table_name).await
                .map_err(|e| ElusionError::Custom(
                    format!("Failed to create DataFrame from view: {}", e)
                ))?;

            Ok(df)
        } else {
            Err(ElusionError::Custom(format!("View '{}' not found", name)))
        }
    }

    pub fn drop_view(&mut self, name: &str) -> ElusionResult<()> {
        if self.views.remove(name).is_some() {
            println!("View '{}' droped.", name);
            Ok(())
        } else {
            Err(ElusionError::Custom(format!("View '{}' not found", name)))
        }
    }

    pub fn list_views(&self) -> Vec<(String, DateTime<Utc>, Option<u64>)> {
        let mut result = Vec::new();

        if self.views.is_empty() {
            return result;
        }

        for (view_name, view) in &self.views {
            println!("{}", view.display_info());
            result.push((view_name.clone(), view.refresh_time, view.ttl));
        }
        result
    }

    #[allow(dead_code)]
    pub fn get_view_metadata(&self, name: &str) -> Option<(String, DateTime<Utc>, Option<u64>)> {
        self.views.get(name).map(|view| (
            view.definition.clone(),
            view.refresh_time,
            view.ttl
        ))
    }
}

// Global state for caching and materialized views
lazy_static! {
    pub static ref QUERY_CACHE: Mutex<QueryCache> = Mutex::new(QueryCache::new(100, Some(3600))); // 1 hour TTL
    pub static ref MATERIALIZED_VIEW_MANAGER: Mutex<MaterializedViewManager> = Mutex::new(MaterializedViewManager::new(50));
}
