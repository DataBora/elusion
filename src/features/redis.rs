use crate::prelude::*;
use redis::{Client, Connection, TypedCommands, RedisResult};
use std::hash::{DefaultHasher, Hash, Hasher};
use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};
use tokio::task;

// =================== REDIS CONNECTION (EMBEDDED) ===================

/// Redis connection configuration for caching
#[derive(Debug, Clone)]
pub struct RedisCacheConfig {
    pub host: String,
    pub port: u16,
    pub password: Option<String>,
    pub database: u8,
    pub timeout_seconds: u64,
}

impl Default for RedisCacheConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 6379,
            password: None,
            database: 0,
            timeout_seconds: 30,
        }
    }
}

impl RedisCacheConfig {
    pub fn new(host: &str, port: u16) -> Self {
        Self {
            host: host.to_string(),
            port,
            ..Default::default()
        }
    }

    pub fn with_auth(mut self, password: &str) -> Self {
        self.password = Some(password.to_string());
        self
    }

    pub fn with_database(mut self, database: u8) -> Self {
        self.database = database;
        self
    }

    fn to_connection_string(&self) -> String {
        match &self.password {
            Some(password) => format!(
                "redis://:{}@{}:{}/{}",
                password, self.host, self.port, self.database
            ),
            None => format!("redis://{}:{}/{}", self.host, self.port, self.database),
        }
    }
}

/// Redis connection wrapper for caching operations
#[derive(Debug, Clone)]
pub struct RedisCacheConnection {
    client: Client,
}

impl RedisCacheConnection {
    /// Create a new Redis cache connection
    pub async fn new(config: RedisCacheConfig) -> crate::ElusionResult<Self> {
        let connection_string = config.to_connection_string();
        
        // Test connection in blocking context
        let conn_str_clone = connection_string.clone();
        let client = task::spawn_blocking(move || {
            let client = Client::open(conn_str_clone)?;
            
            // Test the connection
            let mut conn = client.get_connection()?;
            let _: String = conn.ping()?;
            
            Ok::<Client, redis::RedisError>(client)
        })
        .await
        .map_err(|e| crate::ElusionError::Custom(format!("Failed to create Redis cache task: {}", e)))?
        .map_err(|e| crate::ElusionError::Custom(format!("Redis cache connection failed: {}", e)))?;

        println!("✅ Redis cache connected at {}:{}", config.host, config.port);

        Ok(Self { client })
    }

    /// Quick local connection
    pub async fn local() -> crate::ElusionResult<Self> {
        Self::new(RedisCacheConfig::default()).await
    }

    /// Execute Redis commands with automatic type handling
    async fn execute_typed<T, F>(&self, operation: F) -> crate::ElusionResult<T>
    where
        F: FnOnce(&mut Connection) -> RedisResult<T> + Send + 'static,
        T: Send + 'static,
    {
        let client = self.client.clone();
        
        task::spawn_blocking(move || {
            let mut conn = client.get_connection()?;
            operation(&mut conn)
        })
        .await
        .map_err(|e| crate::ElusionError::Custom(format!("Redis cache task error: {}", e)))?
        .map_err(|e| crate::ElusionError::Custom(format!("Redis cache operation error: {}", e)))
    }

    /// Set a key-value pair with automatic serialization
    async fn set<V>(&self, key: &str, value: V) -> crate::ElusionResult<()>
    where
        V: serde::Serialize + Send + 'static,
    {
        let key = key.to_string();
        let value_json = serde_json::to_string(&value)
            .map_err(|e| crate::ElusionError::Custom(format!("Failed to serialize cache value: {}", e)))?;

        self.execute_typed(move |conn| {
            conn.set(&key, value_json)
        }).await
    }

    /// Set a key-value pair with TTL
    async fn set_with_ttl<V>(&self, key: &str, value: V, ttl_seconds: u64) -> crate::ElusionResult<()>
    where
        V: serde::Serialize + Send + 'static,
    {
        let key = key.to_string();
        let value_json = serde_json::to_string(&value)
            .map_err(|e| crate::ElusionError::Custom(format!("Failed to serialize cache value: {}", e)))?;

        self.execute_typed(move |conn| {
            conn.set_ex(&key, value_json, ttl_seconds)
        }).await
    }

    /// Get a value with automatic deserialization
    async fn get<T>(&self, key: &str) -> crate::ElusionResult<Option<T>>
    where
        T: for<'de> serde::Deserialize<'de> + Send + 'static,
    {
        let key = key.to_string();
        
        let value_str: Option<String> = self.execute_typed(move |conn| {
            conn.get(&key)
        }).await?;

        match value_str {
            Some(json_str) => {
                let value = serde_json::from_str(&json_str)
                    .map_err(|e| crate::ElusionError::Custom(format!("Failed to deserialize cache value: {}", e)))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Delete keys
    async fn delete(&self, keys: &[&str]) -> crate::ElusionResult<u64> {
        let keys: Vec<String> = keys.iter().map(|k| k.to_string()).collect();
        
        self.execute_typed(move |conn| {
            let result: usize = conn.del(&keys)?;
            Ok(result as u64)
        }).await
    }

    /// Get keys matching pattern
    async fn keys(&self, pattern: &str) -> crate::ElusionResult<Vec<String>> {
        let pattern = pattern.to_string();
        
        self.execute_typed(move |conn| {
            conn.keys(&pattern)
        }).await
    }

    /// Get Redis info
    async fn info(&self) -> crate::ElusionResult<String> {
        self.execute_typed(move |conn| {
            redis::cmd("INFO").query(conn)
        }).await
    }
}

// =================== CACHE IMPLEMENTATION ===================

/// Redis cache statistics
#[derive(Debug, Serialize, Deserialize)]
pub struct RedisCacheStats {
    pub total_keys: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub hit_rate: f64,
    pub total_memory_used: String,
    pub avg_query_time_ms: f64,
}

/// Cache entry metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RedisCacheEntry {
    query_sql: String,
    created_at: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
    row_count: usize,
    schema_fingerprint: String,
    compressed: bool,
}

/// Generate cache key for a query
fn generate_cache_key(query: &str) -> String {
    let mut hasher = DefaultHasher::new();
    query.hash(&mut hasher);
    let query_hash = hasher.finish();
    format!("elusion:query_cache:{:x}", query_hash)
}

/// Generate schema fingerprint
fn generate_schema_fingerprint(schema: &arrow::datatypes::SchemaRef) -> String {
    let mut hasher = DefaultHasher::new();
    for field in schema.fields() {
        field.name().hash(&mut hasher);
        format!("{:?}", field.data_type()).hash(&mut hasher);
    }
    format!("{:x}", hasher.finish())
}

/// Execute query with Redis caching
pub async fn elusion_with_redis_cache_impl(
    df: &crate::CustomDataFrame,
    redis_conn: &RedisCacheConnection,
    alias: &str,
    ttl_seconds: Option<u64>,
) -> crate::ElusionResult<crate::CustomDataFrame> {
    let sql = df.construct_sql();
    let cache_key = generate_cache_key(&sql);
    let meta_key = format!("{}_meta", cache_key);
    let data_key = format!("{}_data", cache_key);
    
    println!("🔍 Checking Redis cache for query...");
    
    // Try to get cached result
    match get_cached_result_from_redis(redis_conn, &cache_key, &meta_key, &data_key).await {
        Ok(Some(cached_batches)) => {
            println!("✅ Using Redis cached result for query");
            
            // Create DataFrame from cached result
            let ctx = SessionContext::new();
            let schema = cached_batches[0].schema();
            
            let mem_table = MemTable::try_new(schema.clone(), vec![cached_batches])
                .map_err(|e| crate::ElusionError::Custom(format!("Failed to create memory table from Redis cache: {}", e)))?;
            
            ctx.register_table(alias, Arc::new(mem_table))
                .map_err(|e| crate::ElusionError::Custom(format!("Failed to register table from Redis cache: {}", e)))?;
            
            let df_result = ctx.table(alias).await
                .map_err(|e| crate::ElusionError::Custom(format!("Failed to create DataFrame from Redis cache: {}", e)))?;
            
            // Update cache hit statistics - Fixed: explicitly specify i64 return type
            let _ = redis_conn.execute_typed(move |conn| -> redis::RedisResult<i64> {
                redis::cmd("INCR").arg("elusion:cache_stats:hits").query(conn)
            }).await;
            
            return Ok(crate::CustomDataFrame {
                df: df_result,
                table_alias: alias.to_string(),
                from_table: alias.to_string(),
                selected_columns: Vec::new(),
                alias_map: Vec::new(),
                aggregations: Vec::new(),
                group_by_columns: Vec::new(),
                where_conditions: Vec::new(),
                having_conditions: Vec::new(),
                order_by_columns: Vec::new(),
                limit_count: None,
                joins: Vec::new(),
                window_functions: Vec::new(),
                ctes: Vec::new(),
                subquery_source: None,
                set_operations: Vec::new(),
                query: sql,
                aggregated_df: None,
                union_tables: None,
                original_expressions: df.original_expressions.clone(),
                needs_normalization: false,
                raw_selected_columns: Vec::new(),
                raw_group_by_columns: Vec::new(),
                raw_where_conditions: Vec::new(),
                raw_having_conditions: Vec::new(),
                raw_join_conditions: Vec::new(),
                raw_aggregations: Vec::new(),
                uses_group_by_all: false,
            });
        }
        Ok(None) => {
            println!("💾 No cached result found, executing query...");
        }
        Err(e) => {
            println!("⚠️ Redis cache read error: {}, executing query...", e);
        }
    }
    
    let start_time = std::time::Instant::now();
    let result = df.elusion(alias).await?;
    let query_time = start_time.elapsed();
    
    let batches = result.df.clone().collect().await
        .map_err(|e| crate::ElusionError::Custom(format!("Failed to collect batches for Redis caching: {}", e)))?;
    
    if !batches.is_empty() {
        match cache_result_in_redis(redis_conn, &cache_key, &meta_key, &data_key, &sql, &batches, ttl_seconds).await {
            Ok(_) => {
                let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                println!("✅ Cached {} rows in Redis (query took: {:?})", row_count, query_time);
            }
            Err(e) => {
                println!("⚠️ Failed to cache result in Redis: {}", e);
            }
        }
    }
    
    let _ = redis_conn.execute_typed(move |conn| -> redis::RedisResult<i64> {
        redis::cmd("INCR").arg("elusion:cache_stats:misses").query(conn)
    }).await;
    
    let query_time_ms = query_time.as_millis() as i64;
    let _ = redis_conn.execute_typed(move |conn| -> redis::RedisResult<i64> {
        redis::cmd("LPUSH").arg("elusion:cache_stats:query_times").arg(query_time_ms).query(conn)
    }).await;
    
    let _ = redis_conn.execute_typed(move |conn| -> redis::RedisResult<()> {
        redis::cmd("LTRIM").arg("elusion:cache_stats:query_times").arg(0).arg(99).query(conn)
    }).await;
    
    Ok(result)
}

/// Get cached result from Redis
async fn get_cached_result_from_redis(
    redis_conn: &RedisCacheConnection,
    _cache_key: &str,
    meta_key: &str,
    data_key: &str,
) -> crate::ElusionResult<Option<Vec<RecordBatch>>> {
    // Get metadata first
    let metadata: Option<RedisCacheEntry> = redis_conn.get(meta_key).await?;
    let meta = match metadata {
        Some(m) => m,
        None => return Ok(None),
    };
    
    // Check if expired
    if let Some(expires_at) = meta.expires_at {
        if Utc::now() > expires_at {
            // Expired, clean up
            let _ = redis_conn.delete(&[meta_key, data_key]).await;
            return Ok(None);
        }
    }
    
    // Get the actual data
    let data_buffer: Option<Vec<u8>> = redis_conn.get(data_key).await?;
    let buffer = match data_buffer {
        Some(buf) => buf,
        None => return Ok(None),
    };
    
    // Deserialize Arrow IPC data
    let cursor = std::io::Cursor::new(buffer);
    let mut reader = arrow::ipc::reader::StreamReader::try_new(cursor, None)
        .map_err(|e| crate::ElusionError::Custom(format!("Failed to read cached Arrow data: {}", e)))?;
    
    let mut batches = Vec::new();
    while let Some(batch_result) = reader.next() {
        let batch = batch_result
            .map_err(|e| crate::ElusionError::Custom(format!("Failed to deserialize cached batch: {}", e)))?;
        batches.push(batch);
    }
    
    Ok(Some(batches))
}

/// Cache result in Redis
async fn cache_result_in_redis(
    redis_conn: &RedisCacheConnection,
    _cache_key: &str,
    meta_key: &str,
    data_key: &str,
    query_sql: &str,
    batches: &[RecordBatch],
    ttl_seconds: Option<u64>,
) -> crate::ElusionResult<()> {
    if batches.is_empty() {
        return Ok(());
    }
    
    let schema = batches[0].schema();
    let schema_fingerprint = generate_schema_fingerprint(&schema);
    let row_count = batches.iter().map(|b| b.num_rows()).sum();
    
    // Serialize to Arrow IPC format
    let mut buffer = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buffer, &schema)
            .map_err(|e| crate::ElusionError::Custom(format!("Failed to create Arrow writer: {}", e)))?;
        
        for batch in batches {
            writer.write(batch)
                .map_err(|e| crate::ElusionError::Custom(format!("Failed to write batch to cache: {}", e)))?;
        }
        
        writer.finish()
            .map_err(|e| crate::ElusionError::Custom(format!("Failed to finish Arrow writer: {}", e)))?;
    }
    
    // Create metadata
    let metadata = RedisCacheEntry {
        query_sql: query_sql.to_string(),
        created_at: Utc::now(),
        expires_at: ttl_seconds.map(|ttl| Utc::now() + chrono::Duration::seconds(ttl as i64)),
        row_count,
        schema_fingerprint,
        compressed: false, // add compression later
    };

    let meta_key = meta_key.to_string();
    let data_key = data_key.to_string();
    
    // Store both metadata and data
    if let Some(ttl) = ttl_seconds {
        redis_conn.set_with_ttl(&meta_key, metadata.clone(), ttl).await?;
        redis_conn.set_with_ttl(&data_key, buffer.clone(), ttl).await?;
    } else {
        redis_conn.set(&meta_key, metadata).await?;
        redis_conn.set(&data_key, buffer).await?;
    }
    
    Ok(())
}

/// Clear Redis cache
pub async fn clear_redis_cache_impl(
    redis_conn: &RedisCacheConnection,
    pattern: Option<&str>,
) -> crate::ElusionResult<()> {
    let search_pattern = pattern.unwrap_or("elusion:query_cache:*");
    
    println!("🗑️ Clearing Redis cache with pattern: {}", search_pattern);
    
    let keys = redis_conn.keys(search_pattern).await?;
    
    if keys.is_empty() {
        println!("ℹ️ No cache keys found to clear");
        return Ok(());
    }
    
    // Also get metadata keys
    let meta_pattern = format!("{}_meta", search_pattern);
    let data_pattern = format!("{}_data", search_pattern);
    
    let mut all_keys = keys;
    all_keys.extend(redis_conn.keys(&meta_pattern).await?);
    all_keys.extend(redis_conn.keys(&data_pattern).await?);
    
    if !all_keys.is_empty() {
        let key_refs: Vec<&str> = all_keys.iter().map(|s| s.as_str()).collect();
        let deleted_count = redis_conn.delete(&key_refs).await?;
        println!("✅ Cleared {} Redis cache keys", deleted_count);
    }
    
    // Clear statistics
    let _ = redis_conn.delete(&[
        "elusion:cache_stats:hits",
        "elusion:cache_stats:misses", 
        "elusion:cache_stats:query_times"
    ]).await;
    
    Ok(())
}

/// Get Redis cache statistics
pub async fn get_redis_cache_stats_impl(
    redis_conn: &RedisCacheConnection,
) -> crate::ElusionResult<RedisCacheStats> {
    // Get cache hit/miss stats
    let hits: u64 = redis_conn.get("elusion:cache_stats:hits").await?.unwrap_or(0);
    let misses: u64 = redis_conn.get("elusion:cache_stats:misses").await?.unwrap_or(0);
    
    let total_requests = hits + misses;
    let hit_rate = if total_requests > 0 {
        (hits as f64 / total_requests as f64) * 100.0
    } else {
        0.0
    };
    
    // Count total cache keys
    let cache_keys = redis_conn.keys("elusion:query_cache:*").await?;
    let total_keys = cache_keys.len() as u64;
    
    // Get memory usage info
    let info = redis_conn.info().await?;
    let memory_line = info.lines()
        .find(|line| line.starts_with("used_memory_human:"))
        .unwrap_or("used_memory_human:unknown");
    let memory_used = memory_line.split(':').nth(1).unwrap_or("unknown").to_string();
    
    // Calculate average query time
    let query_times: Vec<String> = redis_conn.execute_typed(move |conn| -> redis::RedisResult<Vec<String>> {
        conn.lrange("elusion:cache_stats:query_times", 0, -1)
    }).await?;
    
    let parsed_times: Vec<i64> = query_times
        .into_iter()
        .filter_map(|s| s.parse().ok())
        .collect();
    
    let avg_query_time_ms = if !parsed_times.is_empty() {
        parsed_times.iter().sum::<i64>() as f64 / parsed_times.len() as f64
    } else {
        0.0
    };
    
    Ok(RedisCacheStats {
        total_keys,
        cache_hits: hits,
        cache_misses: misses,
        hit_rate,
        total_memory_used: memory_used,
        avg_query_time_ms,
    })
}

/// Invalidate Redis cache by table patterns
pub async fn invalidate_redis_cache_impl(
    redis_conn: &RedisCacheConnection,
    table_names: &[&str],
) -> crate::ElusionResult<()> {
    if table_names.is_empty() {
        return Ok(());
    }
    
    println!("🔄 Invalidating Redis cache for tables: {:?}", table_names);
    
    // clear all cache when any table changes
    clear_redis_cache_impl(redis_conn, Some("elusion:query_cache:*")).await?;
    
    println!("✅ Redis cache invalidated");
    Ok(())
}

/// Create a Redis cache connection with default settings
pub async fn create_redis_cache_connection() -> crate::ElusionResult<RedisCacheConnection> {
    RedisCacheConnection::local().await
}

/// Create a Redis cache connection with custom settings
pub async fn create_redis_cache_connection_with_config(
    host: &str,
    port: u16,
    password: Option<&str>,
    database: Option<u8>,
) -> crate::ElusionResult<RedisCacheConnection> {
    let mut config = RedisCacheConfig::new(host, port);
    
    if let Some(pass) = password {
        config = config.with_auth(pass);
    }
    
    if let Some(db) = database {
        config = config.with_database(db);
    }
    
    RedisCacheConnection::new(config).await
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;

    async fn create_test_connection() -> crate::ElusionResult<RedisCacheConnection> {

        match timeout(Duration::from_secs(5), create_redis_cache_connection()).await {
            Ok(result) => result,
            Err(_) => Err(crate::ElusionError::Custom(
                "Redis connection timeout - is Redis running on localhost:6379?".to_string()
            )),
        }
    }

    async fn is_redis_available() -> bool {
        create_test_connection().await.is_ok()
    }

    #[tokio::test]
    async fn test_redis_connection() {
        println!("🧪 Testing Redis connection...");
        
        match create_test_connection().await {
            Ok(_) => {
                println!("✅ Redis connection successful!");
            }
            Err(e) => {
                println!("❌ Redis connection failed: {}", e);
                println!("💡 Make sure Redis is running:");
                println!("   Windows: redis-server");
                println!("   Docker: docker run --name redis-cache -p 6379:6379 -d redis:latest");
                panic!("Redis not available for testing");
            }
        }
    }

    #[tokio::test]
    async fn test_redis_config() {
        println!("🧪 Testing Redis configuration...");
        
        let config = RedisCacheConfig::default();
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 6379);
        assert_eq!(config.database, 0);
        assert!(config.password.is_none());
        
        let config = RedisCacheConfig::new("localhost", 6380)
            .with_auth("mypassword")
            .with_database(1);
        
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 6380);
        assert_eq!(config.database, 1);
        assert_eq!(config.password, Some("mypassword".to_string()));
        
        let conn_str = config.to_connection_string();
        assert!(conn_str.contains("mypassword"));
        assert!(conn_str.contains("localhost"));
        assert!(conn_str.contains("6380"));
        assert!(conn_str.contains("/1"));
        
        println!("✅ Redis configuration tests passed!");
    }

    #[tokio::test]
    async fn test_redis_ttl_operations() {
        if !is_redis_available().await {
            println!("⏭️ Skipping TTL test - Redis not available");
            return;
        }

        println!("🧪 Testing Redis TTL operations...");
        
        let redis_conn = create_test_connection().await.unwrap();
        
        let ttl_key = "elusion:test:ttl";
        let ttl_value = "This will expire";
        
        redis_conn.set_with_ttl(ttl_key, ttl_value, 2).await.unwrap();
        
        let value: Option<String> = redis_conn.get(ttl_key).await.unwrap();
        assert_eq!(value, Some(ttl_value.to_string()));
        
        tokio::time::sleep(Duration::from_secs(3)).await;
        
        let value: Option<String> = redis_conn.get(ttl_key).await.unwrap();
        assert_eq!(value, None);
        
        println!("✅ TTL operations tests passed!");
    }

    #[tokio::test]
    async fn test_redis_delete_operations() {
        if !is_redis_available().await {
            println!("⏭️ Skipping delete test - Redis not available");
            return;
        }

        println!("🧪 Testing Redis delete operations...");
        
        let redis_conn = create_test_connection().await.unwrap();
        
        let keys = ["elusion:test:del1", "elusion:test:del2", "elusion:test:del3"];
        for key in &keys {
            redis_conn.set(key, "delete me").await.unwrap();
        }
        
        for key in &keys {
            let value: Option<String> = redis_conn.get(key).await.unwrap();
            assert_eq!(value, Some("delete me".to_string()));
        }
        
        let deleted_count = redis_conn.delete(&keys).await.unwrap();
        assert_eq!(deleted_count, 3);
        
        for key in &keys {
            let value: Option<String> = redis_conn.get(key).await.unwrap();
            assert_eq!(value, None);
        }
        
        println!("✅ Delete operations tests passed!");
    }

    #[tokio::test]
    async fn test_redis_key_patterns() {
        if !is_redis_available().await {
            println!("⏭️ Skipping key patterns test - Redis not available");
            return;
        }

        println!("🧪 Testing Redis key patterns...");
        
        let redis_conn = create_test_connection().await.unwrap();
        
        let cache_keys = [
            "elusion:query_cache:abc123",
            "elusion:query_cache:def456", 
            "elusion:query_cache:ghi789"
        ];
        let other_keys = [
            "elusion:stats:hits",
            "other:namespace:key1",
            "completely_different"
        ];
        
        for key in cache_keys.iter().chain(other_keys.iter()) {
            redis_conn.set(key, "test_value").await.unwrap();
        }
        
        let found_cache_keys = redis_conn.keys("elusion:query_cache:*").await.unwrap();
        assert_eq!(found_cache_keys.len(), 3);
        
        let found_elusion_keys = redis_conn.keys("elusion:*").await.unwrap();
        assert!(found_elusion_keys.len() >= 4); 
        
        let found_all_keys = redis_conn.keys("*").await.unwrap();
        assert!(found_all_keys.len() >= 6); 
        
        let all_test_keys: Vec<&str> = cache_keys.iter().chain(other_keys.iter()).cloned().collect();
        redis_conn.delete(&all_test_keys).await.unwrap();
        
        println!("✅ Key patterns tests passed!");
    }

    #[tokio::test]
    async fn test_cache_key_generation() {
        println!("🧪 Testing cache key generation...");
        
        let query1 = "SELECT * FROM users WHERE id = 1";
        let query2 = "SELECT * FROM users WHERE id = 1"; 
        let query3 = "SELECT * FROM users WHERE id = 2"; 
        
        let key1 = generate_cache_key(query1);
        let key2 = generate_cache_key(query2);
        let key3 = generate_cache_key(query3);
        
        assert_eq!(key1, key2, "Same queries should generate same cache keys");
        assert_ne!(key1, key3, "Different queries should generate different cache keys");
        
        assert!(key1.starts_with("elusion:query_cache:"));
        assert!(key3.starts_with("elusion:query_cache:"));
        
        println!("✅ Cache key generation tests passed!");
    }

    #[tokio::test]
    async fn test_cache_statistics() {
        if !is_redis_available().await {
            println!("⏭️ Skipping cache statistics test - Redis not available");
            return;
        }

        println!("🧪 Testing cache statistics...");
        
        let redis_conn = create_test_connection().await.unwrap();
        
        clear_redis_cache_impl(&redis_conn, None).await.unwrap();
        
        let initial_stats = get_redis_cache_stats_impl(&redis_conn).await.unwrap();
        assert_eq!(initial_stats.cache_hits, 0);
        assert_eq!(initial_stats.cache_misses, 0);
        assert_eq!(initial_stats.total_keys, 0);

        redis_conn.set("elusion:query_cache:test1", "data1").await.unwrap();
        redis_conn.set("elusion:query_cache:test2", "data2").await.unwrap();
        
        let stats_with_keys = get_redis_cache_stats_impl(&redis_conn).await.unwrap();
        assert_eq!(stats_with_keys.total_keys, 2);
        

        assert_eq!(stats_with_keys.hit_rate, 0.0); 
        
        clear_redis_cache_impl(&redis_conn, None).await.unwrap();
        
        println!("✅ Cache statistics tests passed!");
    }

    #[tokio::test]
    async fn test_cache_clear_operations() {
        if !is_redis_available().await {
            println!("⏭️ Skipping cache clear test - Redis not available");
            return;
        }

        println!("🧪 Testing cache clear operations...");
        
        let redis_conn = create_test_connection().await.unwrap();
             // Set up test data
        let cache_keys = [
            "elusion:query_cache:clear1",
            "elusion:query_cache:clear2",
            "elusion:query_cache:clear1_meta",
            "elusion:query_cache:clear2_data"
        ];
        let other_keys = ["elusion:other:key", "completely:different:key"];
        
        for key in cache_keys.iter().chain(other_keys.iter()) {
            redis_conn.set(key, "test_data").await.unwrap();
        }
        
        clear_redis_cache_impl(&redis_conn, Some("elusion:query_cache:*")).await.unwrap();
        
        for key in &cache_keys {
            let value: Option<String> = redis_conn.get(key).await.unwrap();
            assert_eq!(value, None, "Cache key {} should be deleted", key);
        }
        

        for key in &other_keys {
            let value: Option<String> = redis_conn.get(key).await.unwrap();
            assert_eq!(value, Some("test_data".to_string()), "Non-cache key {} should still exist", key);
        }
        
        redis_conn.delete(&other_keys).await.unwrap();
        
        println!("✅ Cache clear operations tests passed!");
    }

    #[tokio::test]
    async fn test_redis_info() {
        if !is_redis_available().await {
            println!("⏭️ Skipping Redis info test - Redis not available");
            return;
        }

        println!("🧪 Testing Redis info retrieval...");
        
        let redis_conn = create_test_connection().await.unwrap();
        
        let info = redis_conn.info().await.unwrap();
        
        assert!(!info.is_empty(), "Redis info should not be empty");
        assert!(info.contains("redis_version"), "Info should contain Redis version");
        assert!(info.contains("used_memory"), "Info should contain memory usage");
        
        println!("📋 Redis info retrieved ({} chars)", info.len());
        println!("✅ Redis info tests passed!");
    }

    #[tokio::test]
    async fn test_performance_benchmark() {
        if !is_redis_available().await {
            println!("⏭️ Skipping performance test - Redis not available");
            return;
        }

        println!("🧪 Running performance benchmark...");
        
        let redis_conn = create_test_connection().await.unwrap();
        
        let iterations = 50;
        let test_data = "Performance test data - this is a reasonably sized string for testing";
        
        let start_time = std::time::Instant::now();
        
        for i in 0..iterations {
            let key = format!("elusion:perf:test:{}", i);
            redis_conn.set(&key, test_data).await.unwrap();
        }
        
        let set_duration = start_time.elapsed();
        
        let start_time = std::time::Instant::now();
        
        for i in 0..iterations {
            let key = format!("elusion:perf:test:{}", i);
            let _: Option<String> = redis_conn.get(&key).await.unwrap();
        }
        
        let get_duration = start_time.elapsed();
        
        let keys: Vec<String> = (0..iterations)
            .map(|i| format!("elusion:perf:test:{}", i))
            .collect();
        let key_refs: Vec<&str> = keys.iter().map(|k| k.as_str()).collect();
        redis_conn.delete(&key_refs).await.unwrap();
        
        let avg_set_time = set_duration.as_micros() as f64 / iterations as f64;
        let avg_get_time = get_duration.as_micros() as f64 / iterations as f64;
        
        println!("📊 Performance results ({} operations):", iterations);
        println!("   Average SET time: {:.2} μs", avg_set_time);
        println!("   Average GET time: {:.2} μs", avg_get_time);
        println!("   Total SET time: {:?}", set_duration);
        println!("   Total GET time: {:?}", get_duration);
        
        assert!(avg_set_time < 10000.0, "SET operations should be under 10ms on average");
        assert!(avg_get_time < 10000.0, "GET operations should be under 10ms on average");
        
        println!("✅ Performance benchmark passed!");
    }


    #[tokio::test]
    async fn test_error_handling() {
        println!("🧪 Testing error handling...");
        

        let invalid_config = RedisCacheConfig::new("invalid_host", 9999);
        
        match timeout(Duration::from_secs(2), RedisCacheConnection::new(invalid_config)).await {
            Ok(Err(_)) => {
                println!("✅ Properly handled invalid Redis connection");
            }
            Ok(Ok(_)) => {
                panic!("Should not have connected to invalid Redis instance");
            }
            Err(_) => {
                println!("✅ Connection attempt timed out as expected");
            }
        }
        
        println!("✅ Error handling tests passed!");
    }

    #[allow(dead_code)]
    pub async fn run_all_redis_tests() -> Result<(), Box<dyn std::error::Error>> {
        println!("🚀 Running comprehensive Redis cache tests...\n");
        
        if !is_redis_available().await {
            println!("❌ Redis is not available. Please start Redis and try again.");
            println!("💡 Start Redis with one of these methods:");
            println!("   - Windows: redis-server");
            println!("   - Docker: docker run --name redis-cache -p 6379:6379 -d redis:latest");
            return Err("Redis not available".into());
        }
        
        println!("✅ Redis is available, running tests...\n");
        

        Ok(())
    }
    
}


#[cfg(test)]
mod integration_tests {

    use std::time::Instant;

    async fn create_test_dataframe() -> crate::ElusionResult<crate::CustomDataFrame> {
        use datafusion::prelude::*;
        use datafusion::arrow::array::*;
        use datafusion::arrow::datatypes::*;
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float64, false),
        ]));

        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "Diana", "Eve"]);
        let score_array = Float64Array::from(vec![95.5, 87.2, 92.1, 88.8, 94.3]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(score_array),
            ],
        ).map_err(|e| crate::ElusionError::Custom(format!("Arrow error: {}", e)))?;

        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]])
            .map_err(|e| crate::ElusionError::Custom(format!("DataFusion error: {}", e)))?;
        ctx.register_table("test_users", Arc::new(table))
            .map_err(|e| crate::ElusionError::Custom(format!("DataFusion error: {}", e)))?;

        let df = ctx.table("test_users").await
            .map_err(|e| crate::ElusionError::Custom(format!("DataFusion error: {}", e)))?;
        
        Ok(crate::CustomDataFrame {
            df,
            table_alias: "test_users".to_string(),
            from_table: "test_users".to_string(),
            selected_columns: vec!["id".to_string(), "name".to_string(), "score".to_string()],
            alias_map: Vec::new(),
            aggregations: Vec::new(),
            group_by_columns: Vec::new(),
            where_conditions: Vec::new(),
            having_conditions: Vec::new(),
            order_by_columns: Vec::new(),
            limit_count: None,
            joins: Vec::new(),
            window_functions: Vec::new(),
            ctes: Vec::new(),
            subquery_source: None,
            set_operations: Vec::new(),
            query: "SELECT id, name, score FROM test_users".to_string(),
            aggregated_df: None,
            union_tables: None,
            original_expressions: Vec::new(),
            needs_normalization: false,
            raw_selected_columns: Vec::new(),
            raw_group_by_columns: Vec::new(),
            raw_where_conditions: Vec::new(),
            raw_having_conditions: Vec::new(),
            raw_join_conditions: Vec::new(),
            raw_aggregations: Vec::new(),
            uses_group_by_all: false,
        })
    }

    #[tokio::test]
    async fn test_dataframe_redis_cache_integration() {

        let redis_conn = match crate::features::redis::create_redis_cache_connection().await {
            Ok(conn) => conn,
            Err(e) => {
                println!("⏭️ Skipping integration test - Redis not available: {}", e);
                return;
            }
        };

        crate::features::redis::clear_redis_cache_impl(&redis_conn, None).await.unwrap();

        let df = create_test_dataframe().await.unwrap();

        let start_time = Instant::now();
        
        let result1 = crate::features::redis::elusion_with_redis_cache_impl(
            &df,
            &redis_conn,
            "cached_test_users",
            Some(300),
        ).await.unwrap();
        
        let first_duration = start_time.elapsed();

        let batches1 = result1.df.clone().collect().await.unwrap();
        let row_count1 = batches1.iter().map(|b| b.num_rows()).sum::<usize>();

        let start_time = Instant::now();
        
        let result2 = crate::features::redis::elusion_with_redis_cache_impl(
            &df,
            &redis_conn,
            "cached_test_users_2", 
            Some(300),
        ).await.unwrap();
        
        let second_duration = start_time.elapsed();
        println!("Second execution completed in: {:?}", second_duration);

        let batches2 = result2.df.clone().collect().await.unwrap();
        let row_count2 = batches2.iter().map(|b| b.num_rows()).sum::<usize>();
        println!("📋 Second execution returned {} rows", row_count2);

        assert_eq!(row_count1, row_count2, "Both executions should return same number of rows");
        assert_eq!(row_count1, 5, "Should have 5 test rows");


        if second_duration < first_duration {
            let speedup = first_duration.as_micros() as f64 / second_duration.as_micros() as f64;
            println!("Cache speedup: {:.2}x faster!", speedup);
        } else {
            println!("Second execution wasn't faster (this can happen with small datasets)");
        }

        let stats = crate::features::redis::get_redis_cache_stats_impl(&redis_conn).await.unwrap();
        println!("📊 Cache statistics:");
        println!("   Total keys: {}", stats.total_keys);
        println!("   Cache hits: {}", stats.cache_hits);
        println!("   Cache misses: {}", stats.cache_misses);
        println!("   Hit rate: {:.2}%", stats.hit_rate);


        assert!(stats.total_keys > 0, "Should have cached data");
        assert!(stats.cache_hits > 0 || stats.cache_misses > 0, "Should have cache activity");

        crate::features::redis::invalidate_redis_cache_impl(&redis_conn, &["test_users"]).await.unwrap();
        
        let stats_after_invalidation = crate::features::redis::get_redis_cache_stats_impl(&redis_conn).await.unwrap();
        println!("Stats after invalidation - Total keys: {}", stats_after_invalidation.total_keys);

    }

    #[tokio::test]
    async fn test_cache_ttl_with_dataframe() {

        let redis_conn = match crate::features::redis::create_redis_cache_connection().await {
            Ok(conn) => conn,
            Err(_) => {
                println!("⏭️ Skipping TTL test - Redis not available");
                return;
            }
        };

        let df = create_test_dataframe().await.unwrap();

        let _result = crate::features::redis::elusion_with_redis_cache_impl(
            &df,
            &redis_conn,
            "ttl_test_users",
            Some(2),
        ).await.unwrap();

        let stats = crate::features::redis::get_redis_cache_stats_impl(&redis_conn).await.unwrap();
        let keys_before_expiry = stats.total_keys;
        println!("Keys before expiry: {}", keys_before_expiry);

        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

        let stats_after = crate::features::redis::get_redis_cache_stats_impl(&redis_conn).await.unwrap();
        println!("Keys after expiry: {}", stats_after.total_keys);

    }

    #[tokio::test]
    async fn test_concurrent_cache_access() {
        println!("🧪 Testing concurrent cache access...");

        let redis_conn = match crate::features::redis::create_redis_cache_connection().await {
            Ok(conn) => conn,
            Err(_) => {
                println!("Skipping concurrent test - Redis not available");
                return;
            }
        };

        crate::features::redis::clear_redis_cache_impl(&redis_conn, None).await.unwrap();

        let df = create_test_dataframe().await.unwrap();

        let mut tasks = vec![];

        for i in 0..5 {
            let df_clone = df.clone(); 
            let conn_clone = redis_conn.clone();
            let alias = format!("concurrent_test_{}", i);

            let task = tokio::spawn(async move {
                let start_time = Instant::now();
                let _result = crate::features::redis::elusion_with_redis_cache_impl(
                    &df_clone,
                    &conn_clone,
                    &alias,
                    Some(300),
                ).await.unwrap();
                
                (i, start_time.elapsed())
            });

            tasks.push(task);
        }

        let mut results = vec![];
        for task in tasks {
            let result = task.await.unwrap();
            results.push(result);
        }

        for (task_id, duration) in results {
            println!("   Task {}: {:?}", task_id, duration);
        }

        let stats = crate::features::redis::get_redis_cache_stats_impl(&redis_conn).await.unwrap();
        println!("📊 Final stats - Total keys: {}, Hits: {}, Misses: {}", 
                stats.total_keys, stats.cache_hits, stats.cache_misses);

    }
}