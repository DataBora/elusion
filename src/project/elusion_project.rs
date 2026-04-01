use crate::prelude::*;
use super::sources::source_loader::load_source_owned;
pub struct ElusionProject {
    nodes: HashMap<NodeName, Node>,
    connections: Option<ConnectionsFile>,
    project_config: Option<ProjectFile>,
}

impl ElusionProject {
    // ===== BASIC CONSTRUCTOR (no config files) =====
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            connections: None,
            project_config: None,
        }
    }

    // ===== CONFIG CONSTRUCTOR =====
    pub async fn from_config(
        project_config_path: &str,
        connections_path: &str,
    ) -> ElusionResult<Self> {
        println!("\n🚀 Elusion Project - Loading Configuration...");
        println!("{}", "=".repeat(50));

        // Load and validate both config files - fail fast
        let project_config = ProjectFile::load(project_config_path)?;
        let connections = ConnectionsFile::load(connections_path)?;

        println!("{}", "=".repeat(50));
        println!("✅ Configuration loaded successfully\n");

        Ok(Self {
            nodes: HashMap::new(),
            connections: Some(connections),
            project_config: Some(project_config),
        })
    }

    // ===== SOURCE REGISTRATION =====

    // Used with from_config() - loads source from connections.toml
    pub fn source(mut self, name: &str) -> Self {
        let connections = match &self.connections {
            Some(c) => c.clone(),
            None => {
                eprintln!("❌ Cannot use .source('{}') without from_config(). Use .source_fn() instead.", name);
                return self;
            }
        };

        let source_config = match connections.get_source(name) {
            Ok(config) => config.clone(),
            Err(e) => {
                eprintln!("{}", e);
                return self;
            }
        };

        let name_owned = name.to_string();

        let execute: NodeFn = Box::new(move |_registry| {
            let name_clone = name_owned.clone();
            let config_clone = source_config.clone();
            Box::pin(async move {
                load_source_owned(name_clone, config_clone).await
            })
        });

        self.nodes.insert(name.to_string(), Node {
            name: name.to_string(),
            layer: NodeLayer::Source,
            dependencies: vec![],
            execute,
        });

        self
    }

    // Used without config - manual closure (original approach)
    pub fn source_fn<F, Fut>(mut self, name: &str, f: F) -> Self
    where
        F: Fn(NodeRegistry) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ElusionResult<CustomDataFrame>> + Send + 'static,
    {
        self.register(name, NodeLayer::Source, vec![], f);
        self
    }

    // ===== LAYER REGISTRATION =====

    // Bronze
    pub fn bronze<F, Fut, const N: usize>(mut self, name: &str, deps: [&str; N], f: F) -> Self
    where
        F: Fn(NodeRegistry) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ElusionResult<CustomDataFrame>> + Send + 'static,
    {
        let deps = deps.iter().map(|s| s.to_string()).collect();
        self.register(name, NodeLayer::Bronze, deps, f);
        self
    }

    // Silver
    pub fn silver<F, Fut, const N: usize>(mut self, name: &str, deps: [&str; N], f: F) -> Self
    where
        F: Fn(NodeRegistry) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ElusionResult<CustomDataFrame>> + Send + 'static,
    {
        let deps = deps.iter().map(|s| s.to_string()).collect();
        self.register(name, NodeLayer::Silver, deps, f);
        self
    }

    // Gold
    pub fn gold<F, Fut, const N: usize>(mut self, name: &str, deps: [&str; N], f: F) -> Self
    where
        F: Fn(NodeRegistry) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ElusionResult<CustomDataFrame>> + Send + 'static,
    {
        let deps = deps.iter().map(|s| s.to_string()).collect();
        self.register(name, NodeLayer::Gold, deps, f);
        self
    }

    pub fn bronze_sql<const N: usize>(
        mut self,
        name: &str,
        deps: [&str; N],
        query: &'static str,
    ) -> Self {
        let deps_vec = deps.iter().map(|s| s.to_string()).collect();
        let query_owned = query.to_string();
        let name_owned = name.to_string();

        let execute: NodeFn = Box::new(move |registry| {
            let q = query_owned.clone();
            let n = name_owned.clone();
            let reg = registry.clone();
            Box::pin(async move {
                crate::project::sql_runner::run_sql(&q, &n, reg).await
            })
        });

        self.nodes.insert(name.to_string(), Node {
            name: name.to_string(),
            layer: NodeLayer::Bronze,
            dependencies: deps_vec,
            execute,
        });

        self
    }

    pub fn silver_sql<const N: usize>(
        mut self,
        name: &str,
        deps: [&str; N],
        query: &'static str,
    ) -> Self {
        let deps_vec = deps.iter().map(|s| s.to_string()).collect();
        let query_owned = query.to_string();
        let name_owned = name.to_string();

        let execute: NodeFn = Box::new(move |registry| {
            let q = query_owned.clone();
            let n = name_owned.clone();
            let reg = registry.clone();
            Box::pin(async move {
                crate::project::sql_runner::run_sql(&q, &n, reg).await
            })
        });

        self.nodes.insert(name.to_string(), Node {
            name: name.to_string(),
            layer: NodeLayer::Silver,
            dependencies: deps_vec,
            execute,
        });

        self
    }

    pub fn gold_sql<const N: usize>(
        mut self,
        name: &str,
        deps: [&str; N],
        query: &'static str,
    ) -> Self {
        let deps_vec = deps.iter().map(|s| s.to_string()).collect();
        let query_owned = query.to_string();
        let name_owned = name.to_string();

        let execute: NodeFn = Box::new(move |registry| {
            let q = query_owned.clone();
            let n = name_owned.clone();
            let reg = registry.clone();
            Box::pin(async move {
                crate::project::sql_runner::run_sql(&q, &n, reg).await
            })
        });

        self.nodes.insert(name.to_string(), Node {
            name: name.to_string(),
            layer: NodeLayer::Gold,
            dependencies: deps_vec,
            execute,
        });

        self
    }

    // Bronze with slice deps - for use with DEPS const
    pub fn bronze_slice<F, Fut>(mut self, name: &str, deps: &[&str], f: F) -> Self
    where
        F: Fn(NodeRegistry) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ElusionResult<CustomDataFrame>> + Send + 'static,
    {
        let deps = deps.iter().map(|s| s.to_string()).collect();
        self.register(name, NodeLayer::Bronze, deps, f);
        self
    }

    // Silver with slice deps
    pub fn silver_slice<F, Fut>(mut self, name: &str, deps: &[&str], f: F) -> Self
    where
        F: Fn(NodeRegistry) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ElusionResult<CustomDataFrame>> + Send + 'static,
    {
        let deps = deps.iter().map(|s| s.to_string()).collect();
        self.register(name, NodeLayer::Silver, deps, f);
        self
    }

    // Gold with slice deps
    pub fn gold_slice<F, Fut>(mut self, name: &str, deps: &[&str], f: F) -> Self
    where
        F: Fn(NodeRegistry) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ElusionResult<CustomDataFrame>> + Send + 'static,
    {
        let deps = deps.iter().map(|s| s.to_string()).collect();
        self.register(name, NodeLayer::Gold, deps, f);
        self
    }

    // Bronze_sql with slice deps
    pub fn bronze_sql_slice(mut self, name: &str, deps: &[&str], query: &'static str) -> Self {
        let deps_vec = deps.iter().map(|s| s.to_string()).collect();
        let query_owned = query.to_string();
        let name_owned = name.to_string();

        let execute: NodeFn = Box::new(move |registry| {
            let q = query_owned.clone();
            let n = name_owned.clone();
            let reg = registry.clone();
            Box::pin(async move {
                crate::project::sql_runner::run_sql(&q, &n, reg).await
            })
        });

        self.nodes.insert(name.to_string(), Node {
            name: name.to_string(),
            layer: NodeLayer::Bronze,
            dependencies: deps_vec,
            execute,
        });
        self
    }

    // Silver_sql with slice deps
    pub fn silver_sql_slice(mut self, name: &str, deps: &[&str], query: &'static str) -> Self {
        let deps_vec = deps.iter().map(|s| s.to_string()).collect();
        let query_owned = query.to_string();
        let name_owned = name.to_string();

        let execute: NodeFn = Box::new(move |registry| {
            let q = query_owned.clone();
            let n = name_owned.clone();
            let reg = registry.clone();
            Box::pin(async move {
                crate::project::sql_runner::run_sql(&q, &n, reg).await
            })
        });

        self.nodes.insert(name.to_string(), Node {
            name: name.to_string(),
            layer: NodeLayer::Silver,
            dependencies: deps_vec,
            execute,
        });
        self
    }

    // Gold_sql with slice deps
    pub fn gold_sql_slice(mut self, name: &str, deps: &[&str], query: &'static str) -> Self {
        let deps_vec = deps.iter().map(|s| s.to_string()).collect();
        let query_owned = query.to_string();
        let name_owned = name.to_string();

        let execute: NodeFn = Box::new(move |registry| {
            let q = query_owned.clone();
            let n = name_owned.clone();
            let reg = registry.clone();
            Box::pin(async move {
                crate::project::sql_runner::run_sql(&q, &n, reg).await
            })
        });

        self.nodes.insert(name.to_string(), Node {
            name: name.to_string(),
            layer: NodeLayer::Gold,
            dependencies: deps_vec,
            execute,
        });
        self
    }

    // ===== INTERNAL REGISTRATION =====

    fn register<F, Fut>(
        &mut self,
        name: &str,
        layer: NodeLayer,
        dependencies: Vec<NodeName>,
        f: F,
    )
    where
        F: Fn(NodeRegistry) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ElusionResult<CustomDataFrame>> + Send + 'static,
    {
        let execute: NodeFn = Box::new(move |registry| {
            Box::pin(f(registry))
        });

        self.nodes.insert(name.to_string(), Node {
            name: name.to_string(),
            layer,
            dependencies,
            execute,
        });
    }

    // ===== RUN =====
    pub async fn run(self) -> ElusionResult<()> {
        let order = topological_sort(&self.nodes)?;

        let levels = Self::build_execution_levels(&self.nodes, &order);

        // Print execution plan
        println!("\n🗺️  Elusion Project - Execution Plan:");
        println!("{}", "=".repeat(60));
        for (level_idx, level) in levels.iter().enumerate() {
            for name in level {
                let node = &self.nodes[name];
                let mat_info = if let Some(config) = &self.project_config {
                    if node.layer != NodeLayer::Source {
                        let mat = config.get_materialization(name);
                        format!(" → {:?}", mat)
                    } else {
                        String::new()
                    }
                } else {
                    String::new()
                };
                println!(
                    "  Level {} [{:?}] {}{}",
                    level_idx + 1,
                    node.layer,
                    name,
                    mat_info
                );
            }
        }
        println!("{}", "=".repeat(60));
        println!();

        let mut registry = NodeRegistry::new();
        let project_config = std::sync::Arc::new(self.project_config);
        let mut execution_summary: Vec<(String, NodeLayer, usize, String)> = Vec::new();

        // Execute level by level
        for (level_idx, level) in levels.iter().enumerate() {
            if level.len() == 1 {
                // Single node — execute directly
                let name = &level[0];
                let node = self.nodes.get(name).unwrap();

                println!("▶️  Running [{:?}] {}", node.layer, name);
                let start = std::time::Instant::now();

                let df = (node.execute)(registry.clone()).await
                    .map_err(|e| ElusionError::Custom(format!(
                        "❌ Node '{}' failed: {}", name, e
                    )))?;

                let elapsed = start.elapsed();
                let row_count = df.df.clone().count().await.unwrap_or(0);

                println!("  ✅ Done: {} | {} rows | {:?}", name, row_count, elapsed);

                if let Some(config) = project_config.as_ref() {
                    if node.layer != NodeLayer::Source {
                        materialize(&df, name, config).await
                            .map_err(|e| ElusionError::Custom(format!(
                                "❌ Materialization failed for '{}': {}", name, e
                            )))?;
                    }
                }

                execution_summary.push((
                    name.clone(),
                    node.layer.clone(),
                    row_count,
                    format!("{:?}", elapsed),
                ));

                registry.insert(ResolvedNode {
                    name: name.clone(),
                    layer: node.layer.clone(),
                    df,
                });

            } else {
                // Multiple nodes at same level — run in parallel
                println!(
                    "⚡ Running {} nodes in parallel (Level {}):",
                    level.len(),
                    level_idx + 1
                );
                for name in level {
                    println!("   • [{:?}] {}", self.nodes[name].layer, name);
                }

                // Build futures for all nodes in this level
                let futures: Vec<_> = level.iter().map(|name: &String| {
                    let node = self.nodes.get(name).unwrap();
                    let registry_clone = registry.clone();
                    let name_clone = name.clone();      // now String, not str
                    let layer_clone = node.layer.clone();

                    async move {
                        let start = std::time::Instant::now();
                        let df = (node.execute)(registry_clone).await
                            .map_err(|e| ElusionError::Custom(format!(
                                "❌ Node '{}' failed: {}", name_clone, e
                            )))?;
                        let elapsed = start.elapsed();
                        let row_count = df.df.clone().count().await.unwrap_or(0);
                        Ok::<_, ElusionError>((name_clone, layer_clone, df, row_count, elapsed))
                    }
                }).collect();

                // Execute all futures concurrently
                let results = futures::future::join_all(futures).await;

                for result in results {
                    let (name, layer, df, row_count, elapsed): 
                        (String, NodeLayer, CustomDataFrame, usize, std::time::Duration) = result?;

                    println!("  ✅ Done: {} | {} rows | {:?}", name, row_count, elapsed);

                    if let Some(config) = project_config.as_ref() {
                        if layer != NodeLayer::Source {
                            materialize(&df, &name, config).await
                                .map_err(|e| ElusionError::Custom(format!(
                                    "❌ Materialization failed for '{}': {}", name, e
                                )))?;
                        }
                    }

                    execution_summary.push((
                        name.clone(),
                        layer.clone(),
                        row_count,
                        format!("{:?}", elapsed),
                    ));

                    registry.insert(ResolvedNode {
                        name: name.clone(),
                        layer,
                        df,
                    });
                }
            }
        }

        // Print execution summary
        println!("\n{}", "=".repeat(70));
        println!("🎉 Project completed successfully!");
        println!("{}", "=".repeat(70));
        println!("{:<30} {:<15} {:<12} {:<10}", "Model", "Layer", "Rows", "Time");
        println!("{}", "-".repeat(70));
        for (name, layer, rows, time) in &execution_summary {
            println!(
                "{:<30} {:<15} {:<12} {:<10}",
                name,
                format!("{:?}", layer),
                rows,
                time
            );
        }
        println!("{}", "=".repeat(70));

        Ok(())
    }

    fn build_execution_levels(
        nodes: &HashMap<NodeName, Node>,
        order: &[NodeName],
    ) -> Vec<Vec<NodeName>> {
        let mut levels: Vec<Vec<NodeName>> = Vec::new();
        let mut node_level: HashMap<NodeName, usize> = HashMap::new();

        for name in order {
            let node = &nodes[name];

            let level = node.dependencies
                .iter()
                .map(|dep| node_level.get(dep).copied().unwrap_or(0) + 1)
                .max()
                .unwrap_or(0);

            node_level.insert(name.clone(), level);

            if level >= levels.len() {
                levels.resize(level + 1, Vec::new());
            }

            levels[level].push(name.clone());
        }

        levels
    }
}