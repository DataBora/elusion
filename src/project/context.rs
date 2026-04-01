use crate::prelude::*;

#[derive(Clone)]
pub struct NodeRegistry {
    pub(crate) resolved: HashMap<NodeName, ResolvedNode>,
}

impl NodeRegistry {
    pub fn new() -> Self {
        Self {
            resolved: HashMap::new(),
        }
    }

    pub fn insert(&mut self, node: ResolvedNode) {
        self.resolved.insert(node.name.clone(), node);
    }

    // These are what users call inside closures
    pub fn ref_source(&self, name: &str) -> ElusionResult<CustomDataFrame> {
        self.get(name, "source")
    }

    pub fn ref_bronze(&self, name: &str) -> ElusionResult<CustomDataFrame> {
        self.get(name, "bronze")
    }

    pub fn ref_silver(&self, name: &str) -> ElusionResult<CustomDataFrame> {
        self.get(name, "silver")
    }

    pub fn ref_gold(&self, name: &str) -> ElusionResult<CustomDataFrame> {
        self.get(name, "gold")
    }

    fn get(&self, name: &str, _layer_hint: &str) -> ElusionResult<CustomDataFrame> {
        self.resolved
            .get(name)
            .map(|n| n.df.clone())
            .ok_or_else(|| ElusionError::Custom(
                format!("Node '{}' not found in registry. Check name and execution order.", name)
            ))
    }
}