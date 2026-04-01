use crate::prelude::*;

// The registry key - just a name
pub type NodeName = String;

// What layer is this node in
#[derive(Debug, Clone, PartialEq)]
pub enum NodeLayer {
    Source,
    Bronze,
    Silver,
    Gold,
}

// A resolved node after execution - holds the materialized DataFrame
#[derive(Clone)]
pub struct ResolvedNode {
    pub name: NodeName,
    pub layer: NodeLayer,
    pub df: CustomDataFrame,
}

// The async closure type each node holds
pub type NodeFn = Box<
    dyn Fn(NodeRegistry) -> Pin<Box<dyn Future<Output = ElusionResult<CustomDataFrame>> + Send>>
    + Send
    + Sync
>;

// A registered but not yet executed node
pub struct Node {
    pub name: NodeName,
    pub layer: NodeLayer,
    pub dependencies: Vec<NodeName>,  // populated by ref_* calls
    pub execute: NodeFn,
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("name", &self.name)
            .field("layer", &self.layer)
            .field("dependencies", &self.dependencies)
            .finish()
    }
}