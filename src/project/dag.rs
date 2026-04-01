use crate::prelude::*;

// Kahn's algorithm - standard topological sort
// Returns node names in execution order
pub fn topological_sort(nodes: &HashMap<NodeName, Node>) -> ElusionResult<Vec<NodeName>> {
    let mut in_degree: HashMap<NodeName, usize> = HashMap::new();
    let mut dependents: HashMap<NodeName, Vec<NodeName>> = HashMap::new();

    // Initialize
    for name in nodes.keys() {
        in_degree.entry(name.clone()).or_insert(0);
        dependents.entry(name.clone()).or_default();
    }

    // Build edges
    for (name, node) in nodes {
        for dep in &node.dependencies {
            if !nodes.contains_key(dep) {
                return Err(ElusionError::Custom(
                    format!("Node '{}' depends on '{}' which is not registered.", name, dep)
                ));
            }
            *in_degree.entry(name.clone()).or_insert(0) += 1;
            dependents.entry(dep.clone()).or_default().push(name.clone());
        }
    }

    // Kahn's
    let mut queue: VecDeque<NodeName> = in_degree
        .iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(name, _)| name.clone())
        .collect();

    let mut order = Vec::new();

    while let Some(current) = queue.pop_front() {
        order.push(current.clone());

        if let Some(deps) = dependents.get(&current) {
            for dependent in deps {
                let deg = in_degree.get_mut(dependent).unwrap();
                *deg -= 1;
                if *deg == 0 {
                    queue.push_back(dependent.clone());
                }
            }
        }
    }

    // Cycle detection
    if order.len() != nodes.len() {
        return Err(ElusionError::Custom(
            "Circular dependency detected in project nodes.".to_string()
        ));
    }

    Ok(order)
}