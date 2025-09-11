use crate::prelude::*;
use crate::lowercase_column_names;

/// Enhanced XML node with full path information
#[derive(Debug, Clone)]
pub struct XmlNode {
    pub name: String,
    pub text: Option<String>,
    pub attributes: HashMap<String, String>,
    pub children: Vec<XmlNode>,
    pub path: Vec<String>,
}

/// Detailed element information for analysis
#[derive(Debug, Clone)]
pub struct ElementInfo {
    pub total_count: usize,
    pub parents: HashSet<String>,
    pub sample_paths: Vec<String>,
    pub has_attributes: bool,
    pub has_text_content: bool,
    pub sample_text: Vec<String>,
    pub sample_attributes: HashMap<String, Vec<String>>,
}

/// Parent-child relationship information
#[derive(Debug)]
pub struct Relationship {
    pub parent: String,
    pub child: String,
    pub cardinality: Cardinality,
    pub occurrences: Vec<usize>,
}

#[derive(Debug, PartialEq)]
pub enum Cardinality {
    OneToOne,
    OneToMany,
    ManyToMany,
}

/// Multiple value element information (critical for parsing)
#[derive(Debug)]
pub struct MultipleValueInfo {
    pub parent: String,
    pub child: String,
    pub max_count: usize,
    pub parent_count: usize,
    pub avg_count: f64,
}


/// Extraction strategy with metadata
#[derive(Debug)]
pub struct ExtractionStrategy {
    pub name: String,
    pub estimated_records: usize,
}

/// Complete analysis result
#[derive(Debug)]
pub struct DetailedAnalysis {
    pub total_elements: usize,
    pub unique_element_types: usize,
    pub max_depth: usize,
    pub elements: HashMap<String, ElementInfo>,
    pub multiple_value_elements: Vec<MultipleValueInfo>,
    pub recommended_strategies: Vec<ExtractionStrategy>,
}

/// Schema definition for extraction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractionSchema {
    pub name: String,
    pub primary_element: String,
    pub detail_element: Option<String>,
    pub header_fields: Vec<FieldMapping>,
    pub detail_fields: Vec<FieldMapping>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMapping {
    pub xml_path: String,
    pub output_name: String,
    pub required: bool,
    pub default_value: Option<String>,
}

/// Configuration for XML parsing behavior
#[derive(Debug, Clone)]
pub struct XmlParseConfig {
    pub max_depth: usize,
    pub include_empty_fields: bool,
    pub processing_mode: XmlProcessingMode,
}

impl Default for XmlParseConfig {
    fn default() -> Self {
        Self {
            max_depth: 20,
            include_empty_fields: false,
            processing_mode: XmlProcessingMode::Auto,
        }
    }
}

/// Analyzer configuration
pub struct AnalyzerConfig {
    pub max_sample_text: usize,
    pub max_sample_attributes: usize,
    pub max_depth: usize,
    pub include_empty_fields: bool
}

impl Default for AnalyzerConfig {
    fn default() -> Self {
        Self {
            max_sample_text: 3,
            max_sample_attributes: 3,
            max_depth: 20,
            include_empty_fields: true
        }
    }
}

#[derive(Debug)]
pub struct MultiElementInfo {
    pub parent: String,
    pub element_groups: Vec<ElementGroup>,
    pub combination_strategy: CombinationStrategy,
}

#[derive(Debug)]
pub struct ElementGroup {
    pub element_name: String,
    pub max_count: usize,
    pub fields: Vec<String>,
}

#[derive(Debug, Clone)]
pub enum CombinationStrategy {
    CartesianProduct, 
    ParallelArrays,    
    HeaderDetail, 
}

/// Enhanced schema definition for multi-element extraction
#[derive(Debug, Clone)]
pub struct EnhancedExtractionSchema {
    pub name: String,
    pub primary_element: String,
    pub base_fields: Vec<FieldMapping>,           
    pub multi_element_groups: Vec<MultiElementGroup>,  
    pub combination_strategy: CombinationStrategy,
}

#[derive(Debug, Clone)]
pub struct MultiElementGroup {
    pub group_name: String,
    pub element_name: String,
    pub fields: Vec<FieldMapping>,
}

/// Main XML analyzer for structure analysis
pub struct XmlAnalyzer {
    config: AnalyzerConfig,
}

impl XmlAnalyzer {
    pub fn new(config: AnalyzerConfig) -> Self {
        Self { config }
    }

    /// Parse XML file into node tree
    pub fn parse_xml(&self, file_path: &str) -> ElusionResult<XmlNode> {
        let file = File::open(file_path).map_err(|e| ElusionError::WriteError {
            path: file_path.to_string(),
            operation: "read".to_string(),
            reason: e.to_string(),
            suggestion: "Check file permissions and path".to_string(),
        })?;

        let reader = BufReader::new(file);
        let parser = EventReader::new(reader);
        
        let mut stack: Vec<XmlNode> = Vec::new();
        let mut root: Option<XmlNode> = None;
        
        for event in parser {
            match event.map_err(|e| ElusionError::InvalidOperation {
                operation: "XML Parsing".to_string(),
                reason: format!("XML parse error: {}", e),
                suggestion: "Check if XML file is well-formed".to_string(),
            })? {
                XmlEvent::StartElement { name, attributes, .. } => {
                    let element_name = name.local_name;
                    let current_path = if let Some(parent) = stack.last() {
                        let mut path = parent.path.clone();
                        path.push(parent.name.clone());
                        path
                    } else {
                        Vec::new()
                    };
                    
                    let mut node = XmlNode {
                        name: element_name,
                        text: None,
                        attributes: HashMap::new(),
                        children: Vec::new(),
                        path: current_path,
                    };
                    
                    // Add attributes
                    for attr in attributes {
                        node.attributes.insert(attr.name.local_name, attr.value);
                    }
                    
                    stack.push(node);
                }
                XmlEvent::EndElement { .. } => {
                    if let Some(node) = stack.pop() {
                        if let Some(parent) = stack.last_mut() {
                            parent.children.push(node);
                        } else {
                            root = Some(node);
                        }
                    }
                }
                XmlEvent::Characters(text) | XmlEvent::CData(text) => {
                    if let Some(node) = stack.last_mut() {
                        let trimmed = text.trim();
                        if !trimmed.is_empty() || self.config.include_empty_fields {
                            node.text = Some(trimmed.to_string());
                        }
                    }
                }
                _ => {}
            }
        }
        
        root.ok_or_else(|| ElusionError::InvalidOperation {
            operation: "XML Parsing".to_string(),
            reason: "No root element found".to_string(),
            suggestion: "Ensure XML has a valid root element".to_string(),
        })
    }

    pub fn analyze_structure(&self, root: &XmlNode) -> DetailedAnalysis {
        let mut elements: HashMap<String, ElementInfo> = HashMap::new();
        let mut parent_child_counts: HashMap<String, Vec<usize>> = HashMap::new();
        let mut total_elements = 0;
        let mut max_depth = 0;
        
        self.analyze_node_recursive(root, &mut elements, &mut parent_child_counts, &mut total_elements, &mut max_depth, 0);
        
        let relationships = self.build_relationships(&parent_child_counts);

        let multiple_value_elements = self.find_multiple_value_elements(&relationships);
        
        let recommended_strategies = self.generate_strategies(&elements, &multiple_value_elements);
        
        DetailedAnalysis {
            total_elements,
            unique_element_types: elements.len(),
            max_depth,
            elements,
            multiple_value_elements,
            recommended_strategies,
        }
    }

    fn analyze_node_recursive(
        &self,
        node: &XmlNode,
        elements: &mut HashMap<String, ElementInfo>,
        parent_child_counts: &mut HashMap<String, Vec<usize>>,
        total_elements: &mut usize,
        max_depth: &mut usize,
        depth: usize,
    ) {
        if depth > self.config.max_depth {
            return;
        }
        *total_elements += 1;
        *max_depth = (*max_depth).max(depth);
        
        // Update element info
        let info = elements.entry(node.name.clone()).or_insert_with(|| ElementInfo {
            total_count: 0,
            parents: HashSet::new(),
            sample_paths: Vec::new(),
            has_attributes: false,
            has_text_content: false,
            sample_text: Vec::new(),
            sample_attributes: HashMap::new(),
        });
        
        info.total_count += 1;
        info.has_attributes = !node.attributes.is_empty();
        info.has_text_content = node.text.is_some();
        
        // Add parent if not root
        if !node.path.is_empty() {
            if let Some(parent_name) = node.path.last() {
                info.parents.insert(parent_name.clone());
            }
        }
        
        // Sample data collection
        if info.sample_paths.len() < 3 {
            let full_path = if node.path.is_empty() {
                node.name.clone()
            } else {
                format!("{}/{}", node.path.join("/"), node.name)
            };
            info.sample_paths.push(full_path);
        }
        
        if let Some(text) = &node.text {
            if info.sample_text.len() < self.config.max_sample_text {
                info.sample_text.push(text.clone());
            }
        }
        
        for (attr_name, attr_value) in &node.attributes {
            let samples = info.sample_attributes.entry(attr_name.clone()).or_insert_with(Vec::new);
            if samples.len() < self.config.max_sample_attributes {
                samples.push(attr_value.clone());
            }
        }
        
        // Count children by type
        let mut child_counts: HashMap<String, usize> = HashMap::new();
        for child in &node.children {
            *child_counts.entry(child.name.clone()).or_insert(0) += 1;
        }
        
        // Record parent-child relationships
        for (child_name, count) in child_counts {
            let relationship_key = format!("{} -> {}", node.name, child_name);
            parent_child_counts.entry(relationship_key).or_insert_with(Vec::new).push(count);
        }
        
        // Recurse
        for child in &node.children {
            self.analyze_node_recursive(child, elements, parent_child_counts, total_elements, max_depth, depth + 1);
        }
    }

    fn build_relationships(&self, parent_child_counts: &HashMap<String, Vec<usize>>) -> Vec<Relationship> {
        let mut relationships = Vec::new();
        
        for (relationship_key, counts) in parent_child_counts {
            let parts: Vec<&str> = relationship_key.split(" -> ").collect();
            if parts.len() == 2 {
                let parent = parts[0].to_string();
                let child = parts[1].to_string();
                let max_count = *counts.iter().max().unwrap_or(&0);
                let avg_count = counts.iter().sum::<usize>() as f64 / counts.len() as f64;
                
                let cardinality = if max_count <= 1 {
                    Cardinality::OneToOne
                } else if avg_count > 1.5 {
                    Cardinality::OneToMany
                } else {
                    Cardinality::ManyToMany
                };
                
                relationships.push(Relationship {
                    parent,
                    child,
                    cardinality,
                    occurrences: counts.clone(),
                });
            }
        }
        
        relationships
    }

    fn find_multiple_value_elements(&self, relationships: &[Relationship]) -> Vec<MultipleValueInfo> {
        let mut multiple_values = Vec::new();
        
        for rel in relationships {
            if rel.cardinality != Cardinality::OneToOne {
                let max_count = *rel.occurrences.iter().max().unwrap_or(&0);
                let avg_count = rel.occurrences.iter().sum::<usize>() as f64 / rel.occurrences.len() as f64;
                let parent_count = rel.occurrences.len();
                
                multiple_values.push(MultipleValueInfo {
                    parent: rel.parent.clone(),
                    child: rel.child.clone(),
                    max_count,
                    parent_count,
                    avg_count,
                });
            }
        }
        
        // Sort by impact (highest first)
        multiple_values.sort_by(|a, b| {
            (b.max_count * b.parent_count).cmp(&(a.max_count * a.parent_count))
        });
        
        multiple_values
    }

    fn generate_strategies(&self, elements: &HashMap<String, ElementInfo>, multiple_values: &[MultipleValueInfo]) -> Vec<ExtractionStrategy> {
        let mut strategies = Vec::new();
        
        if let Some(header_candidate) = self.find_header_element(elements, multiple_values) {
            if let Some(detail_info) = multiple_values.iter().find(|mv| mv.parent == header_candidate) {
                strategies.push(ExtractionStrategy {
                    name: "Header-Detail".to_string(),
                    estimated_records: detail_info.parent_count * (detail_info.avg_count as usize),
                });
            }
        }
        
        strategies
    }

    fn find_header_element(&self, elements: &HashMap<String, ElementInfo>, multiple_values: &[MultipleValueInfo]) -> Option<String> {
        // Look for elements that are parents of multiple-value relationships
        for mv in multiple_values {
            if let Some(info) = elements.get(&mv.parent) {
                if info.total_count > 10 && info.total_count < 1000 {
                    return Some(mv.parent.clone());
                }
            }
        }
        None
    }
    pub fn find_multi_element_patterns(&self, analysis: &DetailedAnalysis) -> Vec<MultiElementInfo> {
        let mut patterns = Vec::new();
        
        // Group elements by their parents
        let mut parent_children: HashMap<String, Vec<(String, usize)>> = HashMap::new();
        
        for (element_name, info) in &analysis.elements {
            for parent in &info.parents {
                parent_children.entry(parent.clone())
                    .or_insert_with(Vec::new)
                    .push((element_name.clone(), info.total_count));
            }
        }
        
        // Find parents with multiple repeating children
        for (parent, children) in parent_children {
            let repeating_children: Vec<_> = children.iter()
                .filter(|(_, count)| *count > 1)
                .collect();
            
            if repeating_children.len() >= 2 {
                // Multiple repeating elements - potential for combinations
                let element_groups: Vec<ElementGroup> = repeating_children.iter()
                    .map(|(name, count)| {
                        let fields = self.get_fields_for_element(name, analysis);
                        ElementGroup {
                            element_name: name.clone(),
                            max_count: *count,
                            fields,
                        }
                    })
                    .collect();
                
                let strategy = self.determine_combination_strategy(&element_groups);
                
                patterns.push(MultiElementInfo {
                    parent,
                    element_groups,
                    combination_strategy: strategy,
                });
            }
        }
        
        patterns
    }
    
    fn get_fields_for_element(&self, element_name: &str, analysis: &DetailedAnalysis) -> Vec<String> {
        let mut fields = Vec::new();
        
        // Find child elements with text content
        for (name, info) in &analysis.elements {
            if info.parents.contains(element_name) && info.has_text_content {
                fields.push(name.clone());
            }
        }
        
        // Add attributes
        if let Some(info) = analysis.elements.get(element_name) {
            for attr_name in info.sample_attributes.keys() {
                fields.push(format!("@{}", attr_name));
            }
        }
        
        fields
    }
    
    fn determine_combination_strategy(&self, groups: &[ElementGroup]) -> CombinationStrategy {
        // Heuristics for strategy selection
        if groups.len() >= 3 {
            // Many element types - use parallel arrays to avoid explosion
            CombinationStrategy::ParallelArrays
        } else if groups.iter().all(|g| g.max_count <= 3) {
            // Small counts - cartesian product is manageable
            CombinationStrategy::CartesianProduct
        } else {
            // One large group - likely header-detail
            CombinationStrategy::HeaderDetail
        }
    }
    
    /// Generate enhanced schema with multi-element support
    pub fn generate_enhanced_schema(&self, analysis: &DetailedAnalysis) -> EnhancedExtractionSchema {
        let multi_patterns = self.find_multi_element_patterns(analysis);
        
        if let Some(pattern) = multi_patterns.first() {
            // Use first detected pattern
            let base_fields = self.generate_base_fields(&pattern.parent, analysis);
            let multi_groups = self.generate_multi_element_groups(&pattern.element_groups);
            
            EnhancedExtractionSchema {
                name: "EnhancedDynamicSchema".to_string(),
                primary_element: pattern.parent.clone(),
                base_fields,
                multi_element_groups: multi_groups,
                combination_strategy: pattern.combination_strategy.clone(),
            }
        } else {
            // Fallback to simple schema
            let primary_element = self.find_most_frequent_element(analysis);
            let base_fields = self.generate_base_fields(&primary_element, analysis);
            
            EnhancedExtractionSchema {
                name: "SimpleSchema".to_string(),
                primary_element,
                base_fields,
                multi_element_groups: vec![],
                combination_strategy: CombinationStrategy::HeaderDetail,
            }
        }
    }
    
    fn generate_base_fields(&self, element_name: &str, analysis: &DetailedAnalysis) -> Vec<FieldMapping> {
        let mut fields = Vec::new();
        
        // Direct children of primary element
        for (name, info) in &analysis.elements {
            // Only include direct children that aren't part of multi-element groups
            if info.parents.contains(element_name) && info.has_text_content {
                fields.push(FieldMapping {
                    xml_path: name.clone(),
                    output_name: to_snake_case(name),
                    required: false,
                    default_value: None,
                });
            }
        }
        
        // Attributes of primary element
        if let Some(info) = analysis.elements.get(element_name) {
            for (attr_name, _) in &info.sample_attributes {
                fields.push(FieldMapping {
                    xml_path: format!("@{}", attr_name),
                    output_name: to_snake_case(attr_name),
                    required: false,
                    default_value: None,
                });
            }
        }
        
        fields
    }
    
    fn generate_multi_element_groups(&self, element_groups: &[ElementGroup]) -> Vec<MultiElementGroup> {
        element_groups.iter().map(|group| {
            let fields = group.fields.iter().map(|field_name| {
                let output_name = if field_name.starts_with('@') {
                    format!("{}_{}", to_snake_case(&group.element_name), to_snake_case(&field_name[1..]))
                } else {
                    to_snake_case(field_name)
                };
                
                FieldMapping {
                    xml_path: field_name.clone(),
                    output_name,
                    required: false,
                    default_value: None,
                }
            }).collect();
            
            MultiElementGroup {
                group_name: group.element_name.clone(),
                element_name: group.element_name.clone(),
                fields,
            }
        }).collect()
    }
    
    fn find_most_frequent_element(&self, analysis: &DetailedAnalysis) -> String {
        analysis.elements.iter()
            .max_by_key(|(_, info)| info.total_count)
            .map(|(name, _)| name.clone())
            .unwrap_or_else(|| "root".to_string())
    }
}

/// Enhanced extractor that handles multi-element combinations
pub struct EnhancedSchemaExtractor {
    schema: EnhancedExtractionSchema,
}

impl EnhancedSchemaExtractor {
    pub fn new(schema: EnhancedExtractionSchema) -> Self {
        Self { schema }
    }
    
    pub fn extract(&self, root: &XmlNode) -> ElusionResult<Vec<HashMap<String, String>>> {
        let mut records = Vec::new();
        
        println!("Extracting with enhanced schema: {}", self.schema.name);
        println!("Primary element: {}", self.schema.primary_element);
        println!("Multi-element groups: {}", self.schema.multi_element_groups.len());
        
        let primary_elements = self.find_elements_by_name(root, &self.schema.primary_element);
        println!("Found {} primary elements", primary_elements.len());
        
        for primary_element in primary_elements {
            // Extract base data
            let mut base_record = HashMap::new();
            for field in &self.schema.base_fields {
                if let Some(value) = self.extract_field_value(primary_element, field) {
                    base_record.insert(field.output_name.clone(), value);
                } else if let Some(default) = &field.default_value {
                    base_record.insert(field.output_name.clone(), default.clone());
                } else {
                    base_record.insert(field.output_name.clone(), "".to_string());
                }
            }
            
            // Handle multi-element combinations
            if self.schema.multi_element_groups.is_empty() {
                records.push(base_record);
            } else {
                let combinations = self.generate_combinations(primary_element);
                
                for combination in combinations {
                    let mut record = base_record.clone();
                    record.extend(combination);
                    records.push(record);
                }
            }
        }
        
        println!("Extracted {} records", records.len());
        Ok(records)
    }
    
    fn generate_combinations(&self, element: &XmlNode) -> Vec<HashMap<String, String>> {
        match self.schema.combination_strategy {
            CombinationStrategy::CartesianProduct => self.cartesian_product_combinations(element),
            CombinationStrategy::ParallelArrays => self.parallel_array_combinations(element),
            CombinationStrategy::HeaderDetail => self.header_detail_combinations(element),
        }
    }
    
    fn cartesian_product_combinations(&self, element: &XmlNode) -> Vec<HashMap<String, String>> {
        let mut all_element_values = Vec::new();
        
        // Collect all instances of each element type
        for group in &self.schema.multi_element_groups {
            let elements = self.find_elements_by_name(element, &group.element_name);
            let values: Vec<HashMap<String, String>> = elements.iter()
                .map(|elem| self.extract_group_values(elem, group))
                .collect();
            
            all_element_values.push((group.group_name.clone(), values));
        }
        
        // Generate cartesian product
        self.cartesian_product(&all_element_values)
    }
    
    fn parallel_array_combinations(&self, element: &XmlNode) -> Vec<HashMap<String, String>> {
        let mut all_values = Vec::new();
        let mut max_length = 0;
        
        // Collect all instances and find max length
        for group in &self.schema.multi_element_groups {
            let elements = self.find_elements_by_name(element, &group.element_name);
            let values: Vec<HashMap<String, String>> = elements.iter()
                .map(|elem| self.extract_group_values(elem, group))
                .collect();
            
            max_length = max_length.max(values.len());
            all_values.push(values);
        }
        
        // Create combinations by index (like your Python approach)
        let mut combinations = Vec::new();
        for i in 0..max_length.max(1) {
            let mut combination = HashMap::new();
            
            for (group_idx, group_values) in all_values.iter().enumerate() {
                if i < group_values.len() {
                    combination.extend(group_values[i].clone());
                } else {
                    // Fill with empty values for this group
                    let group = &self.schema.multi_element_groups[group_idx];
                    for field in &group.fields {
                        combination.insert(field.output_name.clone(), "".to_string());
                    }
                }
            }
            
            combinations.push(combination);
        }
        
        combinations
    }
    
    fn header_detail_combinations(&self, element: &XmlNode) -> Vec<HashMap<String, String>> {
        // Simple one-to-many relationship (like current sales order logic)
        if let Some(first_group) = self.schema.multi_element_groups.first() {
            let elements = self.find_elements_by_name(element, &first_group.element_name);
            elements.iter()
                .map(|elem| self.extract_group_values(elem, first_group))
                .collect()
        } else {
            vec![HashMap::new()]
        }
    }
    
    fn extract_group_values(&self, element: &XmlNode, group: &MultiElementGroup) -> HashMap<String, String> {
        let mut values = HashMap::new();
        
        for field in &group.fields {
            if let Some(value) = self.extract_field_value(element, field) {
                values.insert(field.output_name.clone(), value);
            } else if let Some(default) = &field.default_value {
                values.insert(field.output_name.clone(), default.clone());
            } else {
                values.insert(field.output_name.clone(), "".to_string());
            }
        }
        
        values
    }
    
    fn cartesian_product(&self, all_values: &[(String, Vec<HashMap<String, String>>)]) -> Vec<HashMap<String, String>> {
        if all_values.is_empty() {
            return vec![HashMap::new()];
        }
        
        let mut result = vec![HashMap::new()];
        
        for (_, values) in all_values {
            let mut new_result = Vec::new();
            
            for existing in &result {
                if values.is_empty() {
                    new_result.push(existing.clone());
                } else {
                    for value in values {
                        let mut combined = existing.clone();
                        combined.extend(value.clone());
                        new_result.push(combined);
                    }
                }
            }
            
            result = new_result;
        }
        
        result
    }
    
    // Reuse existing helper methods from SchemaExtractor
    fn find_elements_by_name<'a>(&self, node: &'a XmlNode, name: &str) -> Vec<&'a XmlNode> {
        let mut results = Vec::new();
        self.find_elements_recursive(node, name, &mut results);
        results
    }

    fn find_elements_recursive<'a>(&self, node: &'a XmlNode, name: &str, results: &mut Vec<&'a XmlNode>) {
        if node.name == name {
            results.push(node);
        }
        for child in &node.children {
            self.find_elements_recursive(child, name, results);
        }
    }

    fn extract_field_value(&self, element: &XmlNode, field: &FieldMapping) -> Option<String> {
        let path_parts: Vec<&str> = field.xml_path.split('/').collect();
        
        if path_parts.len() == 1 {
            let part = path_parts[0];
            if part.starts_with('@') {
                let attr_name = &part[1..];
                element.attributes.get(attr_name).cloned()
            } else if part == "." {
                element.text.clone()
            } else {
                element.children.iter()
                    .find(|child| child.name == part)
                    .and_then(|child| child.text.clone())
            }
        } else {
            self.navigate_path(element, &path_parts)
        }
    }

    fn navigate_path(&self, element: &XmlNode, path_parts: &[&str]) -> Option<String> {
        if path_parts.is_empty() {
            return element.text.clone();
        }
        
        let current_part = path_parts[0];
        if current_part.starts_with('@') {
            let attr_name = &current_part[1..];
            return element.attributes.get(attr_name).cloned();
        }
        
        for child in &element.children {
            if child.name == current_part {
                return self.navigate_path(child, &path_parts[1..]);
            }
        }
        
        None
    }
}

/// Schema-based extractor
pub struct SchemaExtractor {
    schema: ExtractionSchema,
}

impl SchemaExtractor {
    pub fn new(schema: ExtractionSchema) -> Self {
        Self { schema }
    }

    pub fn extract(&self, root: &XmlNode) -> ElusionResult<Vec<HashMap<String, String>>> {
        let mut records = Vec::new();
        
        println!("Extracting with schema: {}", self.schema.name);
        println!("Primary element: {}", self.schema.primary_element);
        
        // Find primary elements
        let primary_elements = self.find_elements_by_name(root, &self.schema.primary_element);
        println!("Found {} primary elements", primary_elements.len());
        
        if primary_elements.is_empty() {
            // If no primary elements found, try to extract from root
            println!("No primary elements found, extracting from root");
            let mut record = HashMap::new();
            
            // Extract header fields from root
            for field in &self.schema.header_fields {
                if let Some(value) = self.extract_field_value(root, field) {
                    record.insert(field.output_name.clone(), value);
                } else if let Some(default) = &field.default_value {
                    record.insert(field.output_name.clone(), default.clone());
                } else {
                    record.insert(field.output_name.clone(), "".to_string());
                }
            }
            
            // Ensure we have at least one field
            if record.is_empty() {
                record.insert("root_content".to_string(), 
                    root.text.clone().unwrap_or_else(|| "empty".to_string()));
            }
            
            records.push(record);
            return Ok(records);
        }
        
        for primary_element in primary_elements {
            // Extract header data
            let mut base_record = HashMap::new();
            for field in &self.schema.header_fields {
                if let Some(value) = self.extract_field_value(primary_element, field) {
                    base_record.insert(field.output_name.clone(), value);
                } else if let Some(default) = &field.default_value {
                    base_record.insert(field.output_name.clone(), default.clone());
                } else {
                    base_record.insert(field.output_name.clone(), "".to_string());
                }
            }
            
            // If we have detail elements, create records for each
            if let Some(detail_element_name) = &self.schema.detail_element {
                let detail_elements = self.find_elements_by_name(primary_element, detail_element_name);
                
                if detail_elements.is_empty() {
                    // base record has all required fields
                    for field in &self.schema.detail_fields {
                        if !base_record.contains_key(&field.output_name) {
                            base_record.insert(field.output_name.clone(), 
                                field.default_value.clone().unwrap_or_else(|| "".to_string()));
                        }
                    }
                    records.push(base_record);
                } else {
                    for detail_element in detail_elements {
                        let mut record = base_record.clone();
                        
                        for field in &self.schema.detail_fields {
                            if let Some(value) = self.extract_field_value(detail_element, field) {
                                record.insert(field.output_name.clone(), value);
                            } else if let Some(default) = &field.default_value {
                                record.insert(field.output_name.clone(), default.clone());
                            } else {
                                record.insert(field.output_name.clone(), "".to_string());
                            }
                        }
                        
                        records.push(record);
                    }
                }
            } else {
                // base record has at least one field
                if base_record.is_empty() {
                    base_record.insert("element_content".to_string(), 
                        primary_element.text.clone().unwrap_or_else(|| "empty".to_string()));
                }
                records.push(base_record);
            }
        }
        
        println!("Extracted {} records", records.len());
        if !records.is_empty() {
            println!("Sample record keys: {:?}", records[0].keys().collect::<Vec<_>>());
        }
        
        Ok(records)
    }

    fn find_elements_by_name<'a>(&self, node: &'a XmlNode, name: &str) -> Vec<&'a XmlNode> {
        let mut results = Vec::new();
        self.find_elements_recursive(node, name, &mut results);
        results
    }

    fn find_elements_recursive<'a>(&self, node: &'a XmlNode, name: &str, results: &mut Vec<&'a XmlNode>) {
        if node.name == name {
            results.push(node);
        }
        for child in &node.children {
            self.find_elements_recursive(child, name, results);
        }
    }

    fn extract_field_value(&self, element: &XmlNode, field: &FieldMapping) -> Option<String> {
        let path_parts: Vec<&str> = field.xml_path.split('/').collect();
        
        if path_parts.len() == 1 {
            let part = path_parts[0];
            if part.starts_with('@') {
                let attr_name = &part[1..];
                element.attributes.get(attr_name).cloned()
            } else if part == "." {
                element.text.clone()
            } else {
                // Try to find the element recursively
                self.find_descendant_text(element, part)
            }
        } else {
            self.navigate_path_improved(element, &path_parts)
        }
    }
    
    // find text in any descendant element
    fn find_descendant_text(&self, element: &XmlNode, target_name: &str) -> Option<String> {
        // Check direct children first
        for child in &element.children {
            if child.name == target_name {
                if let Some(text) = &child.text {
                    return Some(text.clone());
                }
            }
        }
        
        // check grandchildren recursively
        for child in &element.children {
            if let Some(text) = self.find_descendant_text(child, target_name) {
                return Some(text);
            }
        }
        
        None
    }
    
    fn navigate_path_improved(&self, element: &XmlNode, path_parts: &[&str]) -> Option<String> {
        if path_parts.is_empty() {
            return element.text.clone();
        }
        
        let current_part = path_parts[0];
        
        // Handle attributes with path like "child/@attr"
        if current_part.contains("/@") {
            let parts: Vec<&str> = current_part.split("/@").collect();
            if parts.len() == 2 {
                let element_name = parts[0];
                let attr_name = parts[1];
                
                for child in &element.children {
                    if child.name == element_name {
                        return child.attributes.get(attr_name).cloned();
                    }
                }
            }
            return None;
        }
        
        if current_part.starts_with('@') {
            let attr_name = &current_part[1..];
            return element.attributes.get(attr_name).cloned();
        }
        
        for child in &element.children {
            if child.name == current_part {
                return self.navigate_path_improved(child, &path_parts[1..]);
            }
        }
        
        None
    }
}

pub async fn load_xml_intelligent_cartesian(
    file_path: &str,
    alias: &str,
    config: Option<XmlParseConfig>
) -> ElusionResult<AliasedDataFrame> {
    let config = config.unwrap_or_default();
    load_xml_with_enhanced_analysis(file_path, alias, config).await
}


/// Main intelligent XML loading function
pub async fn load_xml_intelligent(
    file_path: &str,
    alias: &str,
    config: Option<XmlParseConfig>
) -> ElusionResult<AliasedDataFrame> {
    let config = config.unwrap_or_default();
    
    println!("Starting intelligent XML processing...");
    
    load_xml_with_analysis(file_path, alias, config).await
}

/// Load XML with automatic analysis
pub async fn load_xml_with_analysis(
    file_path: &str,
    alias: &str,
    config: XmlParseConfig
) -> ElusionResult<AliasedDataFrame> {
    println!("Analyzing XML structure...");
    
    let analyzer_config = AnalyzerConfig {
        max_sample_text: AnalyzerConfig::default().max_sample_text,
        max_sample_attributes: AnalyzerConfig::default().max_sample_attributes,
        max_depth: config.max_depth,
        include_empty_fields: config.include_empty_fields,
    };
    let analyzer = XmlAnalyzer::new(analyzer_config);
    let root = analyzer.parse_xml(file_path)?;
    let analysis = analyzer.analyze_structure(&root);
    
    println!("Analysis complete:");
    println!("  Elements: {}", analysis.total_elements);
    println!("  Element types: {}", analysis.unique_element_types);
    println!("  Max depth: {}", analysis.max_depth);
    
    if !analysis.multiple_value_elements.is_empty() {
        println!("  Multiple value relationships found: {}", analysis.multiple_value_elements.len());
        for mv in &analysis.multiple_value_elements {
            println!("    {} -> {}: max {} per parent", mv.parent, mv.child, mv.max_count);
        }
    }
    
    // best strategy
    let strategy = analysis.recommended_strategies.first()
        .ok_or_else(|| ElusionError::InvalidOperation {
            operation: "Strategy Selection".to_string(),
            reason: "No extraction strategies available".to_string(),
            suggestion: "XML structure may be too simple or complex for automatic analysis".to_string(),
        })?;
    
    println!("Selected strategy: {} (estimated {} records)", strategy.name, strategy.estimated_records);
    
    let schema = if strategy.name.contains("Header-Detail") {
        create_dynamic_schema(&analysis)
    } else {
        create_fallback_schema()
    };
    
    load_xml_with_schema(file_path, alias, schema).await
}

pub async fn load_xml_with_enhanced_analysis(
    file_path: &str,
    alias: &str,
    config: XmlParseConfig
) -> ElusionResult<AliasedDataFrame> {
    println!("Analyzing XML structure for multi-element patterns...");
    
    let analyzer_config = AnalyzerConfig {
        max_sample_text: AnalyzerConfig::default().max_sample_text,
        max_sample_attributes: AnalyzerConfig::default().max_sample_attributes,
        max_depth: config.max_depth,
        include_empty_fields: config.include_empty_fields,
    };
    let analyzer = XmlAnalyzer::new(analyzer_config);
    let root = analyzer.parse_xml(file_path)?;
    let analysis = analyzer.analyze_structure(&root);
    
    println!("Analysis complete:");
    println!("  Elements: {}", analysis.total_elements);
    println!("  Element types: {}", analysis.unique_element_types);
    println!("  Max depth: {}", analysis.max_depth);
    
    // Generate enhanced schema
    let enhanced_schema = analyzer.generate_enhanced_schema(&analysis);
    println!("Enhanced schema generated: {}", enhanced_schema.name);
    println!("  Primary element: {}", enhanced_schema.primary_element);
    println!("  Base fields: {}", enhanced_schema.base_fields.len());
    println!("  Multi-element groups: {}", enhanced_schema.multi_element_groups.len());
    println!("  Combination strategy: {:?}", enhanced_schema.combination_strategy);
    
    // Extract with enhanced schema
    let extractor = EnhancedSchemaExtractor::new(enhanced_schema);
    let records = extractor.extract(&root)?;
    
    if records.is_empty() {
        return Err(ElusionError::InvalidOperation {
            operation: "Enhanced XML Processing".to_string(),
            reason: "No records extracted from XML".to_string(),
            suggestion: "Check enhanced schema configuration and XML content".to_string(),
        });
    }
    
    println!("Extracted {} records with enhanced processing", records.len());
    
    // Convert to DataFrame using existing logic
    let schema = infer_schema_from_records(&records, &HashSet::new());
    let record_batch = build_record_batch_from_records(&records, &schema)?;
    
    let ctx = SessionContext::new();
    let mem_table = MemTable::try_new(schema.into(), vec![vec![record_batch]])
        .map_err(|e| ElusionError::SchemaError {
            message: format!("Failed to create MemTable: {}", e),
            schema: None,
            suggestion: "Verify data types and schema compatibility".to_string(),
        })?;
    
    ctx.register_table(alias, Arc::new(mem_table))
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Table registration".to_string(),
            reason: format!("Failed to register table: {}", e),
            suggestion: "Try using a different alias".to_string(),
        })?;
    
    let df = ctx.table(alias).await.map_err(|e| ElusionError::InvalidOperation {
        operation: "Table creation".to_string(),
        reason: format!("Failed to create table: {}", e),
        suggestion: "Verify table creation parameters".to_string(),
    })?;
    
    let normalized_df = lowercase_column_names(df).await?;
    
    println!("Enhanced DataFrame created for table: '{}'", alias);
    
    Ok(AliasedDataFrame {
        dataframe: normalized_df,
        alias: alias.to_string(),
    })
}

pub async fn load_xml_with_schema(
    file_path: &str,
    alias: &str,
    schema: ExtractionSchema
) -> ElusionResult<AliasedDataFrame> {
    println!("Extracting XML with schema: {}", schema.name);
    
    let analyzer = XmlAnalyzer::new(AnalyzerConfig::default());
    let root = analyzer.parse_xml(file_path)?;
    
    let extractor = SchemaExtractor::new(schema);
    let records = extractor.extract(&root)?;
    
    if records.is_empty() {
        return Err(ElusionError::InvalidOperation {
            operation: "XML Processing".to_string(),
            reason: "No records extracted from XML".to_string(),
            suggestion: "Check schema configuration and XML content".to_string(),
        });
    }
    
    println!("Extracted {} records", records.len());
    
    // Convert to DataFrame using existing logic
    let schema = infer_schema_from_records(&records, &HashSet::new());
    let record_batch = build_record_batch_from_records(&records, &schema)?;
    
    let ctx = SessionContext::new();
    let mem_table = MemTable::try_new(schema.into(), vec![vec![record_batch]])
        .map_err(|e| ElusionError::SchemaError {
            message: format!("Failed to create MemTable: {}", e),
            schema: None,
            suggestion: "Verify data types and schema compatibility".to_string(),
        })?;
    
    ctx.register_table(alias, Arc::new(mem_table))
        .map_err(|e| ElusionError::InvalidOperation {
            operation: "Table registration".to_string(),
            reason: format!("Failed to register table: {}", e),
            suggestion: "Try using a different alias".to_string(),
        })?;
    
    let df = ctx.table(alias).await.map_err(|e| ElusionError::InvalidOperation {
        operation: "Table creation".to_string(),
        reason: format!("Failed to create table: {}", e),
        suggestion: "Verify table creation parameters".to_string(),
    })?;
    
    let normalized_df = lowercase_column_names(df).await?;
    
    println!("DataFrame created for table: '{}'", alias);
    
    Ok(AliasedDataFrame {
        dataframe: normalized_df,
        alias: alias.to_string(),
    })
}

fn generate_element_fields(element_name: &str, analysis: &DetailedAnalysis) -> Vec<FieldMapping> {
    let mut fields = Vec::new();
    
    // Get all children of this element
    for (name, info) in &analysis.elements {
        if info.parents.contains(element_name) {
            if info.has_text_content {
                fields.push(FieldMapping {
                    xml_path: name.clone(),
                    output_name: to_snake_case(name),
                    required: false,
                    default_value: None,
                });
            }
        }
    }
    
    // Add attributes of the element itself
    if let Some(info) = analysis.elements.get(element_name) {
        for (attr_name, _) in &info.sample_attributes {
            fields.push(FieldMapping {
                xml_path: format!("@{}", attr_name),
                output_name: to_snake_case(attr_name),
                required: false,
                default_value: None,
            });
        }
        
        // Add element text if it has any
        if info.has_text_content {
            fields.push(FieldMapping {
                xml_path: ".".to_string(),
                output_name: format!("{}_text", to_snake_case(element_name)),
                required: false,
                default_value: None,
            });
        }
    }
    
    // Fallback: create a basic field
    if fields.is_empty() {
        fields.push(FieldMapping {
            xml_path: ".".to_string(),
            output_name: to_snake_case(element_name),
            required: false,
            default_value: Some("".to_string()),
        });
    }
    
    fields
}


fn create_fallback_schema() -> ExtractionSchema {
    println!("Using fallback schema");
    ExtractionSchema {
        name: "FallbackSchema".to_string(),
        primary_element: "root".to_string(),
        detail_element: None,
        header_fields: vec![
            FieldMapping {
                xml_path: ".".to_string(),
                output_name: "content".to_string(),
                required: false,
                default_value: Some("".to_string()),
            }
        ],
        detail_fields: vec![],
    }
}

fn create_dynamic_schema(analysis: &DetailedAnalysis) -> ExtractionSchema {
    println!("Creating dynamic schema from analysis...");
    
    if let Some(mv) = analysis.multiple_value_elements.first() {
        let header_fields = generate_header_fields(&mv.parent, analysis);
        let detail_fields = generate_detail_fields(&mv.child, analysis);
        
        println!("Header fields generated: {}", header_fields.len());
        println!("Detail fields generated: {}", detail_fields.len());
        
        ExtractionSchema {
            name: "DynamicSchema".to_string(),
            primary_element: mv.parent.clone(),
            detail_element: Some(mv.child.clone()),
            header_fields,
            detail_fields,
        }
    } else {
        // Fallback to most frequent element
        let mut sorted_elements: Vec<_> = analysis.elements.iter().collect();
        sorted_elements.sort_by(|a, b| b.1.total_count.cmp(&a.1.total_count));
        
        if let Some((element_name, _)) = sorted_elements.first() {
            let fields = generate_element_fields(element_name, analysis);
            println!("Fallback fields generated: {}", fields.len());
            
            ExtractionSchema {
                name: "DynamicSchema".to_string(),
                primary_element: element_name.to_string(),
                detail_element: None,
                header_fields: fields,
                detail_fields: vec![],
            }
        } else {
            create_fallback_schema()
        }
    }
}

fn extract_relative_path(full_path: &str, parent_element: &str, target_element: &str) -> Option<String> {
    let parts: Vec<&str> = full_path.split('/').collect();
    
    // Find the parent element in the path
    if let Some(parent_index) = parts.iter().position(|&part| part == parent_element) {
        // Get everything after the parent element
        let remaining_parts: Vec<&str> = parts.iter().skip(parent_index + 1).copied().collect();
        
        if remaining_parts.is_empty() {
            // The target is the parent itself
            Some(".".to_string())
        } else if remaining_parts.len() == 1 && remaining_parts[0] == target_element {
            // Direct child
            Some(target_element.to_string())
        } else {
            // Nested path
            Some(remaining_parts.join("/"))
        }
    } else {
        // Fallback to simple element name
        Some(target_element.to_string())
    }
}


fn generate_header_fields(element_name: &str, analysis: &DetailedAnalysis) -> Vec<FieldMapping> {
    let mut fields = Vec::new();
    
    // Strategy 1: Find ALL descendants (not just direct children) with text content
    for (name, info) in &analysis.elements {
        // Check if this element has the target element in its path
        if info.sample_paths.iter().any(|path| path.contains(element_name)) && info.has_text_content {
            // Create a path relative to the target element
            if let Some(sample_path) = info.sample_paths.first() {
                if let Some(relative_path) = extract_relative_path(sample_path, element_name, name) {
                    fields.push(FieldMapping {
                        xml_path: relative_path,
                        output_name: to_snake_case(name),
                         required: false,
                        default_value: None,
                    });
                }
            }
        }
    }
    
    // Strategy 2: If we found too few fields, also include direct children attributes
    if fields.len() < 3 {
        for (name, info) in &analysis.elements {
            if info.parents.contains(element_name) {
                // Add child elements even without text content
                fields.push(FieldMapping {
                    xml_path: name.clone(),
                    output_name: to_snake_case(name),
                     required: false,
                    default_value: Some("".to_string()),
                });
                
                // Add attributes of child elements
                for (attr_name, _) in &info.sample_attributes {
                    fields.push(FieldMapping {
                        xml_path: format!("{}/@{}", name, attr_name),
                        output_name: format!("{}_{}", to_snake_case(name), to_snake_case(attr_name)),
                         required: false,
                        default_value: None,
                    });
                }
            }
        }
    }
    
    // Strategy 3: Add attributes of the element itself
    if let Some(info) = analysis.elements.get(element_name) {
        for (attr_name, _) in &info.sample_attributes {
            fields.push(FieldMapping {
                xml_path: format!("@{}", attr_name),
                output_name: to_snake_case(attr_name),
                 required: false,
                default_value: None,
            });
        }
    }
    
    // Strategy 4: If still empty, add the element's text content
    if fields.is_empty() {
        fields.push(FieldMapping {
            xml_path: ".".to_string(),
            output_name: to_snake_case(element_name),
             required: false,
            default_value: None,
        });
    }
    
    // Remove duplicates
    fields.sort_by(|a, b| a.output_name.cmp(&b.output_name));
    fields.dedup_by(|a, b| a.output_name == b.output_name);
    
    println!("Header fields for {}: {:?}", element_name, fields.iter().map(|f| &f.output_name).collect::<Vec<_>>());
    fields
}

fn generate_detail_fields(element_name: &str, analysis: &DetailedAnalysis) -> Vec<FieldMapping> {
    let mut fields = Vec::new();
    
    // Strategy 1: Find ALL descendants with text content
    for (name, info) in &analysis.elements {
        if info.sample_paths.iter().any(|path| path.contains(element_name)) && info.has_text_content {
            if let Some(sample_path) = info.sample_paths.first() {
                if let Some(relative_path) = extract_relative_path(sample_path, element_name, name) {
                    fields.push(FieldMapping {
                        xml_path: relative_path,
                        output_name: to_snake_case(name),
                         required: false,
                        default_value: None,
                    });
                }
            }
        }
    }
    
    // Strategy 2: Add direct children and their attributes
    for (name, info) in &analysis.elements {
        if info.parents.contains(element_name) {
            // Add child elements
            fields.push(FieldMapping {
                xml_path: name.clone(),
                output_name: to_snake_case(name),
                 required: false,
                default_value: Some("".to_string()),
            });
            
            // Add attributes of child elements
            for (attr_name, _) in &info.sample_attributes {
                fields.push(FieldMapping {
                    xml_path: format!("{}/@{}", name, attr_name),
                    output_name: format!("{}_{}", to_snake_case(name), to_snake_case(attr_name)),
                     required: false,
                    default_value: None,
                });
            }
        }
    }
    
    // Strategy 3: Add attributes of the detail element itself
    if let Some(info) = analysis.elements.get(element_name) {
        for (attr_name, _) in &info.sample_attributes {
            fields.push(FieldMapping {
                xml_path: format!("@{}", attr_name),
                output_name: format!("{}_attr", to_snake_case(attr_name)),
                 required: false,
                default_value: None,
            });
        }
    }
    
    // Strategy 4: Add the element's own text content if it has any
    if let Some(info) = analysis.elements.get(element_name) {
        if info.has_text_content {
            fields.push(FieldMapping {
                xml_path: ".".to_string(),
                output_name: to_snake_case(element_name),
                 required: false,
                default_value: None,
            });
        }
    }
    
    // Always ensure we have at least one field
    if fields.is_empty() {
        fields.push(FieldMapping {
            xml_path: ".".to_string(),
            output_name: "detail_content".to_string(),
             required: false,
            default_value: Some("".to_string()),
        });
    }
    
    // Remove duplicates
    fields.sort_by(|a, b| a.output_name.cmp(&b.output_name));
    fields.dedup_by(|a, b| a.output_name == b.output_name);
    
    println!("Detail fields for {}: {:?}", element_name, fields.iter().map(|f| &f.output_name).collect::<Vec<_>>());
    fields
}

fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    let mut prev_was_upper = false;
    
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 && !prev_was_upper {
                result.push('_');
            }
            result.push(c.to_lowercase().next().unwrap());
            prev_was_upper = true;
        } else {
            result.push(c);
            prev_was_upper = false;
        }
    }
    
    result.to_lowercase()
}


fn infer_schema_from_records(records: &[HashMap<String, String>], _unique_paths: &HashSet<String>) -> Schema {
    let mut all_columns = HashSet::new();
    
    for record in records {
        for key in record.keys() {
            all_columns.insert(key.clone());
        }
    }
    
    // Ensure we have at least one column
    if all_columns.is_empty() {
        all_columns.insert("default_column".to_string());
    }
    
    let mut fields = Vec::new();
    for column in all_columns {
        fields.push(Field::new(column, ArrowDataType::Utf8, true));
    }
    
    fields.sort_by(|a, b| a.name().cmp(b.name()));
    Schema::new(fields)
}

fn build_record_batch_from_records(
    records: &[HashMap<String, String>], 
    schema: &Schema
) -> ElusionResult<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::new();
    
    for field in schema.fields() {
        let column_name = field.name();
        let mut values = Vec::new();
        
        for record in records {
            let value = record.get(column_name)
                .map(|v| v.as_str())
                .unwrap_or("");
            values.push(value);
        }
        
        let array = StringArray::from(values);
        columns.push(Arc::new(array) as ArrayRef);
    }
    
    RecordBatch::try_new(Arc::new(schema.clone()), columns)
        .map_err(|e| ElusionError::SchemaError {
            message: format!("Failed to create RecordBatch: {}", e),
            schema: Some(schema.to_string()),
            suggestion: "Check data consistency and schema compatibility".to_string(),
        })
}


// ============ AUTO mode detection
#[derive(Debug, Clone, Copy)]
pub enum XmlProcessingMode {
    Standard,
    Cartesian,
    Auto,
}

impl Default for XmlProcessingMode {
    fn default() -> Self {
        XmlProcessingMode::Auto
    }
}


 /// Unified LOAD function for XML files with configurable processing mode
    pub fn load_xml_with_mode<'a>(
        file_path: &'a str, 
        alias: &'a str, 
        mode: XmlProcessingMode
    ) -> BoxFuture<'a, ElusionResult<AliasedDataFrame>> {
        Box::pin(async move {
            let config = XmlParseConfig {
                processing_mode: mode,
                ..Default::default()
            };
            load_xml_unified(file_path, alias, Some(config)).await
        })
    }

    pub async fn load_xml_unified(
        file_path: &str,
        alias: &str,
        config: Option<XmlParseConfig>
    ) -> ElusionResult<AliasedDataFrame> {
        let config = config.unwrap_or_default();
        
        let processing_mode = match config.processing_mode {
            XmlProcessingMode::Auto => {
                // Analyze structure to determine best approach
                determine_optimal_processing_mode(file_path, &config).await?
            },
            mode => mode,
        };
        
        println!("Using XML processing mode: {:?}", processing_mode);
        
        match processing_mode {
            XmlProcessingMode::Standard => {
                load_xml_intelligent(file_path, alias, Some(config)).await
            },
            XmlProcessingMode::Cartesian => {
                load_xml_intelligent_cartesian(file_path, alias, Some(config)).await  
            },
            XmlProcessingMode::Auto => {
                 load_xml_intelligent(file_path, alias, Some(config)).await
            }
        }
    }

async fn determine_optimal_processing_mode(
    file_path: &str,
    config: &XmlParseConfig
) -> ElusionResult<XmlProcessingMode> {
    println!("Analyzing XML structure to determine optimal processing mode...");
    
    let analyzer_config = AnalyzerConfig {
        max_sample_text: 3,
        max_sample_attributes: 3,
        max_depth: config.max_depth,
        include_empty_fields: config.include_empty_fields,
    };
    
    let analyzer = XmlAnalyzer::new(analyzer_config);
    let root = analyzer.parse_xml(file_path)?;
    let analysis = analyzer.analyze_structure(&root);
    
    println!("XML Analysis Summary:");
    println!("  Total elements: {}", analysis.total_elements);
    println!("  Element types: {}", analysis.unique_element_types);
    println!("  Multiple value relationships: {}", analysis.multiple_value_elements.len());
    
    // Rule 1: Single parent with single child type = Header-Detail (Sales Order pattern)
    if analysis.multiple_value_elements.len() == 1 {
        let mv = &analysis.multiple_value_elements[0];
        println!("  Single relationship: {} -> {} (max: {}, avg: {:.1})", 
            mv.parent, mv.child, mv.max_count, mv.avg_count);
        
        if mv.max_count > 1 {
            println!("Detected simple header-detail structure, using Standard processing");
            return Ok(XmlProcessingMode::Standard);
        }
    }
    
    // Rule 2: Same parent with multiple child types = Potential Cartesian
    if analysis.multiple_value_elements.len() >= 2 {
        let unique_parents: HashSet<String> = analysis.multiple_value_elements
            .iter()
            .map(|mv| mv.parent.clone())
            .collect();
        
        if unique_parents.len() == 1 {
            let parent = unique_parents.iter().next().unwrap();
            println!("  Single parent '{}' has {} different child types", parent, analysis.multiple_value_elements.len());
            
            // Check if each child type appears multiple times
            let all_have_multiples = analysis.multiple_value_elements
                .iter()
                .all(|mv| mv.max_count > 1);
            
            if all_have_multiples {
                println!("Detected multiple relationship types with same parent, using Cartesian processing");
                return Ok(XmlProcessingMode::Cartesian);
            }
        }
    }
    
    // Default to Standard for everything else
    println!("Using Standard processing as default");
    Ok(XmlProcessingMode::Standard)
}


// =========== TESTING

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_auto_detection_logic() {
        // Test 1: Simple Sales Order (should choose Standard)
        let sales_order_xml = r#"
        <SalesOrders>
            <SalesOrder>
                <OrderNumber>SO001</OrderNumber>
                <CustomerNumber>CUST001</CustomerNumber>
                <Items>
                    <Item>
                        <ItemNumber>1</ItemNumber>
                        <ProductNumber>PROD001</ProductNumber>
                        <Quantity>10</Quantity>
                    </Item>
                    <Item>
                        <ItemNumber>2</ItemNumber>
                        <ProductNumber>PROD002</ProductNumber>
                        <Quantity>5</Quantity>
                    </Item>
                </Items>
            </SalesOrder>
        </SalesOrders>
        "#;

        let temp_dir = tempdir().unwrap();
        let sales_order_path = temp_dir.path().join("sales_order.xml");
        fs::write(&sales_order_path, sales_order_xml).unwrap();

        let config = XmlParseConfig::default();
        let mode = determine_optimal_processing_mode(sales_order_path.to_str().unwrap(), &config).await.unwrap();
        
        println!("Sales Order detected mode: {:?}", mode);
        // Should be Standard because it's a simple one-to-many (order -> items)

        // Test 2: Customer with multiple relationship types (should choose Cartesian)
        let customer_xml = r#"
        <Customers>
            <Customer>
                <CustomerNumber>CUST001</CustomerNumber>
                <Name>Acme Corp</Name>
                <Addresses>
                    <Address>
                        <Type>Billing</Type>
                        <Street>123 Main St</Street>
                        <City>New York</City>
                    </Address>
                    <Address>
                        <Type>Shipping</Type>
                        <Street>456 Oak Ave</Street>
                        <City>Boston</City>
                    </Address>
                </Addresses>
                <Contacts>
                    <Contact>
                        <Type>Primary</Type>
                        <Name>John Doe</Name>
                        <Email>john@acme.com</Email>
                    </Contact>
                    <Contact>
                        <Type>Accounting</Type>
                        <Name>Jane Smith</Name>
                        <Email>jane@acme.com</Email>
                    </Contact>
                </Contacts>
                <PhoneNumbers>
                    <Phone>
                        <Type>Office</Type>
                        <Number>555-1234</Number>
                    </Phone>
                    <Phone>
                        <Type>Mobile</Type>
                        <Number>555-5678</Number>
                    </Phone>
                </PhoneNumbers>
            </Customer>
        </Customers>
        "#;

        let customer_path = temp_dir.path().join("customer.xml");
        fs::write(&customer_path, customer_xml).unwrap();

        let mode = determine_optimal_processing_mode(customer_path.to_str().unwrap(), &config).await.unwrap();
        
        println!("Customer detected mode: {:?}", mode);

        // Test 3: Simple structure (should choose Standard)
        let simple_xml = r#"
        <Products>
            <Product>
                <ProductNumber>PROD001</ProductNumber>
                <Name>Widget A</Name>
                <Price>10.99</Price>
            </Product>
            <Product>
                <ProductNumber>PROD002</ProductNumber>
                <Name>Widget B</Name>
                <Price>15.99</Price>
            </Product>
        </Products>
        "#;

        let simple_path = temp_dir.path().join("simple.xml");
        fs::write(&simple_path, simple_xml).unwrap();

        let mode = determine_optimal_processing_mode(simple_path.to_str().unwrap(), &config).await.unwrap();
        
        println!("Simple structure detected mode: {:?}", mode);
    }

    #[tokio::test]
    async fn test_auto_detection_full_workflow() {
        // Create test XML files
        let temp_dir = tempdir().unwrap();

        // Sales Order XML (should use Standard processing)
        let sales_order_xml = r#"
        <SalesOrders>
            <SalesOrder>
                <OrderNumber>SO001</OrderNumber>
                <CustomerNumber>CUST001</CustomerNumber>
                <OrderDate>2024-01-15</OrderDate>
                <Items>
                    <Item>
                        <ItemNumber>1</ItemNumber>
                        <ProductNumber>PROD001</ProductNumber>
                        <Quantity>10</Quantity>
                        <UnitPrice>5.99</UnitPrice>
                    </Item>
                    <Item>
                        <ItemNumber>2</ItemNumber>
                        <ProductNumber>PROD002</ProductNumber>
                        <Quantity>5</Quantity>
                        <UnitPrice>12.50</UnitPrice>
                    </Item>
                </Items>
            </SalesOrder>
            <SalesOrder>
                <OrderNumber>SO002</OrderNumber>
                <CustomerNumber>CUST002</CustomerNumber>
                <OrderDate>2024-01-16</OrderDate>
                <Items>
                    <Item>
                        <ItemNumber>1</ItemNumber>
                        <ProductNumber>PROD003</ProductNumber>
                        <Quantity>3</Quantity>
                        <UnitPrice>25.00</UnitPrice>
                    </Item>
                </Items>
            </SalesOrder>
        </SalesOrders>
        "#;

        let sales_order_path = temp_dir.path().join("sales_orders.xml");
        fs::write(&sales_order_path, sales_order_xml).unwrap();

        println!("\n=== Testing Sales Order Auto-Detection ===");
        let result = load_xml_with_mode(
            sales_order_path.to_str().unwrap(),
            "sales_test",
            XmlProcessingMode::Auto
        ).await;

        match result {
            Ok(df) => {
                println!("✅ Sales Order processing successful");
                println!("DataFrame alias: {}", df.alias);
            },
            Err(e) => {
                println!("❌ Sales Order processing failed: {:?}", e);
            }
        }

        // Customer XML with multiple relationship types (should use Cartesian)
        let customer_xml = r#"
        <Customers>
            <Customer>
                <CustomerNumber>CUST001</CustomerNumber>
                <Name>Acme Corp</Name>
                <Addresses>
                    <Address>
                        <Type>Billing</Type>
                        <Street>123 Main St</Street>
                        <City>New York</City>
                        <State>NY</State>
                    </Address>
                    <Address>
                        <Type>Shipping</Type>
                        <Street>456 Oak Ave</Street>
                        <City>Boston</City>
                        <State>MA</State>
                    </Address>
                </Addresses>
                <Contacts>
                    <Contact>
                        <Type>Primary</Type>
                        <Name>John Doe</Name>
                        <Email>john@acme.com</Email>
                        <Phone>555-1234</Phone>
                    </Contact>
                    <Contact>
                        <Type>Accounting</Type>
                        <Name>Jane Smith</Name>
                        <Email>jane@acme.com</Email>
                        <Phone>555-5678</Phone>
                    </Contact>
                </Contacts>
            </Customer>
        </Customers>
        "#;

        let customer_path = temp_dir.path().join("customers.xml");
        fs::write(&customer_path, customer_xml).unwrap();

        println!("\n=== Testing Customer Auto-Detection ===");
        let result = load_xml_with_mode(
            customer_path.to_str().unwrap(),
            "customer_test",
            XmlProcessingMode::Auto
        ).await;

        match result {
            Ok(df) => {
                println!("✅ Customer processing successful");
                println!("DataFrame alias: {}", df.alias);
            },
            Err(e) => {
                println!("❌ Customer processing failed: {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_detection_scenarios() {
        let temp_dir = tempdir().unwrap();
        let config = XmlParseConfig::default();

        // Scenario 1: Multiple repeating groups (should trigger Cartesian)
        let multi_group_xml = r#"
        <Suppliers>
            <Supplier>
                <SupplierID>SUP001</SupplierID>
                <Name>Tech Supplies Inc</Name>
                <Locations>
                    <Location>
                        <Type>Warehouse</Type>
                        <Address>Factory District</Address>
                    </Location>
                    <Location>
                        <Type>Office</Type>
                        <Address>Business Center</Address>
                    </Location>
                </Locations>
                <BankAccounts>
                    <Account>
                        <Type>Primary</Type>
                        <BankName>First Bank</BankName>
                        <AccountNumber>12345</AccountNumber>
                    </Account>
                    <Account>
                        <Type>Secondary</Type>
                        <BankName>Second Bank</BankName>
                        <AccountNumber>67890</AccountNumber>
                    </Account>
                </BankAccounts>
                <ContactPersons>
                    <Person>
                        <Role>Manager</Role>
                        <Name>Alice Johnson</Name>
                    </Person>
                    <Person>
                        <Role>Accountant</Role>
                        <Name>Bob Wilson</Name>
                    </Person>
                </ContactPersons>
            </Supplier>
        </Suppliers>
        "#;

        let multi_path = temp_dir.path().join("multi_group.xml");
        fs::write(&multi_path, multi_group_xml).unwrap();

        let mode = determine_optimal_processing_mode(multi_path.to_str().unwrap(), &config).await.unwrap();
        println!("Multi-group structure mode: {:?}", mode);
        
        // Scenario 2: Simple one-to-many (should be Standard)
        let simple_one_to_many = r#"
        <Orders>
            <Order>
                <OrderID>ORD001</OrderID>
                <Date>2024-01-15</Date>
                <LineItem>
                    <Product>ProductA</Product>
                    <Quantity>10</Quantity>
                </LineItem>
                <LineItem>
                    <Product>ProductB</Product>
                    <Quantity>5</Quantity>
                </LineItem>
                <LineItem>
                    <Product>ProductC</Product>
                    <Quantity>3</Quantity>
                </LineItem>
            </Order>
        </Orders>
        "#;

        let simple_path = temp_dir.path().join("simple_one_to_many.xml");
        fs::write(&simple_path, simple_one_to_many).unwrap();

        let mode = determine_optimal_processing_mode(simple_path.to_str().unwrap(), &config).await.unwrap();
        println!("Simple one-to-many mode: {:?}", mode);

        // Scenario 3: Flat structure (should be Standard)
        let flat_xml = r#"
        <Inventory>
            <Item>
                <SKU>SKU001</SKU>
                <Name>Widget</Name>
                <Stock>100</Stock>
            </Item>
            <Item>
                <SKU>SKU002</SKU>
                <Name>Gadget</Name>
                <Stock>50</Stock>
            </Item>
        </Inventory>
        "#;

        let flat_path = temp_dir.path().join("flat.xml");
        fs::write(&flat_path, flat_xml).unwrap();

        let mode = determine_optimal_processing_mode(flat_path.to_str().unwrap(), &config).await.unwrap();
        println!("Flat structure mode: {:?}", mode);
    }

    #[tokio::test]
    async fn test_real_sales_order() {
        let sales_order_path = "C:\\Borivoj\\RUST\\Elusion\\SalesOrder_26052027.xml";
        
        if std::path::Path::new(sales_order_path).exists() {
            println!("\n=== Testing Real Sales Order File ===");
            
            let config = XmlParseConfig::default();
            let mode = determine_optimal_processing_mode(sales_order_path, &config).await.unwrap();
            println!("Real sales order detected mode: {:?}", mode);

            let result = load_xml_with_mode(sales_order_path, "real_sales", XmlProcessingMode::Auto).await;
            
            match result {
                Ok(df) => {
                    println!("✅ Real sales order processing successful");
                    println!("DataFrame alias: {}", df.alias);
                },
                Err(e) => {
                    println!("❌ Real sales order processing failed: {:?}", e);
                }
            }
        } else {
            println!("Real sales order file not found, skipping test");
        }
    }

    #[tokio::test]
    async fn test_auto_detection_performance() {
        let temp_dir = tempdir().unwrap();
  
        let mut large_xml = String::from("<LargeDataset>");
        
        for i in 1..=100 {
            large_xml.push_str(&format!(r#"
                <Record>
                    <ID>{}</ID>
                    <Name>Record {}</Name>
                    <Details>
                        <Detail>
                            <Type>Type1</Type>
                            <Value>Value{}</Value>
                        </Detail>
                        <Detail>
                            <Type>Type2</Type>
                            <Value>Value{}</Value>
                        </Detail>
                    </Details>
                </Record>
            "#, i, i, i, i + 100));
        }
        
        large_xml.push_str("</LargeDataset>");
        
        let large_path = temp_dir.path().join("large.xml");
        fs::write(&large_path, large_xml).unwrap();

        let start = std::time::Instant::now();
        let config = XmlParseConfig::default();
        let mode = determine_optimal_processing_mode(large_path.to_str().unwrap(), &config).await.unwrap();
        let duration = start.elapsed();
        
        println!("Large file ({} records) analysis took: {:?}", 100, duration);
        println!("Detected mode: {:?}", mode);
        
        assert!(duration.as_secs() < 5, "Analysis took too long: {:?}", duration);
    }
}