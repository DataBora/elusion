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
}

impl Default for XmlParseConfig {
    fn default() -> Self {
        Self {
            max_depth: 20,
            include_empty_fields: false,
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