use crate::prelude::*;

// ================ ELUSION API
#[derive(Clone)]
pub struct ElusionApi;

#[cfg(feature = "api")]
enum JsonType {
    Array,
    Object,
}

#[cfg(feature = "api")]
fn validate_https_url(url: &str) -> ElusionResult<()> {
    if !url.starts_with("https://") {
        return Err(ElusionError::Custom("URL must start with 'https://'".to_string()));
    }
    Ok(())
}

impl ElusionApi{

    pub fn new () -> Self {
        Self
    }

/// Create a JSON from a REST API endpoint that returns JSON
#[cfg(feature = "api")]
pub async fn from_api(
    &self,  
    url: &str,
    file_path: &str
) -> ElusionResult<()> {
    validate_https_url(url)?;
    let client = Client::new();
    let response = client.get(url)
        .send()
        .await
        .map_err(|e| ElusionError::Custom(format!("❌ HTTP request failed: {}", e)))?;
    
    let content = response.bytes()
        .await
        .map_err(|e| ElusionError::Custom(format!("Failed to get response content: {}", e)))?;

    println!("Generated URL: {}", url);

    Self::save_json_to_file(content, file_path).await
}

#[cfg(not(feature = "api"))]
pub async fn from_api(
    &self,  
    _url: &str,
    _file_path: &str
) -> ElusionResult<()> {
    Err(ElusionError::Custom("*** Warning ***: API feature not enabled. Add feature under [dependencies]".to_string()))
}

/// Create a JSON from a REST API endpoint with custom headers
#[cfg(feature = "api")]
pub async fn from_api_with_headers(
    &self,
    url: &str, 
    headers: HashMap<String, String>,
    file_path: &str
) -> ElusionResult<()> {
    validate_https_url(url)?;
    let client = Client::new();
    let mut request = client.get(url);
    
    for (key, value) in headers {
        request = request.header(&key, value);
    }
    
    let response = request
        .send()
        .await
        .map_err(|e| ElusionError::Custom(format!("❌ HTTP request failed: {}", e)))?;

    let content = response.bytes()
        .await
        .map_err(|e| ElusionError::Custom(format!("Failed to get response content: {}", e)))?;
    println!("Generated URL: {}", url);

    Self::save_json_to_file(content, file_path).await
}
#[cfg(not(feature = "api"))]
    pub async fn from_api_with_headers(
        &self,
        _url: &str, 
        _headers: HashMap<String, String>,
        _file_path: &str
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom("*** Warning ***: API feature not enabled. Add feature under [dependencies]".to_string()))
    }

/// Create JSON from API with custom query parameters
#[cfg(feature = "api")]
pub async fn from_api_with_params(
    &self,
    base_url: &str, 
    params: HashMap<&str, &str>,
    file_path: &str
) -> ElusionResult<()> {
    validate_https_url(base_url)?;

    if params.is_empty() {
        return Self::from_api( &self, base_url, file_path).await;
    }

    let query_string: String = params
        .iter()
        .map(|(k, v)| {
            // Only encode if the value contains a space
            if v.contains(' ') {
                format!("{}={}", k, v)
            } else {
                format!("{}={}", urlencoding::encode(k), urlencoding::encode(v))
            }
        })
        .collect::<Vec<String>>()
        .join("&");

    let url = format!("{}?{}", base_url, query_string);

    Self::from_api( &self, &url, file_path).await
}
#[cfg(not(feature = "api"))]
pub async fn from_api_with_params(
    &self,
    _base_url: &str, 
    _params: HashMap<&str, &str>,
    _file_path: &str
) -> ElusionResult<()> {
    Err(ElusionError::Custom("*** Warning ***: API feature not enabled. Add feature under [dependencies]".to_string()))
}

/// Create JSON from API with parameters and headers
#[cfg(feature = "api")]
pub async fn from_api_with_params_and_headers(
    &self,
    base_url: &str,
    params: HashMap<&str, &str>,
    headers: HashMap<String, String>,
    file_path: &str
) -> ElusionResult<()> {
    if params.is_empty() {
        return Self::from_api_with_headers( &self, base_url, headers, file_path).await;
    }

    let query_string: String = params
        .iter()
        .map(|(k, v)| {
            // Only encode if the key or value contains no spaces
            if !k.contains(' ') && !v.contains(' ') {
                format!("{}={}", 
                    urlencoding::encode(k), 
                    urlencoding::encode(v)
                )
            } else {
                format!("{}={}", k, v)
            }
        })
        .collect::<Vec<String>>()
        .join("&");

    let url = format!("{}?{}", base_url, query_string);

    Self::from_api_with_headers( &self, &url, headers, file_path).await
}

#[cfg(not(feature = "api"))]
    pub async fn from_api_with_params_and_headers(
        &self,
        _base_url: &str,
        _params: HashMap<&str, &str>,
        _headers: HashMap<String, String>,
        _file_path: &str
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom("*** Warning ***: API feature not enabled. Add feature under [dependencies]".to_string()))
    }

/// Create JSON from API with date range parameters
#[cfg(feature = "api")]
pub async fn from_api_with_dates(
    &self,
    base_url: &str, 
    from_date: &str, 
    to_date: &str,
    file_path: &str
) -> ElusionResult<()> {
    let url = format!("{}?from={}&to={}", 
        base_url,
        // Only encode if the date contains a space
        if from_date.contains(' ') { from_date.to_string() } else { urlencoding::encode(from_date).to_string() },
        if to_date.contains(' ') { to_date.to_string() } else { urlencoding::encode(to_date).to_string() }
    );

    Self::from_api( &self, &url, file_path).await
}
#[cfg(not(feature = "api"))]
pub async fn from_api_with_dates(
    &self,
    _base_url: &str, 
    _from_date: &str, 
    _to_date: &str,
    _file_path: &str
) -> ElusionResult<()> {
    Err(ElusionError::Custom("*** Warning ***: API feature not enabled. Add feature under [dependencies]".to_string()))
}

/// Create JSON from API with pagination
#[cfg(feature = "api")]
pub async fn from_api_with_pagination(
    &self,
    base_url: &str,
    page: u32,
    per_page: u32,
    file_path: &str
) -> ElusionResult<()> {
    let url = format!("{}?page={}&per_page={}", base_url, page, per_page);
 
    Self::from_api( &self, &url, file_path).await
}

#[cfg(not(feature = "api"))]
pub async fn from_api_with_pagination(
    &self,
    _base_url: &str,
    _page: u32,
    _per_page: u32,
    _file_path: &str
) -> ElusionResult<()> {
    Err(ElusionError::Custom("*** Warning ***: API feature not enabled. Add feature under [dependencies]".to_string()))
}

/// Create JSON from API with sorting
#[cfg(feature = "api")]
pub async fn from_api_with_sort(
    &self,
    base_url: &str,
    sort_field: &str,
    order: &str,
    file_path: &str
) -> ElusionResult<()> {
    let url = format!("{}?sort={}&order={}", 
        base_url,
        if sort_field.contains(' ') { sort_field.to_string() } else { urlencoding::encode(sort_field).to_string() },
        if order.contains(' ') { order.to_string() } else { urlencoding::encode(order).to_string() }
    );

    Self::from_api( &self, &url, file_path).await
}
#[cfg(not(feature = "api"))]
pub async fn from_api_with_sort(
    &self,
    _base_url: &str,
    _sort_field: &str,
    _order: &str,
    _file_path: &str
) -> ElusionResult<()> {
    Err(ElusionError::Custom("*** Warning ***: API feature not enabled. Add feature under [dependencies]".to_string()))
}

/// Create JSON from API with sorting and headers
#[cfg(feature = "api")]
pub async fn from_api_with_headers_and_sort(
    &self,
    base_url: &str,
    headers: HashMap<String, String>,
    sort_field: &str,
    order: &str,
    file_path: &str
) -> ElusionResult<()> {
    validate_https_url(base_url)?;
    
    let url = format!("{}?sort={}&order={}", 
        base_url,
        if sort_field.contains(' ') { sort_field.to_string() } else { urlencoding::encode(sort_field).to_string() },
        if order.contains(' ') { order.to_string() } else { urlencoding::encode(order).to_string() }
    );

    Self::from_api_with_headers(&self, &url, headers, file_path).await
}
#[cfg(not(feature = "api"))]
    pub async fn from_api_with_headers_and_sort(
        &self,
        _base_url: &str,
        _headers: HashMap<String, String>,
        _sort_field: &str,
        _order: &str,
        _file_path: &str
    ) -> ElusionResult<()> {
        Err(ElusionError::Custom("*** Warning ***: API feature not enabled. Add feature under [dependencies]".to_string()))
    }

/// Process JSON response into JSON 
#[cfg(feature = "api")]
async fn save_json_to_file(content: Bytes, file_path: &str) -> ElusionResult<()> {

    if content.is_empty() {
        return Err(ElusionError::InvalidOperation {
            operation: "JSON Processing".to_string(),
            reason: "Empty content provided".to_string(),
            suggestion: "💡 Ensure API response contains data".to_string(),
        });
    }

    let reader = std::io::BufReader::new(content.as_ref());
    let stream = serde_json::Deserializer::from_reader(reader).into_iter::<Value>();
    let mut stream = stream.peekable();

    let json_type = match stream.peek() {
        Some(Ok(Value::Array(_))) => JsonType::Array,
        Some(Ok(Value::Object(_))) => JsonType::Object,
        Some(Ok(_)) => return Err(ElusionError::InvalidOperation {
            operation: "JSON Validation".to_string(),
            reason: "Invalid JSON structure".to_string(),
            suggestion: "💡 JSON must be either an array or object at root level".to_string(),
        }),
        Some(Err(e)) => return Err(ElusionError::InvalidOperation {
            operation: "JSON Parsing".to_string(),
            reason: format!("JSON syntax error: {}", e),
            suggestion: "💡 Check if the JSON content is well-formed".to_string(),
        }),
        None => return Err(ElusionError::InvalidOperation {
            operation: "JSON Reading".to_string(),
            reason: "Empty or invalid JSON content".to_string(),
            suggestion: "💡 Verify the API response contains valid JSON data".to_string(),
        }),
    };

    // validating file path and create directories
    let path = LocalPath::new(file_path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| ElusionError::WriteError {
            path: parent.display().to_string(),
            operation: "Directory Creation".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Check directory permissions and path validity".to_string(),
        })?;
    }

    // Open file for writing
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(file_path)
        .map_err(|e| ElusionError::WriteError {
            path: file_path.to_string(),
            operation: "File Creation".to_string(),
            reason: e.to_string(),
            suggestion: "💡 Verify file path and write permissions".to_string(),
        })?;

    let mut writer = std::io::BufWriter::new(file);
    let mut first = true;
    let mut items_written = 0;

    match json_type {
        JsonType::Array => {
            writeln!(writer, "[").map_err(|e| ElusionError::WriteError {
                path: file_path.to_string(),
                operation: "Array Start".to_string(),
                reason: e.to_string(),
                suggestion: "💡 Check disk space and write permissions".to_string(),
            })?;

            for value in stream {
                match value {
                    Ok(Value::Array(array)) => {
                        for item in array {
                            if !first {
                                writeln!(writer, ",").map_err(|e| ElusionError::WriteError {
                                    path: file_path.to_string(),
                                    operation: "Array Separator".to_string(),
                                    reason: e.to_string(),
                                    suggestion: "💡 Check disk space and write permissions".to_string(),
                                })?;
                            }
                            first = false;
                            items_written += 1;

                            serde_json::to_writer_pretty(&mut writer, &item)
                                .map_err(|e| ElusionError::WriteError {
                                    path: file_path.to_string(),
                                    operation: format!("Write Array Item {}", items_written),
                                    reason: format!("JSON serialization error: {}", e),
                                    suggestion: "💡 Check if item contains valid JSON data".to_string(),
                                })?;
                        }
                    }
                    Ok(_) => continue,
                    Err(e) => return Err(ElusionError::InvalidOperation {
                        operation: "Array Processing".to_string(),
                        reason: format!("Failed to parse array item: {}", e),
                        suggestion: "💡 Verify JSON array structure is valid".to_string(),
                    }),
                }
            }
            writeln!(writer, "\n]").map_err(|e| ElusionError::WriteError {
                path: file_path.to_string(),
                operation: "Array End".to_string(),
                reason: e.to_string(),
                suggestion: "💡 Check disk space and write permissions".to_string(),
            })?;
        }
        JsonType::Object => {
            for value in stream {
                match value {
                    Ok(Value::Object(map)) => {
                        items_written += 1;
                        serde_json::to_writer_pretty(&mut writer, &Value::Object(map))
                            .map_err(|e| ElusionError::WriteError {
                                path: file_path.to_string(),
                                operation: format!("Write Object {}", items_written),
                                reason: format!("JSON serialization error: {}", e),
                                suggestion: "💡 Check if object contains valid JSON data".to_string(),
                            })?;
                    }
                    Ok(_) => return Err(ElusionError::InvalidOperation {
                        operation: "Object Processing".to_string(),
                        reason: "Non-object value in object stream".to_string(),
                        suggestion: "💡 Ensure all items are valid JSON objects".to_string(),
                    }),
                    Err(e) => return Err(ElusionError::InvalidOperation {
                        operation: "Object Processing".to_string(),
                        reason: format!("Failed to parse object: {}", e),
                        suggestion: "💡 Verify JSON object structure is valid".to_string(),
                    }),
                }
            }
        }
    }

    writer.flush().map_err(|e| ElusionError::WriteError {
        path: file_path.to_string(),
        operation: "File Finalization".to_string(),
        reason: e.to_string(),
        suggestion: "💡 Check disk space and write permissions".to_string(),
    })?;

    println!("✅ Successfully created {} with {} items", file_path, items_written);
    
    if items_written == 0 {
        println!("*** Warning ***: No items were written to the file. Check if this is expected.");
    }

    Ok(())

}



}


