use crate::custom_error::cust_error::{ElusionError, ElusionResult};

/// Resolves a value that might be an env var name or a literal string.
/// If the value matches an existing env var, returns that.
/// Otherwise returns the value as-is (literal string).
/// Checks system env vars first, then .env file via dotenvy.
pub fn resolve_env_value(value: &str) -> ElusionResult<String> {
    // Load .env file if present - dotenvy is silent if .env doesn't exist
    let _ = dotenvy::dotenv();

    // Try system env var first
    match std::env::var(value) {
        Ok(resolved) => {
            println!("🔑 Resolved env var: {}", value);
            Ok(resolved)
        }
        Err(_) => {
            // Not an env var - treat as literal value
            Ok(value.to_string())
        }
    }
}

/// Resolve and fail fast if value is empty after resolution
pub fn resolve_required(key: &str, value: &str) -> ElusionResult<String> {
    let resolved = resolve_env_value(value)?;
    if resolved.trim().is_empty() {
        return Err(ElusionError::Custom(format!(
            "❌ Required config value '{}' is empty. Set it as an env var or in your .env file.",
            key
        )));
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_literal_value_returned_as_is() {
        let result = resolve_env_value("some_literal_value").unwrap();
        assert_eq!(result, "some_literal_value");
    }

    #[test]
    fn test_env_var_resolved() {
        std::env::set_var("TEST_ELUSION_KEY", "my_secret_value");
        let result = resolve_env_value("TEST_ELUSION_KEY").unwrap();
        assert_eq!(result, "my_secret_value");
        std::env::remove_var("TEST_ELUSION_KEY");
    }

    #[test]
    fn test_required_fails_on_empty_string() {
        let result = resolve_required("my_key", "");
        assert!(result.is_err());
    }

    #[test]
    fn test_required_returns_value_when_set() {
        std::env::set_var("TEST_REQUIRED_KEY", "real_value");
        let result = resolve_required("TEST_REQUIRED_KEY", "TEST_REQUIRED_KEY").unwrap();
        assert_eq!(result, "real_value");
        std::env::remove_var("TEST_REQUIRED_KEY");
    }
}