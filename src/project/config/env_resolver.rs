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