//! Configuration system for MLQL server
//!
//! Loads configuration from:
//! 1. config.yaml - operational settings (port, logging, execution mode)
//! 2. .env file - secrets (API keys)
//!
//! Environment variables always override config.yaml values.

use serde::{Deserialize, Serialize};
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Failed to read config file: {0}")]
    Io(#[from] std::io::Error),

    #[error("Failed to parse YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("Missing required environment variable: {0}")]
    MissingEnvVar(String),
}

/// Server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8080,
        }
    }
}

/// Execution configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    /// Execution mode: "sql" or "substrait"
    pub mode: String,

    /// Path to Substrait extension (only needed when mode = "substrait")
    #[serde(default)]
    pub substrait_extension_path: Option<String>,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            mode: "sql".to_string(),
            substrait_extension_path: None,
        }
    }
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level (trace, debug, info, warn, error) or module-specific
    pub level: String,

    /// Output format: pretty, json, compact
    pub format: String,

    /// Output destination: stdout, file, both
    pub output: String,

    /// Directory for log files
    pub directory: String,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            format: "pretty".to_string(),
            output: "stdout".to_string(),
            directory: "./logs".to_string(),
        }
    }
}

/// Main application configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub execution: ExecutionConfig,
    pub logging: LoggingConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            execution: ExecutionConfig::default(),
            logging: LoggingConfig::default(),
        }
    }
}

impl Config {
    /// Load configuration from YAML file with environment variable overrides
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        // Read and parse YAML file
        let contents = std::fs::read_to_string(path)?;
        let mut config: Config = serde_yaml::from_str(&contents)?;

        // Override with environment variables if present
        if let Ok(host) = std::env::var("MLQL_SERVER_HOST") {
            config.server.host = host;
        }
        if let Ok(port) = std::env::var("MLQL_SERVER_PORT") {
            if let Ok(port_num) = port.parse() {
                config.server.port = port_num;
            }
        }

        if let Ok(mode) = std::env::var("MLQL_EXECUTION_MODE") {
            config.execution.mode = mode;
        }
        if let Ok(path) = std::env::var("SUBSTRAIT_EXTENSION_PATH") {
            config.execution.substrait_extension_path = Some(path);
        }

        if let Ok(level) = std::env::var("RUST_LOG") {
            config.logging.level = level;
        }
        if let Ok(format) = std::env::var("LOG_FORMAT") {
            config.logging.format = format;
        }
        if let Ok(output) = std::env::var("LOG_OUTPUT") {
            config.logging.output = output;
        }
        if let Ok(dir) = std::env::var("LOG_DIR") {
            config.logging.directory = dir;
        }

        Ok(config)
    }

    /// Get OpenAI API key from environment (must be in .env)
    pub fn get_openai_api_key() -> Result<String, ConfigError> {
        std::env::var("OPENAI_API_KEY")
            .map_err(|_| ConfigError::MissingEnvVar("OPENAI_API_KEY".to_string()))
    }

    /// Set logging environment variables for the logging module
    pub fn apply_logging_env(&self) {
        std::env::set_var("RUST_LOG", &self.logging.level);
        std::env::set_var("LOG_FORMAT", &self.logging.format);
        std::env::set_var("LOG_OUTPUT", &self.logging.output);
        std::env::set_var("LOG_DIR", &self.logging.directory);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.server.host, "127.0.0.1");
        assert_eq!(config.server.port, 8080);
        assert_eq!(config.execution.mode, "sql");
        assert_eq!(config.logging.level, "info");
        assert_eq!(config.logging.format, "pretty");
        assert_eq!(config.logging.output, "stdout");
    }

    #[test]
    fn test_env_var_override() {
        std::env::set_var("MLQL_SERVER_PORT", "9090");
        std::env::set_var("MLQL_EXECUTION_MODE", "substrait");

        // Create a temp config file
        let config_yaml = r#"
server:
  host: "127.0.0.1"
  port: 8080
execution:
  mode: "sql"
logging:
  level: "info"
  format: "pretty"
  output: "stdout"
  directory: "./logs"
"#;
        let temp_file = std::env::temp_dir().join("test_config.yaml");
        std::fs::write(&temp_file, config_yaml).unwrap();

        let config = Config::load(&temp_file).unwrap();
        assert_eq!(config.server.port, 9090); // Overridden
        assert_eq!(config.execution.mode, "substrait"); // Overridden

        std::env::remove_var("MLQL_SERVER_PORT");
        std::env::remove_var("MLQL_EXECUTION_MODE");
        std::fs::remove_file(temp_file).ok();
    }
}
