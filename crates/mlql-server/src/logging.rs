//! Comprehensive structured logging system for MLQL server
//!
//! Features:
//! - Structured JSON logging for production
//! - Human-readable console logging for development
//! - File rotation with daily log files
//! - Request ID tracking
//! - Performance metrics
//! - Configurable log levels per module

use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};
use tracing_appender::rolling::{RollingFileAppender, Rotation};

/// Log format configuration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    /// Human-readable format for development
    Pretty,
    /// JSON format for production (structured logging)
    Json,
    /// Compact format for testing
    Compact,
}

impl LogFormat {
    /// Parse from environment variable
    pub fn from_env() -> Self {
        match std::env::var("LOG_FORMAT").as_deref() {
            Ok("json") => LogFormat::Json,
            Ok("compact") => LogFormat::Compact,
            Ok("pretty") | Ok(_) => LogFormat::Pretty,
            Err(_) => LogFormat::Pretty, // Default to pretty for development
        }
    }
}

/// Log output configuration
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogOutput {
    /// Log to stdout only
    Stdout,
    /// Log to file only
    File,
    /// Log to both stdout and file
    Both,
}

impl LogOutput {
    /// Parse from environment variable
    pub fn from_env() -> Self {
        match std::env::var("LOG_OUTPUT").as_deref() {
            Ok("file") => LogOutput::File,
            Ok("both") => LogOutput::Both,
            Ok("stdout") | Ok(_) => LogOutput::Stdout,
            Err(_) => LogOutput::Stdout, // Default to stdout for development
        }
    }
}

/// Initialize the logging system with comprehensive configuration
///
/// Environment variables:
/// - `RUST_LOG`: Log level (e.g., "debug", "info", "mlql_server=debug")
/// - `LOG_FORMAT`: Output format ("pretty", "json", "compact")
/// - `LOG_OUTPUT`: Where to write logs ("stdout", "file", "both")
/// - `LOG_DIR`: Directory for log files (default: "./logs")
///
/// Examples:
/// ```bash
/// # Development: pretty console output at debug level
/// RUST_LOG=debug LOG_FORMAT=pretty cargo run
///
/// # Production: JSON to file with info level
/// RUST_LOG=info LOG_FORMAT=json LOG_OUTPUT=file LOG_DIR=/var/log/mlql cargo run
///
/// # Testing: compact format with specific module filtering
/// RUST_LOG=mlql_server=trace,axum=warn LOG_FORMAT=compact cargo test
/// ```
pub fn init() {
    let format = LogFormat::from_env();
    let output = LogOutput::from_env();

    // Build environment filter - DEFAULT TO DEBUG for mlql_server
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("debug"))
        .unwrap()
        // Ensure mlql_server logs are visible
        .add_directive("mlql_server=debug".parse().unwrap())
        // Filter out noisy third-party crates
        .add_directive("hyper=warn".parse().unwrap())
        .add_directive("tokio=warn".parse().unwrap())
        .add_directive("runtime=warn".parse().unwrap())
        .add_directive("tower=warn".parse().unwrap())
        .add_directive("h2=warn".parse().unwrap());

    // Initialize based on output and format
    match (output, format) {
        // Stdout only
        (LogOutput::Stdout, LogFormat::Pretty) => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt::layer().pretty().with_thread_ids(true).with_target(true))
                .init();
        }
        (LogOutput::Stdout, LogFormat::Json) => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt::layer().json().with_current_span(true))
                .init();
        }
        (LogOutput::Stdout, LogFormat::Compact) => {
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt::layer().compact())
                .init();
        }
        // File only
        (LogOutput::File, _) => {
            let log_dir = std::env::var("LOG_DIR").unwrap_or_else(|_| "./logs".to_string());
            std::fs::create_dir_all(&log_dir).ok();
            let file_appender = RollingFileAppender::new(Rotation::DAILY, &log_dir, "mlql-server.log");

            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt::layer().with_writer(file_appender).with_ansi(false))
                .init();
        }
        // Both stdout and file - use boxed layers for dynamic dispatch
        (LogOutput::Both, format) => {
            let log_dir = std::env::var("LOG_DIR").unwrap_or_else(|_| "./logs".to_string());
            std::fs::create_dir_all(&log_dir).ok();
            let file_appender = RollingFileAppender::new(Rotation::DAILY, &log_dir, "mlql-server.log");

            let stdout_layer = match format {
                LogFormat::Pretty => fmt::layer()
                    .pretty()
                    .with_thread_ids(true)
                    .with_target(true)
                    .boxed(),
                LogFormat::Json => fmt::layer()
                    .json()
                    .with_current_span(true)
                    .boxed(),
                LogFormat::Compact => fmt::layer()
                    .compact()
                    .boxed(),
            };

            let file_layer = fmt::layer()
                .with_writer(file_appender)
                .with_ansi(false)
                .boxed();

            tracing_subscriber::registry()
                .with(env_filter)
                .with(stdout_layer)
                .with(file_layer)
                .init();
        }
    }

    // Log initialization message with details
    tracing::info!(
        format = ?format,
        output = ?output,
        "✅ Logging system initialized"
    );

    // Log environment details
    tracing::debug!("Environment:");
    tracing::debug!("  RUST_LOG: {}", std::env::var("RUST_LOG").unwrap_or_else(|_| "not set".to_string()));
    tracing::debug!("  LOG_FORMAT: {}", std::env::var("LOG_FORMAT").unwrap_or_else(|_| "not set".to_string()));
    tracing::debug!("  LOG_OUTPUT: {}", std::env::var("LOG_OUTPUT").unwrap_or_else(|_| "not set".to_string()));
    if matches!(output, LogOutput::File | LogOutput::Both) {
        let log_dir = std::env::var("LOG_DIR").unwrap_or_else(|_| "not set".to_string());
        tracing::debug!("  LOG_DIR: {}", log_dir);
    }
}


/// Helper macro for logging with structured fields
///
/// Usage:
/// ```rust
/// log_event!(
///     level: tracing::Level::INFO,
///     event: "query_executed",
///     query_id: "abc123",
///     duration_ms: 42,
///     rows: 100
/// );
/// ```
#[macro_export]
macro_rules! log_event {
    (level: $level:expr, event: $event:expr $(, $key:ident: $value:expr)* $(,)?) => {
        tracing::event!(
            $level,
            event = $event
            $(, $key = ?$value)*
        );
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_format_from_env() {
        std::env::set_var("LOG_FORMAT", "json");
        assert_eq!(LogFormat::from_env(), LogFormat::Json);

        std::env::set_var("LOG_FORMAT", "pretty");
        assert_eq!(LogFormat::from_env(), LogFormat::Pretty);

        std::env::set_var("LOG_FORMAT", "compact");
        assert_eq!(LogFormat::from_env(), LogFormat::Compact);

        std::env::remove_var("LOG_FORMAT");
        assert_eq!(LogFormat::from_env(), LogFormat::Pretty);
    }

    #[test]
    fn test_log_output_from_env() {
        std::env::set_var("LOG_OUTPUT", "file");
        assert_eq!(LogOutput::from_env(), LogOutput::File);

        std::env::set_var("LOG_OUTPUT", "both");
        assert_eq!(LogOutput::from_env(), LogOutput::Both);

        std::env::set_var("LOG_OUTPUT", "stdout");
        assert_eq!(LogOutput::from_env(), LogOutput::Stdout);

        std::env::remove_var("LOG_OUTPUT");
        assert_eq!(LogOutput::from_env(), LogOutput::Stdout);
    }
}
