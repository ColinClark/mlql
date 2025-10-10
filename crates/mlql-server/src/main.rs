//! MLQL MCP Server with OpenAI integration
//!
//! Model Context Protocol server that accepts natural language queries,
//! converts them to MLQL IR using OpenAI, and executes against DuckDB.

use rust_mcp_sdk::mcp_server::{hyper_server, HyperServerOptions};

mod catalog;
mod config;
mod llm;
mod logging;
mod mcp;
mod query;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Print startup banner BEFORE logging initialization
    eprintln!("\n╔════════════════════════════════════════════════════╗");
    eprintln!("║         MLQL Server Starting Up                    ║");
    eprintln!("╚════════════════════════════════════════════════════╝\n");

    // Load environment variables first (for secrets like API keys)
    eprintln!("[1/6] Loading environment variables from .env...");
    dotenvy::dotenv().ok();

    // Load configuration from config.yaml (with env var overrides)
    eprintln!("[2/6] Loading configuration from config.yaml...");
    let config = config::Config::load("config.yaml")
        .unwrap_or_else(|e| {
            eprintln!("⚠️  Warning: Failed to load config.yaml: {}", e);
            eprintln!("    Using default configuration");
            config::Config::default()
        });

    // Apply logging configuration to environment
    eprintln!("[3/6] Applying logging configuration...");
    eprintln!("    Log Level:  {}", config.logging.level);
    eprintln!("    Log Format: {}", config.logging.format);
    eprintln!("    Log Output: {}", config.logging.output);
    config.apply_logging_env();

    // Apply execution mode to environment
    std::env::set_var("MLQL_EXECUTION_MODE", &config.execution.mode);
    eprintln!("    Execution Mode: {}", config.execution.mode);

    // Initialize comprehensive logging system
    eprintln!("[4/6] Initializing structured logging system...");
    logging::init();
    eprintln!("    ✅ Logging initialized");

    eprintln!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    eprintln!("Configuration Summary:");
    eprintln!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    eprintln!("  Server Host:        {}", config.server.host);
    eprintln!("  Server Port:        {}", config.server.port);
    eprintln!("  Execution Mode:     {} ✓", config.execution.mode.to_uppercase());
    if let Some(ref path) = config.execution.substrait_extension_path {
        eprintln!("  Substrait Path:     {}", path);
    }
    eprintln!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    // Get OpenAI API key from environment (.env file)
    eprintln!("[5/6] Loading OpenAI API key...");
    let api_key = config::Config::get_openai_api_key()
        .expect("OPENAI_API_KEY must be set in .env file");

    let key_preview = format!("{}...{}",
        &api_key.chars().take(10).collect::<String>(),
        &api_key.chars().rev().take(4).collect::<String>().chars().rev().collect::<String>()
    );
    eprintln!("    ✅ API key loaded: {}", key_preview);
    eprintln!("    Using model: gpt-4o-mini");

    // Create OpenAI client
    let openai_config = async_openai::config::OpenAIConfig::new().with_api_key(api_key);
    let openai_client = async_openai::Client::with_config(openai_config);

    // Create MCP server handler
    let handler = mcp::MlqlServerHandler::new(openai_client);
    let server_info = mcp::MlqlServerHandler::server_info();

    eprintln!("\n[6/6] Starting MCP server...");
    eprintln!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    eprintln!("🚀 MLQL MCP Server");
    eprintln!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    eprintln!("  Address:    http://{}:{}", config.server.host, config.server.port);
    eprintln!("  Protocol:   MCP with SSE (Server-Sent Events)");
    eprintln!("\n  Endpoints:");
    eprintln!("    GET  /health          - Health check");
    eprintln!("    POST /mcp/v1/message  - MCP message handler");
    eprintln!("    GET  /mcp/v1/sse      - SSE streaming");
    eprintln!("\n  📋 Use with Claude Desktop or MCP clients");
    eprintln!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Create MCP server with SSE support
    let server = hyper_server::create_server(
        server_info,
        handler,
        HyperServerOptions {
            host: config.server.host.clone(),
            port: config.server.port,
            sse_support: true, // Enable SSE for streaming
            ..Default::default()
        },
    );

    eprintln!("\n✅ Server ready and listening!\n");

    // Start the server
    server.start().await?;

    Ok(())
}
