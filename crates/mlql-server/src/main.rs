//! MLQL MCP Server with OpenAI integration
//!
//! Model Context Protocol server that accepts natural language queries,
//! converts them to MLQL IR using OpenAI, and executes against DuckDB.

use rust_mcp_sdk::mcp_server::{hyper_server, HyperServerOptions};
use tracing::info;

mod catalog;
mod llm;
mod mcp;
mod query;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load environment variables
    dotenvy::dotenv().ok();

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Get OpenAI API key
    let api_key = std::env::var("OPENAI_API_KEY")
        .expect("OPENAI_API_KEY must be set in .env file");

    info!("OpenAI API key loaded: {}...", &api_key.chars().take(20).collect::<String>());
    info!("Using OpenAI model: gpt-4o-mini");

    // Create OpenAI client
    let openai_config = async_openai::config::OpenAIConfig::new().with_api_key(api_key);
    let openai_client = async_openai::Client::with_config(openai_config);

    // Create MCP server handler
    let handler = mcp::MlqlServerHandler::new(openai_client);
    let server_info = mcp::MlqlServerHandler::server_info();

    // Get server configuration
    let host = std::env::var("MLQL_SERVER_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port: u16 = std::env::var("MLQL_SERVER_PORT")
        .unwrap_or_else(|_| "8080".to_string())
        .parse()
        .unwrap_or(8080);

    info!("Starting MLQL MCP server on {}:{}", host, port);
    info!("Protocol: MCP with SSE (Server-Sent Events) support");
    info!("Use with Claude Desktop or MCP clients");

    // Create MCP server with SSE support
    let server = hyper_server::create_server(
        server_info,
        handler,
        HyperServerOptions {
            host,
            port,
            sse_support: true, // Enable SSE for streaming
            ..Default::default()
        },
    );

    // Start the server
    server.start().await?;

    Ok(())
}
