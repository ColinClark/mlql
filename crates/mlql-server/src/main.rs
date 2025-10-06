//! MLQL HTTP Server with OpenAI integration
//!
//! This server accepts natural language queries, converts them to MLQL IR using OpenAI,
//! executes them against DuckDB, and returns results as streaming JSONL.

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::{error, info};

mod llm;
mod query;

/// Server state shared across handlers
#[derive(Clone)]
struct AppState {
    openai_client: async_openai::Client<async_openai::config::OpenAIConfig>,
}

/// Health check response
#[derive(Serialize)]
struct HealthResponse {
    status: String,
    version: String,
}

/// Query request from client
#[derive(Deserialize)]
struct QueryRequest {
    /// Natural language query
    query: String,
    /// Optional database name (default: in-memory)
    #[serde(default)]
    database: Option<String>,
}

/// Query response with results
#[derive(Serialize)]
struct QueryResponse {
    /// Generated MLQL IR (for debugging/transparency)
    ir: serde_json::Value,
    /// Generated SQL (for debugging/transparency)
    sql: String,
    /// Query results
    results: serde_json::Value,
}

/// Error response
#[derive(Serialize)]
struct ErrorResponse {
    error: String,
    details: Option<String>,
}

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

    // Create OpenAI client
    let openai_config = async_openai::config::OpenAIConfig::new()
        .with_api_key(api_key);
    let openai_client = async_openai::Client::with_config(openai_config);

    // Create shared state
    let state = AppState {
        openai_client,
    };

    // Build router
    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/query", post(query_handler))
        .with_state(state);

    // Get server address
    let host = std::env::var("MLQL_SERVER_HOST")
        .unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = std::env::var("MLQL_SERVER_PORT")
        .unwrap_or_else(|_| "8080".to_string());
    let addr = format!("{}:{}", host, port);

    info!("Starting MLQL server on {}", addr);

    // Start server
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

/// Health check endpoint
async fn health_handler() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// Query endpoint - accepts natural language, returns results with error retry loop
async fn query_handler(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    info!("Received query: {}", req.query);

    const MAX_EXECUTION_RETRIES: usize = 2;

    // Try up to MAX_EXECUTION_RETRIES times to get valid IR and execute it
    for attempt in 0..MAX_EXECUTION_RETRIES {
        // Step 1: Convert natural language to MLQL IR using OpenAI (with built-in retries)
        let ir = llm::natural_language_to_ir(&state.openai_client, &req.query)
            .await
            .map_err(|e| {
                error!("Failed to convert NL to IR: {}", e);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: "Failed to convert query to MLQL IR".to_string(),
                        details: Some(e.to_string()),
                    }),
                )
            })?;

        // Step 2: Execute IR against DuckDB
        match query::execute_ir(ir.clone(), req.database.clone()).await {
            Ok((sql, results)) => {
                // Success!
                info!("Query executed successfully on attempt {}", attempt + 1);
                return Ok(Json(QueryResponse {
                    ir: serde_json::to_value(&ir).unwrap_or(json!({})),
                    sql,
                    results,
                }));
            }
            Err(e) => {
                error!("Execution attempt {} failed: {}", attempt + 1, e);

                if attempt == MAX_EXECUTION_RETRIES - 1 {
                    // Last attempt, return error
                    return Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ErrorResponse {
                            error: "Failed to execute query after retries".to_string(),
                            details: Some(e.to_string()),
                        }),
                    ));
                }

                // Note: In a full implementation, we would feed the execution error back to the LLM here
                // For now, we just retry with the same query
                // TODO: Add execution error feedback loop to LLM
                info!("Retrying after execution error: {}", e);
            }
        }
    }

    Err((
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error: "Exceeded maximum execution retries".to_string(),
            details: None,
        }),
    ))
}
