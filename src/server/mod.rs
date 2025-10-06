//! HTTP Server for LLM/tool integration
//!
//! Provides REST API endpoints for:
//! - Query execution
//! - Schema introspection
//! - Catalog browsing

#[cfg(feature = "server")]
use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};

#[cfg(feature = "server")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "server")]
use crate::{
    catalog::Catalog,
    compile::Compiler,
    exec::{ExecutionBudget, Executor},
    parser::parse,
    policy::PolicyEngine,
    validate::Validator,
};

#[cfg(feature = "server")]
#[derive(Clone)]
struct AppState {
    executor: std::sync::Arc<Executor>,
    catalog: std::sync::Arc<Catalog>,
    validator: std::sync::Arc<Validator>,
    compiler: std::sync::Arc<Compiler>,
    policy: std::sync::Arc<PolicyEngine>,
}

#[cfg(feature = "server")]
#[derive(Deserialize)]
struct QueryRequest {
    query: String,
    budget: Option<BudgetRequest>,
}

#[cfg(feature = "server")]
#[derive(Deserialize)]
struct BudgetRequest {
    max_time_ms: Option<u64>,
    max_memory_mb: Option<u64>,
    max_rows: Option<u64>,
}

#[cfg(feature = "server")]
#[derive(Serialize)]
struct QueryResponse {
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
    row_count: usize,
}

#[cfg(feature = "server")]
#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

#[cfg(feature = "server")]
pub async fn serve(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let executor = Executor::new()?;
    let catalog = Catalog::new();
    let validator = Validator::new(catalog.clone());
    let compiler = Compiler::new(catalog.clone());
    let policy = PolicyEngine::new();

    let state = AppState {
        executor: std::sync::Arc::new(executor),
        catalog: std::sync::Arc::new(catalog),
        validator: std::sync::Arc::new(validator),
        compiler: std::sync::Arc::new(compiler),
        policy: std::sync::Arc::new(policy),
    };

    let app = Router::new()
        .route("/query", post(execute_query))
        .route("/schema", get(get_schema))
        .route("/health", get(health_check))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("MLQL server listening on {}", addr);

    axum::serve(listener, app).await?;

    Ok(())
}

#[cfg(feature = "server")]
async fn execute_query(
    State(_state): State<AppState>,
    Json(_req): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    // TODO: Implement query execution
    todo!("Server query execution not yet implemented")
}

#[cfg(feature = "server")]
async fn get_schema(
    State(_state): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ErrorResponse>)> {
    // TODO: Return catalog schema
    todo!("Server schema endpoint not yet implemented")
}

#[cfg(feature = "server")]
async fn health_check() -> &'static str {
    "OK"
}
