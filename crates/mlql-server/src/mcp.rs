//! MCP (Model Context Protocol) server implementation

use async_openai::Client;
use async_trait::async_trait;
use rust_mcp_schema::{
    schema_utils::CallToolError, CallToolRequest, CallToolResult, ContentBlock, Implementation,
    InitializeResult, ListToolsRequest, ListToolsResult, RpcError, ServerCapabilities,
    ServerCapabilitiesTools, TextContent, Tool, ToolInputSchema, LATEST_PROTOCOL_VERSION,
};
use rust_mcp_sdk::{mcp_server::ServerHandler, McpServer};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info};

use crate::{llm, query};

/// MLQL MCP Server Handler
pub struct MlqlServerHandler {
    openai_client: Client<async_openai::config::OpenAIConfig>,
}

impl MlqlServerHandler {
    pub fn new(openai_client: Client<async_openai::config::OpenAIConfig>) -> Self {
        Self { openai_client }
    }

    /// Create server initialization details
    pub fn server_info() -> InitializeResult {
        InitializeResult {
            protocol_version: LATEST_PROTOCOL_VERSION.to_string(),
            capabilities: ServerCapabilities {
                tools: Some(ServerCapabilitiesTools {
                    list_changed: None,
                }),
                ..Default::default()
            },
            server_info: Implementation {
                name: "mlql-server".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                title: Some("MLQL Natural Language to SQL Server".to_string()),
            },
            instructions: Some(
                "MLQL Server - Natural language to SQL queries. \
                 Use the 'query' tool to execute natural language database queries."
                    .to_string(),
            ),
            meta: None,
        }
    }

    /// Define available MCP tools
    fn tools() -> Vec<Tool> {
        let mut tools = Vec::new();

        // Query tool
        {
            let mut properties = HashMap::new();

            let mut query_prop = Map::new();
            query_prop.insert("type".to_string(), Value::String("string".to_string()));
            query_prop.insert("description".to_string(), Value::String("Natural language database query (e.g., 'show me users over age 25')".to_string()));
            properties.insert("query".to_string(), query_prop);

            let mut database_prop = Map::new();
            database_prop.insert("type".to_string(), Value::String("string".to_string()));
            database_prop.insert("description".to_string(), Value::String("Path to DuckDB database file (defaults to data/demo.duckdb)".to_string()));
            database_prop.insert("default".to_string(), Value::String("data/demo.duckdb".to_string()));
            properties.insert("database".to_string(), database_prop);

            tools.push(Tool {
                name: "query".to_string(),
                description: Some(
                    "Execute a natural language database query. \
                     The query will be converted to MLQL IR and executed against DuckDB. \
                     Returns the IR, generated SQL, and query results."
                        .to_string(),
                ),
                input_schema: ToolInputSchema::new(
                    vec!["query".to_string()],
                    Some(properties),
                ),
                title: None,
                annotations: None,
                meta: None,
                output_schema: None,
            });
        }

        // Catalog tool
        {
            let mut properties = HashMap::new();

            let mut database_prop = Map::new();
            database_prop.insert("type".to_string(), Value::String("string".to_string()));
            database_prop.insert("description".to_string(), Value::String("Path to DuckDB database file (defaults to data/demo.duckdb)".to_string()));
            properties.insert("database".to_string(), database_prop);

            tools.push(Tool {
                name: "catalog".to_string(),
                description: Some(
                    "Get database catalog information including all tables, their columns, \
                     column types, and sample data for each column. Returns the catalog in JSONL format \
                     with one table per line."
                        .to_string(),
                ),
                input_schema: ToolInputSchema::new(
                    vec![],
                    Some(properties),
                ),
                title: None,
                annotations: None,
                meta: None,
                output_schema: None,
            });
        }

        tools
    }
}

#[async_trait]
impl ServerHandler for MlqlServerHandler {
    async fn handle_list_tools_request(
        &self,
        _request: ListToolsRequest,
        _runtime: Arc<dyn McpServer>,
    ) -> std::result::Result<ListToolsResult, RpcError> {
        info!("Listing available tools");

        Ok(ListToolsResult {
            tools: Self::tools(),
            next_cursor: None,
            meta: None,
        })
    }

    async fn handle_call_tool_request(
        &self,
        request: CallToolRequest,
        _runtime: Arc<dyn McpServer>,
    ) -> std::result::Result<CallToolResult, CallToolError> {
        info!("Tool called: {}", request.params.name);

        match request.params.name.as_str() {
            "query" => self.handle_query_tool(request.params.arguments.map(|m| serde_json::Value::Object(m))).await,
            "catalog" => self.handle_catalog_tool(request.params.arguments.map(|m| serde_json::Value::Object(m))).await,
            _ => Err(CallToolError::unknown_tool(request.params.name.clone())),
        }
    }
}

impl MlqlServerHandler {
    async fn handle_query_tool(
        &self,
        arguments: Option<serde_json::Value>,
    ) -> std::result::Result<CallToolResult, CallToolError> {
        // Extract query and optional database from arguments
        let args = arguments.ok_or_else(|| CallToolError::from_message("Missing arguments"))?;

        let query = args
            .get("query")
            .and_then(|v| v.as_str())
            .ok_or_else(|| CallToolError::from_message("Missing required argument: query"))?
            .to_string();

        let database = args
            .get("database")
            .and_then(|v| v.as_str())
            .map(String::from)
            .or_else(|| Some("data/demo.duckdb".to_string()));

        info!("Executing query: {}", query);
        info!("Database: {:?}", database);

        // Step 1: Load catalog if database is specified
        let catalog_json = if let Some(ref db_path) = database {
            match crate::catalog::DatabaseCatalog::from_database(db_path) {
                Ok(catalog) => {
                    // Convert catalog to JSONL
                    let mut jsonl_lines = Vec::new();
                    for table in &catalog.tables {
                        if let Ok(table_json) = serde_json::to_string(&table) {
                            jsonl_lines.push(table_json);
                        }
                    }
                    Some(jsonl_lines.join("\n"))
                }
                Err(e) => {
                    tracing::warn!("Failed to load catalog: {}", e);
                    None
                }
            }
        } else {
            None
        };

        // Step 2: Convert natural language to MLQL IR using OpenAI (with catalog context)
        let ir = llm::natural_language_to_ir_with_catalog(
            &self.openai_client,
            &query,
            catalog_json.as_deref(),
        )
        .await
        .map_err(|e| {
            error!("Failed to convert NL to IR: {}", e);
            CallToolError::from_message(format!("Failed to convert query to MLQL IR: {}", e))
        })?;

        info!("Generated IR: {}", serde_json::to_string_pretty(&ir).unwrap_or_default());

        // Step 3: Execute IR against DuckDB (uses MLQL_EXECUTION_MODE env var)
        let (execution_info, results) = query::execute_ir_auto(ir.clone(), database)
            .await
            .map_err(|e| {
                error!("Failed to execute query: {}", e);
                error!("IR that caused error: {}", serde_json::to_string_pretty(&ir).unwrap_or_default());
                CallToolError::from_message(format!("Failed to execute query: {}\n\nIR:\n{}", e, serde_json::to_string_pretty(&ir).unwrap_or_default()))
            })?;

        info!("Execution info: {}", execution_info);
        info!("Query results: {} rows", results.get("row_count").and_then(|v| v.as_u64()).unwrap_or(0));

        // Format response as MCP content
        let response_text = format!(
            "Query: {}\n\nGenerated IR:\n{}\n\nExecution: {}\n\nResults:\n{}",
            query,
            serde_json::to_string_pretty(&ir).unwrap_or_default(),
            execution_info,
            serde_json::to_string_pretty(&results).unwrap_or_default()
        );

        Ok(CallToolResult {
            content: vec![ContentBlock::TextContent(TextContent::new(
                response_text,
                None,
                None,
            ))],
            is_error: None,
            meta: None,
            structured_content: None,
        })
    }

    async fn handle_catalog_tool(
        &self,
        arguments: Option<serde_json::Value>,
    ) -> std::result::Result<CallToolResult, CallToolError> {
        // Extract optional database path
        let database_path = arguments
            .and_then(|args| args.get("database").and_then(|v| v.as_str()).map(String::from))
            .unwrap_or_else(|| "data/demo.duckdb".to_string());

        info!("Extracting catalog from database: {}", database_path);

        // Extract catalog from database
        let catalog = crate::catalog::DatabaseCatalog::from_database(&database_path)
            .map_err(|e| {
                error!("Failed to extract catalog: {}", e);
                CallToolError::from_message(format!("Failed to extract catalog: {}", e))
            })?;

        // Convert to JSONL format (one table per line)
        let mut jsonl_lines = Vec::new();
        for table in &catalog.tables {
            let table_json = serde_json::to_string(&table).map_err(|e| {
                error!("Failed to serialize table: {}", e);
                CallToolError::from_message(format!("Failed to serialize table: {}", e))
            })?;
            jsonl_lines.push(table_json);
        }

        let jsonl_output = jsonl_lines.join("\n");

        // Also create a summary
        let summary = format!(
            "Database Catalog: {} tables\n\nTables: {}\n\nJSONL Output:\n{}",
            catalog.tables.len(),
            catalog.tables.iter().map(|t| t.name.as_str()).collect::<Vec<_>>().join(", "),
            jsonl_output
        );

        Ok(CallToolResult {
            content: vec![ContentBlock::TextContent(TextContent::new(
                summary,
                None,
                None,
            ))],
            is_error: None,
            meta: None,
            structured_content: None,
        })
    }
}
