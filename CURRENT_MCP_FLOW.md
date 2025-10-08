# Current MCP Server Flow Analysis

**Branch**: `mcp-mlql-substrait`
**Date**: 2025-10-08

## Architecture Overview

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Natural   │────▶│   OpenAI    │────▶│  MLQL IR    │────▶│     SQL     │────▶│   DuckDB    │
│  Language   │     │  (GPT-4o)   │     │   (JSON)    │     │  Generator  │     │   Results   │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
      │                    │                    │                    │                    │
   main.rs              llm.rs               mcp.rs             query.rs           mlql-duck
```

## Component Breakdown

### 1. Entry Point (`main.rs`)

**Purpose**: Initialize and start the MCP server

**Key Code**:
```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load environment variables
    dotenv::dotenv().ok();

    // Initialize OpenAI client
    let openai_client = Client::with_config(config);

    // Create MCP handler
    let handler = MlqlServerHandler::new(openai_client);

    // Get server configuration from env
    let host = std::env::var("MLQL_SERVER_HOST").unwrap_or("127.0.0.1");
    let port = std::env::var("MLQL_SERVER_PORT").unwrap_or("8080").parse()?;

    // Create and start MCP server with SSE support
    let server = hyper_server::create_server(
        server_info,
        handler,
        HyperServerOptions { host, port, sse_support: true, ..Default::default() }
    );

    server.start().await?;
}
```

**Environment Variables**:
- `OPENAI_API_KEY` - Required for OpenAI API calls
- `MLQL_SERVER_HOST` - Default: "127.0.0.1"
- `MLQL_SERVER_PORT` - Default: "8080"

### 2. Natural Language → IR Conversion (`llm.rs`)

**Purpose**: Convert natural language queries to MLQL IR using OpenAI

**Key Function**:
```rust
pub async fn natural_language_to_ir_with_catalog(
    client: &Client<OpenAIConfig>,
    query: &str,
    catalog: Option<&str>,
) -> Result<Pipeline, Box<dyn std::error::Error>>
```

**How It Works**:
1. Takes natural language query string
2. Optionally includes database catalog (table schemas) from DuckDB
3. Sends query + catalog to OpenAI GPT-4o-mini with comprehensive MLQL IR prompt
4. Parses response as JSON into `Pipeline` struct
5. **Error Retry Loop**: If parsing fails, feeds error back to OpenAI (up to 3 attempts)

**System Prompt** (excerpt):
```
You are an expert at converting natural language queries to MLQL IR (JSON format).

MLQL IR Format:
{
  "pipeline": {
    "source": {"type": "Table", "name": "table_name"},
    "ops": [
      {"op": "Filter", "condition": {...}},
      {"op": "Select", "projections": [...]},
      ...
    ]
  }
}

Supported Operators:
- Filter: WHERE conditions
- Select: Column projections
- Sort: ORDER BY
- Take: LIMIT
- Distinct: Deduplication
- GroupBy: Aggregations
- Join: Table joins

Examples:
[... comprehensive examples for each operator type ...]
```

**Catalog Integration**:
- If database path provided, queries DuckDB catalog first
- Extracts table schemas (table names, column names, types)
- Includes schema info in prompt so OpenAI generates valid column references

### 3. MCP Protocol Handler (`mcp.rs`)

**Purpose**: Implement Model Context Protocol server with query execution

**MCP Server Info**:
```rust
InitializeResult {
    protocol_version: LATEST_PROTOCOL_VERSION,
    capabilities: ServerCapabilities {
        tools: Some(ServerCapabilitiesTools { list_changed: None }),
        ..Default::default()
    },
    server_info: Implementation {
        name: "mlql-server",
        version: "0.1.0",
        title: "MLQL Natural Language to SQL Server",
    },
}
```

**Available Tools**:

#### Tool 1: `query`
Execute natural language database queries

**Parameters**:
- `query` (required): Natural language query string
- `database` (optional): Path to DuckDB file (default: "data/demo.duckdb")

**Implementation Flow** (`handle_query_tool`):
```rust
async fn handle_query_tool(&self, arguments: Option<Value>) -> Result<CallToolResult, CallToolError> {
    // 1. Extract arguments
    let query = args.get("query").and_then(|v| v.as_str())?;
    let database = args.get("database").map(String::from)
        .or_else(|| Some("data/demo.duckdb".to_string()));

    // 2. Load catalog if database specified
    let catalog = if let Some(ref db_path) = database {
        DatabaseCatalog::from_database(db_path).ok()
    } else {
        None
    };

    // 3. Convert NL → MLQL IR (via OpenAI)
    let ir = llm::natural_language_to_ir_with_catalog(
        &self.openai_client,
        &query,
        catalog.as_deref(),
    ).await?;

    // 4. Execute IR → SQL → Results
    let (sql, results) = query::execute_ir(ir.clone(), database).await?;

    // 5. Format response
    let response_text = format!(
        "Query: {}\n\nGenerated IR:\n{}\n\nGenerated SQL:\n{}\n\nResults:\n{}",
        query,
        serde_json::to_string_pretty(&ir)?,
        sql,
        serde_json::to_string_pretty(&results)?
    );

    Ok(CallToolResult {
        content: vec![ContentBlock::Text(TextContent {
            type_: "text".to_string(),
            text: response_text,
            annotations: None,
        })],
        is_error: None,
        meta: None,
    })
}
```

#### Tool 2: `catalog`
View database schema (tables, columns, types)

**Parameters**:
- `database` (optional): Path to DuckDB file (default: "data/demo.duckdb")

**Implementation**: Queries `information_schema` tables in DuckDB

### 4. Query Execution (`query.rs`)

**Purpose**: Execute MLQL IR against DuckDB

**Key Function**:
```rust
pub async fn execute_ir(
    pipeline: Pipeline,
    database: Option<String>,
) -> Result<(String, serde_json::Value), Box<dyn std::error::Error>>
```

**How It Works**:
1. Create DuckDB executor:
   ```rust
   let executor = if let Some(db_path) = database {
       DuckExecutor::open(db_path)?  // Open file-based database
   } else {
       DuckExecutor::new()?          // In-memory database
   };
   ```

2. Convert `Pipeline` to `Program`:
   ```rust
   let program = Program {
       pragma: None,
       lets: vec![],
       pipeline: pipeline.clone(),
   };
   ```

3. Execute via `mlql-duck` crate (IR → SQL):
   ```rust
   let result = executor.execute_ir(&program, None)?;
   ```

4. Extract SQL and results:
   ```rust
   let sql = result.sql.clone().unwrap_or("No SQL generated");
   let json_result = result_to_json(&result)?;
   Ok((sql, json_result))
   ```

**Result Format**:
```json
{
  "columns": ["id", "name", "age"],
  "rows": [
    {"id": 1, "name": "Alice", "age": 30},
    {"id": 2, "name": "Bob", "age": 28}
  ],
  "row_count": 2
}
```

### 5. SQL Generation (`mlql-duck` crate)

**Purpose**: Translate MLQL IR to SQL and execute against DuckDB

**Key Type**: `DuckExecutor`
- Wraps `duckdb::Connection`
- Implements `execute_ir(&Program) -> QueryResult`
- Generates SQL from IR operators
- Executes via DuckDB and returns Arrow results as JSON

**SQL Generation Logic** (simplified):
```rust
impl DuckExecutor {
    pub fn execute_ir(&self, program: &Program, params: Option<&[Value]>) -> Result<QueryResult> {
        // Build SQL from pipeline
        let mut sql = String::from("SELECT * FROM ");

        // Add source (table name)
        sql.push_str(&pipeline.source.name);

        // Add WHERE clause from Filter ops
        for op in &pipeline.ops {
            match op {
                Operator::Filter { condition } => {
                    sql.push_str(" WHERE ");
                    sql.push_str(&self.expr_to_sql(condition)?);
                }
                Operator::Select { projections } => {
                    // Replace SELECT *
                    sql = format!("SELECT {} FROM {}", ..., ...);
                }
                Operator::Sort { keys } => {
                    sql.push_str(" ORDER BY ");
                    // ... append sort keys
                }
                Operator::Take { count } => {
                    sql.push_str(&format!(" LIMIT {}", count));
                }
                // ... other operators
            }
        }

        // Execute SQL
        let mut stmt = self.conn.prepare(&sql)?;
        let results = stmt.query_arrow(params)?;

        // Convert Arrow to JSON
        Ok(QueryResult { sql: Some(sql), columns, rows, ... })
    }
}
```

## Data Flow Example

**Input**: "Show me users over age 25, sorted by name"

**Step 1 - Natural Language** (user input):
```
"Show me users over age 25, sorted by name"
```

**Step 2 - OpenAI Conversion** (`llm.rs`):
```json
{
  "pipeline": {
    "source": {"type": "Table", "name": "users"},
    "ops": [
      {
        "op": "Filter",
        "condition": {
          "type": "BinaryOp",
          "op": "Gt",
          "left": {"type": "Column", "col": {"column": "age"}},
          "right": {"type": "Literal", "value": 25}
        }
      },
      {
        "op": "Sort",
        "keys": [{"column": "name", "desc": false}]
      }
    ]
  }
}
```

**Step 3 - SQL Generation** (`mlql-duck`):
```sql
SELECT * FROM users WHERE (age > 25) ORDER BY name ASC
```

**Step 4 - DuckDB Execution**:
```json
{
  "columns": ["id", "name", "age"],
  "rows": [
    {"id": 2, "name": "Alice", "age": 30},
    {"id": 3, "name": "Bob", "age": 28}
  ],
  "row_count": 2
}
```

**Step 5 - MCP Response** (to Claude):
```
Query: Show me users over age 25, sorted by name

Generated IR:
{
  "pipeline": { ... }
}

Generated SQL:
SELECT * FROM users WHERE (age > 25) ORDER BY name ASC

Results:
{
  "columns": ["id", "name", "age"],
  "rows": [...]
}
```

## Integration Points for Substrait

### What Stays the Same
1. ✅ Entry point (`main.rs`) - No changes needed
2. ✅ NL → IR conversion (`llm.rs`) - Already generates perfect MLQL IR
3. ✅ MCP protocol handler (`mcp.rs`) - Just needs to call different execution function
4. ✅ Catalog loading - Already queries DuckDB schemas

### What Changes
1. **Query execution** (`query.rs`):
   - ADD: New function `execute_ir_substrait(pipeline, database)`
   - KEEP: Existing `execute_ir()` for SQL mode

2. **Database connection** (`query.rs`):
   - ADD: Load Substrait extension on connection
   - KEEP: Existing connection logic

3. **Execution path**:
   - CURRENT: `mlql-duck` (IR → SQL)
   - NEW: `mlql-ir/substrait` (IR → Substrait Plan)

### New Components Needed
1. **DuckDB SchemaProvider** (`schema.rs`):
   - Query DuckDB `information_schema` tables
   - Implement `SchemaProvider` trait from `mlql-ir/substrait`
   - Cache schemas for performance

2. **Substrait execution** (`query.rs`):
   - Initialize `SubstraitTranslator` with `DuckDbSchemaProvider`
   - Translate IR → Substrait Plan
   - Serialize with `prost`
   - Execute: `SELECT * FROM from_substrait(?)`
   - Parse results (same as current)

3. **Configuration** (`main.rs` / `.env`):
   - Add `MLQL_EXECUTION_MODE` env var (sql/substrait)
   - Add `SUBSTRAIT_EXTENSION_PATH` env var

## Key Observations

### Advantages of Current Architecture
1. **Clean separation**: NL → IR → SQL → Results
2. **Error handling**: OpenAI retry loop for invalid IR
3. **Schema-aware**: Catalog loaded before IR generation
4. **Tested**: 28 SQL generation tests passing

### Why Substrait is Better
1. **No SQL intermediate**: IR → Substrait → DuckDB (one less translation)
2. **Type safety**: Substrait is strongly typed, SQL is text
3. **Optimization**: DuckDB can optimize Substrait plans directly
4. **Semantics**: Preserves MLQL semantics that may not map to SQL

### Integration Strategy
1. **Keep both modes**: SQL for backward compatibility, Substrait for performance
2. **Minimal changes**: Only modify `query.rs` and `mcp.rs`
3. **Reuse existing**: SchemaProvider trait already defined in `mlql-ir/substrait`
4. **Test-driven**: Verify both modes produce identical results

## Next Steps

See `MCP_SUBSTRAIT_TODO.md` for detailed implementation plan.

**Ready for**:
- Task 2: Design new execution path
- Task 3: Add Substrait dependencies
- Task 4: Implement DuckDB SchemaProvider

---

**Analysis Complete**: 2025-10-08
**Analyzed by**: Claude Code
