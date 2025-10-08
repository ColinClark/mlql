# Substrait Execution Path Design

**Branch**: `mcp-mlql-substrait`
**Date**: 2025-10-08

## Target Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Natural   │────▶│   OpenAI    │────▶│  MLQL IR    │────▶│  Substrait  │────▶│   DuckDB    │
│  Language   │     │  (GPT-4o)   │     │   (JSON)    │     │    Plan     │     │from_substrait│
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
      │                    │                    │                    │                    │
   main.rs              llm.rs               mcp.rs          query.rs +          DuckDB 1.4
                                                          substrait translator     + extension
```

## New Execution Flow

### Phase 1: Natural Language → MLQL IR (Unchanged)

**Component**: `llm.rs::natural_language_to_ir_with_catalog()`

**Input**: "Show me users over age 25"

**Output**:
```json
{
  "pipeline": {
    "source": {"type": "Table", "name": "users"},
    "ops": [{
      "op": "Filter",
      "condition": {
        "type": "BinaryOp",
        "op": "Gt",
        "left": {"type": "Column", "col": {"column": "age"}},
        "right": {"type": "Literal", "value": 25}
      }
    }]
  }
}
```

### Phase 2: MLQL IR → Substrait Plan (NEW)

**Component**: `query.rs::execute_ir_substrait()`

**Step 1**: Initialize SchemaProvider
```rust
// Query DuckDB catalog for schema
let schema_provider = DuckDbSchemaProvider::new(&conn);
```

**Step 2**: Create Substrait Translator
```rust
use mlql_ir::substrait::SubstraitTranslator;

let translator = SubstraitTranslator::new(&schema_provider);
```

**Step 3**: Translate IR → Substrait Plan
```rust
let program = Program {
    pragma: None,
    lets: vec![],
    pipeline: pipeline.clone(),
};

let plan = translator.translate(&program)
    .map_err(|e| format!("Substrait translation error: {}", e))?;
```

**Step 4**: Serialize to protobuf bytes
```rust
use prost::Message;

let mut plan_bytes = Vec::new();
plan.encode(&mut plan_bytes)
    .map_err(|e| format!("Protobuf encoding error: {}", e))?;
```

**Output**: Binary protobuf plan (e.g., 306 bytes for simple filter)

### Phase 3: Substrait Plan → DuckDB Execution (NEW)

**Component**: `query.rs::execute_ir_substrait()`

**Step 1**: Load Substrait extension (if not already loaded)
```rust
conn.execute_batch("
    LOAD '/path/to/substrait.duckdb_extension';
").ok(); // Ignore error if already loaded
```

**Step 2**: Execute via from_substrait()
```rust
let mut stmt = conn.prepare("SELECT * FROM from_substrait(?)")?;
let results = stmt.query_arrow([plan_bytes])?;
```

**Step 3**: Convert Arrow to JSON (same as current)
```rust
let json_result = arrow_to_json(&results)?;
Ok((format!("Substrait plan: {} bytes", plan_bytes.len()), json_result))
```

**Output**:
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

## Component Specifications

### 1. DuckDbSchemaProvider (NEW)

**File**: `crates/mlql-server/src/schema.rs`

**Purpose**: Implement `SchemaProvider` trait by querying DuckDB catalog

**Interface** (from `mlql-ir/substrait`):
```rust
pub trait SchemaProvider {
    fn get_table_schema(&self, table_name: &str) -> Result<TableSchema, String>;
}

pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
}

pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}
```

**Implementation**:
```rust
use mlql_ir::substrait::{SchemaProvider, TableSchema, ColumnInfo};
use duckdb::Connection;
use std::sync::Arc;

pub struct DuckDbSchemaProvider {
    conn: Arc<Connection>,
}

impl DuckDbSchemaProvider {
    pub fn new(conn: Arc<Connection>) -> Self {
        Self { conn }
    }
}

impl SchemaProvider for DuckDbSchemaProvider {
    fn get_table_schema(&self, table_name: &str) -> Result<TableSchema, String> {
        // Query information_schema
        let mut stmt = self.conn.prepare("
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = ?
            ORDER BY ordinal_position
        ").map_err(|e| format!("Schema query failed: {}", e))?;

        let columns: Vec<ColumnInfo> = stmt
            .query_map([table_name], |row| {
                Ok(ColumnInfo {
                    name: row.get(0)?,
                    data_type: row.get(1)?,
                    nullable: row.get::<_, String>(2)? == "YES",
                })
            })
            .map_err(|e| format!("Failed to read schema: {}", e))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Row parsing failed: {}", e))?;

        if columns.is_empty() {
            return Err(format!("Table '{}' not found", table_name));
        }

        Ok(TableSchema {
            name: table_name.to_string(),
            columns,
        })
    }
}
```

**Caching Strategy** (Optional, for performance):
```rust
use std::collections::HashMap;
use std::sync::RwLock;

pub struct CachedSchemaProvider {
    inner: DuckDbSchemaProvider,
    cache: RwLock<HashMap<String, TableSchema>>,
}

impl SchemaProvider for CachedSchemaProvider {
    fn get_table_schema(&self, table_name: &str) -> Result<TableSchema, String> {
        // Check cache first
        if let Some(schema) = self.cache.read().unwrap().get(table_name) {
            return Ok(schema.clone());
        }

        // Query database
        let schema = self.inner.get_table_schema(table_name)?;

        // Cache result
        self.cache.write().unwrap().insert(table_name.to_string(), schema.clone());

        Ok(schema)
    }
}
```

### 2. execute_ir_substrait() (NEW)

**File**: `crates/mlql-server/src/query.rs`

**Signature**:
```rust
pub async fn execute_ir_substrait(
    pipeline: Pipeline,
    database: Option<String>,
) -> Result<(String, serde_json::Value), Box<dyn std::error::Error>>
```

**Implementation**:
```rust
use mlql_ir::{Pipeline, Program};
use mlql_ir::substrait::SubstraitTranslator;
use crate::schema::DuckDbSchemaProvider;
use duckdb::Connection;
use prost::Message;
use std::sync::Arc;

pub async fn execute_ir_substrait(
    pipeline: Pipeline,
    database: Option<String>,
) -> Result<(String, serde_json::Value), Box<dyn std::error::Error>> {
    // 1. Open DuckDB connection
    let conn = if let Some(db_path) = database {
        Connection::open(db_path)?
    } else {
        Connection::open_in_memory()?
    };
    let conn = Arc::new(conn);

    // 2. Load Substrait extension
    load_substrait_extension(&conn)?;

    // 3. Create schema provider
    let schema_provider = DuckDbSchemaProvider::new(conn.clone());

    // 4. Initialize translator
    let translator = SubstraitTranslator::new(&schema_provider);

    // 5. Convert Pipeline to Program
    let program = Program {
        pragma: None,
        lets: vec![],
        pipeline: pipeline.clone(),
    };

    // 6. Translate to Substrait
    let plan = translator.translate(&program)
        .map_err(|e| format!("Substrait translation failed: {}", e))?;

    // 7. Serialize to protobuf
    let mut plan_bytes = Vec::new();
    plan.encode(&mut plan_bytes)
        .map_err(|e| format!("Protobuf encoding failed: {}", e))?;

    // 8. Execute via from_substrait()
    let mut stmt = conn.prepare("SELECT * FROM from_substrait(?)")?;
    let mut rows = stmt.query([plan_bytes.as_slice()])?;

    // 9. Convert to JSON (reuse existing logic)
    let json_result = rows_to_json(&mut rows)?;

    // 10. Return plan info + results
    let plan_info = format!("Substrait plan: {} bytes", plan_bytes.len());
    Ok((plan_info, json_result))
}

fn load_substrait_extension(conn: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    // Try to load extension, ignore if already loaded
    let extension_path = std::env::var("SUBSTRAIT_EXTENSION_PATH")
        .unwrap_or_else(|_| "/Users/colin/Dev/truepop/mlql/duckdb-substrait-upgrade/build/release/extension/substrait/substrait.duckdb_extension".to_string());

    conn.execute_batch(&format!("LOAD '{}'", extension_path)).ok();

    Ok(())
}

fn rows_to_json(rows: &mut duckdb::Rows) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    // Convert DuckDB rows to JSON
    // TODO: Implement similar to current result_to_json()
    todo!("Implement rows_to_json")
}
```

### 3. Execution Mode Configuration (NEW)

**File**: `.env` or environment variables

**New Variables**:
```env
# Execution mode: "sql" or "substrait"
MLQL_EXECUTION_MODE=substrait

# Path to Substrait extension (optional, has sensible default)
SUBSTRAIT_EXTENSION_PATH=/path/to/substrait.duckdb_extension
```

**File**: `crates/mlql-server/src/query.rs`

**Enum**:
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    Sql,
    Substrait,
}

impl ExecutionMode {
    pub fn from_env() -> Self {
        match std::env::var("MLQL_EXECUTION_MODE")
            .unwrap_or_else(|_| "sql".to_string())
            .to_lowercase()
            .as_str()
        {
            "substrait" => ExecutionMode::Substrait,
            _ => ExecutionMode::Sql,
        }
    }
}
```

**Dispatcher**:
```rust
pub async fn execute_ir_auto(
    pipeline: Pipeline,
    database: Option<String>,
) -> Result<(String, serde_json::Value), Box<dyn std::error::Error>> {
    match ExecutionMode::from_env() {
        ExecutionMode::Substrait => execute_ir_substrait(pipeline, database).await,
        ExecutionMode::Sql => execute_ir(pipeline, database).await,
    }
}
```

### 4. MCP Handler Update (MODIFIED)

**File**: `crates/mlql-server/src/mcp.rs`

**Change** in `handle_query_tool()`:
```rust
// OLD:
let (sql, results) = query::execute_ir(ir.clone(), database).await?;

// NEW:
let (execution_info, results) = query::execute_ir_auto(ir.clone(), database).await?;

// Update response format
let response_text = format!(
    "Query: {}\n\nGenerated IR:\n{}\n\nExecution: {}\n\nResults:\n{}",
    query,
    serde_json::to_string_pretty(&ir)?,
    execution_info,  // "Substrait plan: 306 bytes" or "SQL: SELECT..."
    serde_json::to_string_pretty(&results)?
);
```

## Error Handling Strategy

### 1. Translation Errors

**Scenario**: MLQL IR → Substrait translation fails

**Handling**:
```rust
let plan = translator.translate(&program)
    .map_err(|e| {
        tracing::error!("Substrait translation failed: {}", e);
        format!("Failed to translate query to Substrait: {}", e)
    })?;
```

**User-facing error**:
```
Failed to translate query to Substrait: Unsupported operator 'Window' not yet implemented
```

### 2. Extension Loading Errors

**Scenario**: Substrait extension not found or fails to load

**Handling**:
```rust
fn load_substrait_extension(conn: &Connection) -> Result<(), String> {
    let path = std::env::var("SUBSTRAIT_EXTENSION_PATH")
        .unwrap_or_else(|_| DEFAULT_EXTENSION_PATH.to_string());

    if !std::path::Path::new(&path).exists() {
        return Err(format!("Substrait extension not found at: {}", path));
    }

    conn.execute_batch(&format!("LOAD '{}'", path))
        .map_err(|e| format!("Failed to load Substrait extension: {}", e))?;

    Ok(())
}
```

**User-facing error**:
```
Substrait extension not found at: /path/to/substrait.duckdb_extension

Please build the extension or set SUBSTRAIT_EXTENSION_PATH environment variable.
```

### 3. Execution Errors

**Scenario**: DuckDB fails to execute Substrait plan

**Handling**:
```rust
let mut stmt = conn.prepare("SELECT * FROM from_substrait(?)")
    .map_err(|e| format!("Failed to prepare Substrait query: {}", e))?;

let mut rows = stmt.query([plan_bytes.as_slice()])
    .map_err(|e| {
        tracing::error!("Substrait execution failed: {}", e);
        format!("DuckDB execution failed: {}", e)
    })?;
```

**User-facing error**:
```
DuckDB execution failed: Binder Error: Referenced column "xyz" not found in table
```

### 4. Fallback Strategy (Optional)

**Configuration**:
```env
MLQL_EXECUTION_MODE=substrait
MLQL_FALLBACK_TO_SQL=true
```

**Implementation**:
```rust
pub async fn execute_ir_with_fallback(
    pipeline: Pipeline,
    database: Option<String>,
) -> Result<(String, serde_json::Value), Box<dyn std::error::Error>> {
    // Try Substrait first
    match execute_ir_substrait(pipeline.clone(), database.clone()).await {
        Ok(result) => Ok(result),
        Err(e) => {
            tracing::warn!("Substrait execution failed: {}, falling back to SQL", e);

            // Fallback to SQL
            execute_ir(pipeline, database).await
        }
    }
}
```

## Performance Considerations

### 1. Schema Caching

**Problem**: Querying `information_schema` on every request is slow

**Solution**: Cache schemas in `DuckDbSchemaProvider`
```rust
use std::collections::HashMap;
use std::sync::RwLock;

pub struct DuckDbSchemaProvider {
    conn: Arc<Connection>,
    cache: RwLock<HashMap<String, TableSchema>>,
}
```

**Impact**: 50-100ms saved per query for large databases

### 2. Connection Pooling

**Problem**: Opening new DuckDB connection per request is expensive

**Solution**: Use `r2d2` connection pool
```rust
use r2d2::Pool;
use r2d2_duckdb::DuckdbConnectionManager;

pub struct QueryExecutor {
    pool: Pool<DuckdbConnectionManager>,
}
```

**Note**: Deferred to future work (DuckDB Connection is not `Send + Sync`)

### 3. Extension Loading

**Problem**: Loading extension on every connection is redundant

**Solution**: Load once, reuse connection
```rust
static EXTENSION_LOADED: AtomicBool = AtomicBool::new(false);

fn load_substrait_extension_once(conn: &Connection) -> Result<(), String> {
    if EXTENSION_LOADED.load(Ordering::Relaxed) {
        return Ok(());
    }

    conn.execute_batch(&format!("LOAD '{}'", extension_path))?;
    EXTENSION_LOADED.store(true, Ordering::Relaxed);

    Ok(())
}
```

## Testing Strategy

### Unit Tests

**File**: `crates/mlql-server/src/schema.rs`
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_provider_reads_tables() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)").unwrap();

        let provider = DuckDbSchemaProvider::new(Arc::new(conn));
        let schema = provider.get_table_schema("users").unwrap();

        assert_eq!(schema.name, "users");
        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.columns[0].name, "id");
        assert_eq!(schema.columns[0].data_type, "INTEGER");
    }
}
```

### Integration Tests

**File**: `crates/mlql-server/tests/substrait_execution.rs`
```rust
#[tokio::test]
async fn test_substrait_execution_simple_filter() {
    // Setup database
    let db_path = "/tmp/test_substrait.db";
    let conn = Connection::open(db_path).unwrap();
    conn.execute_batch("
        CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER);
        INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 20);
    ").unwrap();

    // Create pipeline
    let pipeline = Pipeline {
        source: Source::Table { name: "users".to_string(), alias: None },
        ops: vec![
            Operator::Filter {
                condition: Expr::BinaryOp {
                    op: BinaryOp::Gt,
                    left: Box::new(Expr::Column { col: ColumnRef { table: None, column: "age".to_string() } }),
                    right: Box::new(Expr::Literal { value: json!(25) }),
                },
            },
        ],
    };

    // Execute
    let (info, results) = execute_ir_substrait(pipeline, Some(db_path.to_string())).await.unwrap();

    // Verify
    assert!(info.contains("Substrait plan"));
    assert_eq!(results["row_count"], 1);
    assert_eq!(results["rows"][0]["name"], "Alice");
}
```

### End-to-End Tests

**Manual test**:
```bash
# Terminal 1: Start server
MLQL_EXECUTION_MODE=substrait cargo run -p mlql-server

# Terminal 2: Query via MCP
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "Show me users over age 25"}'

# Verify response includes "Substrait plan: X bytes"
```

## Migration Path

### Phase 1: Build Foundation (Tasks 3-4)
- Add dependencies
- Implement `DuckDbSchemaProvider`
- Unit test schema provider

### Phase 2: Implement Substrait Path (Tasks 5-6)
- Implement `execute_ir_substrait()`
- Load extension logic
- Integration tests

### Phase 3: Configuration & Integration (Tasks 7-8)
- Add `ExecutionMode` enum
- Implement `execute_ir_auto()` dispatcher
- Update MCP handler

### Phase 4: Testing & Polish (Tasks 9-12)
- End-to-end tests
- Error handling
- Documentation
- Production testing with demo.duckdb

### Phase 5: Deployment
- Update `.env.example` with new variables
- Document extension installation
- Performance benchmarking

## Success Metrics

1. ✅ All MLQL operators work via Substrait path
2. ✅ Results match SQL-based execution exactly
3. ✅ Error messages are clear and actionable
4. ✅ Performance is comparable or better than SQL
5. ✅ Both SQL and Substrait modes coexist
6. ✅ Documentation is complete

---

**Design Complete**: 2025-10-08
**Ready for Implementation**: Tasks 3-12
