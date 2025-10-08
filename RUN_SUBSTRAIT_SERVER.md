# Running MCP Server with Substrait Execution

**Date**: 2025-10-08
**Branch**: `mcp-mlql-substrait`

## Quick Start

```bash
cd /Users/colin/Dev/truepop/mlql/mlql-rs

# Run the server with Substrait mode enabled
cargo run -p mlql-server
```

The server will:
- ✅ Use Substrait execution (configured in `.env`)
- ✅ Connect to `data/demo.duckdb` database (default for MCP query tool)
- ✅ Load Substrait extension automatically from system DuckDB
- ✅ Listen on `http://127.0.0.1:8080`

## Configuration

Your `.env` file is already configured:

```env
MLQL_EXECUTION_MODE=substrait  # ← Substrait mode enabled!
MLQL_SERVER_HOST=127.0.0.1
MLQL_SERVER_PORT=8080
OPENAI_API_KEY=<your key>
```

### Optional Configuration

If the system DuckDB doesn't have Substrait, you can specify the extension path:

```env
SUBSTRAIT_EXTENSION_PATH=/Users/colin/Dev/truepop/mlql/duckdb-substrait-upgrade/build/release/extension/substrait/substrait.duckdb_extension
```

But this is **not needed** - your system DuckDB already has it installed!

## Verify Substrait Extension

Test that system DuckDB can use Substrait:

```bash
duckdb -c "LOAD substrait; SELECT 'Substrait loaded!' as status;"
```

**Expected output**: "Substrait loaded!"

## Database Setup

Your demo database is ready:

```bash
ls -lh data/demo.duckdb
# -rw-r--r--  1 colin  staff  4.9M Oct  7 09:54 data/demo.duckdb
```

To see what tables are in it:

```bash
duckdb data/demo.duckdb -c "SHOW TABLES;"
```

## Testing the Server

### Option 1: Health Check

```bash
curl http://localhost:8080/health
```

**Expected**: `{"status":"healthy","version":"0.1.0"}`

### Option 2: MCP Query Tool (via curl)

```bash
# Simple query via MCP protocol
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Show me all records from the first table",
    "database": "data/demo.duckdb"
  }'
```

**Expected response format**:
```json
{
  "Query": "Show me all records...",
  "Generated IR": { ... MLQL IR ... },
  "Execution": "Substrait plan: XXX bytes",  ← Confirms Substrait!
  "Results": { "columns": [...], "rows": [...] }
}
```

### Option 3: Direct Integration Test

Use the existing Substrait integration tests:

```bash
# Test with environment variable for custom DuckDB
env DUCKDB_CUSTOM_BUILD=1 cargo test --package mlql-ir --test substrait_operators -- --show-output
```

All 8 tests should pass:
- ✅ test_table_scan
- ✅ test_take_limit
- ✅ test_plan_generation
- ✅ test_combined_pipeline
- ✅ test_distinct
- ✅ test_groupby
- ✅ test_join
- ✅ test_all_aggregates

## How It Works

### Architecture Flow

```
Natural Language Query (via MCP)
    ↓
OpenAI GPT-4 (converts to MLQL IR)
    ↓
MLQL IR (JSON)
    ↓
SubstraitTranslator (crates/mlql-ir/src/substrait/translator.rs)
    ↓
Substrait Plan (protobuf, ~300 bytes for simple queries)
    ↓
DuckDB from_substrait() function
    ↓
Query Results (JSON)
```

### Code Path

1. **Entry**: `crates/mlql-server/src/main.rs` - Starts MCP server
2. **LLM**: `crates/mlql-server/src/llm.rs` - Converts natural language → MLQL IR (unchanged)
3. **MCP Handler**: `crates/mlql-server/src/mcp.rs` - Calls `query::execute_ir_auto()`
4. **Dispatcher**: `crates/mlql-server/src/query.rs::execute_ir_auto()` - Checks `MLQL_EXECUTION_MODE`
5. **Substrait Execution**: `crates/mlql-server/src/query.rs::execute_ir_substrait()`:
   - Opens DuckDB connection
   - Creates `DuckDbSchemaProvider` (queries information_schema)
   - Initializes `SubstraitTranslator`
   - Translates IR → Substrait Plan
   - Serializes to protobuf bytes
   - Executes: `SELECT * FROM from_substrait(?)`
   - Converts results to JSON

### Schema Discovery

The `DuckDbSchemaProvider` queries DuckDB catalog at runtime:

```sql
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = ?
ORDER BY ordinal_position
```

This means **no schema files needed** - schemas are discovered dynamically!

## Troubleshooting

### Error: "Table 'xyz' not found"

The `DuckDbSchemaProvider` queries the database catalog. Make sure:
1. Database file exists: `ls data/demo.duckdb`
2. Table exists in database: `duckdb data/demo.duckdb -c "SHOW TABLES;"`

### Error: "Substrait extension not found"

If you see this error, set the extension path:

```bash
export SUBSTRAIT_EXTENSION_PATH=/Users/colin/Dev/truepop/mlql/duckdb-substrait-upgrade/build/release/extension/substrait/substrait.duckdb_extension
cargo run -p mlql-server
```

But this shouldn't happen - your system DuckDB already has it!

### Error: "Failed to execute query"

Check the server logs for details:
```bash
RUST_LOG=info cargo run -p mlql-server
```

Look for lines like:
- `INFO mlql_server: Generated IR: ...`
- `INFO mlql_server: Execution info: Substrait plan: XXX bytes`
- `INFO mlql_server: Query results: N rows`

### Error: "OpenAI API key not found"

Make sure `.env` has your API key:
```bash
grep OPENAI_API_KEY .env
```

## Switching Between SQL and Substrait

To go back to SQL mode:

```bash
# Option 1: Edit .env
# Change: MLQL_EXECUTION_MODE=substrait
# To:     MLQL_EXECUTION_MODE=sql

# Option 2: Override via environment
MLQL_EXECUTION_MODE=sql cargo run -p mlql-server
```

Both modes should produce **identical results** - Substrait is just a different execution path!

## Performance Comparison

You can benchmark SQL vs Substrait:

```bash
# Terminal 1: SQL mode
MLQL_EXECUTION_MODE=sql cargo run -p mlql-server

# Terminal 2: Send query and measure time
time curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "Count all records", "database": "data/demo.duckdb"}'
```

Then repeat with `MLQL_EXECUTION_MODE=substrait`.

Expected: Substrait should be **comparable or faster** since it skips SQL parsing.

## Next Steps

1. **Start the server**: `cargo run -p mlql-server`
2. **Test with curl**: Send a query via HTTP
3. **Verify Substrait**: Check response shows "Substrait plan: XXX bytes"
4. **Compare results**: Try same query in SQL mode and Substrait mode

## References

- **Substrait Spec**: https://substrait.io/
- **DuckDB Substrait Extension**: https://github.com/substrait-io/duckdb-substrait-extension
- **MLQL IR Format**: `docs/llm-json-format.md`
- **Implementation Plan**: `MCP_SUBSTRAIT_TODO.md`
- **Design Doc**: `SUBSTRAIT_EXECUTION_DESIGN.md`

---

**Ready to run!** 🚀
