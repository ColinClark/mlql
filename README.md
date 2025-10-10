# MLQL-RS

Rust implementation of MLQL (Machine Learning Query Language) - a domain-specific language designed for LLMs to communicate with SQL-based databases.

## Architecture

**Two execution paths available:**

### 1. Substrait-based (Default - Production)
```
MLQL Text → AST → JSON IR → Substrait JSON → DuckDB → Arrow/JSON
```

### 2. SQL-based (Fallback)
```
MLQL Text → AST → JSON IR → SQL → DuckDB → Arrow/JSON
```

### Key Design Decisions

1. **Dual execution modes**: Substrait for portability (default), SQL for fallback
2. **Canonical JSON IR**: Deterministic, serializable, LLM-friendly, cache-friendly
3. **DuckDB 1.4.1 with Substrait**: Custom build with statically-linked Substrait extension
4. **JSON format for Substrait**: Uses `from_substrait_json()` for reliability
5. **SQL injection safe**: Parameterized values, no string interpolation

## Workspace Structure

Multi-crate workspace:

- **mlql-ast**: MLQL grammar parser (Pest)
- **mlql-ir**: Canonical JSON IR + Substrait translator
- **mlql-registry**: Function registry and policy definitions
- **mlql-duck**: DuckDB executor with IR-to-SQL translator
- **mlql-server**: MCP server (HTTP + SSE) with OpenAI integration

## Features

- ✅ **Pipeline syntax**: Unix-like pipes for data transformation
- ✅ **Substrait execution**: Portable query plans via JSON format
- ✅ **LLM integration**: Natural language → MLQL IR via OpenAI
- ✅ **MCP protocol**: Model Context Protocol server for Claude Desktop
- ✅ **Dual execution**: SQL and Substrait paths both working
- ✅ **Schema discovery**: Runtime catalog introspection
- ✅ **Arrow-native**: Streaming results via Arrow IPC

## Quick Start

### Build the project
```bash
# Build workspace
cargo build

# Run all tests (28 passing)
cargo test

# Run Substrait integration tests (8 passing)
env DUCKDB_CUSTOM_BUILD=1 cargo test -p mlql-ir --test substrait_operators
```

### Run the MCP Server

```bash
# Using the helper script (recommended)
./run_server.sh

# Or manually with custom DuckDB
env DUCKDB_CUSTOM_BUILD=1 \
  DYLD_LIBRARY_PATH=/Users/colin/Dev/duckdb-substrait-extension/build/release/src:$DYLD_LIBRARY_PATH \
  cargo run -p mlql-server
```

Server will start on `http://127.0.0.1:8080` with:
- MCP endpoint: `/mcp` (Streamable HTTP)
- SSE endpoint: `/sse` (Server-Sent Events)

### Configure Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "mlql": {
      "command": "npx",
      "args": ["mcp-remote", "http://127.0.0.1:8080/mcp"]
    }
  }
}
```

See `docs/claude-desktop-setup.md` for details.

## Example Queries

### Natural Language (via MCP)
```
"Show me the top 10 largest bank failures by assets"
"Count bank failures by state and show total assets"
"Show total failures, total assets, average assets, and largest failure"
```

### MLQL Syntax
```mlql
from sales s
| filter s.region == "EU"
| group by s.product_id { revenue: sum(s.price * s.qty) }
| sort -revenue
| take 10
```

### JSON IR (for LLMs)
```json
{
  "pipeline": {
    "source": {"type": "Table", "name": "bank_failures"},
    "ops": [
      {
        "op": "GroupBy",
        "keys": [{"column": "State"}],
        "aggs": {
          "total": {"func": "count"}
        }
      },
      {
        "op": "Sort",
        "keys": [{"expr": {"type": "Column", "col": {"column": "total"}}, "desc": true}]
      },
      {"op": "Take", "limit": 10}
    ]
  }
}
```

See `docs/llm-json-format.md` for complete JSON IR specification.

## Operator Support

### SQL-based Execution (28 tests ✅)

| MLQL Operator | SQL Translation        | Status |
|---------------|------------------------|--------|
| `select`      | `SELECT ... FROM`      | ✅     |
| `filter`      | `WHERE`                | ✅     |
| `sort`        | `ORDER BY`             | ✅     |
| `take`        | `LIMIT`                | ✅     |
| `join`        | `JOIN ... ON`          | ✅     |
| `group`       | `GROUP BY`             | ✅     |
| `distinct`    | `SELECT DISTINCT`      | ✅     |

**Aggregates**: count, sum, avg, min, max
**Joins**: INNER, LEFT, RIGHT, FULL, CROSS

### Substrait-based Execution (8 tests ✅)

| MLQL Operator | Substrait Relation | Status |
|---------------|-------------------|--------|
| `from`        | `ReadRel`         | ✅     |
| `filter`      | `FilterRel`       | ✅     |
| `select`      | `ProjectRel`      | ✅     |
| `sort`        | `SortRel`         | ✅     |
| `take`        | `FetchRel`        | ✅     |
| `group`       | `AggregateRel`    | ✅     |
| `join`        | `JoinRel`         | ✅     |
| `distinct`    | `AggregateRel`    | ✅     |

**Format**: JSON (via `from_substrait_json()`)
**Aggregates**: count, sum, avg, min, max
**Schema tracking**: Automatic through pipeline

## Environment Configuration

Create `.env` file:

```env
# OpenAI API (for natural language queries)
OPENAI_API_KEY=sk-...

# Server configuration
MLQL_SERVER_HOST=127.0.0.1
MLQL_SERVER_PORT=8080

# Execution mode (defaults to substrait if not set)
# Set to "sql" to use SQL-based execution fallback
# MLQL_EXECUTION_MODE=sql

# Custom DuckDB with Substrait (required for substrait mode)
DUCKDB_CUSTOM_BUILD=1
SUBSTRAIT_EXTENSION_PATH=/Users/colin/Dev/duckdb-substrait-extension/build/release/package/extensions/substrait.duckdb_extension
```

## Custom DuckDB Build

The Substrait execution requires DuckDB 1.4.1 with statically-linked Substrait extension:

```bash
cd /Users/colin/Dev/duckdb-substrait-extension

# Build statically-linked DuckDB with Substrait
EXTENSION_STATIC_BUILD=1 make release

# Artifacts:
# - build/release/duckdb (CLI binary)
# - build/release/src/libduckdb.dylib (library for Rust)
```

The `.cargo/config.toml` configures Rust to link against this custom library when `DUCKDB_CUSTOM_BUILD=1` is set.

## Development

### Running Tests

```bash
# All tests
cargo test

# Specific crate
cargo test -p mlql-duck
cargo test -p mlql-ir

# Substrait tests (requires custom DuckDB)
env DUCKDB_CUSTOM_BUILD=1 cargo test -p mlql-ir --test substrait_operators -- --show-output
```

### Code Quality

```bash
# Format
cargo fmt

# Lint
cargo clippy --all-targets --all-features

# Auto-fix
cargo clippy --fix --all-targets --all-features
```

### Commit Messages

Follow conventional commits:
- `feat(duck): implement JOIN operator`
- `fix(ir): schema tracking in GroupBy`
- `docs: update README with Substrait execution`
- `test(substrait): add GroupBy integration test`

## Key Files

### Core Implementation
- `crates/mlql-ast/src/parser.rs` - Pest grammar for MLQL text
- `crates/mlql-ir/src/types.rs` - IR type definitions
- `crates/mlql-ir/src/substrait/translator.rs` - IR → Substrait translator
- `crates/mlql-duck/src/lib.rs` - IR → SQL translator

### Server
- `crates/mlql-server/src/main.rs` - HTTP server and routing
- `crates/mlql-server/src/mcp.rs` - MCP protocol implementation
- `crates/mlql-server/src/llm.rs` - OpenAI integration (NL → IR)
- `crates/mlql-server/src/query.rs` - Query execution (SQL + Substrait)
- `crates/mlql-server/src/catalog.rs` - DuckDB schema provider

### Documentation
- `docs/llm-json-format.md` - JSON IR format for LLMs
- `docs/claude-desktop-setup.md` - Claude Desktop MCP setup
- `CLAUDE.md` - Development session notes
- `README.md` - This file

## Architecture Deep Dive

### Substrait Execution Flow

1. **Natural Language** → OpenAI GPT-4o-mini → **MLQL JSON IR**
2. **JSON IR** → SubstraitTranslator → **Substrait Plan** (JSON format)
3. **Substrait JSON** → DuckDB `from_substrait_json()` → **Results**

### Schema Tracking

The translator maintains schema context through the pipeline:
- After `GroupBy`: `[grouping_keys..., aggregate_aliases...]`
- After `Select`: `[projected_column_names...]`
- After `Join`: `[left_columns..., right_columns...]`
- Other operators preserve schema

This enables correct field resolution in subsequent operators (e.g., sorting by aggregate columns).

### Why JSON Format?

- **Reliability**: Binary protobuf format had hanging issues on macOS
- **Debugging**: Human-readable plans for troubleshooting
- **Compatibility**: Works with DuckDB's `from_substrait_json()`
- **Validation**: Easier to inspect and validate plans

## Troubleshooting

### Server won't start

- Check `DUCKDB_CUSTOM_BUILD=1` is set
- Verify `DYLD_LIBRARY_PATH` points to custom DuckDB library
- Check OpenAI API key is in `.env` file (not shell env)

### Queries hang

- Ensure using JSON format (`from_substrait_json()`)
- Check DuckDB version matches (1.4.1)
- Verify Substrait extension is statically linked

### Schema errors

- Check schema tracking in translator.rs:350-395
- Verify `current_schema` updates after each operator
- Test with simpler single-operator queries first

## Contributing

1. Write tests first (TDD)
2. Run tests after each change
3. Update documentation
4. Follow commit message conventions
5. Keep CLAUDE.md updated with learnings

## References

- [Substrait Specification](https://substrait.io/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [DuckDB Substrait Extension](https://duckdb.org/docs/extensions/substrait)
- [Model Context Protocol](https://modelcontextprotocol.io/)
- [OpenAI API](https://platform.openai.com/docs/api-reference)

## License

[Your License Here]
