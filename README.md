# MLQL-RS

Rust implementation of MLQL (Machine Learning Query Language) - a domain-specific language designed for LLMs to communicate with SQL-based databases.

## Architecture

**MLQL → SQL → DuckDB** (Clean and simple)

```
MLQL Text → AST → JSON IR → SQL → DuckDB → Arrow/JSON
```

### Key Design Decisions

1. **Direct SQL compilation**: We compile MLQL IR to SQL for execution
2. **DuckDB native**: Direct SQL execution - no extension dependencies
3. **Canonical JSON IR**: Deterministic, serializable, cache-friendly
4. **SQL injection safe**: Parameterized values, no string interpolation

## Workspace Structure

Multi-crate workspace:

- **mlql-ast**: MLQL grammar parser (Pest)
- **mlql-ir**: Canonical JSON IR with deterministic serialization
- **mlql-registry**: Function registry and policy definitions
- **mlql-substrait**: IR → Substrait encoder (for future compatibility)
- **mlql-duck**: DuckDB executor with IR-to-SQL translator
- **mlql-server**: HTTP API (Axum)

## Features

- **Pipeline syntax**: Unix-like pipes for data transformation
- **ML primitives**: Vector search (KNN), graph traversal, time-series
- **Resource governance**: Memory/time budgets, query interrupts
- **Policy enforcement**: PII masking, row-level security
- **Deterministic caching**: SHA-256 plan fingerprinting
- **Arrow-native**: Streaming results via Arrow IPC

## Quick Start

```bash
# Build workspace
cargo build

# Run tests
cargo test

# Build server
cargo build -p mlql-server

# Run example
cargo run --example run_pipeline
```

## Example Queries

### Basic Pipeline
```mlql
from sales s
| filter s.region == "EU"
| group by s.product_id { revenue: sum(s.price * s.qty) }
| sort -revenue
| take 10
```

### Vector Search
```mlql
from documents
| knn q: <0.1, 0.2, 0.3> k: 10 index: "embedding_idx" metric: "cosine"
| select [doc_id, title, similarity]
```

### With Policies
```mlql
pragma { timeout: 30000, max_memory: "1GB" }

from customers c
| join from orders o on c.id == o.customer_id
| select [mask(c.email) as email, o.total]
| filter o.total > 100
```

## SQL Operator Mapping

| MLQL Operator | SQL Translation        | Status |
|---------------|------------------------|--------|
| `select`      | `SELECT ... FROM`      | ✅     |
| `filter`      | `WHERE`                | ✅     |
| `sort`        | `ORDER BY`             | ✅     |
| `take`        | `LIMIT`                | ✅     |
| `join`        | `JOIN ... ON`          | 🚧     |
| `group`       | `GROUP BY ... HAVING`  | 🚧     |
| `distinct`    | `DISTINCT`             | 🚧     |
| `knn`         | DuckDB vector search   | 📋     |
| `resample`    | Window + aggregate     | 📋     |

## Development

```bash
# Format
cargo fmt

# Lint
cargo clippy --all-targets --all-features

# Test specific crate
cargo test -p mlql-substrait

# Benchmark
cargo bench
```

## HTTP API

```bash
# Start server
cargo run -p mlql-server

# Execute query
curl -X POST http://localhost:3000/v1/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "from users | filter is_active == true | take 10"}'
```

## Roadmap

- **v0.1**: Core operators, Arrow/JSON results, masking
- **v0.2**: Window functions, set operations
- **v0.3**: KNN, resample, caching
- **v0.4**: Streaming, provenance tracking

## Current Status

**✅ Working:**
- MLQL parser with Pest grammar
- AST → IR conversion
- IR → SQL translation for: select, filter, sort, take
- Wildcard (*) support
- End-to-end test: `from users | select [*]`

**🚧 In Progress:**
- Additional operators (join, groupby, distinct)
- Expression types (comparison, functions, aggregates)

**📋 Planned:**
- HTTP server API
- Policy enforcement (budgets, masking)
- Vector search (KNN)
- Streaming results

## References

- [EBNF Grammar](../docs/EBNF.md)
- [DuckDB Documentation](https://duckdb.org/docs/)

## License

**Creative Commons Attribution-NonCommercial 4.0 International (CC BY-NC 4.0)**

This work is licensed under a Creative Commons Attribution-NonCommercial 4.0 International License.

- ✅ **Free for non-commercial use** - Research, education, personal projects
- ❌ **Commercial use requires a license** - Contact for licensing inquiries
- 📧 **Commercial licensing**: Contact repository owner

For the full license text, see [LICENSE](LICENSE) or visit:
https://creativecommons.org/licenses/by-nc/4.0/
