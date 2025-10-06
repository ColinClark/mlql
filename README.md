# MLQL-RS

Rust implementation of MLQL (Machine Learning Query Language) - a domain-specific language designed for LLMs to communicate with SQL-based databases.

## Architecture

**MLQL → Substrait → DuckDB** (No SQL generation on hot path)

```
MLQL Text → AST → JSON IR → Substrait Plan → DuckDB → Arrow/JSON
```

### Key Design Decisions

1. **Substrait-based compilation**: We compile MLQL to Substrait logical plans instead of SQL
2. **DuckDB Substrait extension**: Execution via `from_substrait_json(?)`
3. **Canonical JSON IR**: Deterministic, serializable, cache-friendly
4. **No SQL injection surface**: Plans are protobuf/JSON, not string interpolation

## Workspace Structure

Multi-crate workspace:

- **mlql-ast**: MLQL grammar parser (Pest)
- **mlql-ir**: Canonical JSON IR with deterministic serialization
- **mlql-registry**: Function registry and policy definitions
- **mlql-substrait**: IR → Substrait encoder (prost protobufs)
- **mlql-duck**: DuckDB executor using Substrait extension
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

## Substrait Operator Mapping

| MLQL Operator | Substrait Relation |
|---------------|-------------------|
| `select`      | `ProjectRel`      |
| `filter`      | `FilterRel`       |
| `join`        | `JoinRel`         |
| `group`       | `AggregateRel`    |
| `sort`        | `SortRel`         |
| `take`        | `FetchRel`        |
| `knn`         | Custom extension  |
| `resample`    | Custom extension  |

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

## References

- [Design Spec](../docs/MLQL_to_Substrait_to_DuckDB_Technical_Design_Spec.pdf)
- [EBNF Grammar](../docs/EBNF.md)
- [Substrait Specification](https://substrait.io/)
- [DuckDB Substrait Extension](https://duckdb.org/docs/extensions/substrait)

## License

MIT OR Apache-2.0
