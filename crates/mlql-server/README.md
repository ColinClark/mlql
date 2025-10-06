# MLQL Server

HTTP server that accepts natural language queries, converts them to MLQL IR using OpenAI, executes them against DuckDB, and returns JSON results.

## Features

- **Natural Language to SQL**: Uses OpenAI GPT-4o-mini to convert natural language queries into MLQL IR
- **Error Retry Loop**: Automatically retries failed IR generation/parsing by feeding errors back to the LLM
- **DuckDB Execution**: Executes MLQL IR against in-memory DuckDB databases
- **JSON API**: Simple HTTP REST API with JSON request/response

## Architecture

```
Natural Language → OpenAI (GPT-4o-mini) → MLQL IR → DuckDB SQL → Results
                        ↑__________________________|
                        (Error feedback loop)
```

### Error Handling Loop

The server implements a sophisticated error handling loop:

1. **Parse Errors**: If OpenAI generates invalid JSON, the error is fed back into the conversation with instructions to fix it (up to 3 attempts)
2. **Execution Errors**: If the IR fails to execute, the query is retried (up to 2 attempts)

This ensures robust query translation even when the LLM makes mistakes.

## API Endpoints

### `GET /health`

Health check endpoint.

**Response:**
```json
{
  "status": "healthy",
  "version": "0.1.0"
}
```

### `POST /query`

Execute a natural language query.

**Request:**
```json
{
  "query": "Show me all users over age 25",
  "database": "optional_db_name"
}
```

**Response:**
```json
{
  "ir": {
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
        }
      ]
    }
  },
  "sql": "SELECT * FROM users WHERE (age > 25)",
  "results": {
    "columns": ["id", "name", "age"],
    "rows": [
      {"id": 1, "name": "Alice", "age": 30},
      {"id": 2, "name": "Bob", "age": 28}
    ],
    "row_count": 2
  }
}
```

**Error Response:**
```json
{
  "error": "Failed to convert query to MLQL IR",
  "details": "Parse error details..."
}
```

## Setup

### Prerequisites

- Rust 1.70+
- OpenAI API key

### Configuration

Create a `.env` file in the project root:

```env
OPENAI_API_KEY=your_openai_api_key_here
MLQL_SERVER_HOST=127.0.0.1
MLQL_SERVER_PORT=8080
```

Or copy from `.env.example`:

```bash
cp .env.example .env
# Edit .env with your API key
```

### Running

```bash
cargo run -p mlql-server
```

The server will start on `http://127.0.0.1:8080` (or your configured host/port).

## Example Usage

### Using curl

```bash
# Health check
curl http://localhost:8080/health

# Query
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Count users by city"
  }'
```

### Using httpie

```bash
# Query
http POST localhost:8080/query query="Show me users over 25, sorted by age"
```

## LLM Prompt Engineering

The server uses a comprehensive system prompt that teaches OpenAI how to generate valid MLQL IR. The prompt includes:

- Complete MLQL IR JSON schema
- All supported operators (Filter, Select, Sort, Take, Distinct, GroupBy, Join)
- Binary operators (arithmetic, comparison, logical)
- Multiple examples for each operator type
- Strict JSON formatting requirements

See `src/llm.rs` for the full prompt.

## Supported Query Types

The server supports all core SQL operations through natural language:

- **Filtering**: "users over 25", "names starting with 'A'"
- **Projection**: "show me just the name and age", "age times 2 as double_age"
- **Sorting**: "sorted by age descending"
- **Limiting**: "top 10 results"
- **Distinct**: "unique cities"
- **Grouping**: "count users by city", "average price by product"
- **Joining**: "users with their orders", "products with sales"

## Limitations

- **DuckDB Threading**: Currently creates a new DuckDB executor per request (DuckDB Connection is not `Send + Sync`)
- **In-Memory Only**: No persistent database support yet
- **No Schema Discovery**: LLM must infer table schemas from natural language
- **No SET Operations**: UNION/EXCEPT/INTERSECT deferred (architectural limitation)

## Future Improvements

- [ ] Connection pooling for DuckDB
- [ ] Schema catalog for table/column discovery
- [ ] Feed execution errors back to LLM for correction
- [ ] Streaming results (JSONL)
- [ ] MCP (Model Context Protocol) server implementation
- [ ] Persistent database support
- [ ] Query caching based on IR fingerprint
- [ ] Rate limiting and API authentication

## Development

### Running Tests

```bash
cargo test -p mlql-server
```

### Project Structure

```
crates/mlql-server/
├── src/
│   ├── main.rs       # HTTP server and routing
│   ├── llm.rs        # OpenAI integration
│   └── query.rs      # DuckDB query execution
├── Cargo.toml
└── README.md
```

## License

MIT OR Apache-2.0
