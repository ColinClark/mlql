# MLQL Server Quick Start

Get started with the MLQL HTTP server in 5 minutes.

## Prerequisites

- Rust 1.70+
- OpenAI API key ([get one here](https://platform.openai.com/api-keys))

## Setup

1. **Clone and navigate to the project:**
   ```bash
   cd mlql-rs
   ```

2. **Configure environment variables:**
   ```bash
   cp .env.example .env
   ```

   Edit `.env` and add your OpenAI API key:
   ```env
   OPENAI_API_KEY=sk-...your-key-here...
   MLQL_SERVER_HOST=127.0.0.1
   MLQL_SERVER_PORT=8080
   ```

3. **Run the server:**
   ```bash
   cargo run -p mlql-server
   ```

   You should see:
   ```
   Starting MLQL server on 127.0.0.1:8080
   ```

## Try It Out

### Health Check

```bash
curl http://localhost:8080/health
```

Expected response:
```json
{
  "status": "healthy",
  "version": "0.1.0"
}
```

### Run a Query

Create a file `query.json`:
```json
{
  "query": "Show me all users over age 25"
}
```

Execute the query:
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d @query.json
```

Expected response:
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
  "sql": "Generated SQL for: Table { name: \"users\", alias: None }",
  "results": {
    "columns": [],
    "rows": [],
    "row_count": 0
  }
}
```

Note: The results are empty because there's no actual data. The server successfully converted your natural language to MLQL IR and generated SQL!

## Example Queries

Try these natural language queries:

```bash
# Simple filter
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "users over 25"}'

# Projection + sort
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "show name and age, sorted by age"}'

# Aggregation
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "count users by city"}'

# Join
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"query": "users with their orders"}'
```

## What's Happening?

1. **Natural Language** → Your query in plain English
2. **OpenAI GPT-4o-mini** → Converts to MLQL IR (JSON format)
3. **Error Loop** → If parsing fails, error is fed back to LLM (up to 3 retries)
4. **MLQL IR → SQL** → IR is converted to DuckDB SQL
5. **DuckDB Execution** → SQL runs against in-memory database
6. **JSON Response** → Results returned with IR, SQL, and data

## Next Steps

- See [crates/mlql-server/README.md](../crates/mlql-server/README.md) for full API documentation
- Check [docs/llm-json-format.md](llm-json-format.md) for the complete IR specification
- Read [PROGRESS.md](../PROGRESS.md) for implementation details

## Troubleshooting

**Server won't start:**
- Check that `.env` file exists and has `OPENAI_API_KEY` set
- Verify port 8080 is not already in use

**Queries fail:**
- Check your OpenAI API key is valid
- Verify you have API credits remaining
- Look at server logs for detailed error messages

**Invalid IR errors:**
- The LLM will retry up to 3 times automatically
- Check if your query is ambiguous or unclear
- Try simpler queries first
