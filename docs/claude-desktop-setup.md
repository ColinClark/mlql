# Claude Desktop Configuration for MLQL MCP Server

This guide shows how to configure Claude Desktop to use the MLQL MCP server with streaming HTTP/SSE transport.

## Prerequisites

- Claude Desktop installed
- MLQL server built and ready to run
- OpenAI API key configured in `.env`

## Configuration

### 1. Start the MLQL MCP Server

The server must be running before Claude Desktop can connect to it.

```bash
cd /Users/colin/Dev/truepop/mlql/mlql-rs
cargo run -p mlql-server
```

You should see:
```
INFO Starting MLQL MCP server on 127.0.0.1:8080
INFO Protocol: MCP with SSE (Server-Sent Events) support
INFO Use with Claude Desktop or MCP clients
• Streamable HTTP Server is available at http://127.0.0.1:8080/mcp
• SSE Server is available at http://127.0.0.1:8080/sse
```

### 2. Configure Claude Desktop

Edit your Claude Desktop configuration file:

**macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
**Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

Add the following configuration:

```json
{
  "mcpServers": {
    "mlql": {
      "url": "http://127.0.0.1:8080/sse",
      "transport": {
        "type": "sse"
      }
    }
  }
}
```

**Note**: The URL points to the `/sse` endpoint, not `/mcp`. Claude Desktop uses Server-Sent Events for streaming responses.

### 3. Restart Claude Desktop

After updating the configuration:
1. Quit Claude Desktop completely
2. Restart Claude Desktop
3. The MLQL server should appear in the MCP servers list

## Using the MLQL Server

Once configured, you can use natural language queries in Claude Desktop:

```
Use the mlql query tool to show me all users over age 25
```

Claude will:
1. Call the `query` tool with your natural language query
2. The server converts it to MLQL IR using OpenAI
3. The IR is executed against DuckDB
4. Results are returned with the IR, SQL, and data

## Server Configuration

You can customize the server host and port using environment variables in `.env`:

```env
OPENAI_API_KEY=sk-...your-key...
MLQL_SERVER_HOST=127.0.0.1
MLQL_SERVER_PORT=8080
```

After changing the port, update your Claude Desktop configuration to match.

## Troubleshooting

### Server won't start
- Check `.env` file has `OPENAI_API_KEY` set
- Verify port 8080 is not in use: `lsof -i :8080`

### Claude Desktop can't connect
- Ensure the MLQL server is running before starting Claude Desktop
- Check the URL in `claude_desktop_config.json` matches the server's SSE endpoint
- Look at server logs for connection attempts

### Queries fail
- Verify your OpenAI API key is valid and has credits
- Check server logs (`RUST_LOG=debug cargo run -p mlql-server`) for detailed errors
- Try simpler queries first to verify the connection works

## Transport Details

The MLQL MCP server supports two transports:

1. **SSE (Server-Sent Events)** - `/sse` endpoint
   - Used by Claude Desktop
   - One-way streaming from server to client
   - Compatible with MCP SSE transport specification

2. **Streamable HTTP** - `/mcp` endpoint
   - Two-way communication
   - More flexible for custom clients
   - Supports resumability with event stores

For Claude Desktop, always use the SSE transport at `/sse`.
