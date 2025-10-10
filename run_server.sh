#!/bin/bash
# Helper script to run MCP server

# Unset any OPENAI_API_KEY from shell environment (.env file will be used)
unset OPENAI_API_KEY

# Using SQL execution mode - no custom DuckDB build needed
export RUST_LOG=debug
cargo run -p mlql-server
