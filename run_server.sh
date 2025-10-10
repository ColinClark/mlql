#!/bin/bash
# Helper script to run MCP server

# Using SQL execution mode - no custom DuckDB build needed
export RUST_LOG=debug
cargo run -p mlql-server
