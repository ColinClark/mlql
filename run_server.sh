#!/bin/bash
# Helper script to run MCP server with correct DuckDB library path

# Use the NEW build (DuckDB v1.4.1)
export DYLD_LIBRARY_PATH=/Users/colin/Dev/duckdb-substrait-extension/build/duckdb-substrait/src:$DYLD_LIBRARY_PATH
export DUCKDB_CUSTOM_BUILD=1
export RUST_LOG=debug
cargo run -p mlql-server
