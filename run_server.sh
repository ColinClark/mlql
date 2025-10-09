#!/bin/bash
# Helper script to run MCP server with correct DuckDB library path

export DYLD_LIBRARY_PATH=/Users/colin/Dev/duckdb-substrait-extension/build/release/src:$DYLD_LIBRARY_PATH
export DUCKDB_CUSTOM_BUILD=1
export RUST_LOG=debug
cargo run -p mlql-server
