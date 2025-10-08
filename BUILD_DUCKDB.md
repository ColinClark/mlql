# Building MLQL with Statically-Linked Substrait Extension

## Problem

The DuckDB substrait extension has a protobuf bug on macOS when loaded as a dynamic library (dlopen). The bug causes `from_substrait()` to hang indefinitely due to recursive locking in protobuf's GoogleOnceInitImpl.

See: https://github.com/protocolbuffers/protobuf/issues/4203

## Solution

Build DuckDB 1.4 with the substrait extension statically linked, then link the Rust mlql-server against this custom DuckDB build.

## Steps

### 1. Build DuckDB with Substrait Extension

```bash
cd /Users/colin/Dev/truepop/mlql/duckdb-substrait-upgrade

# Build DuckDB with substrait statically linked
# EXTENSION_STATIC_BUILD=1 includes extension in libduckdb
EXTENSION_STATIC_BUILD=1 make

# This produces:
# - build/release/src/libduckdb.dylib (macOS)
# - build/release/src/libduckdb_static.a (static library)
# - build/release/duckdb (CLI with substrait built-in)
```

### 2. Configure Rust to Use Custom DuckDB

Option A: Environment variables (simpler):
```bash
export DUCKDB_LIB_DIR=/Users/colin/Dev/truepop/mlql/duckdb-substrait-upgrade/build/release/src
export DUCKDB_INCLUDE_DIR=/Users/colin/Dev/truepop/mlql/duckdb-substrait-upgrade/duckdb/src/include

cd /Users/colin/Dev/truepop/mlql/mlql-rs
cargo build
```

Option B: .cargo/config.toml (permanent):
```toml
[env]
DUCKDB_LIB_DIR = "/Users/colin/Dev/truepop/mlql/duckdb-substrait-upgrade/build/release/src"
DUCKDB_INCLUDE_DIR = "/Users/colin/Dev/truepop/mlql/duckdb-substrait-upgrade/duckdb/src/include"
```

### 3. Use from_substrait in Rust

```rust
use duckdb::Connection;

let conn = Connection::open_in_memory()?;

// Substrait extension is already loaded (statically linked)!
conn.execute_batch("
    CREATE TABLE test (id INTEGER, name VARCHAR);
    INSERT INTO test VALUES (1, 'Alice');
")?;

// Use from_substrait with binary plan
let plan_bytes: Vec<u8> = generate_substrait_plan(); // from mlql-ir
conn.execute("SELECT * FROM from_substrait(?)", &[&plan_bytes])?;
```

## Benefits

- ✅ No dynamic loading (avoids macOS protobuf bug)
- ✅ Substrait always available (no `LOAD substrait` needed)
- ✅ Single binary deployment
- ✅ Better performance (no dlopen overhead)
- ✅ Works on all platforms (macOS, Linux, Windows)

## Testing

```bash
# Verify substrait is built-in:
/Users/colin/Dev/truepop/mlql/duckdb-substrait-upgrade/build/release/duckdb << EOF
SELECT * FROM from_substrait_json('{"version":{"minorNumber":53},"relations":[]}');
EOF
```

Should work without `LOAD substrait`!
