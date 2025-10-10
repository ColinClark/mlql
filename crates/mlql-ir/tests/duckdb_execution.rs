//! Integration tests that execute Substrait plans using our custom DuckDB build
//!
//! These tests:
//! 1. Generate Substrait plans from MLQL IR
//! 2. Serialize plans to JSON format
//! 3. Execute them via DuckDB CLI with substrait extension
//! 4. Verify the results
//!
//! **IMPORTANT - macOS Issue**:
//! These tests are IGNORED on macOS due to a protobuf bug in dylib loading:
//! - Bug: https://github.com/protocolbuffers/protobuf/issues/4203
//! - Symptom: `from_substrait_json()` hangs due to recursive locking in GoogleOnceInitImpl
//! - Workaround: Tests work correctly on Linux/Windows
//! - Alternative: Build custom Rust duckdb crate linked against static libduckdb
//!
//! Run with `--ignored` on Linux/Windows to execute these tests.

use mlql_ir::substrait::{SubstraitTranslator, MockSchemaProvider, TableSchema, ColumnInfo};
use mlql_ir::{Program, Pipeline, Source, Operator, Expr, Value, BinOp, ColumnRef, Projection, SortKey};
use prost::Message;
use std::fs;
use std::process::Command;

const DUCKDB_PATH: &str = "/Users/colin/Dev/duckdb-substrait-extension/build/release/duckdb";

/// Helper to check if our custom DuckDB build exists
fn duckdb_available() -> bool {
    std::path::Path::new(DUCKDB_PATH).exists()
}

/// Execute a Substrait plan via DuckDB CLI using JSON format
fn execute_plan(plan: &substrait::proto::Plan, setup_sql: &str) -> Result<String, String> {
    // Serialize plan to JSON (DuckDB supports from_substrait_json)
    let plan_json = serde_json::to_string(plan)
        .map_err(|e| format!("Failed to serialize plan to JSON: {}", e))?;

    // Escape single quotes in JSON for SQL string literal
    let escaped_json = plan_json.replace("'", "''");

    // Create SQL script that:
    // 1. Sets up test data
    // 2. Executes plan via from_substrait_json() with inline JSON string
    let sql_script = format!(
        r#"
{}
SELECT * FROM from_substrait_json('{}');
"#,
        setup_sql, escaped_json
    );

    let script_path = "/tmp/test_substrait_exec.sql";
    fs::write(script_path, &sql_script).map_err(|e| format!("Failed to write script: {}", e))?;

    // Execute via DuckDB CLI
    let output = Command::new(DUCKDB_PATH)
        .arg("-c")
        .arg(format!(".read {}", script_path))
        .output()
        .map_err(|e| format!("Failed to execute DuckDB: {}", e))?;

    if !output.status.success() {
        return Err(format!(
            "DuckDB execution failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

#[test]
#[cfg_attr(target_os = "macos", ignore = "Skipped on macOS: from_substrait_json hangs due to protobuf dylib bug (https://github.com/protocolbuffers/protobuf/issues/4203)")]
fn test_table_scan_execution() {
    if !duckdb_available() {
        println!("⚠️  Skipping: DuckDB with substrait not found at {}", DUCKDB_PATH);
        return;
    }

    // Setup schema
    let mut schema_provider = MockSchemaProvider::new();
    schema_provider.add_table(TableSchema {
        name: "users".to_string(),
        columns: vec![
            ColumnInfo {
                name: "id".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
            },
            ColumnInfo {
                name: "name".to_string(),
                data_type: "VARCHAR".to_string(),
                nullable: true,
            },
            ColumnInfo {
                name: "age".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: true,
            },
        ],
    });

    // MLQL: from users
    let program = Program {
        pragma: None,
        lets: vec![],
        pipeline: Pipeline {
            source: Source::Table {
                name: "users".to_string(),
                alias: None,
            },
            ops: vec![],
        },
    };

    // Translate to Substrait
    let translator = SubstraitTranslator::new(&schema_provider);
    let plan = translator.translate(&program).expect("Translation failed");

    // Execute in DuckDB
    let setup = "CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER); \
                 INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35);";

    let output = execute_plan(&plan, setup).expect("Execution failed");

    // Verify output contains all 3 rows
    assert!(output.contains("Alice"), "Should contain Alice");
    assert!(output.contains("Bob"), "Should contain Bob");
    assert!(output.contains("Charlie"), "Should contain Charlie");

    println!("✅ Table scan execution test passed");
    println!("Output:\n{}", output);
}

#[test]
#[cfg_attr(target_os = "macos", ignore = "Skipped on macOS: protobuf dylib bug")]
fn test_filter_execution() {
    if !duckdb_available() {
        println!("⚠️  Skipping: DuckDB with substrait not found");
        return;
    }

    let mut schema_provider = MockSchemaProvider::new();
    schema_provider.add_table(TableSchema {
        name: "users".to_string(),
        columns: vec![
            ColumnInfo { name: "id".to_string(), data_type: "INTEGER".to_string(), nullable: false },
            ColumnInfo { name: "name".to_string(), data_type: "VARCHAR".to_string(), nullable: true },
            ColumnInfo { name: "age".to_string(), data_type: "INTEGER".to_string(), nullable: true },
        ],
    });

    // MLQL: from users | filter age > 25
    let program = Program {
        pragma: None,
        lets: vec![],
        pipeline: Pipeline {
            source: Source::Table {
                name: "users".to_string(),
                alias: None,
            },
            ops: vec![
                Operator::Filter {
                    condition: Expr::BinaryOp {
                        op: BinOp::Gt,
                        left: Box::new(Expr::Column {
                            col: ColumnRef { table: None, column: "age".to_string() },
                        }),
                        right: Box::new(Expr::Literal { value: Value::Int(25) }),
                    },
                },
            ],
        },
    };

    let translator = SubstraitTranslator::new(&schema_provider);
    let plan = translator.translate(&program).expect("Translation failed");

    let setup = "CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER); \
                 INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35);";

    let output = execute_plan(&plan, setup).expect("Execution failed");

    // Should have Alice and Charlie, NOT Bob
    assert!(output.contains("Alice"), "Should contain Alice (age 30)");
    assert!(output.contains("Charlie"), "Should contain Charlie (age 35)");
    assert!(!output.contains("Bob"), "Should NOT contain Bob (age 25)");

    println!("✅ Filter execution test passed");
    println!("Output:\n{}", output);
}

#[test]
#[cfg_attr(target_os = "macos", ignore = "Skipped on macOS: protobuf dylib bug")]
fn test_select_execution() {
    if !duckdb_available() {
        println!("⚠️  Skipping: DuckDB with substrait not found");
        return;
    }

    let mut schema_provider = MockSchemaProvider::new();
    schema_provider.add_table(TableSchema {
        name: "users".to_string(),
        columns: vec![
            ColumnInfo { name: "id".to_string(), data_type: "INTEGER".to_string(), nullable: false },
            ColumnInfo { name: "name".to_string(), data_type: "VARCHAR".to_string(), nullable: true },
            ColumnInfo { name: "age".to_string(), data_type: "INTEGER".to_string(), nullable: true },
        ],
    });

    // MLQL: from users | select [name, age]
    let program = Program {
        pragma: None,
        lets: vec![],
        pipeline: Pipeline {
            source: Source::Table {
                name: "users".to_string(),
                alias: None,
            },
            ops: vec![
                Operator::Select {
                    projections: vec![
                        Projection::Expr(Expr::Column {
                            col: ColumnRef { table: None, column: "name".to_string() },
                        }),
                        Projection::Expr(Expr::Column {
                            col: ColumnRef { table: None, column: "age".to_string() },
                        }),
                    ],
                },
            ],
        },
    };

    let translator = SubstraitTranslator::new(&schema_provider);
    let plan = translator.translate(&program).expect("Translation failed");

    let setup = "CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER); \
                 INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35);";

    let output = execute_plan(&plan, setup).expect("Execution failed");

    // Should contain names and ages
    assert!(output.contains("Alice"), "Should contain Alice");
    assert!(output.contains("30"), "Should contain age 30");

    println!("✅ Select execution test passed");
    println!("Output:\n{}", output);
}

#[test]
#[cfg_attr(target_os = "macos", ignore = "Skipped on macOS: protobuf dylib bug")]
fn test_sort_execution() {
    if !duckdb_available() {
        println!("⚠️  Skipping: DuckDB with substrait not found");
        return;
    }

    let mut schema_provider = MockSchemaProvider::new();
    schema_provider.add_table(TableSchema {
        name: "users".to_string(),
        columns: vec![
            ColumnInfo { name: "id".to_string(), data_type: "INTEGER".to_string(), nullable: false },
            ColumnInfo { name: "name".to_string(), data_type: "VARCHAR".to_string(), nullable: true },
            ColumnInfo { name: "age".to_string(), data_type: "INTEGER".to_string(), nullable: true },
        ],
    });

    // MLQL: from users | sort -age (descending)
    let program = Program {
        pragma: None,
        lets: vec![],
        pipeline: Pipeline {
            source: Source::Table {
                name: "users".to_string(),
                alias: None,
            },
            ops: vec![
                Operator::Sort {
                    keys: vec![
                        SortKey {
                            expr: Expr::Column {
                                col: ColumnRef { table: None, column: "age".to_string() },
                            },
                            desc: true,
                        },
                    ],
                },
            ],
        },
    };

    let translator = SubstraitTranslator::new(&schema_provider);
    let plan = translator.translate(&program).expect("Translation failed");

    let setup = "CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER); \
                 INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35);";

    let output = execute_plan(&plan, setup).expect("Execution failed");

    // Should be sorted by age descending: Charlie(35), Alice(30), Bob(25)
    let charlie_pos = output.find("Charlie").expect("Should contain Charlie");
    let alice_pos = output.find("Alice").expect("Should contain Alice");
    let bob_pos = output.find("Bob").expect("Should contain Bob");

    assert!(charlie_pos < alice_pos, "Charlie should appear before Alice");
    assert!(alice_pos < bob_pos, "Alice should appear before Bob");

    println!("✅ Sort execution test passed");
    println!("Output:\n{}", output);
}
