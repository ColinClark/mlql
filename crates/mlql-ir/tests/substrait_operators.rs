//! Comprehensive test suite for MLQL IR → Substrait → DuckDB execution
//!
//! Run with: env DUCKDB_CUSTOM_BUILD=1 cargo test --package mlql-ir --test substrait_operators

use duckdb::Connection;
use mlql_ir::{Program, Pipeline, Source, Operator};
use mlql_ir::substrait::{SubstraitTranslator, MockSchemaProvider, TableSchema, ColumnInfo};
use prost::Message;

fn setup_schema_provider() -> MockSchemaProvider {
    let mut schema_provider = MockSchemaProvider::new();
    schema_provider.add_table(TableSchema {
        name: "users".to_string(),
        columns: vec![
            ColumnInfo { name: "id".to_string(), data_type: "INTEGER".to_string(), nullable: true },
            ColumnInfo { name: "name".to_string(), data_type: "VARCHAR".to_string(), nullable: true },
            ColumnInfo { name: "age".to_string(), data_type: "INTEGER".to_string(), nullable: true },
        ],
    });
    schema_provider
}

#[test]
fn test_table_scan() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("
        CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER);
        INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35);
    ").unwrap();

    let schema_provider = setup_schema_provider();
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

    let translator = SubstraitTranslator::new(&schema_provider);
    let plan = translator.translate(&program).expect("Translation should succeed");

    let mut plan_bytes = Vec::new();
    plan.encode(&mut plan_bytes).expect("Serialization should succeed");
    println!("Generated plan: {} bytes", plan_bytes.len());

    // Execute and count rows
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM from_substrait(?)", [plan_bytes], |row| row.get(0)).unwrap();
    assert_eq!(count, 3);
    println!("✅ Table scan: {} rows", count);
}

#[test]
fn test_take_limit() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("
        CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER);
        INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35), (4, 'Diana', 28), (5, 'Eve', 32);
    ").unwrap();

    let schema_provider = setup_schema_provider();
    let program = Program {
        pragma: None,
        lets: vec![],
        pipeline: Pipeline {
            source: Source::Table {
                name: "users".to_string(),
                alias: None,
            },
            ops: vec![Operator::Take { limit: 2 }],
        },
    };

    let translator = SubstraitTranslator::new(&schema_provider);
    let plan = translator.translate(&program).expect("Translation should succeed");

    let mut plan_bytes = Vec::new();
    plan.encode(&mut plan_bytes).expect("Serialization should succeed");
    println!("Generated plan: {} bytes", plan_bytes.len());

    // Debug: Try to see what we get back
    let mut stmt = conn.prepare("SELECT * FROM from_substrait(?)").unwrap();
    let mut rows = stmt.query([&plan_bytes]).unwrap();
    let mut actual_count = 0;
    while let Some(_) = rows.next().unwrap() {
        actual_count += 1;
    }

    println!("Actual rows returned: {}", actual_count);
    assert_eq!(actual_count, 2, "Expected 2 rows, got {}", actual_count);
    println!("✅ Take/Limit: {} rows", actual_count);
}

#[test]
fn test_plan_generation() {
    let schema_provider = setup_schema_provider();

    let program = Program {
        pragma: None,
        lets: vec![],
        pipeline: Pipeline {
            source: Source::Table {
                name: "users".to_string(),
                alias: None,
            },
            ops: vec![Operator::Take { limit: 10 }],
        },
    };

    let translator = SubstraitTranslator::new(&schema_provider);
    let plan = translator.translate(&program).expect("Translation should succeed");

    let mut plan_bytes = Vec::new();
    plan.encode(&mut plan_bytes).expect("Serialization should succeed");
    assert!(plan_bytes.len() > 0);
    println!("✅ Plan generation: {} bytes", plan_bytes.len());
}
