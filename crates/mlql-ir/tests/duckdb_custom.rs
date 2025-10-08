//! Test using custom DuckDB build with substrait extension statically linked
//!
//! This test verifies if static linking fixes the macOS protobuf bug

use duckdb::Connection;

#[test]
fn test_custom_duckdb_basic() {
    let conn = Connection::open_in_memory().unwrap();

    // Basic query should work
    conn.execute_batch("
        CREATE TABLE test (id INTEGER);
        INSERT INTO test VALUES (1), (2), (3);
    ").unwrap();

    let mut stmt = conn.prepare("SELECT * FROM test").unwrap();
    let count = stmt.query_map([], |row| row.get::<_, i32>(0))
        .unwrap()
        .count();

    assert_eq!(count, 3);
    println!("✅ Basic DuckDB test passed");
}

#[test]
fn test_substrait_json_execution() {
    let conn = Connection::open_in_memory().unwrap();

    // Create test table
    conn.execute_batch("
        CREATE TABLE users (id INTEGER, name VARCHAR);
        INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
    ").unwrap();

    // This is the critical test - does from_substrait_json work with static linking?
    // The macOS protobuf bug causes this to hang with dylib linking
    let plan_json = r#"{
        "version": {"minorNumber": 53},
        "relations": [{
            "root": {
                "input": {
                    "read": {
                        "baseSchema": {
                            "names": ["id", "name"],
                            "struct": {
                                "types": [
                                    {"i32": {"nullability": "NULLABILITY_NULLABLE"}},
                                    {"string": {"nullability": "NULLABILITY_NULLABLE"}}
                                ]
                            }
                        },
                        "namedTable": {"names": ["users"]}
                    }
                },
                "names": ["id", "name"]
            }
        }]
    }"#;

    let mut stmt = conn.prepare(&format!(
        "SELECT * FROM from_substrait_json('{}')",
        plan_json.replace("'", "''")
    )).unwrap();

    let results: Vec<(i32, String)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0], (1, "Alice".to_string()));
    assert_eq!(results[1], (2, "Bob".to_string()));

    println!("✅ from_substrait_json test passed - static linking WORKS on macOS!");
    println!("   Results: {:?}", results);
}

#[test]
fn test_mlql_ir_to_substrait_execution() {
    use mlql_ir::{Program, Pipeline, Source, Operator, Expr, Value, BinOp, ColumnRef};
    use mlql_ir::substrait::{SubstraitTranslator, MockSchemaProvider, TableSchema, ColumnInfo};
    use prost::Message;

    // Setup connection
    let conn = Connection::open_in_memory().unwrap();

    // Create test table
    conn.execute_batch("
        CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER);
        INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35);
    ").unwrap();

    // Setup schema provider
    let mut schema_provider = MockSchemaProvider::new();
    schema_provider.add_table(TableSchema {
        name: "users".to_string(),
        columns: vec![
            ColumnInfo { name: "id".to_string(), data_type: "INTEGER".to_string(), nullable: true },
            ColumnInfo { name: "name".to_string(), data_type: "VARCHAR".to_string(), nullable: true },
            ColumnInfo { name: "age".to_string(), data_type: "INTEGER".to_string(), nullable: true },
        ],
    });

    // Create MLQL IR Program: from users | filter age > 25
    let program = Program {
        pragma: None,
        lets: vec![],
        pipeline: Pipeline {
            source: Source::Table {
                name: "users".to_string(),
                alias: None,
            },
            ops: vec![Operator::Filter {
                condition: Expr::BinaryOp {
                    op: BinOp::Gt,
                    left: Box::new(Expr::Column {
                        col: ColumnRef {
                            table: None,
                            column: "age".to_string(),
                        },
                    }),
                    right: Box::new(Expr::Literal {
                        value: Value::Int(25),
                    }),
                },
            }],
        },
    };

    // Translate to Substrait
    let translator = SubstraitTranslator::new(&schema_provider);
    let plan = translator.translate(&program).expect("Translation should succeed");

    // Serialize to bytes
    let mut plan_bytes = Vec::new();
    plan.encode(&mut plan_bytes).expect("Failed to serialize plan");
    println!("Generated Substrait plan: {} bytes", plan_bytes.len());

    // Execute via DuckDB from_substrait()
    let mut stmt = conn.prepare("SELECT * FROM from_substrait(?)").unwrap();
    let results: Vec<(i32, String, i32)> = stmt
        .query_map([plan_bytes], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    // Verify results - should only get Alice (30) and Charlie (35), not Bob (25)
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], (1, "Alice".to_string(), 30));
    assert_eq!(results[1], (3, "Charlie".to_string(), 35));

    println!("✅ MLQL IR → Substrait → DuckDB execution test PASSED!");
    println!("   Filter 'age > 25' correctly returned: {:?}", results);
}
