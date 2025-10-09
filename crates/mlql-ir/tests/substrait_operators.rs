//! Comprehensive test suite for MLQL IR → Substrait → DuckDB execution
//!
//! Run with: env DUCKDB_CUSTOM_BUILD=1 cargo test --package mlql-ir --test substrait_operators

use duckdb::Connection;
use mlql_ir::{Program, Pipeline, Source, Operator};
use mlql_ir::substrait::{SubstraitTranslator, MockSchemaProvider, TableSchema, ColumnInfo};
use prost::Message;

/// Load Substrait extension into test connection
fn load_substrait_extension(conn: &Connection) {
    // Check if already loaded
    if let Ok(count) = conn.query_row::<i64, _, _>(
        "SELECT COUNT(*) FROM duckdb_functions() WHERE function_name = 'from_substrait'",
        [],
        |row| row.get(0)
    ) {
        if count > 0 {
            return; // Already loaded
        }
    }

    // Try to load from SUBSTRAIT_EXTENSION_PATH
    if let Ok(path) = std::env::var("SUBSTRAIT_EXTENSION_PATH") {
        conn.execute_batch(&format!("LOAD '{}'", path))
            .expect("Failed to load Substrait extension from SUBSTRAIT_EXTENSION_PATH");
    } else {
        // Try to install from repository
        conn.execute_batch("INSTALL substrait; LOAD substrait;")
            .expect("Failed to load Substrait extension. Set SUBSTRAIT_EXTENSION_PATH environment variable.");
    }
}

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
    load_substrait_extension(&conn);
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

#[test]
fn test_combined_pipeline() {
    use mlql_ir::{Expr, Value, BinOp, ColumnRef, SortKey};

    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("
        CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER);
        INSERT INTO users VALUES
            (1, 'Alice', 30),
            (2, 'Bob', 25),
            (3, 'Charlie', 35),
            (4, 'Diana', 28),
            (5, 'Eve', 32);
    ").unwrap();

    let schema_provider = setup_schema_provider();

    // Test: from users | filter age > 26 | sort -age | take 2
    // Should return: Charlie (35), Eve (32)
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
                            col: ColumnRef {
                                table: None,
                                column: "age".to_string(),
                            },
                        }),
                        right: Box::new(Expr::Literal {
                            value: Value::Int(26),
                        }),
                    },
                },
                Operator::Sort {
                    keys: vec![SortKey {
                        expr: Expr::Column {
                            col: ColumnRef {
                                table: None,
                                column: "age".to_string(),
                            },
                        },
                        desc: true, // Sort descending
                    }],
                },
                Operator::Take { limit: 2 },
            ],
        },
    };

    let translator = SubstraitTranslator::new(&schema_provider);
    let plan = translator.translate(&program).expect("Translation should succeed");

    let mut plan_bytes = Vec::new();
    plan.encode(&mut plan_bytes).expect("Serialization should succeed");
    println!("Combined pipeline plan: {} bytes", plan_bytes.len());

    // Execute and get results
    let mut stmt = conn.prepare("SELECT id, name, age FROM from_substrait(?)").unwrap();
    let results: Vec<(i32, String, i32)> = stmt
        .query_map([plan_bytes], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    // Should get Charlie (35) and Eve (32) in that order
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], (3, "Charlie".to_string(), 35));
    assert_eq!(results[1], (5, "Eve".to_string(), 32));

    println!("✅ Combined pipeline (filter + sort + take): {:?}", results);
}

#[test]
fn test_distinct() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("
        CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER);
        INSERT INTO users VALUES
            (1, 'Alice', 30),
            (2, 'Bob', 25),
            (1, 'Alice', 30),  -- Exact duplicate of row 1
            (2, 'Bob', 25),    -- Exact duplicate of row 2
            (3, 'Charlie', 35);
    ").unwrap();

    let schema_provider = setup_schema_provider();

    // Test: from users | distinct
    // Should return 3 unique rows (Alice 30, Bob 25, Charlie 35)
    let program = Program {
        pragma: None,
        lets: vec![],
        pipeline: Pipeline {
            source: Source::Table {
                name: "users".to_string(),
                alias: None,
            },
            ops: vec![Operator::Distinct],
        },
    };

    let translator = SubstraitTranslator::new(&schema_provider);
    let plan = translator.translate(&program).expect("Translation should succeed");

    let mut plan_bytes = Vec::new();
    plan.encode(&mut plan_bytes).expect("Serialization should succeed");
    println!("Distinct plan: {} bytes", plan_bytes.len());

    // Execute and count distinct rows
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM from_substrait(?)", [plan_bytes], |row| row.get(0)).unwrap();
    assert_eq!(count, 3, "Expected 3 distinct rows, got {}", count);
    println!("✅ Distinct: {} unique rows", count);
}

#[test]
fn test_groupby() {
    use mlql_ir::{AggCall, Expr, ColumnRef};
    use std::collections::HashMap;

    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("
        CREATE TABLE sales (id INTEGER, product VARCHAR, amount INTEGER);
        INSERT INTO sales VALUES
            (1, 'Apple', 100),
            (2, 'Banana', 150),
            (3, 'Apple', 200),
            (4, 'Banana', 50),
            (5, 'Cherry', 300);
    ").unwrap();

    // Setup schema provider for sales table
    let mut schema_provider = MockSchemaProvider::new();
    schema_provider.add_table(TableSchema {
        name: "sales".to_string(),
        columns: vec![
            ColumnInfo { name: "id".to_string(), data_type: "INTEGER".to_string(), nullable: true },
            ColumnInfo { name: "product".to_string(), data_type: "VARCHAR".to_string(), nullable: true },
            ColumnInfo { name: "amount".to_string(), data_type: "INTEGER".to_string(), nullable: true },
        ],
    });

    // Test: from sales | group by product { total: sum(amount) }
    // Should return: Apple 300, Banana 200, Cherry 300
    let mut aggs = HashMap::new();
    aggs.insert("total".to_string(), AggCall {
        func: "sum".to_string(),
        args: vec![Expr::Column {
            col: ColumnRef {
                table: None,
                column: "amount".to_string(),
            },
        }],
    });

    let program = Program {
        pragma: None,
        lets: vec![],
        pipeline: Pipeline {
            source: Source::Table {
                name: "sales".to_string(),
                alias: None,
            },
            ops: vec![Operator::GroupBy {
                keys: vec![ColumnRef {
                    table: None,
                    column: "product".to_string(),
                }],
                aggs,
            }],
        },
    };

    let translator = SubstraitTranslator::new(&schema_provider);
    let plan = translator.translate(&program).expect("Translation should succeed");

    let mut plan_bytes = Vec::new();
    plan.encode(&mut plan_bytes).expect("Serialization should succeed");
    println!("GroupBy plan: {} bytes", plan_bytes.len());

    // Execute and get results
    let mut stmt = conn.prepare("SELECT * FROM from_substrait(?)").unwrap();
    let results: Vec<(String, i64)> = stmt
        .query_map([plan_bytes], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    println!("GroupBy results: {:?}", results);

    // Check that we have 3 groups
    assert_eq!(results.len(), 3, "Expected 3 groups");

    // Check totals (order may vary)
    let totals: HashMap<String, i64> = results.into_iter().collect();
    assert_eq!(totals.get("Apple"), Some(&300));
    assert_eq!(totals.get("Banana"), Some(&200));
    assert_eq!(totals.get("Cherry"), Some(&300));

    println!("✅ GroupBy: 3 groups with correct totals");
}

#[test]
fn test_join() {
    use mlql_ir::{Expr, BinOp, ColumnRef, JoinType};

    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("
        CREATE TABLE users (id INTEGER, name VARCHAR);
        CREATE TABLE orders (order_id INTEGER, user_id INTEGER, amount INTEGER);
        INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
        INSERT INTO orders VALUES (101, 1, 100), (102, 1, 200), (103, 2, 150);
    ").unwrap();

    let mut schema_provider = MockSchemaProvider::new();
    schema_provider.add_table(TableSchema {
        name: "users".to_string(),
        columns: vec![
            ColumnInfo { name: "id".to_string(), data_type: "INTEGER".to_string(), nullable: true },
            ColumnInfo { name: "name".to_string(), data_type: "VARCHAR".to_string(), nullable: true },
        ],
    });
    schema_provider.add_table(TableSchema {
        name: "orders".to_string(),
        columns: vec![
            ColumnInfo { name: "order_id".to_string(), data_type: "INTEGER".to_string(), nullable: true },
            ColumnInfo { name: "user_id".to_string(), data_type: "INTEGER".to_string(), nullable: true },
            ColumnInfo { name: "amount".to_string(), data_type: "INTEGER".to_string(), nullable: true },
        ],
    });

    // Test: from users | join orders on users.id == orders.user_id
    // Should return: (1, 'Alice', 101, 1, 100), (1, 'Alice', 102, 1, 200), (2, 'Bob', 103, 2, 150)
    let program = Program {
        pragma: None,
        lets: vec![],
        pipeline: Pipeline {
            source: Source::Table {
                name: "users".to_string(),
                alias: None,
            },
            ops: vec![Operator::Join {
                source: Source::Table {
                    name: "orders".to_string(),
                    alias: None,
                },
                on: Expr::BinaryOp {
                    op: BinOp::Eq,
                    left: Box::new(Expr::Column {
                        col: ColumnRef {
                            table: Some("users".to_string()),
                            column: "id".to_string(),
                        },
                    }),
                    right: Box::new(Expr::Column {
                        col: ColumnRef {
                            table: Some("orders".to_string()),
                            column: "user_id".to_string(),
                        },
                    }),
                },
                join_type: Some(JoinType::Inner),
            }],
        },
    };

    let translator = SubstraitTranslator::new(&schema_provider);
    let plan = translator.translate(&program).expect("Translation should succeed");

    let mut plan_bytes = Vec::new();
    plan.encode(&mut plan_bytes).expect("Serialization should succeed");
    println!("Join plan: {} bytes", plan_bytes.len());

    // Execute and get results
    let mut stmt = conn.prepare("SELECT * FROM from_substrait(?)").unwrap();
    let results: Vec<(i32, String, i32, i32, i32)> = stmt
        .query_map([plan_bytes], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    println!("Join results: {:?}", results);

    // Check that we have 3 join results
    assert_eq!(results.len(), 3, "Expected 3 join results");

    // Check specific rows
    assert!(results.contains(&(1, "Alice".to_string(), 101, 1, 100)));
    assert!(results.contains(&(1, "Alice".to_string(), 102, 1, 200)));
    assert!(results.contains(&(2, "Bob".to_string(), 103, 2, 150)));

    println!("✅ Join: 3 rows with correct values");
}

#[test]
fn test_all_aggregates() {
    use mlql_ir::{AggCall, Expr, ColumnRef};
    use std::collections::HashMap;

    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("
        CREATE TABLE sales (id INTEGER, product VARCHAR, amount INTEGER);
        INSERT INTO sales VALUES
            (1, 'Apple', 100),
            (2, 'Apple', 200),
            (3, 'Apple', 150),
            (4, 'Banana', 50),
            (5, 'Banana', 100);
    ").unwrap();

    let mut schema_provider = MockSchemaProvider::new();
    schema_provider.add_table(TableSchema {
        name: "sales".to_string(),
        columns: vec![
            ColumnInfo { name: "id".to_string(), data_type: "INTEGER".to_string(), nullable: true },
            ColumnInfo { name: "product".to_string(), data_type: "VARCHAR".to_string(), nullable: true },
            ColumnInfo { name: "amount".to_string(), data_type: "INTEGER".to_string(), nullable: true },
        ],
    });

    // Test: from sales | group by product { total: sum(amount), count: count(amount), avg: avg(amount), min: min(amount), max: max(amount) }
    let mut aggs = HashMap::new();
    aggs.insert("total".to_string(), AggCall {
        func: "sum".to_string(),
        args: vec![Expr::Column {
            col: ColumnRef {
                table: None,
                column: "amount".to_string(),
            },
        }],
    });
    aggs.insert("count".to_string(), AggCall {
        func: "count".to_string(),
        args: vec![Expr::Column {
            col: ColumnRef {
                table: None,
                column: "amount".to_string(),
            },
        }],
    });
    aggs.insert("avg".to_string(), AggCall {
        func: "avg".to_string(),
        args: vec![Expr::Column {
            col: ColumnRef {
                table: None,
                column: "amount".to_string(),
            },
        }],
    });
    aggs.insert("min".to_string(), AggCall {
        func: "min".to_string(),
        args: vec![Expr::Column {
            col: ColumnRef {
                table: None,
                column: "amount".to_string(),
            },
        }],
    });
    aggs.insert("max".to_string(), AggCall {
        func: "max".to_string(),
        args: vec![Expr::Column {
            col: ColumnRef {
                table: None,
                column: "amount".to_string(),
            },
        }],
    });

    let program = Program {
        pragma: None,
        lets: vec![],
        pipeline: Pipeline {
            source: Source::Table {
                name: "sales".to_string(),
                alias: None,
            },
            ops: vec![Operator::GroupBy {
                keys: vec![ColumnRef {
                    table: None,
                    column: "product".to_string(),
                }],
                aggs,
            }],
        },
    };

    let translator = SubstraitTranslator::new(&schema_provider);
    let plan = translator.translate(&program).expect("Translation should succeed");

    let mut plan_bytes = Vec::new();
    plan.encode(&mut plan_bytes).expect("Serialization should succeed");
    println!("All aggregates plan: {} bytes", plan_bytes.len());

    // Execute and get results
    // NOTE: HashMap iteration order is non-deterministic, so we need to check column names to know the order
    // Use dynamic SQL to select columns in known order
    let mut stmt = conn.prepare(&format!(
        "SELECT product, \"total\", \"count\", avg, min, max FROM from_substrait(?)"
    )).unwrap();
    let results: Vec<(String, i64, i64, f64, i32, i32)> = stmt
        .query_map([plan_bytes], |row| Ok((
            row.get(0)?,  // product
            row.get(1)?,  // total (sum)
            row.get(2)?,  // count
            row.get(3)?,  // avg
            row.get(4)?,  // min
            row.get(5)?,  // max
        )))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    println!("All aggregates results: {:?}", results);

    // Check that we have 2 groups
    assert_eq!(results.len(), 2, "Expected 2 groups");

    // Check Apple: total=450, count=3, avg=150, min=100, max=200
    let apple = results.iter().find(|(product, _, _, _, _, _)| product == "Apple").expect("Apple not found");
    assert_eq!(apple.1, 450, "Apple total should be 450");
    assert_eq!(apple.2, 3, "Apple count should be 3");
    assert!((apple.3 - 150.0).abs() < 0.1, "Apple avg should be ~150");
    assert_eq!(apple.4, 100, "Apple min should be 100");
    assert_eq!(apple.5, 200, "Apple max should be 200");

    // Check Banana: total=150, count=2, avg=75, min=50, max=100
    let banana = results.iter().find(|(product, _, _, _, _, _)| product == "Banana").expect("Banana not found");
    assert_eq!(banana.1, 150, "Banana total should be 150");
    assert_eq!(banana.2, 2, "Banana count should be 2");
    assert!((banana.3 - 75.0).abs() < 0.1, "Banana avg should be ~75");
    assert_eq!(banana.4, 50, "Banana min should be 50");
    assert_eq!(banana.5, 100, "Banana max should be 100");

    println!("✅ All aggregates: sum, count, avg, min, max all working correctly");
}
