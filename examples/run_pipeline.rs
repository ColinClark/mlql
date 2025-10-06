//! Example: Running an MLQL pipeline
//!
//! Demonstrates parsing, validating, compiling, and executing an MLQL query.

use mlql_rs::prelude::*;

fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Example MLQL query
    let query = r#"
        pragma {
            timeout: 30000,
            cache: true
        }

        let active_users = from users | filter is_active == true

        from active_users
        | select [user_id, name, email, created_at]
        | filter created_at > "2024-01-01"
        | sort -created_at
        | take 100
    "#;

    println!("Parsing MLQL query...");
    // TODO: Uncomment when parser is implemented
    // let program = parse(query)?;
    // println!("Parsed program: {:#?}", program);

    println!("\nSetting up catalog...");
    let catalog = Catalog::new();
    // TODO: Discover schema from database
    // catalog.discover_from_duckdb(&conn)?;

    println!("\nValidating query...");
    let validator = Validator::new(catalog.clone());
    // TODO: Uncomment when validator is implemented
    // let schema = validator.validate(&program)?;
    // println!("Output schema: {:#?}", schema);

    println!("\nApplying policies...");
    let policy_engine = PolicyEngine::new();
    // TODO: Apply policies
    // policy_engine.apply(&mut program)?;

    println!("\nCompiling to SQL...");
    let compiler = Compiler::new(catalog);
    // TODO: Uncomment when compiler is implemented
    // let compiled = compiler.compile(&program)?;
    // println!("Generated SQL: {}", compiled.sql);

    println!("\nExecuting query...");
    let executor = Executor::new()?;
    // TODO: Uncomment when executor is implemented
    // let result = executor.execute(&compiled, None)?;
    // println!("Results: {} rows", result.row_count);
    // for (i, row) in result.rows.iter().take(5).enumerate() {
    //     println!("  Row {}: {:?}", i + 1, row);
    // }

    println!("\nExample MLQL queries:");

    println!("\n1. Vector search (KNN):");
    println!(r#"
    from documents
    | knn q: <0.1, 0.2, 0.3> k: 10 index: "embedding_idx" metric: "cosine"
    | select [doc_id, title, similarity]
    "#);

    println!("\n2. Graph traversal:");
    println!(r#"
    from graph(social) users
    | neighbors start: "user123" depth: 2 edge: "follows"
    | select [user_id, name, distance]
    "#);

    println!("\n3. Time-series aggregation:");
    println!(r#"
    from events
    | filter timestamp > "2024-01-01"
    | agg by tumbling(timestamp, 1h) {
        count: count(*),
        avg_value: avg(value)
    }
    | sort timestamp
    "#);

    println!("\n4. Window functions:");
    println!(r#"
    from sales
    | window {
        rank: row_number() over part: [category] order: [-revenue],
        running_total: sum(revenue) over part: [category] order: [date] frame: rows [unbounded_preceding, current_row]
    }
    | filter rank <= 10
    "#);

    println!("\n5. Complex join with masking:");
    println!(r#"
    from orders o
    | join from customers c on o.customer_id == c.id type: left
    | select [
        o.order_id,
        mask(c.email) as email,
        o.total
    ]
    | filter o.total > 100
    "#);

    Ok(())
}
