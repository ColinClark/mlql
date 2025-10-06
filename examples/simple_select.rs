//! Simple example showing 'from users | select [*]' working
//!
//! Demonstrates the full MLQL pipeline:
//! MLQL text → AST → IR → Substrait → DuckDB

use mlql_ast::parse;
use mlql_ast::ToIr;
use mlql_substrait::SubstraitEncoder;
use mlql_duck::DuckExecutor;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create executor and sample table
    println!("1. Creating DuckDB instance and sample table...");
    let executor = DuckExecutor::new()?;
    executor.connection().execute_batch(
        "CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER);
         INSERT INTO users VALUES
           (1, 'Alice', 30),
           (2, 'Bob', 25),
           (3, 'Charlie', 35);"
    )?;
    println!("   ✓ Created table with 3 rows\n");

    // 2. Parse MLQL query
    let mlql_query = "from users | select [*]";
    println!("2. Parsing MLQL query: {}", mlql_query);
    let ast_program = parse(mlql_query)?;
    println!("   ✓ AST parsed\n");

    // 3. Convert AST to IR
    println!("3. Converting AST to canonical IR...");
    let ir_program = ast_program.to_ir();
    let ir_json = serde_json::to_string_pretty(&ir_program)?;
    println!("   IR JSON:\n{}\n", ir_json);
    println!("   Fingerprint: {}\n", ir_program.fingerprint());

    // 4. Encode IR to Substrait JSON
    println!("4. Encoding IR to Substrait plan...");
    let encoder = SubstraitEncoder::new();
    let substrait_json = encoder.encode(&ir_program)?;
    println!("   Substrait JSON (first 200 chars):\n   {}\n",
        &substrait_json.chars().take(200).collect::<String>());

    // 5. Execute via DuckDB
    println!("5. Executing Substrait plan via DuckDB...");
    let result = executor.execute_substrait_json(&substrait_json, None)?;
    println!("   ✓ Query executed\n");

    // 6. Display results
    println!("6. Results:");
    println!("   Columns: {:?}", result.columns);
    println!("   Rows: {}", result.row_count);
    for (i, row) in result.rows.iter().enumerate() {
        println!("   Row {}: {:?}", i + 1, row);
    }

    println!("\n✅ Success! MLQL → AST → IR → Substrait → DuckDB pipeline complete");

    Ok(())
}
