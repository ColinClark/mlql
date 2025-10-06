//! DuckDB executor for Substrait plans

use duckdb::{Connection, Result as DuckResult};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("Database error: {0}")]
    Database(#[from] duckdb::Error),

    #[error("Budget exceeded: {0}")]
    BudgetExceeded(String),

    #[error("Query timeout")]
    Timeout,

    #[error("Substrait execution failed: {0}")]
    SubstraitError(String),
}

pub struct ExecutionBudget {
    pub max_time_ms: Option<u64>,
    pub max_memory_mb: Option<u64>,
    pub max_rows: Option<u64>,
}

pub struct DuckExecutor {
    conn: Connection,
}

impl DuckExecutor {
    pub fn new() -> DuckResult<Self> {
        let conn = Connection::open_in_memory()?;
        Ok(Self { conn })
    }

    pub fn from_connection(conn: Connection) -> Self {
        Self { conn }
    }

    /// Execute MLQL IR program by converting to SQL
    pub fn execute_ir(
        &self,
        program: &mlql_ir::Program,
        budget: Option<ExecutionBudget>,
    ) -> Result<QueryResult, ExecutionError> {
        // Apply budget constraints
        if let Some(ref budget) = budget {
            self.apply_budget(budget)?;
        }

        // Convert IR to SQL
        let sql = ir_to_sql(program)?;

        eprintln!("Generated SQL: {}", sql);

        // Execute SQL query
        self.execute_sql(&sql, budget)
    }

    /// Execute SQL query directly
    fn execute_sql(
        &self,
        sql: &str,
        budget: Option<ExecutionBudget>,
    ) -> Result<QueryResult, ExecutionError> {
        // Execute query and collect rows
        let mut stmt = self.conn.prepare(sql)?;
        let mut rows = stmt.query([])?;

        // Collect rows
        let mut result_rows = Vec::new();
        let mut row_count = 0;
        let mut column_names: Vec<String> = Vec::new();
        let mut column_count = 0;

        while let Some(row) = rows.next()? {
            // Get column info from first row
            if column_names.is_empty() {
                column_count = row.as_ref().column_count();
                column_names = (0..column_count)
                    .map(|i| row.as_ref().column_name(i).unwrap_or(&format!("col{}", i)).to_string())
                    .collect();
            }

            let mut json_row = Vec::new();

            for i in 0..column_count {
                // Convert each cell to JSON value
                let value_ref = row.get_ref(i)?;
                let value: serde_json::Value = match value_ref {
                    duckdb::types::ValueRef::Null => serde_json::Value::Null,
                    duckdb::types::ValueRef::Boolean(b) => serde_json::Value::Bool(b),
                    duckdb::types::ValueRef::TinyInt(i) => serde_json::Value::from(i),
                    duckdb::types::ValueRef::SmallInt(i) => serde_json::Value::from(i),
                    duckdb::types::ValueRef::Int(i) => serde_json::Value::from(i),
                    duckdb::types::ValueRef::BigInt(i) => serde_json::Value::from(i),
                    duckdb::types::ValueRef::Float(f) => serde_json::json!(f),
                    duckdb::types::ValueRef::Double(f) => serde_json::json!(f),
                    duckdb::types::ValueRef::Text(bytes) => {
                        // Convert bytes to UTF-8 string
                        let s = std::str::from_utf8(bytes).unwrap_or("");
                        serde_json::Value::String(s.to_string())
                    },
                    _ => serde_json::Value::Null,  // TODO: Handle more types
                };

                json_row.push(value);
            }

            result_rows.push(json_row);
            row_count += 1;

            // Check row budget
            if let Some(ref budget) = budget {
                if let Some(max_rows) = budget.max_rows {
                    if row_count >= max_rows as usize {
                        return Err(ExecutionError::BudgetExceeded(
                            format!("Max rows ({}) exceeded", max_rows)
                        ));
                    }
                }
            }
        }

        Ok(QueryResult {
            columns: column_names,
            rows: result_rows,
            row_count,
        })
    }

    fn apply_budget(&self, budget: &ExecutionBudget) -> Result<(), ExecutionError> {
        // Set PRAGMAs for resource limits
        if let Some(max_memory_mb) = budget.max_memory_mb {
            let pragma = format!("PRAGMA memory_limit='{}MB'", max_memory_mb);
            self.conn.execute_batch(&pragma)
                .map_err(|e| ExecutionError::Database(e))?;
        }

        // TODO: Set timeout (requires DuckDB interrupt mechanism)
        // TODO: Set max rows (via FetchRel in Substrait plan)

        Ok(())
    }

    pub fn connection(&self) -> &Connection {
        &self.conn
    }
}

#[derive(Debug)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub row_count: usize,
}

impl Default for DuckExecutor {
    fn default() -> Self {
        Self::new().expect("Failed to create DuckDB executor")
    }
}

/// Convert MLQL IR to DuckDB SQL
fn ir_to_sql(program: &mlql_ir::Program) -> Result<String, ExecutionError> {
    let pipeline = &program.pipeline;

    // Build SQL from operators, starting with the source table
    let table_name = match &pipeline.source {
        mlql_ir::Source::Table { name, alias } => {
            if let Some(a) = alias {
                format!("{} AS {}", name, a)
            } else {
                name.clone()
            }
        }
        _ => return Err(ExecutionError::SubstraitError("Unsupported source type".to_string())),
    };

    // Build the SQL query by processing operators
    build_sql_query(&table_name, &pipeline.ops)
}

/// Build SQL query from table and operators
fn build_sql_query(table: &str, operators: &[mlql_ir::Operator]) -> Result<String, ExecutionError> {
    let mut select_clause = "*".to_string();
    let mut from_clause = table.to_string();
    let mut where_clause = None;
    let mut group_clause = None;
    let mut order_clause = None;
    let mut limit_clause = None;
    let mut distinct = false;

    // Process operators in order
    for op in operators {
        match op {
            mlql_ir::Operator::Select { projections } => {
                // Build SELECT list
                let select_items: Vec<String> = projections.iter().map(|proj| {
                    match proj {
                        mlql_ir::Projection::Expr(expr) => {
                            // Check if it's a wildcard (column named "*")
                            if let mlql_ir::Expr::Column { col } = expr {
                                if col.column == "*" && col.table.is_none() {
                                    return "*".to_string();
                                }
                            }
                            expr_to_sql(expr)
                        }
                        mlql_ir::Projection::Aliased { expr, alias } => {
                            format!("{} AS {}", expr_to_sql(expr), alias)
                        }
                    }
                }).collect();

                select_clause = select_items.join(", ");
            }
            mlql_ir::Operator::Filter { condition } => {
                where_clause = Some(expr_to_sql(condition));
            }
            mlql_ir::Operator::Join { source, on, join_type } => {
                // Build JOIN clause
                let join_type_sql = match join_type {
                    Some(mlql_ir::JoinType::Inner) | None => "INNER JOIN",
                    Some(mlql_ir::JoinType::Left) => "LEFT JOIN",
                    Some(mlql_ir::JoinType::Right) => "RIGHT JOIN",
                    Some(mlql_ir::JoinType::Full) => "FULL OUTER JOIN",
                    Some(mlql_ir::JoinType::Cross) => "CROSS JOIN",
                    Some(mlql_ir::JoinType::Semi) => return Err(ExecutionError::SubstraitError("SEMI JOIN not yet supported".to_string())),
                    Some(mlql_ir::JoinType::Anti) => return Err(ExecutionError::SubstraitError("ANTI JOIN not yet supported".to_string())),
                };

                // Get the source table/alias
                let source_sql = match source {
                    mlql_ir::Source::Table { name, alias } => {
                        if let Some(a) = alias {
                            format!("{} AS {}", name, a)
                        } else {
                            name.clone()
                        }
                    }
                    _ => return Err(ExecutionError::SubstraitError("Unsupported JOIN source type".to_string())),
                };

                // Build ON condition
                let on_condition = expr_to_sql(on);

                // Append to FROM clause
                from_clause.push_str(&format!(" {} {} ON {}", join_type_sql, source_sql, on_condition));
            }
            mlql_ir::Operator::GroupBy { keys, aggs } => {
                // Build GROUP BY keys
                let group_keys: Vec<String> = keys.iter()
                    .map(column_ref_to_sql)
                    .collect();

                // Build SELECT clause with keys + aggregates
                let mut select_items = group_keys.clone();

                for (alias, agg_call) in aggs.iter() {
                    let agg_func = &agg_call.func;
                    let agg_args: Vec<String> = agg_call.args.iter()
                        .map(expr_to_sql)
                        .collect();

                    let agg_expr = if agg_args.is_empty() {
                        // count(*) case
                        format!("{}(*)", agg_func)
                    } else {
                        format!("{}({})", agg_func, agg_args.join(", "))
                    };

                    select_items.push(format!("{} AS {}", agg_expr, alias));
                }

                select_clause = select_items.join(", ");
                group_clause = Some(group_keys.join(", "));
            }
            mlql_ir::Operator::Sort { keys } => {
                let order_items: Vec<String> = keys.iter().map(|key| {
                    let expr = expr_to_sql(&key.expr);
                    if key.desc {
                        format!("{} DESC", expr)
                    } else {
                        format!("{} ASC", expr)
                    }
                }).collect();

                order_clause = Some(order_items.join(", "));
            }
            mlql_ir::Operator::Take { limit } => {
                limit_clause = Some(limit.to_string());
            }
            mlql_ir::Operator::Distinct => {
                distinct = true;
            }
            _ => return Err(ExecutionError::SubstraitError(format!("Unsupported operator: {:?}", op))),
        }
    }

    // Build final SQL
    let distinct_sql = if distinct { "DISTINCT " } else { "" };
    let mut sql = format!("SELECT {}{} FROM {}", distinct_sql, select_clause, from_clause);

    if let Some(where_sql) = where_clause {
        sql.push_str(&format!(" WHERE {}", where_sql));
    }

    if let Some(group_sql) = group_clause {
        sql.push_str(&format!(" GROUP BY {}", group_sql));
    }

    if let Some(order_sql) = order_clause {
        sql.push_str(&format!(" ORDER BY {}", order_sql));
    }

    if let Some(limit_sql) = limit_clause {
        sql.push_str(&format!(" LIMIT {}", limit_sql));
    }

    Ok(sql)
}

fn expr_to_sql(expr: &mlql_ir::Expr) -> String {
    match expr {
        mlql_ir::Expr::Column { col } => column_ref_to_sql(col),
        mlql_ir::Expr::Literal { value } => literal_to_sql(value),
        mlql_ir::Expr::BinaryOp { op, left, right } => {
            format!("({} {} {})", expr_to_sql(left), binop_to_sql(op), expr_to_sql(right))
        }
        mlql_ir::Expr::FuncCall { func, args } => {
            let arg_strs: Vec<String> = args.iter().map(expr_to_sql).collect();
            format!("{}({})", func, arg_strs.join(", "))
        }
        _ => "NULL".to_string(),  // TODO: Handle more expression types
    }
}

fn column_ref_to_sql(col: &mlql_ir::ColumnRef) -> String {
    if let Some(ref table) = col.table {
        format!("{}.{}", table, col.column)
    } else {
        col.column.clone()
    }
}

fn binop_to_sql(op: &mlql_ir::BinOp) -> &'static str {
    match op {
        mlql_ir::BinOp::Add => "+",
        mlql_ir::BinOp::Sub => "-",
        mlql_ir::BinOp::Mul => "*",
        mlql_ir::BinOp::Div => "/",
        mlql_ir::BinOp::Mod => "%",
        mlql_ir::BinOp::Eq => "=",
        mlql_ir::BinOp::Ne => "!=",
        mlql_ir::BinOp::Lt => "<",
        mlql_ir::BinOp::Le => "<=",
        mlql_ir::BinOp::Gt => ">",
        mlql_ir::BinOp::Ge => ">=",
        mlql_ir::BinOp::And => "AND",
        mlql_ir::BinOp::Or => "OR",
        mlql_ir::BinOp::Like => "LIKE",
        mlql_ir::BinOp::ILike => "ILIKE",
    }
}

fn literal_to_sql(val: &mlql_ir::Value) -> String {
    match val {
        mlql_ir::Value::Null => "NULL".to_string(),
        mlql_ir::Value::Bool(b) => if *b { "TRUE".to_string() } else { "FALSE".to_string() },
        mlql_ir::Value::Int(i) => i.to_string(),
        mlql_ir::Value::Float(f) => f.to_string(),
        mlql_ir::Value::String(s) => format!("'{}'", s.replace("'", "''")),  // Escape quotes
        _ => "NULL".to_string(),  // TODO: Handle more value types
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_init() -> DuckResult<()> {
        let _executor = DuckExecutor::new()?;
        Ok(())
    }

    #[test]
    fn test_end_to_end_simple_select() -> Result<(), Box<dyn std::error::Error>> {
        // 1. Create executor and sample table
        let executor = DuckExecutor::new()?;
        executor.connection().execute_batch(
            "CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER);
             INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25);"
        )?;

        // 2. Parse MLQL query
        let mlql_query = "from users | select [*]";
        let ast_program = mlql_ast::parse(mlql_query)?;

        // 3. Convert AST to IR
        let ir_program = ast_program.to_ir();

        println!("IR: {:?}", ir_program);

        // 4. Execute via DuckDB (IR -> SQL)
        let result = executor.execute_ir(&ir_program, None)?;

        // 5. Verify results
        println!("Results: {:?}", result);
        assert_eq!(result.row_count, 2);
        assert_eq!(result.columns.len(), 3);
        assert_eq!(result.columns, vec!["id", "name", "age"]);

        Ok(())
    }

    #[test]
    fn test_select_specific_columns() -> Result<(), Box<dyn std::error::Error>> {
        // Setup
        let executor = DuckExecutor::new()?;
        executor.connection().execute_batch(
            "CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER);
             INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25);"
        )?;

        // Test: select specific columns
        let mlql_query = "from users | select [name, age]";
        let ast_program = mlql_ast::parse(mlql_query)?;
        let ir_program = ast_program.to_ir();
        let result = executor.execute_ir(&ir_program, None)?;

        // Verify
        println!("Results: {:?}", result);
        assert_eq!(result.row_count, 2);
        assert_eq!(result.columns, vec!["name", "age"]);
        assert_eq!(result.rows[0][0], serde_json::Value::String("Alice".to_string()));
        assert_eq!(result.rows[0][1], serde_json::Value::Number(30.into()));

        Ok(())
    }

    #[test]
    fn test_select_with_expression() -> Result<(), Box<dyn std::error::Error>> {
        // Setup
        let executor = DuckExecutor::new()?;
        executor.connection().execute_batch(
            "CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER);
             INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25);"
        )?;

        // Test: select with arithmetic expression
        let mlql_query = "from users | select [age * 2 as double_age]";
        let ast_program = mlql_ast::parse(mlql_query)?;
        let ir_program = ast_program.to_ir();
        let result = executor.execute_ir(&ir_program, None)?;

        // Verify
        println!("Results: {:?}", result);
        assert_eq!(result.row_count, 2);
        assert_eq!(result.columns, vec!["double_age"]);
        assert_eq!(result.rows[0][0], serde_json::Value::Number(60.into()));
        assert_eq!(result.rows[1][0], serde_json::Value::Number(50.into()));

        Ok(())
    }

    #[test]
    fn test_filter_simple_comparison() -> Result<(), Box<dyn std::error::Error>> {
        // Setup
        let executor = DuckExecutor::new()?;
        executor.connection().execute_batch(
            "CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER);
             INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35);"
        )?;

        // Test: filter age > 25
        let mlql_query = "from users | filter age > 25";
        let ast_program = mlql_ast::parse(mlql_query)?;
        let ir_program = ast_program.to_ir();
        let result = executor.execute_ir(&ir_program, None)?;

        // Verify: Should return Alice (30) and Charlie (35), not Bob (25)
        println!("Results: {:?}", result);
        assert_eq!(result.row_count, 2);
        assert_eq!(result.rows[0][1], serde_json::Value::String("Alice".to_string()));
        assert_eq!(result.rows[1][1], serde_json::Value::String("Charlie".to_string()));

        Ok(())
    }

    #[test]
    fn test_filter_and_condition() -> Result<(), Box<dyn std::error::Error>> {
        // Setup
        let executor = DuckExecutor::new()?;
        executor.connection().execute_batch(
            "CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER);
             INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35), (4, 'Diana', 40);"
        )?;

        // Test: filter age > 25 && age < 40
        let mlql_query = "from users | filter age > 25 && age < 40";
        let ast_program = mlql_ast::parse(mlql_query)?;
        let ir_program = ast_program.to_ir();
        let result = executor.execute_ir(&ir_program, None)?;

        // Verify: Should return Alice (30) and Charlie (35)
        println!("Results: {:?}", result);
        assert_eq!(result.row_count, 2);
        assert_eq!(result.rows[0][1], serde_json::Value::String("Alice".to_string()));
        assert_eq!(result.rows[1][1], serde_json::Value::String("Charlie".to_string()));

        Ok(())
    }

    #[test]
    fn test_filter_or_condition() -> Result<(), Box<dyn std::error::Error>> {
        // Setup
        let executor = DuckExecutor::new()?;
        executor.connection().execute_batch(
            "CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER);
             INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35);"
        )?;

        // Test: filter name == "Alice" || name == "Bob"
        let mlql_query = "from users | filter name == \"Alice\" || name == \"Bob\"";
        let ast_program = mlql_ast::parse(mlql_query)?;
        let ir_program = ast_program.to_ir();
        let result = executor.execute_ir(&ir_program, None)?;

        // Verify: Should return Alice and Bob, not Charlie
        println!("Results: {:?}", result);
        assert_eq!(result.row_count, 2);
        assert_eq!(result.rows[0][1], serde_json::Value::String("Alice".to_string()));
        assert_eq!(result.rows[1][1], serde_json::Value::String("Bob".to_string()));

        Ok(())
    }

    #[test]
    fn test_filter_like_operator() -> Result<(), Box<dyn std::error::Error>> {
        // Setup
        let executor = DuckExecutor::new()?;
        executor.connection().execute_batch(
            "CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER);
             INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Aaron', 35);"
        )?;

        // Test: filter name like "A%"
        let mlql_query = "from users | filter name like \"A%\"";
        let ast_program = mlql_ast::parse(mlql_query)?;
        let ir_program = ast_program.to_ir();
        let result = executor.execute_ir(&ir_program, None)?;

        // Verify: Should return Alice and Aaron, not Bob
        println!("Results: {:?}", result);
        assert_eq!(result.row_count, 2);
        assert_eq!(result.rows[0][1], serde_json::Value::String("Alice".to_string()));
        assert_eq!(result.rows[1][1], serde_json::Value::String("Aaron".to_string()));

        Ok(())
    }

    #[test]
    fn test_llm_json_direct_execution() -> Result<(), Box<dyn std::error::Error>> {
        // Setup
        let executor = DuckExecutor::new()?;
        executor.connection().execute_batch(
            "CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER);
             INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35);"
        )?;

        // Simulate LLM-generated JSON for: "Show me users older than 25"
        let llm_json = r#"{
            "pipeline": {
                "source": {
                    "type": "Table",
                    "name": "users"
                },
                "ops": [
                    {
                        "op": "Filter",
                        "condition": {
                            "type": "BinaryOp",
                            "op": "Gt",
                            "left": {
                                "type": "Column",
                                "col": {"column": "age"}
                            },
                            "right": {
                                "type": "Literal",
                                "value": 25
                            }
                        }
                    },
                    {
                        "op": "Select",
                        "projections": [
                            {
                                "type": "Column",
                                "col": {"column": "name"}
                            },
                            {
                                "type": "Column",
                                "col": {"column": "age"}
                            }
                        ]
                    }
                ]
            }
        }"#;

        // Parse JSON directly into IR
        let ir_program: mlql_ir::Program = serde_json::from_str(llm_json)?;

        // Execute via DuckDB
        let result = executor.execute_ir(&ir_program, None)?;

        // Verify: Should return Alice (30) and Charlie (35), not Bob (25)
        println!("LLM JSON Results: {:?}", result);
        assert_eq!(result.row_count, 2);
        assert_eq!(result.columns, vec!["name", "age"]);
        assert_eq!(result.rows[0][0], serde_json::Value::String("Alice".to_string()));
        assert_eq!(result.rows[1][0], serde_json::Value::String("Charlie".to_string()));

        Ok(())
    }

    #[test]
    fn test_llm_json_with_complex_filter() -> Result<(), Box<dyn std::error::Error>> {
        // Setup
        let executor = DuckExecutor::new()?;
        executor.connection().execute_batch(
            "CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER);
             INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Aaron', 35), (4, 'Diana', 45);"
        )?;

        // Simulate LLM-generated JSON for: "Show users aged 25-40 or names starting with A"
        let llm_json = r#"{
            "pipeline": {
                "source": {
                    "type": "Table",
                    "name": "users"
                },
                "ops": [
                    {
                        "op": "Filter",
                        "condition": {
                            "type": "BinaryOp",
                            "op": "Or",
                            "left": {
                                "type": "BinaryOp",
                                "op": "And",
                                "left": {
                                    "type": "BinaryOp",
                                    "op": "Ge",
                                    "left": {"type": "Column", "col": {"column": "age"}},
                                    "right": {"type": "Literal", "value": 25}
                                },
                                "right": {
                                    "type": "BinaryOp",
                                    "op": "Le",
                                    "left": {"type": "Column", "col": {"column": "age"}},
                                    "right": {"type": "Literal", "value": 40}
                                }
                            },
                            "right": {
                                "type": "BinaryOp",
                                "op": "Like",
                                "left": {"type": "Column", "col": {"column": "name"}},
                                "right": {"type": "Literal", "value": "A%"}
                            }
                        }
                    }
                ]
            }
        }"#;

        // Parse and execute
        let ir_program: mlql_ir::Program = serde_json::from_str(llm_json)?;
        let result = executor.execute_ir(&ir_program, None)?;

        // Verify: Should return Alice (30), Bob (25), Aaron (35), NOT Diana (45)
        println!("Complex Filter Results: {:?}", result);
        assert_eq!(result.row_count, 3);

        // Check names
        let names: Vec<String> = result.rows.iter()
            .map(|row| row[1].as_str().unwrap().to_string())
            .collect();
        assert!(names.contains(&"Alice".to_string()));
        assert!(names.contains(&"Bob".to_string()));
        assert!(names.contains(&"Aaron".to_string()));
        assert!(!names.contains(&"Diana".to_string()));

        Ok(())
    }

    #[test]
    fn test_distinct_single_column() -> Result<(), Box<dyn std::error::Error>> {
        // Setup
        let executor = DuckExecutor::new()?;
        executor.connection().execute_batch(
            "CREATE TABLE users (id INTEGER, name VARCHAR, city VARCHAR);
             INSERT INTO users VALUES
                (1, 'Alice', 'NYC'),
                (2, 'Bob', 'LA'),
                (3, 'Charlie', 'NYC'),
                (4, 'Diana', 'LA');"
        )?;

        // Test: from users | select [city] | distinct
        let mlql_query = "from users | select [city] | distinct";
        let ast_program = mlql_ast::parse(mlql_query)?;
        let ir_program = ast_program.to_ir();
        let result = executor.execute_ir(&ir_program, None)?;

        // Verify: Should return only 2 unique cities: NYC, LA
        println!("Distinct Results: {:?}", result);
        assert_eq!(result.row_count, 2);
        assert_eq!(result.columns, vec!["city"]);

        // Collect unique cities
        let cities: Vec<String> = result.rows.iter()
            .map(|row| row[0].as_str().unwrap().to_string())
            .collect();
        assert!(cities.contains(&"NYC".to_string()));
        assert!(cities.contains(&"LA".to_string()));

        Ok(())
    }

    #[test]
    fn test_distinct_multiple_columns() -> Result<(), Box<dyn std::error::Error>> {
        // Setup
        let executor = DuckExecutor::new()?;
        executor.connection().execute_batch(
            "CREATE TABLE locations (city VARCHAR, state VARCHAR, zip INTEGER);
             INSERT INTO locations VALUES
                ('NYC', 'NY', 10001),
                ('NYC', 'NY', 10002),
                ('LA', 'CA', 90001),
                ('LA', 'CA', 90001);"
        )?;

        // Test: from locations | select [city, state] | distinct
        let mlql_query = "from locations | select [city, state] | distinct";
        let ast_program = mlql_ast::parse(mlql_query)?;
        let ir_program = ast_program.to_ir();
        let result = executor.execute_ir(&ir_program, None)?;

        // Verify: Should return 2 unique (city, state) pairs
        println!("Distinct Multi-Column Results: {:?}", result);
        assert_eq!(result.row_count, 2);
        assert_eq!(result.columns, vec!["city", "state"]);

        Ok(())
    }

    #[test]
    fn test_group_by_simple() -> Result<(), Box<dyn std::error::Error>> {
        // Setup
        let executor = DuckExecutor::new()?;
        executor.connection().execute_batch(
            "CREATE TABLE orders (id INTEGER, city VARCHAR, amount DECIMAL);
             INSERT INTO orders VALUES
                (1, 'NYC', 100.0),
                (2, 'LA', 150.0),
                (3, 'NYC', 200.0),
                (4, 'LA', 75.0);"
        )?;

        // Test GROUP BY with JSON IR (since parser doesn't support it yet)
        let json_ir = r#"{
            "pipeline": {
                "source": {
                    "type": "Table",
                    "name": "orders"
                },
                "ops": [
                    {
                        "op": "GroupBy",
                        "keys": [{"column": "city"}],
                        "aggs": {
                            "total": {
                                "func": "count",
                                "args": []
                            }
                        }
                    }
                ]
            }
        }"#;

        let ir_program: mlql_ir::Program = serde_json::from_str(json_ir)?;
        let result = executor.execute_ir(&ir_program, None)?;

        // Verify: Should return 2 rows (NYC, LA) with counts
        println!("GroupBy Results: {:?}", result);
        assert_eq!(result.row_count, 2);
        assert_eq!(result.columns, vec!["city", "total"]);

        Ok(())
    }

    #[test]
    fn test_group_by_multiple_aggregates() -> Result<(), Box<dyn std::error::Error>> {
        // Setup
        let executor = DuckExecutor::new()?;
        executor.connection().execute_batch(
            "CREATE TABLE sales (id INTEGER, product VARCHAR, price DECIMAL, qty INTEGER);
             INSERT INTO sales VALUES
                (1, 'Widget', 10.0, 5),
                (2, 'Widget', 12.0, 3),
                (3, 'Gadget', 20.0, 2),
                (4, 'Gadget', 18.0, 4);"
        )?;

        // Test GROUP BY with multiple aggregates
        let json_ir = r#"{
            "pipeline": {
                "source": {
                    "type": "Table",
                    "name": "sales"
                },
                "ops": [
                    {
                        "op": "GroupBy",
                        "keys": [{"column": "product"}],
                        "aggs": {
                            "total_qty": {
                                "func": "sum",
                                "args": [
                                    {"type": "Column", "col": {"column": "qty"}}
                                ]
                            },
                            "avg_price": {
                                "func": "avg",
                                "args": [
                                    {"type": "Column", "col": {"column": "price"}}
                                ]
                            }
                        }
                    }
                ]
            }
        }"#;

        let ir_program: mlql_ir::Program = serde_json::from_str(json_ir)?;
        let result = executor.execute_ir(&ir_program, None)?;

        // Verify: Should return 2 products with aggregates
        println!("GroupBy Multi-Agg Results: {:?}", result);
        assert_eq!(result.row_count, 2);

        // Check columns (order may vary due to HashMap)
        assert!(result.columns.contains(&"product".to_string()));
        assert!(result.columns.contains(&"total_qty".to_string()));
        assert!(result.columns.contains(&"avg_price".to_string()));
        assert_eq!(result.columns.len(), 3);

        Ok(())
    }

    #[test]
    fn test_join_inner() -> Result<(), Box<dyn std::error::Error>> {
        // Setup
        let executor = DuckExecutor::new()?;
        executor.connection().execute_batch(
            "CREATE TABLE users (id INTEGER, name VARCHAR);
             CREATE TABLE orders (id INTEGER, user_id INTEGER, amount DECIMAL);
             INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
             INSERT INTO orders VALUES (101, 1, 100.0), (102, 1, 150.0), (103, 2, 200.0);"
        )?;

        // Test INNER JOIN using JSON IR
        let json_ir = r#"{
            "pipeline": {
                "source": {
                    "type": "Table",
                    "name": "users",
                    "alias": "u"
                },
                "ops": [
                    {
                        "op": "Join",
                        "source": {
                            "type": "Table",
                            "name": "orders",
                            "alias": "o"
                        },
                        "on": {
                            "type": "BinaryOp",
                            "op": "Eq",
                            "left": {
                                "type": "Column",
                                "col": {"table": "u", "column": "id"}
                            },
                            "right": {
                                "type": "Column",
                                "col": {"table": "o", "column": "user_id"}
                            }
                        },
                        "join_type": "Inner"
                    }
                ]
            }
        }"#;

        let ir_program: mlql_ir::Program = serde_json::from_str(json_ir)?;
        let result = executor.execute_ir(&ir_program, None)?;

        // Verify: Should return 3 rows (all orders matched with users)
        println!("INNER JOIN Results: {:?}", result);
        assert_eq!(result.row_count, 3);

        Ok(())
    }

    #[test]
    fn test_join_left() -> Result<(), Box<dyn std::error::Error>> {
        // Setup
        let executor = DuckExecutor::new()?;
        executor.connection().execute_batch(
            "CREATE TABLE users (id INTEGER, name VARCHAR);
             CREATE TABLE orders (id INTEGER, user_id INTEGER, amount DECIMAL);
             INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');
             INSERT INTO orders VALUES (101, 1, 100.0), (102, 2, 150.0);"
        )?;

        // Test LEFT JOIN using JSON IR
        let json_ir = r#"{
            "pipeline": {
                "source": {
                    "type": "Table",
                    "name": "users",
                    "alias": "u"
                },
                "ops": [
                    {
                        "op": "Join",
                        "source": {
                            "type": "Table",
                            "name": "orders",
                            "alias": "o"
                        },
                        "on": {
                            "type": "BinaryOp",
                            "op": "Eq",
                            "left": {
                                "type": "Column",
                                "col": {"table": "u", "column": "id"}
                            },
                            "right": {
                                "type": "Column",
                                "col": {"table": "o", "column": "user_id"}
                            }
                        },
                        "join_type": "Left"
                    }
                ]
            }
        }"#;

        let ir_program: mlql_ir::Program = serde_json::from_str(json_ir)?;
        let result = executor.execute_ir(&ir_program, None)?;

        // Verify: Should return 3 rows (all users, including Charlie with NULL order)
        println!("LEFT JOIN Results: {:?}", result);
        assert_eq!(result.row_count, 3);

        Ok(())
    }

    #[test]
    fn test_join_multiple() -> Result<(), Box<dyn std::error::Error>> {
        // Setup
        let executor = DuckExecutor::new()?;
        executor.connection().execute_batch(
            "CREATE TABLE users (id INTEGER, name VARCHAR);
             CREATE TABLE orders (id INTEGER, user_id INTEGER, product_id INTEGER);
             CREATE TABLE products (id INTEGER, name VARCHAR);
             INSERT INTO users VALUES (1, 'Alice');
             INSERT INTO orders VALUES (101, 1, 201);
             INSERT INTO products VALUES (201, 'Widget');"
        )?;

        // Test multiple JOINs using JSON IR
        let json_ir = r#"{
            "pipeline": {
                "source": {
                    "type": "Table",
                    "name": "users",
                    "alias": "u"
                },
                "ops": [
                    {
                        "op": "Join",
                        "source": {
                            "type": "Table",
                            "name": "orders",
                            "alias": "o"
                        },
                        "on": {
                            "type": "BinaryOp",
                            "op": "Eq",
                            "left": {"type": "Column", "col": {"table": "u", "column": "id"}},
                            "right": {"type": "Column", "col": {"table": "o", "column": "user_id"}}
                        }
                    },
                    {
                        "op": "Join",
                        "source": {
                            "type": "Table",
                            "name": "products",
                            "alias": "p"
                        },
                        "on": {
                            "type": "BinaryOp",
                            "op": "Eq",
                            "left": {"type": "Column", "col": {"table": "o", "column": "product_id"}},
                            "right": {"type": "Column", "col": {"table": "p", "column": "id"}}
                        }
                    }
                ]
            }
        }"#;

        let ir_program: mlql_ir::Program = serde_json::from_str(json_ir)?;
        let result = executor.execute_ir(&ir_program, None)?;

        // Verify: Should return 1 row joining all 3 tables
        println!("Multiple JOIN Results: {:?}", result);
        assert_eq!(result.row_count, 1);

        Ok(())
    }
}
