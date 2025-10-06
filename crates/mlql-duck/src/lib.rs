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
    let mut where_clause = None;
    let mut order_clause = None;
    let mut limit_clause = None;

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
            _ => return Err(ExecutionError::SubstraitError(format!("Unsupported operator: {:?}", op))),
        }
    }

    // Build final SQL
    let mut sql = format!("SELECT {} FROM {}", select_clause, table);

    if let Some(where_sql) = where_clause {
        sql.push_str(&format!(" WHERE {}", where_sql));
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
}
