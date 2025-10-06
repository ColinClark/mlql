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

        // Install and load Substrait extension from community repository
        conn.execute_batch("INSTALL substrait FROM community; LOAD substrait;")?;

        Ok(Self { conn })
    }

    pub fn from_connection(conn: Connection) -> Self {
        Self { conn }
    }

    /// Execute Substrait plan (JSON format)
    pub fn execute_substrait_json(
        &self,
        substrait_json: &str,
        budget: Option<ExecutionBudget>,
    ) -> Result<QueryResult, ExecutionError> {
        // Apply budget constraints
        if let Some(ref budget) = budget {
            self.apply_budget(budget)?;
        }

        // Execute via DuckDB's Substrait extension
        let query = "SELECT * FROM from_substrait_json(?)";

        // Prepare statement
        let mut stmt = self.conn.prepare(query)?;

        // Extract column names before executing
        let column_count = stmt.column_count();
        let column_names: Vec<String> = (0..column_count)
            .map(|i| stmt.column_name(i).unwrap_or(&"unknown".to_string()).to_string())
            .collect();

        // Execute query
        let mut rows = stmt.query([substrait_json])?;

        // Collect rows
        let mut result_rows = Vec::new();
        let mut row_count = 0;

        while let Some(row) = rows.next()? {
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

    /// Execute Substrait plan (binary format)
    pub fn execute_substrait_binary(
        &self,
        substrait_bytes: &[u8],
        budget: Option<ExecutionBudget>,
    ) -> Result<QueryResult, ExecutionError> {
        // Apply budget constraints
        if let Some(ref budget) = budget {
            self.apply_budget(budget)?;
        }

        // Execute via DuckDB's Substrait extension
        // Uses: SELECT * FROM from_substrait(?)

        todo!("Binary Substrait execution not yet implemented")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_init() -> DuckResult<()> {
        let executor = DuckExecutor::new()?;

        // Verify Substrait extension is loaded
        let mut stmt = executor.connection()
            .prepare("SELECT * FROM duckdb_extensions() WHERE extension_name = 'substrait'")?;

        let mut rows = stmt.query([])?;
        assert!(rows.next()?.is_some(), "Substrait extension should be loaded");

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

        // 4. Encode IR to Substrait JSON
        let encoder = mlql_substrait::SubstraitEncoder::new();
        let substrait_json = encoder.encode(&ir_program)?;

        println!("Substrait JSON:\n{}", substrait_json);

        // 5. Execute via DuckDB
        let result = executor.execute_substrait_json(&substrait_json, None)?;

        // 6. Verify results
        println!("Results: {:?}", result);
        assert_eq!(result.row_count, 2);
        assert!(!result.columns.is_empty());

        Ok(())
    }
}
