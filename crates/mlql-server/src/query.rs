//! Query execution against DuckDB using MLQL IR

use mlql_duck::{DuckExecutor, QueryResult};
use mlql_ir::{Pipeline, Program};
use serde_json::json;
use std::sync::Arc;

/// Execution mode for MLQL queries
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionMode {
    /// SQL-based execution (current production path)
    Sql,
    /// Substrait-based execution (new experimental path)
    Substrait,
}

impl ExecutionMode {
    /// Get execution mode from environment variable
    ///
    /// Reads `MLQL_EXECUTION_MODE` env var:
    /// - "substrait" → ExecutionMode::Substrait
    /// - anything else → ExecutionMode::Sql (default)
    pub fn from_env() -> Self {
        match std::env::var("MLQL_EXECUTION_MODE")
            .unwrap_or_else(|_| "sql".to_string())
            .to_lowercase()
            .as_str()
        {
            "substrait" => ExecutionMode::Substrait,
            _ => ExecutionMode::Sql,
        }
    }
}

/// Execute MLQL IR with automatic mode selection based on environment
///
/// Uses `MLQL_EXECUTION_MODE` environment variable to choose execution path:
/// - "substrait" → Substrait-based execution
/// - anything else → SQL-based execution (default)
pub async fn execute_ir_auto(
    pipeline: Pipeline,
    database: Option<String>,
) -> Result<(String, serde_json::Value), Box<dyn std::error::Error>> {
    match ExecutionMode::from_env() {
        ExecutionMode::Substrait => execute_ir_substrait(pipeline, database).await,
        ExecutionMode::Sql => execute_ir(pipeline, database).await,
    }
}

/// Execute MLQL IR against DuckDB and return SQL + results
pub async fn execute_ir(
    pipeline: Pipeline,
    database: Option<String>,
) -> Result<(String, serde_json::Value), Box<dyn std::error::Error>> {
    // For now, create a new executor per-request since DuckDB Connection is not Send+Sync
    // TODO: Use connection pooling or serialize access
    let executor = if let Some(db_path) = database {
        DuckExecutor::open(db_path)?
    } else {
        DuckExecutor::new()?
    };

    // Convert Pipeline to Program for execution
    let program = Program {
        pragma: None,
        lets: vec![],
        pipeline: pipeline.clone(),
    };

    // Execute program and capture SQL
    let result = executor.execute_ir(&program, None)?;

    // Get the actual SQL that was executed
    let sql = result.sql.clone().unwrap_or_else(|| "No SQL generated".to_string());

    // Convert result to JSON
    let json_result = result_to_json(&result)?;

    Ok((sql, json_result))
}

/// Execute MLQL IR via Substrait translation (new execution path)
pub async fn execute_ir_substrait(
    pipeline: Pipeline,
    database: Option<String>,
) -> Result<(String, serde_json::Value), Box<dyn std::error::Error>> {
    use mlql_ir::substrait::SubstraitTranslator;
    use crate::catalog::DuckDbSchemaProvider;
    use prost::Message;

    // 1. Open DuckDB connection
    let conn = if let Some(db_path) = database {
        duckdb::Connection::open(db_path)?
    } else {
        duckdb::Connection::open_in_memory()?
    };
    let conn = Arc::new(conn);

    // 2. Load Substrait extension
    load_substrait_extension(&conn)?;

    // 3. Create schema provider
    let schema_provider = DuckDbSchemaProvider::new(conn.clone());

    // 4. Initialize translator
    let translator = SubstraitTranslator::new(&schema_provider);

    // 5. Convert Pipeline to Program
    let program = Program {
        pragma: None,
        lets: vec![],
        pipeline: pipeline.clone(),
    };

    // 6. Translate to Substrait
    let plan = translator.translate(&program)
        .map_err(|e| format!("Substrait translation failed: {}", e))?;

    // 7. Serialize to protobuf
    let mut plan_bytes = Vec::new();
    plan.encode(&mut plan_bytes)
        .map_err(|e| format!("Protobuf encoding failed: {}", e))?;

    // 8. Execute via from_substrait()
    let mut stmt = conn.prepare("SELECT * FROM from_substrait(?)")?;
    let mut rows = stmt.query([plan_bytes.as_slice()])?;

    // 9. Convert rows to JSON
    let json_result = duckdb_rows_to_json(&mut rows)?;

    // 10. Return plan info + results
    let plan_info = format!("Substrait plan: {} bytes", plan_bytes.len());
    Ok((plan_info, json_result))
}

/// Load Substrait extension into DuckDB connection
fn load_substrait_extension(conn: &duckdb::Connection) -> Result<(), Box<dyn std::error::Error>> {
    // Try to load the Substrait extension
    // Option 1: If SUBSTRAIT_EXTENSION_PATH is set, use that
    if let Ok(extension_path) = std::env::var("SUBSTRAIT_EXTENSION_PATH") {
        if !std::path::Path::new(&extension_path).exists() {
            return Err(format!(
                "Substrait extension not found at: {}\n\
                 Please build the extension or unset SUBSTRAIT_EXTENSION_PATH.",
                extension_path
            ).into());
        }

        conn.execute_batch(&format!("LOAD '{}'", extension_path))
            .map_err(|e| format!("Failed to load extension from {}: {}", extension_path, e))?;

        tracing::info!("Loaded Substrait extension from: {}", extension_path);
    } else {
        // Option 2: Try to install from DuckDB's extension repository
        tracing::info!("SUBSTRAIT_EXTENSION_PATH not set, trying to load substrait extension");

        // Try: INSTALL substrait; LOAD substrait;
        match conn.execute_batch("INSTALL substrait; LOAD substrait;") {
            Ok(_) => {
                tracing::info!("Successfully loaded substrait extension from repository");
            }
            Err(e) => {
                return Err(format!(
                    "Failed to load substrait extension: {}\n\
                     Please set SUBSTRAIT_EXTENSION_PATH to the path of your custom extension:\n\
                     export SUBSTRAIT_EXTENSION_PATH=/Users/colin/Dev/truepop/mlql/duckdb-substrait-upgrade/build/release/extension/substrait/substrait.duckdb_extension",
                    e
                ).into());
            }
        }
    }

    Ok(())
}

/// Convert DuckDB rows to JSON format
fn duckdb_rows_to_json(rows: &mut duckdb::Rows) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let mut json_rows = Vec::new();
    let mut columns = Vec::new();

    // Get column names from first row metadata
    if let Some(first_row) = rows.next()? {
        let col_count = first_row.as_ref().column_count();

        for i in 0..col_count {
            columns.push(first_row.as_ref().column_name(i)?.to_string());
        }

        // Process first row
        let mut row_obj = serde_json::Map::new();
        for (i, col_name) in columns.iter().enumerate() {
            let value = duckdb_value_to_json(&first_row, i)?;
            row_obj.insert(col_name.clone(), value);
        }
        json_rows.push(serde_json::Value::Object(row_obj));
    }

    // Process remaining rows
    while let Some(row) = rows.next()? {
        let mut row_obj = serde_json::Map::new();
        for (i, col_name) in columns.iter().enumerate() {
            let value = duckdb_value_to_json(&row, i)?;
            row_obj.insert(col_name.clone(), value);
        }
        json_rows.push(serde_json::Value::Object(row_obj));
    }

    Ok(json!({
        "columns": columns,
        "rows": json_rows,
        "row_count": json_rows.len()
    }))
}

/// Convert DuckDB value to JSON
fn duckdb_value_to_json(row: &duckdb::Row, idx: usize) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    use duckdb::types::ValueRef;

    match row.get_ref(idx)? {
        ValueRef::Null => Ok(serde_json::Value::Null),
        ValueRef::Boolean(b) => Ok(serde_json::Value::Bool(b)),
        ValueRef::TinyInt(i) => Ok(serde_json::json!(i)),
        ValueRef::SmallInt(i) => Ok(serde_json::json!(i)),
        ValueRef::Int(i) => Ok(serde_json::json!(i)),
        ValueRef::BigInt(i) => Ok(serde_json::json!(i)),
        ValueRef::HugeInt(i) => Ok(serde_json::json!(i)),
        ValueRef::UTinyInt(i) => Ok(serde_json::json!(i)),
        ValueRef::USmallInt(i) => Ok(serde_json::json!(i)),
        ValueRef::UInt(i) => Ok(serde_json::json!(i)),
        ValueRef::UBigInt(i) => Ok(serde_json::json!(i)),
        ValueRef::Float(f) => Ok(serde_json::json!(f)),
        ValueRef::Double(f) => Ok(serde_json::json!(f)),
        ValueRef::Text(s) => Ok(serde_json::Value::String(String::from_utf8_lossy(s).to_string())),
        ValueRef::Blob(b) => Ok(serde_json::Value::String(format!("<blob {} bytes>", b.len()))),
        _ => Ok(serde_json::Value::String("<unsupported>".to_string())),
    }
}

/// Convert QueryResult to JSON
fn result_to_json(result: &QueryResult) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let mut rows = Vec::new();

    for row in &result.rows {
        let mut row_obj = serde_json::Map::new();

        for (i, col_name) in result.columns.iter().enumerate() {
            if let Some(value) = row.get(i) {
                row_obj.insert(col_name.clone(), value.clone());
            }
        }

        rows.push(serde_json::Value::Object(row_obj));
    }

    Ok(json!({
        "columns": result.columns,
        "rows": rows,
        "row_count": result.rows.len()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mlql_ir::*;

    #[tokio::test]
    async fn test_execute_simple_query() {
        // Create simple pipeline: SELECT * FROM users
        let pipeline = Pipeline {
            source: Source::Table {
                name: "users".to_string(),
                alias: None,
            },
            ops: vec![],
        };

        // This should fail because table doesn't exist, but we're testing the flow
        let result = execute_ir(pipeline, None).await;

        // We expect an error since the table doesn't exist
        assert!(result.is_err());
    }

    #[test]
    fn test_result_to_json() {
        let result = QueryResult {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![
                vec![
                    serde_json::json!(1),
                    serde_json::json!("Alice"),
                ],
                vec![
                    serde_json::json!(2),
                    serde_json::json!("Bob"),
                ],
            ],
            row_count: 2,
        };

        let json = result_to_json(&result).unwrap();

        assert_eq!(json["row_count"], 2);
        assert_eq!(json["columns"].as_array().unwrap().len(), 2);
        assert_eq!(json["rows"].as_array().unwrap().len(), 2);
    }
}
