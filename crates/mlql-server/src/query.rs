//! Query execution against DuckDB using MLQL IR

use mlql_duck::{DuckExecutor, QueryResult};
use mlql_ir::{Pipeline, Program};
use serde_json::json;

/// Execute MLQL IR against DuckDB and return SQL + results
pub async fn execute_ir(
    pipeline: Pipeline,
    _database: Option<String>,
) -> Result<(String, serde_json::Value), Box<dyn std::error::Error>> {
    // For now, create a new executor per-request since DuckDB Connection is not Send+Sync
    // TODO: Use connection pooling or serialize access
    let executor = DuckExecutor::new()?;

    // Convert Pipeline to Program for execution
    let program = Program {
        pragma: None,
        lets: vec![],
        pipeline: pipeline.clone(),
    };

    // Execute program
    let result = executor.execute_ir(&program, None)?;

    // Extract SQL for debugging (construct from pipeline)
    let sql = format!("Generated SQL for: {:?}", pipeline.source);

    // Convert result to JSON
    let json_result = result_to_json(&result)?;

    Ok((sql, json_result))
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
