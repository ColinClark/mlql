//! Database catalog extraction and management

use duckdb::{Connection, Result as DuckResult};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableCatalog {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    pub sample_data: Vec<serde_json::Map<String, serde_json::Value>>,
    pub row_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub sample_values: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseCatalog {
    pub database_path: String,
    pub tables: Vec<TableCatalog>,
}

impl DatabaseCatalog {
    /// Extract catalog information from a DuckDB database
    pub fn from_database<P: AsRef<Path>>(db_path: P) -> DuckResult<Self> {
        let path_str = db_path.as_ref().to_string_lossy().to_string();
        let conn = Connection::open(&db_path)?;

        let mut tables = Vec::new();

        // Get all table names
        let mut stmt = conn.prepare("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")?;
        let table_names: Vec<String> = stmt
            .query_map([], |row| row.get(0))?
            .collect::<DuckResult<Vec<_>>>()?;

        // For each table, get schema and sample data
        for table_name in table_names {
            if let Ok(table_catalog) = Self::extract_table_info(&conn, &table_name) {
                tables.push(table_catalog);
            }
        }

        Ok(DatabaseCatalog {
            database_path: path_str,
            tables,
        })
    }

    /// Extract information for a single table
    fn extract_table_info(conn: &Connection, table_name: &str) -> DuckResult<TableCatalog> {
        // Get column information
        let query = format!(
            "SELECT column_name, data_type, is_nullable \
             FROM information_schema.columns \
             WHERE table_name = '{}' \
             ORDER BY ordinal_position",
            table_name
        );

        let mut stmt = conn.prepare(&query)?;
        let columns: Vec<(String, String, String)> = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .collect::<DuckResult<Vec<_>>>()?;

        // Get row count
        let count_query = format!("SELECT COUNT(*) FROM \"{}\"", table_name);
        let row_count: i64 = conn.query_row(&count_query, [], |row| row.get(0))?;

        // Get sample data (up to 5 rows)
        let sample_query = format!("SELECT * FROM \"{}\" LIMIT 5", table_name);
        let mut sample_stmt = conn.prepare(&sample_query)?;

        let column_names: Vec<String> = columns.iter().map(|(name, _, _)| name.clone()).collect();
        let mut sample_data = Vec::new();

        let rows = sample_stmt.query_map([], |row| {
            let mut row_map = serde_json::Map::new();
            for (idx, col_name) in column_names.iter().enumerate() {
                // Try to extract value as JSON
                let value = match row.get_ref(idx)? {
                    duckdb::types::ValueRef::Null => serde_json::Value::Null,
                    duckdb::types::ValueRef::Boolean(b) => serde_json::Value::Bool(b),
                    duckdb::types::ValueRef::TinyInt(i) => serde_json::json!(i),
                    duckdb::types::ValueRef::SmallInt(i) => serde_json::json!(i),
                    duckdb::types::ValueRef::Int(i) => serde_json::json!(i),
                    duckdb::types::ValueRef::BigInt(i) => serde_json::json!(i),
                    duckdb::types::ValueRef::HugeInt(i) => serde_json::json!(i),
                    duckdb::types::ValueRef::UTinyInt(i) => serde_json::json!(i),
                    duckdb::types::ValueRef::USmallInt(i) => serde_json::json!(i),
                    duckdb::types::ValueRef::UInt(i) => serde_json::json!(i),
                    duckdb::types::ValueRef::UBigInt(i) => serde_json::json!(i),
                    duckdb::types::ValueRef::Float(f) => serde_json::json!(f),
                    duckdb::types::ValueRef::Double(f) => serde_json::json!(f),
                    duckdb::types::ValueRef::Text(s) => serde_json::Value::String(String::from_utf8_lossy(s).to_string()),
                    duckdb::types::ValueRef::Blob(b) => serde_json::Value::String(format!("<blob {} bytes>", b.len())),
                    _ => serde_json::Value::String("<unsupported>".to_string()),
                };
                row_map.insert(col_name.clone(), value);
            }
            Ok(row_map)
        })?;

        for row_result in rows {
            if let Ok(row_map) = row_result {
                sample_data.push(row_map);
            }
        }

        // Build column info with sample values
        let mut column_infos = Vec::new();
        for (col_name, data_type, is_nullable) in columns {
            let sample_values: Vec<serde_json::Value> = sample_data
                .iter()
                .filter_map(|row| row.get(&col_name).cloned())
                .collect();

            column_infos.push(ColumnInfo {
                name: col_name,
                data_type,
                is_nullable: is_nullable == "YES",
                sample_values,
            });
        }

        Ok(TableCatalog {
            name: table_name.to_string(),
            columns: column_infos,
            sample_data,
            row_count: row_count as usize,
        })
    }

    /// Format catalog as markdown for MCP resource
    #[allow(dead_code)]
    pub fn to_markdown(&self) -> String {
        let mut md = String::new();

        md.push_str(&format!("# Database Catalog\n\n"));
        md.push_str(&format!("**Database:** `{}`\n\n", self.database_path));
        md.push_str(&format!("**Tables:** {}\n\n", self.tables.len()));

        for table in &self.tables {
            md.push_str(&format!("## Table: `{}`\n\n", table.name));
            md.push_str(&format!("**Rows:** {}\n\n", table.row_count));

            md.push_str("### Columns\n\n");
            md.push_str("| Column | Type | Nullable | Sample Values |\n");
            md.push_str("|--------|------|----------|---------------|\n");

            for col in &table.columns {
                let nullable = if col.is_nullable { "✓" } else { "" };
                let samples: Vec<String> = col.sample_values
                    .iter()
                    .take(3)
                    .map(|v| match v {
                        serde_json::Value::String(s) => format!("\"{}\"", s),
                        _ => v.to_string(),
                    })
                    .collect();
                let samples_str = samples.join(", ");

                md.push_str(&format!(
                    "| `{}` | {} | {} | {} |\n",
                    col.name, col.data_type, nullable, samples_str
                ));
            }

            md.push_str("\n");
        }

        md
    }
}

/// DuckDB-backed schema provider for Substrait translation
///
/// Queries the DuckDB information_schema at runtime to resolve table schemas.
/// Holds a reference to a DuckDB connection for catalog queries.
pub struct DuckDbSchemaProvider {
    conn: Arc<Connection>,
}

impl DuckDbSchemaProvider {
    /// Create a new schema provider with the given DuckDB connection
    pub fn new(conn: Arc<Connection>) -> Self {
        Self { conn }
    }
}

impl mlql_ir::substrait::SchemaProvider for DuckDbSchemaProvider {
    fn get_table_schema(&self, table_name: &str) -> Result<mlql_ir::substrait::TableSchema, String> {
        // Query information_schema for column information
        let query = "
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = ?
            ORDER BY ordinal_position
        ";

        let mut stmt = self.conn.prepare(query)
            .map_err(|e| format!("Failed to prepare schema query: {}", e))?;

        let columns: Result<Vec<_>, _> = stmt
            .query_map([table_name], |row| {
                Ok(mlql_ir::substrait::ColumnInfo {
                    name: row.get(0)?,
                    data_type: row.get(1)?,
                    nullable: row.get::<_, String>(2)? == "YES",
                })
            })
            .map_err(|e| format!("Schema query failed: {}", e))?
            .collect();

        let columns = columns
            .map_err(|e| format!("Failed to read schema rows: {}", e))?;

        if columns.is_empty() {
            return Err(format!("Table '{}' not found in database", table_name));
        }

        Ok(mlql_ir::substrait::TableSchema {
            name: table_name.to_string(),
            columns,
        })
    }
}
