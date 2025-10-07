//! Schema provider trait and types for table metadata lookup

use std::collections::HashMap;

/// Column metadata
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String, // TODO: Use proper type enum
    pub nullable: bool,
}

/// Table schema information
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
}

/// Trait for resolving table schemas at translation time
pub trait SchemaProvider {
    /// Get schema for a table by name
    fn get_table_schema(&self, table_name: &str) -> Result<TableSchema, String>;
}

/// Mock schema provider for testing
pub struct MockSchemaProvider {
    tables: HashMap<String, TableSchema>,
}

impl MockSchemaProvider {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn add_table(&mut self, schema: TableSchema) {
        self.tables.insert(schema.name.clone(), schema);
    }
}

impl SchemaProvider for MockSchemaProvider {
    fn get_table_schema(&self, table_name: &str) -> Result<TableSchema, String> {
        self.tables
            .get(table_name)
            .cloned()
            .ok_or_else(|| format!("Table '{}' not found", table_name))
    }
}
