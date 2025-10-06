//! Catalog - discovers and manages metadata
//!
//! Discovers:
//! - Tables and views
//! - Functions (built-in and UDFs)
//! - Data types
//! - Extensions (graph, vector, etc.)

use std::collections::HashMap;
use thiserror::Error;

use crate::ir::{DataType, Schema};

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("Object not found: {0}")]
    NotFound(String),

    #[error("Database error: {0}")]
    Database(String),
}

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: String,
    pub schema: Schema,
    pub table_type: TableType,
}

#[derive(Debug, Clone)]
pub enum TableType {
    Table,
    View,
    Graph,
}

#[derive(Debug, Clone)]
pub struct FunctionInfo {
    pub name: String,
    pub args: Vec<DataType>,
    pub return_type: DataType,
    pub is_aggregate: bool,
    pub is_window: bool,
}

pub struct Catalog {
    tables: HashMap<String, TableInfo>,
    functions: HashMap<String, Vec<FunctionInfo>>, // Name -> overloads
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            functions: HashMap::new(),
        }
    }

    /// Discover schema from DuckDB connection
    pub fn discover_from_duckdb(&mut self, _conn: &duckdb::Connection) -> Result<(), CatalogError> {
        // TODO: Query information_schema to populate catalog
        todo!("DuckDB discovery not yet implemented")
    }

    pub fn get_table(&self, name: &str) -> Result<&TableInfo, CatalogError> {
        self.tables
            .get(name)
            .ok_or_else(|| CatalogError::NotFound(name.to_string()))
    }

    pub fn get_function(&self, name: &str, args: &[DataType]) -> Result<&FunctionInfo, CatalogError> {
        let overloads = self.functions
            .get(name)
            .ok_or_else(|| CatalogError::NotFound(name.to_string()))?;

        // TODO: Implement overload resolution
        overloads.first()
            .ok_or_else(|| CatalogError::NotFound(name.to_string()))
    }

    pub fn register_table(&mut self, info: TableInfo) {
        self.tables.insert(info.name.clone(), info);
    }

    pub fn register_function(&mut self, info: FunctionInfo) {
        self.functions
            .entry(info.name.clone())
            .or_insert_with(Vec::new)
            .push(info);
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}
