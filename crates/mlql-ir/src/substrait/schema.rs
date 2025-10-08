//! Schema provider trait and types for table metadata lookup
//!
//! The schema provider abstracts table metadata lookup during Substrait translation.
//! Implementations can query DuckDB catalog, use cached metadata, or provide mock
//! schemas for testing.

use std::collections::HashMap;

/// Column metadata describing a single column in a table.
///
/// Contains the column name, data type (as a SQL type string), and nullability flag.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column name (e.g., "id", "name", "age")
    pub name: String,
    /// Data type as SQL type string (e.g., "INTEGER", "VARCHAR", "TIMESTAMP")
    pub data_type: String, // TODO: Use proper type enum
    /// Whether the column allows NULL values
    pub nullable: bool,
}

/// Table schema information including table name and all columns.
///
/// Used by the translator to resolve column references and generate proper
/// field indices in Substrait relations.
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// Table name
    pub name: String,
    /// List of columns in declaration order (order matters for field indices)
    pub columns: Vec<ColumnInfo>,
}

/// Trait for resolving table schemas at translation time.
///
/// Implementations can query database catalogs, use cached metadata, or provide
/// mock schemas for testing. The translator calls [`get_table_schema`](SchemaProvider::get_table_schema)
/// for each table referenced in the MLQL IR.
///
/// # Example Implementation
///
/// ```rust
/// use mlql_ir::substrait::{SchemaProvider, TableSchema, ColumnInfo};
///
/// struct MySchemaProvider {
///     // Your schema storage
/// }
///
/// impl SchemaProvider for MySchemaProvider {
///     fn get_table_schema(&self, table_name: &str) -> Result<TableSchema, String> {
///         // Look up schema from database, cache, etc.
///         match table_name {
///             "users" => Ok(TableSchema {
///                 name: "users".to_string(),
///                 columns: vec![
///                     ColumnInfo { name: "id".to_string(), data_type: "INTEGER".to_string(), nullable: false },
///                     ColumnInfo { name: "name".to_string(), data_type: "VARCHAR".to_string(), nullable: true },
///                 ],
///             }),
///             _ => Err(format!("Unknown table: {}", table_name)),
///         }
///     }
/// }
/// ```
pub trait SchemaProvider {
    /// Get schema for a table by name.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The name of the table to look up
    ///
    /// # Returns
    ///
    /// Table schema with columns, or an error string if the table doesn't exist.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use mlql_ir::substrait::{SchemaProvider, MockSchemaProvider, TableSchema, ColumnInfo};
    /// # let mut provider = MockSchemaProvider::new();
    /// # provider.add_table(TableSchema {
    /// #     name: "users".to_string(),
    /// #     columns: vec![ColumnInfo { name: "id".to_string(), data_type: "INTEGER".to_string(), nullable: false }],
    /// # });
    /// let schema = provider.get_table_schema("users").expect("Table not found");
    /// assert_eq!(schema.name, "users");
    /// ```
    fn get_table_schema(&self, table_name: &str) -> Result<TableSchema, String>;
}

/// Mock schema provider for testing.
///
/// Stores table schemas in a HashMap and provides them on demand. Useful for
/// unit tests and integration tests where you don't want to connect to a real
/// database.
///
/// # Example
///
/// ```rust
/// use mlql_ir::substrait::{MockSchemaProvider, TableSchema, ColumnInfo, SchemaProvider};
///
/// let mut provider = MockSchemaProvider::new();
/// provider.add_table(TableSchema {
///     name: "users".to_string(),
///     columns: vec![
///         ColumnInfo { name: "id".to_string(), data_type: "INTEGER".to_string(), nullable: false },
///         ColumnInfo { name: "name".to_string(), data_type: "VARCHAR".to_string(), nullable: true },
///     ],
/// });
///
/// let schema = provider.get_table_schema("users").unwrap();
/// assert_eq!(schema.columns.len(), 2);
/// ```
pub struct MockSchemaProvider {
    tables: HashMap<String, TableSchema>,
}

impl MockSchemaProvider {
    /// Create a new empty mock schema provider.
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Add a table schema to the provider.
    ///
    /// If a table with the same name already exists, it will be replaced.
    pub fn add_table(&mut self, schema: TableSchema) {
        self.tables.insert(schema.name.clone(), schema);
    }
}

impl Default for MockSchemaProvider {
    fn default() -> Self {
        Self::new()
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
