//! Substrait translation module
//!
//! Translates MLQL IR to Substrait protobuf plans for execution via DuckDB.

mod schema;
mod translator;

pub use schema::{SchemaProvider, TableSchema, ColumnInfo, MockSchemaProvider};
pub use translator::{SubstraitTranslator, TranslateError};
