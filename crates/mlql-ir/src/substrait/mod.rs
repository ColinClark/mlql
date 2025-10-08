//! Substrait translation module
//!
//! Translates MLQL IR to Substrait protobuf plans for execution via DuckDB.
//!
//! # Overview
//!
//! This module provides translation from MLQL's JSON intermediate representation (IR)
//! to [Substrait](https://substrait.io/) protocol buffers. Substrait is a cross-language
//! serialization format for relational query plans, allowing MLQL queries to be executed
//! by any Substrait-compatible engine (e.g., DuckDB).
//!
//! # Architecture
//!
//! ```text
//! MLQL Text → AST → JSON IR → Substrait Plan → DuckDB → Results
//!                      ↑            ↑
//!                   This crate   This module
//! ```
//!
//! # Usage
//!
//! ```rust
//! use mlql_ir::{Program, Pipeline, Source, Operator};
//! use mlql_ir::substrait::{SubstraitTranslator, MockSchemaProvider, TableSchema, ColumnInfo};
//! use prost::Message;
//!
//! // Set up schema provider with table metadata
//! let mut schema_provider = MockSchemaProvider::new();
//! schema_provider.add_table(TableSchema {
//!     name: "users".to_string(),
//!     columns: vec![
//!         ColumnInfo { name: "id".to_string(), data_type: "INTEGER".to_string(), nullable: true },
//!         ColumnInfo { name: "name".to_string(), data_type: "VARCHAR".to_string(), nullable: true },
//!         ColumnInfo { name: "age".to_string(), data_type: "INTEGER".to_string(), nullable: true },
//!     ],
//! });
//!
//! // Create MLQL IR program
//! let program = Program {
//!     pragma: None,
//!     lets: vec![],
//!     pipeline: Pipeline {
//!         source: Source::Table { name: "users".to_string(), alias: None },
//!         ops: vec![],
//!     },
//! };
//!
//! // Translate to Substrait
//! let translator = SubstraitTranslator::new(&schema_provider);
//! let plan = translator.translate(&program).expect("Translation failed");
//!
//! // Serialize to bytes for execution
//! let mut plan_bytes = Vec::new();
//! plan.encode(&mut plan_bytes).expect("Serialization failed");
//!
//! // Execute via DuckDB's from_substrait() function
//! // let results = conn.query("SELECT * FROM from_substrait(?)", [plan_bytes])?;
//! ```
//!
//! # Operator Support
//!
//! ## Implemented Operators
//!
//! | MLQL Operator | Substrait Relation | Status |
//! |---------------|-------------------|---------|
//! | `from table` | `ReadRel` | ✅ Complete |
//! | `filter` | `FilterRel` | ✅ Complete |
//! | `select` | `ProjectRel` | ✅ Complete |
//! | `sort` | `SortRel` | ✅ Complete |
//! | `take` | `FetchRel` | ✅ Complete |
//! | `distinct` | `AggregateRel` | ✅ Complete |
//! | `group by` | `AggregateRel` | ✅ Complete (sum, count, avg, min, max) |
//! | `join` | `JoinRel` | ✅ Complete |
//!
//! ## Future Work
//!
//! - Window functions (`WindowRel`)
//! - Set operations (`SetRel` for UNION/EXCEPT/INTERSECT)
//! - Subquery sources (`SubPipeline`)
//!
//! # Schema Provider
//!
//! The [`SchemaProvider`] trait abstracts table schema lookup. Implementations can:
//! - Query DuckDB catalog at runtime
//! - Use cached schema metadata
//! - Mock schemas for testing
//!
//! Example custom provider:
//!
//! ```rust
//! use mlql_ir::substrait::{SchemaProvider, TableSchema, ColumnInfo};
//!
//! struct MySchemaProvider {
//!     // Your schema storage
//! }
//!
//! impl SchemaProvider for MySchemaProvider {
//!     fn get_table_schema(&self, table_name: &str) -> Result<TableSchema, String> {
//!         // Look up schema from your source
//!         Ok(TableSchema {
//!             name: table_name.to_string(),
//!             columns: vec![
//!                 ColumnInfo { name: "id".to_string(), data_type: "INTEGER".to_string(), nullable: false },
//!             ],
//!         })
//!     }
//! }
//! ```
//!
//! # Error Handling
//!
//! Translation can fail with [`TranslateError`] for:
//! - **Schema errors**: Unknown table or column
//! - **Unsupported operators**: Window, Union (not yet implemented)
//! - **Translation errors**: Invalid expression structure
//!
//! All errors include descriptive context for debugging.
//!
//! # Testing
//!
//! Integration tests in `tests/substrait_operators.rs` verify end-to-end execution:
//! - All operators tested against real DuckDB via `from_substrait()`
//! - Schema tracking validated across pipeline transformations
//! - Results compared with expected output
//!
//! Run tests with:
//! ```bash
//! env DUCKDB_CUSTOM_BUILD=1 cargo test --package mlql-ir --test substrait_operators
//! ```

mod schema;
mod translator;

pub use schema::{SchemaProvider, TableSchema, ColumnInfo, MockSchemaProvider};
pub use translator::{SubstraitTranslator, TranslateError};
