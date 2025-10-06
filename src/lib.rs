//! MLQL - Machine Learning Query Language
//!
//! A domain-specific language designed for LLMs to communicate with SQL-based databases.
//! Combines traditional SQL operations with ML-specific primitives like vector search,
//! graph traversal, and time-series operations.

pub mod catalog;
pub mod compile;
pub mod exec;
pub mod functions;
pub mod ir;
pub mod parser;
pub mod policy;
pub mod validate;

#[cfg(feature = "server")]
pub mod server;

pub use ir::{Pipeline, Program};
pub use parser::parse;

/// Re-export commonly used types
pub mod prelude {
    pub use crate::catalog::Catalog;
    pub use crate::compile::Compiler;
    pub use crate::exec::Executor;
    pub use crate::ir::{Pipeline, Program};
    pub use crate::parser::parse;
    pub use crate::policy::PolicyEngine;
    pub use crate::validate::Validator;
}
