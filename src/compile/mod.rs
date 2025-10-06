//! Compiler - transforms IR to DuckDB SQL
//!
//! Converts the validated IR into executable DuckDB SQL with bound parameters.

use thiserror::Error;

use crate::catalog::Catalog;
use crate::ir::Program;

#[derive(Debug, Error)]
pub enum CompileError {
    #[error("Unsupported operation: {0}")]
    Unsupported(String),

    #[error("Code generation failed: {0}")]
    Codegen(String),
}

pub struct CompiledQuery {
    pub sql: String,
    pub params: Vec<QueryParam>,
}

#[derive(Debug, Clone)]
pub enum QueryParam {
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    Null,
}

pub struct Compiler {
    catalog: Catalog,
}

impl Compiler {
    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }

    /// Compile MLQL program to DuckDB SQL
    pub fn compile(&self, program: &Program) -> Result<CompiledQuery, CompileError> {
        // TODO: Implement compilation
        // 1. Convert pipeline to SQL SELECT
        // 2. Handle special operators (knn, graph, etc.)
        // 3. Extract parameters for binding
        // 4. Optimize SQL generation

        todo!("Compilation not yet implemented")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compile() {
        // TODO: Add compilation tests
    }
}
