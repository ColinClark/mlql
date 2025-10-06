//! MLQL Parser - converts text to AST using Pest
//!
//! The parser transforms MLQL source text into an abstract syntax tree (AST)
//! that can then be converted to the canonical JSON IR.

use pest::Parser;
use pest_derive::Parser;
use thiserror::Error;

use crate::ir::Program;

#[derive(Parser)]
#[grammar = "parser/mlql.pest"]
pub struct MlqlParser;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("Syntax error: {0}")]
    Syntax(String),

    #[error("Invalid token at line {line}, column {col}: {token}")]
    InvalidToken {
        line: usize,
        col: usize,
        token: String,
    },

    #[error("Unexpected EOF")]
    UnexpectedEof,

    #[error("Pest parsing error: {0}")]
    Pest(#[from] pest::error::Error<Rule>),
}

/// Parse MLQL source text into a Program AST
pub fn parse(source: &str) -> Result<Program, ParseError> {
    let pairs = MlqlParser::parse(Rule::program, source)?;

    // TODO: Convert pest pairs to AST
    // For now, return a placeholder
    todo!("AST construction not yet implemented")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_parse() {
        // TODO: Add parser tests
    }
}
