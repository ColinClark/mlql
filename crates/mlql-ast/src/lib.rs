//! MLQL AST - parser and AST types

pub mod ast;
mod parser;
mod to_ir;

pub use ast::*;
pub use parser::{parse, ParseError};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic() {
        // Will implement once parser is ready
    }
}
