//! Validation layer for MLQL programs
//!
//! Validates:
//! - Schema compatibility
//! - Type checking
//! - Policy attachments
//! - Resource budgets

use thiserror::Error;

use crate::ir::{DataType, Program, Schema};
use crate::catalog::Catalog;

#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("Type mismatch: expected {expected:?}, got {actual:?}")]
    TypeMismatch {
        expected: DataType,
        actual: DataType,
    },

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Function not found: {0}")]
    FunctionNotFound(String),

    #[error("Invalid aggregation: {0}")]
    InvalidAggregation(String),

    #[error("Budget exceeded: {0}")]
    BudgetExceeded(String),

    #[error("Policy violation: {0}")]
    PolicyViolation(String),
}

pub struct Validator {
    catalog: Catalog,
}

impl Validator {
    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }

    /// Validate a program and return its output schema
    pub fn validate(&self, program: &Program) -> Result<Schema, ValidationError> {
        // TODO: Implement validation
        // 1. Resolve all table/column references
        // 2. Type check expressions
        // 3. Validate aggregations
        // 4. Check policy constraints
        // 5. Verify resource budgets

        todo!("Validation not yet implemented")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate() {
        // TODO: Add validation tests
    }
}
