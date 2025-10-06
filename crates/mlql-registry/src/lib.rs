//! Function registry and policy definitions

use mlql_ir::DataType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("Function not found: {0}")]
    FunctionNotFound(String),

    #[error("Type mismatch for function {func}: expected {expected:?}, got {actual:?}")]
    TypeMismatch {
        func: String,
        expected: Vec<DataType>,
        actual: Vec<DataType>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionSignature {
    pub name: String,
    pub args: Vec<DataType>,
    pub return_type: DataType,
    pub is_aggregate: bool,
    pub is_window: bool,
    pub substrait_uri: Option<String>, // For custom extensions
}

pub struct FunctionRegistry {
    functions: HashMap<String, Vec<FunctionSignature>>,
    version: String, // Semver for plan compatibility
}

impl FunctionRegistry {
    pub fn new(version: impl Into<String>) -> Self {
        let mut registry = Self {
            functions: HashMap::new(),
            version: version.into(),
        };
        registry.register_builtins();
        registry
    }

    fn register_builtins(&mut self) {
        // PII Masking
        self.register(FunctionSignature {
            name: "mask".to_string(),
            args: vec![DataType::String],
            return_type: DataType::String,
            is_aggregate: false,
            is_window: false,
            substrait_uri: Some("mlql:mask:v1".to_string()),
        });

        // Approximate percentile
        self.register(FunctionSignature {
            name: "approx_p".to_string(),
            args: vec![DataType::Float64, DataType::Float64],
            return_type: DataType::Float64,
            is_aggregate: true,
            is_window: false,
            substrait_uri: Some("mlql:approx_percentile:v1".to_string()),
        });

        // BM25 full-text search
        self.register(FunctionSignature {
            name: "bm25".to_string(),
            args: vec![DataType::String, DataType::String],
            return_type: DataType::Float64,
            is_aggregate: false,
            is_window: false,
            substrait_uri: Some("mlql:bm25:v1".to_string()),
        });

        // Vector similarity
        self.register(FunctionSignature {
            name: "similarity".to_string(),
            args: vec![DataType::Vector(None), DataType::Vector(None)],
            return_type: DataType::Float64,
            is_aggregate: false,
            is_window: false,
            substrait_uri: Some("mlql:vector_similarity:v1".to_string()),
        });

        // Standard aggregates (map to DuckDB/Substrait builtins)
        for (name, ret_type) in [
            ("count", DataType::Int64),
            ("sum", DataType::Float64),
            ("avg", DataType::Float64),
            ("min", DataType::Float64),
            ("max", DataType::Float64),
        ] {
            self.register(FunctionSignature {
                name: name.to_string(),
                args: vec![DataType::Unknown], // Polymorphic
                return_type: ret_type,
                is_aggregate: true,
                is_window: false,
                substrait_uri: None, // Built-in
            });
        }
    }

    pub fn register(&mut self, sig: FunctionSignature) {
        self.functions
            .entry(sig.name.clone())
            .or_insert_with(Vec::new)
            .push(sig);
    }

    pub fn lookup(&self, name: &str, arg_types: &[DataType]) -> Result<&FunctionSignature, RegistryError> {
        let overloads = self.functions
            .get(name)
            .ok_or_else(|| RegistryError::FunctionNotFound(name.to_string()))?;

        // Simple overload resolution (exact match or polymorphic)
        overloads
            .iter()
            .find(|sig| {
                sig.args.len() == arg_types.len()
                    && sig.args.iter().zip(arg_types).all(|(expected, actual)| {
                        expected == actual || *expected == DataType::Unknown
                    })
            })
            .ok_or_else(|| RegistryError::TypeMismatch {
                func: name.to_string(),
                expected: overloads[0].args.clone(),
                actual: arg_types.to_vec(),
            })
    }

    pub fn version(&self) -> &str {
        &self.version
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new("0.1.0")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builtin_lookup() {
        let registry = FunctionRegistry::default();

        let sig = registry.lookup("mask", &[DataType::String]).unwrap();
        assert_eq!(sig.name, "mask");
        assert_eq!(sig.return_type, DataType::String);
    }

    #[test]
    fn test_aggregate_lookup() {
        let registry = FunctionRegistry::default();

        let sig = registry.lookup("sum", &[DataType::Float64]).unwrap();
        assert!(sig.is_aggregate);
    }
}
