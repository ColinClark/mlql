//! Function registry
//!
//! Built-in functions:
//! - mask() - PII masking
//! - approx_p() - Approximate percentiles
//! - bm25() - Full-text search ranking
//! - embed() - Vector embeddings
//! - similarity() - Vector similarity

use std::collections::HashMap;

use crate::catalog::FunctionInfo;
use crate::ir::DataType;

pub struct FunctionRegistry {
    functions: HashMap<String, Vec<FunctionInfo>>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            functions: HashMap::new(),
        };
        registry.register_builtin();
        registry
    }

    fn register_builtin(&mut self) {
        // PII Masking
        self.register(FunctionInfo {
            name: "mask".to_string(),
            args: vec![DataType::String],
            return_type: DataType::String,
            is_aggregate: false,
            is_window: false,
        });

        // Approximate percentile
        self.register(FunctionInfo {
            name: "approx_p".to_string(),
            args: vec![DataType::Float64, DataType::Float64],
            return_type: DataType::Float64,
            is_aggregate: true,
            is_window: false,
        });

        // BM25 ranking
        self.register(FunctionInfo {
            name: "bm25".to_string(),
            args: vec![DataType::String, DataType::String],
            return_type: DataType::Float64,
            is_aggregate: false,
            is_window: false,
        });

        // Vector similarity
        self.register(FunctionInfo {
            name: "similarity".to_string(),
            args: vec![DataType::Vector(None), DataType::Vector(None)],
            return_type: DataType::Float64,
            is_aggregate: false,
            is_window: false,
        });
    }

    pub fn register(&mut self, func: FunctionInfo) {
        self.functions
            .entry(func.name.clone())
            .or_insert_with(Vec::new)
            .push(func);
    }

    pub fn get(&self, name: &str) -> Option<&Vec<FunctionInfo>> {
        self.functions.get(name)
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}
