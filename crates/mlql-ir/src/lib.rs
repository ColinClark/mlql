//! MLQL Intermediate Representation (IR)
//!
//! Canonical JSON representation that bridges AST and Substrait.
//! All types are deterministically serializable for caching and provenance.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

mod types;
pub use types::*;

/// Top-level MLQL program
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Program {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pragma: Option<Pragma>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lets: Vec<LetBinding>,

    pub pipeline: Pipeline,
}

impl Program {
    /// Calculate fingerprint (SHA-256) for deterministic caching
    pub fn fingerprint(&self) -> String {
        let json = serde_json::to_string(self).expect("IR should always serialize");
        let mut hasher = Sha256::new();
        hasher.update(json.as_bytes());
        format!("{:x}", hasher.finalize())
    }
}

/// Pragma configuration block
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pragma {
    pub options: HashMap<String, Value>,
}

/// Let binding for reusable pipelines
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LetBinding {
    pub name: String,
    pub pipeline: Pipeline,
}

/// Pipeline - source + operators
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pipeline {
    pub source: Source,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ops: Vec<Operator>,
}

/// Data source
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Source {
    Table {
        name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        alias: Option<String>,
    },
    Graph {
        graph_name: String,
        alias: String,
    },
    SubPipeline {
        pipeline: Box<Pipeline>,
        #[serde(skip_serializing_if = "Option::is_none")]
        alias: Option<String>,
    },
}

/// Pipeline operators
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum Operator {
    Select {
        projections: Vec<Projection>,
    },
    Filter {
        condition: Expr,
    },
    Join {
        source: Source,
        on: Expr,
        #[serde(skip_serializing_if = "Option::is_none")]
        join_type: Option<JoinType>,
    },
    GroupBy {
        keys: Vec<ColumnRef>,
        aggs: HashMap<String, AggCall>,
    },
    Window {
        windows: HashMap<String, WindowDef>,
    },
    Sort {
        keys: Vec<SortKey>,
    },
    Take {
        limit: i64,
    },
    Distinct,
    Union {
        #[serde(default)]
        all: bool,
    },
    Except,
    Intersect,
    Map {
        mappings: HashMap<String, Expr>,
    },
    Expand {
        expr: Expr,
        #[serde(skip_serializing_if = "Option::is_none")]
        alias: Option<String>,
    },
    Resample {
        interval: String,
        method: String,
        on: ColumnRef,
    },
    Agg {
        group_key: GroupKey,
        aggs: HashMap<String, AggCall>,
    },
    Knn {
        query: Expr,
        k: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        index: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        metric: Option<String>,
    },
    Rank {
        by: Expr,
    },
    Neighbors {
        start: Expr,
        depth: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        edge: Option<String>,
    },
    TopK {
        k: i64,
        by: Expr,
    },
    Sample {
        fraction: f64,
        #[serde(skip_serializing_if = "Option::is_none")]
        seed: Option<i64>,
    },
    Assert {
        condition: Expr,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    Explain {
        mode: ExplainMode,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExplainMode {
    Logical,
    Physical,
    Cost,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
    Cross,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Projection {
    Expr(Expr),
    Aliased { expr: Expr, alias: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortKey {
    pub expr: Expr,
    #[serde(default)]
    pub desc: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowDef {
    pub func: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<Expr>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition: Option<Vec<ColumnRef>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<Vec<SortKey>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frame: Option<FrameSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrameSpec {
    pub mode: FrameMode,
    pub start: FrameBound,
    pub end: FrameBound,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FrameMode {
    Rows,
    Range,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FrameBound {
    UnboundedPreceding,
    UnboundedFollowing,
    CurrentRow,
    Preceding(i64),
    Following(i64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GroupKey {
    Tumbling { expr: Expr, interval: String },
    Hopping { expr: Expr, size: String, slide: String },
    Session { expr: Expr, gap: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggCall {
    pub func: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    pub column: String,
}

/// Expression types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Expr {
    Literal { value: Value },
    Column { col: ColumnRef },
    BinaryOp { op: BinOp, left: Box<Expr>, right: Box<Expr> },
    UnaryOp { op: UnOp, expr: Box<Expr> },
    FuncCall { func: String, args: Vec<Expr> },
    FieldAccess { expr: Box<Expr>, field: String },
    Index { expr: Box<Expr>, index: Box<Expr> },
    Array { elements: Vec<Expr> },
    Object { fields: HashMap<String, Expr> },
    Vector { values: Vec<f64> },
    InRange { expr: Box<Expr>, start: Box<Expr>, end: Box<Expr>, inclusive: bool },
    InSet { expr: Box<Expr>, set: Vec<Expr> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BinOp {
    // Arithmetic
    Add, Sub, Mul, Div, Mod,
    // Comparison
    Eq, Ne, Lt, Le, Gt, Ge,
    // Logical
    And, Or,
    // String
    Like, ILike,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UnOp {
    Neg,
    Not,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    Date(String),      // ISO format
    Time(String),      // ISO format
    Timestamp(String), // ISO format
    Array(Vec<Value>),
    Object(HashMap<String, Value>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fingerprint_deterministic() {
        let program1 = Program {
            pragma: None,
            lets: vec![],
            pipeline: Pipeline {
                source: Source::Table {
                    name: "users".to_string(),
                    alias: None,
                },
                ops: vec![],
            },
        };

        let program2 = program1.clone();

        assert_eq!(program1.fingerprint(), program2.fingerprint());
    }

    #[test]
    fn test_json_round_trip() {
        let program = Program {
            pragma: Some(Pragma {
                options: HashMap::from([
                    ("timeout".to_string(), Value::Int(30000)),
                ]),
            }),
            lets: vec![],
            pipeline: Pipeline {
                source: Source::Table {
                    name: "sales".to_string(),
                    alias: Some("s".to_string()),
                },
                ops: vec![
                    Operator::Filter {
                        condition: Expr::BinaryOp {
                            op: BinOp::Eq,
                            left: Box::new(Expr::Column {
                                col: ColumnRef {
                                    table: Some("s".to_string()),
                                    column: "region".to_string(),
                                },
                            }),
                            right: Box::new(Expr::Literal {
                                value: Value::String("EU".to_string()),
                            }),
                        },
                    },
                ],
            },
        };

        let json = serde_json::to_string(&program).unwrap();
        let parsed: Program = serde_json::from_str(&json).unwrap();

        assert_eq!(program.fingerprint(), parsed.fingerprint());
    }
}
