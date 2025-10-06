//! Intermediate Representation (IR)
//!
//! Canonical JSON IR that bridges the AST and the compiler.
//! All nodes are serializable via serde for interoperability.

pub mod types;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub use types::*;

/// Top-level MLQL program
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Program {
    pub pragma: Option<Pragma>,
    pub lets: Vec<LetBinding>,
    pub pipeline: Pipeline,
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
    pub ops: Vec<Operator>,
}

/// Data source
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Source {
    Table {
        name: String,
        alias: Option<String>,
    },
    Graph {
        graph_name: String,
        alias: String,
    },
    SubPipeline {
        pipeline: Box<Pipeline>,
        alias: Option<String>,
    },
}

/// Pipeline operators
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum Operator {
    Select { projections: Vec<Projection> },
    Filter { condition: Expr },
    Join { source: Source, on: Expr, join_type: Option<JoinType> },
    GroupBy { keys: Vec<ColumnRef>, aggs: HashMap<String, AggCall> },
    Window { windows: HashMap<String, WindowDef> },
    Sort { keys: Vec<SortKey> },
    Take { limit: i64 },
    Distinct,
    Union { all: bool },
    Except,
    Intersect,
    Map { mappings: HashMap<String, Expr> },
    Expand { expr: Expr, alias: Option<String> },
    Resample { interval: String, method: String, on: ColumnRef },
    Agg { group_key: GroupKey, aggs: HashMap<String, AggCall> },
    Knn { query: Expr, k: i64, index: Option<String>, metric: Option<String> },
    Rank { by: Expr },
    Neighbors { start: Expr, depth: i64, edge: Option<String> },
    TopK { k: i64, by: Expr },
    Sample { fraction: f64, seed: Option<i64> },
    Assert { condition: Expr, message: Option<String> },
    Explain { mode: ExplainMode },
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
    pub desc: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowDef {
    pub func: String,
    pub args: Vec<Expr>,
    pub partition: Option<Vec<ColumnRef>>,
    pub order: Option<Vec<SortKey>>,
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
    pub args: Vec<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnRef {
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
