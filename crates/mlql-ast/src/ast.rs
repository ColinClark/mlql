//! AST types for MLQL
//!
//! Minimal AST representation closely following the Pest grammar.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Program {
    pub pragma: Option<Pragma>,
    pub lets: Vec<LetStatement>,
    pub pipeline: Pipeline,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pragma {
    pub options: Vec<(String, Value)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LetStatement {
    pub name: String,
    pub pipeline: Pipeline,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pipeline {
    pub source: Source,
    pub operators: Vec<Operator>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Source {
    Table { name: String, alias: Option<String> },
    Graph { graph_name: String, alias: String },
    SubQuery { pipeline: Box<Pipeline>, alias: Option<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Operator {
    Select { items: Vec<SelectItem> },
    Filter { expr: Expr },
    Join { source: Source, on: Expr, join_type: Option<JoinType> },
    GroupBy { keys: Vec<ColumnRef>, aggs: Vec<(String, FuncCall)> },
    Sort { keys: Vec<SortKey> },
    Take { limit: i64 },
    Distinct,
    // ... more operators as needed
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SelectItem {
    Wildcard,
    Expr(Expr),
    Aliased { expr: Expr, alias: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortKey {
    pub expr: Expr,
    pub desc: bool,
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
pub enum Expr {
    Literal(Value),
    Column(ColumnRef),
    BinaryOp { op: BinOp, left: Box<Expr>, right: Box<Expr> },
    UnaryOp { op: UnOp, expr: Box<Expr> },
    FuncCall(FuncCall),
    // ... more expression types
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuncCall {
    pub name: String,
    pub args: Vec<Expr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnRef {
    pub table: Option<String>,
    pub column: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BinOp {
    Add, Sub, Mul, Div, Mod,
    Eq, Ne, Lt, Le, Gt, Ge,
    And, Or,
    Like, ILike,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UnOp {
    Neg,
    Not,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
}
