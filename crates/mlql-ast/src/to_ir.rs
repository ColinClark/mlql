//! Convert AST to canonical IR

use mlql_ir::{self as ir};
use crate::ast::*;

impl Program {
    /// Convert AST Program to IR Program
    pub fn to_ir(self) -> ir::Program {
        ir::Program {
            pragma: self.pragma.map(|p| p.to_ir()),
            lets: self.lets.into_iter().map(|l| l.to_ir()).collect(),
            pipeline: self.pipeline.to_ir(),
        }
    }
}

impl Pragma {
    fn to_ir(self) -> ir::Pragma {
        use std::collections::HashMap;

        let options = self.options.into_iter()
            .map(|(k, v)| (k, v.to_ir()))
            .collect::<HashMap<_, _>>();

        ir::Pragma { options }
    }
}

impl LetStatement {
    fn to_ir(self) -> ir::LetBinding {
        ir::LetBinding {
            name: self.name,
            pipeline: self.pipeline.to_ir(),
        }
    }
}

impl Pipeline {
    fn to_ir(self) -> ir::Pipeline {
        ir::Pipeline {
            source: self.source.to_ir(),
            ops: self.operators.into_iter().map(|op| op.to_ir()).collect(),
        }
    }
}

impl Source {
    fn to_ir(self) -> ir::Source {
        match self {
            Source::Table { name, alias } => ir::Source::Table { name, alias },
            Source::Graph { graph_name, alias } => ir::Source::Graph { graph_name, alias },
            Source::SubQuery { pipeline, alias } => ir::Source::SubPipeline {
                pipeline: Box::new(pipeline.to_ir()),
                alias,
            },
        }
    }
}

impl Operator {
    fn to_ir(self) -> ir::Operator {
        match self {
            Operator::Select { items } => {
                ir::Operator::Select {
                    projections: items.into_iter().map(|i| i.to_ir()).collect(),
                }
            }
            Operator::Filter { expr } => {
                ir::Operator::Filter {
                    condition: expr.to_ir(),
                }
            }
            Operator::Join { source, on, join_type } => {
                ir::Operator::Join {
                    source: source.to_ir(),
                    on: on.to_ir(),
                    join_type: join_type.map(|jt| jt.to_ir()),
                }
            }
            Operator::GroupBy { keys, aggs } => {
                use std::collections::HashMap;

                let aggs_map = aggs.into_iter()
                    .map(|(name, func)| (name, func.to_ir()))
                    .collect::<HashMap<_, _>>();

                ir::Operator::GroupBy {
                    keys: keys.into_iter().map(|k| k.to_ir()).collect(),
                    aggs: aggs_map,
                }
            }
            Operator::Sort { keys } => {
                ir::Operator::Sort {
                    keys: keys.into_iter().map(|k| k.to_ir()).collect(),
                }
            }
            Operator::Take { limit } => {
                ir::Operator::Take { limit }
            }
            Operator::Distinct => ir::Operator::Distinct,
        }
    }
}

impl SelectItem {
    fn to_ir(self) -> ir::Projection {
        match self {
            SelectItem::Wildcard => {
                // Represent wildcard as column reference "*"
                ir::Projection::Expr(ir::Expr::Column {
                    col: ir::ColumnRef {
                        table: None,
                        column: "*".to_string(),
                    },
                })
            }
            SelectItem::Expr(expr) => ir::Projection::Expr(expr.to_ir()),
            SelectItem::Aliased { expr, alias } => {
                ir::Projection::Aliased {
                    expr: expr.to_ir(),
                    alias,
                }
            }
        }
    }
}

impl SortKey {
    fn to_ir(self) -> ir::SortKey {
        ir::SortKey {
            expr: self.expr.to_ir(),
            desc: self.desc,
        }
    }
}

impl JoinType {
    fn to_ir(self) -> ir::JoinType {
        match self {
            JoinType::Inner => ir::JoinType::Inner,
            JoinType::Left => ir::JoinType::Left,
            JoinType::Right => ir::JoinType::Right,
            JoinType::Full => ir::JoinType::Full,
            JoinType::Semi => ir::JoinType::Semi,
            JoinType::Anti => ir::JoinType::Anti,
            JoinType::Cross => ir::JoinType::Cross,
        }
    }
}

impl Expr {
    fn to_ir(self) -> ir::Expr {
        match self {
            Expr::Literal(v) => ir::Expr::Literal { value: v.to_ir() },
            Expr::Column(col) => ir::Expr::Column { col: col.to_ir() },
            Expr::BinaryOp { op, left, right } => {
                ir::Expr::BinaryOp {
                    op: op.to_ir(),
                    left: Box::new(left.to_ir()),
                    right: Box::new(right.to_ir()),
                }
            }
            Expr::UnaryOp { op, expr } => {
                ir::Expr::UnaryOp {
                    op: op.to_ir(),
                    expr: Box::new(expr.to_ir()),
                }
            }
            Expr::FuncCall(func) => {
                ir::Expr::FuncCall {
                    func: func.name,
                    args: func.args.into_iter().map(|e| e.to_ir()).collect(),
                }
            }
        }
    }
}

impl FuncCall {
    fn to_ir(self) -> ir::AggCall {
        ir::AggCall {
            func: self.name,
            args: self.args.into_iter().map(|e| e.to_ir()).collect(),
        }
    }
}

impl ColumnRef {
    fn to_ir(self) -> ir::ColumnRef {
        ir::ColumnRef {
            table: self.table,
            column: self.column,
        }
    }
}

impl BinOp {
    fn to_ir(self) -> ir::BinOp {
        match self {
            BinOp::Add => ir::BinOp::Add,
            BinOp::Sub => ir::BinOp::Sub,
            BinOp::Mul => ir::BinOp::Mul,
            BinOp::Div => ir::BinOp::Div,
            BinOp::Mod => ir::BinOp::Mod,
            BinOp::Eq => ir::BinOp::Eq,
            BinOp::Ne => ir::BinOp::Ne,
            BinOp::Lt => ir::BinOp::Lt,
            BinOp::Le => ir::BinOp::Le,
            BinOp::Gt => ir::BinOp::Gt,
            BinOp::Ge => ir::BinOp::Ge,
            BinOp::And => ir::BinOp::And,
            BinOp::Or => ir::BinOp::Or,
            BinOp::Like => ir::BinOp::Like,
            BinOp::ILike => ir::BinOp::ILike,
        }
    }
}

impl UnOp {
    fn to_ir(self) -> ir::UnOp {
        match self {
            UnOp::Neg => ir::UnOp::Neg,
            UnOp::Not => ir::UnOp::Not,
        }
    }
}

impl Value {
    fn to_ir(self) -> ir::Value {
        match self {
            Value::Null => ir::Value::Null,
            Value::Bool(b) => ir::Value::Bool(b),
            Value::Int(i) => ir::Value::Int(i),
            Value::Float(f) => ir::Value::Float(f),
            Value::String(s) => ir::Value::String(s),
        }
    }
}
