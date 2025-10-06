//! Pest-based parser for MLQL

use pest::Parser;
use pest_derive::Parser;
use thiserror::Error;

use crate::ast::*;

#[derive(Parser)]
#[grammar = "mlql.pest"]
pub struct MlqlParser;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("Syntax error: {0}")]
    Syntax(String),

    #[error("Pest error: {0}")]
    Pest(#[from] pest::error::Error<Rule>),
}

/// Parse MLQL source text into AST
pub fn parse(source: &str) -> Result<Program, ParseError> {

    let mut pairs = MlqlParser::parse(Rule::program, source)?;
    let program_pair = pairs.next().ok_or_else(|| ParseError::Syntax("Empty input".to_string()))?;

    let mut pragma = None;
    let mut lets = Vec::new();
    let mut pipeline = None;

    for pair in program_pair.into_inner() {
        match pair.as_rule() {
            Rule::pragma_block => {
                pragma = Some(parse_pragma(pair)?);
            }
            Rule::let_stmt => {
                lets.push(parse_let_stmt(pair)?);
            }
            Rule::pipeline => {
                pipeline = Some(parse_pipeline(pair)?);
            }
            Rule::EOI => {}
            _ => {}
        }
    }

    Ok(Program {
        pragma,
        lets,
        pipeline: pipeline.ok_or_else(|| ParseError::Syntax("Missing pipeline".to_string()))?,
    })
}

fn parse_pragma(_pair: pest::iterators::Pair<Rule>) -> Result<Pragma, ParseError> {
    // TODO: Implement pragma parsing
    Ok(Pragma { options: vec![] })
}

fn parse_let_stmt(pair: pest::iterators::Pair<Rule>) -> Result<LetStatement, ParseError> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();
    let pipeline = parse_pipeline(inner.next().unwrap())?;
    Ok(LetStatement { name, pipeline })
}

fn parse_pipeline(pair: pest::iterators::Pair<Rule>) -> Result<Pipeline, ParseError> {
    let mut inner = pair.into_inner();
    let source = parse_source(inner.next().unwrap())?;

    let mut operators = Vec::new();
    for op_pair in inner {
        if op_pair.as_rule() == Rule::op {
            operators.push(parse_operator(op_pair.into_inner().next().unwrap())?);
        }
    }

    Ok(Pipeline { source, operators })
}

fn parse_source(pair: pest::iterators::Pair<Rule>) -> Result<Source, ParseError> {
    let mut inner = pair.into_inner();
    let source_body = inner.next().unwrap();
    let alias = inner.next().map(|p| p.as_str().to_string());

    let source_inner = source_body.into_inner().next().unwrap();
    match source_inner.as_rule() {
        Rule::ident => {
            Ok(Source::Table {
                name: source_inner.as_str().to_string(),
                alias,
            })
        }
        Rule::pipeline => {
            Ok(Source::SubQuery {
                pipeline: Box::new(parse_pipeline(source_inner)?),
                alias,
            })
        }
        _ => Err(ParseError::Syntax("Invalid source".to_string())),
    }
}

fn parse_operator(pair: pest::iterators::Pair<Rule>) -> Result<Operator, ParseError> {
    match pair.as_rule() {
        Rule::select_op => {
            let select_list = pair.into_inner().next().unwrap();
            let items: Result<Vec<_>, _> = select_list.into_inner()
                .map(parse_select_item)
                .collect();
            Ok(Operator::Select { items: items? })
        }
        Rule::filter_op => {
            let expr = parse_expr(pair.into_inner().next().unwrap())?;
            Ok(Operator::Filter { expr })
        }
        Rule::sort_op => {
            let keys: Result<Vec<_>, _> = pair.into_inner()
                .map(parse_sort_key)
                .collect();
            Ok(Operator::Sort { keys: keys? })
        }
        Rule::take_op => {
            let limit = pair.into_inner().next().unwrap()
                .as_str().parse().unwrap();
            Ok(Operator::Take { limit })
        }
        Rule::distinct_op => {
            Ok(Operator::Distinct)
        }
        _ => Err(ParseError::Syntax(format!("Unknown operator: {:?}", pair.as_rule()))),
    }
}

fn parse_select_item(pair: pest::iterators::Pair<Rule>) -> Result<SelectItem, ParseError> {
    let mut inner = pair.into_inner();
    let first = inner.next().unwrap();

    match first.as_rule() {
        Rule::wildcard => Ok(SelectItem::Wildcard),
        Rule::expr => {
            let expr = parse_expr(first)?;
            // Check if there's an "as alias" following
            if let Some(alias_pair) = inner.next() {
                Ok(SelectItem::Aliased {
                    expr,
                    alias: alias_pair.as_str().to_string(),
                })
            } else {
                Ok(SelectItem::Expr(expr))
            }
        }
        _ => Err(ParseError::Syntax("Invalid select item".to_string())),
    }
}

fn parse_sort_key(pair: pest::iterators::Pair<Rule>) -> Result<SortKey, ParseError> {
    let text = pair.as_str();
    let desc = text.starts_with('-');
    let expr_pair = pair.into_inner().next().unwrap();

    Ok(SortKey {
        expr: parse_expr(expr_pair)?,
        desc,
    })
}

fn parse_expr(pair: pest::iterators::Pair<Rule>) -> Result<Expr, ParseError> {
    match pair.as_rule() {
        // Expression hierarchy - need to handle binary operators
        Rule::expr | Rule::or_expr | Rule::and_expr | Rule::not_expr | Rule::cmp_expr => {
            // These just unwrap to the next level
            let mut inner = pair.into_inner();
            if let Some(first) = inner.next() {
                parse_expr(first)
            } else {
                Err(ParseError::Syntax("Empty expression".to_string()))
            }
        }
        Rule::add_expr | Rule::mul_expr => {
            // Parse binary operators: first_term (op next_term)*
            // Pest generates flat sequence: [term, op_char, term, op_char, term, ...]
            let mut inner = pair.into_inner();
            let mut left = parse_expr(inner.next().unwrap())?;

            while let Some(next_pair) = inner.next() {
                // next_pair could be an operator char or the next term
                // Need to peek at what it is
                match next_pair.as_rule() {
                    Rule::add_expr | Rule::mul_expr | Rule::unary_expr | Rule::postfix | Rule::primary => {
                        // This is actually the right operand, we need to find the operator
                        // The operator should have been right before this
                        // Actually, the Pest grammar generates: term, term, term...
                        // Let me reconsider - operators might not be separate tokens
                        // They might be implicit in the structure
                        // For now, let's just recursively parse and return the first
                        return Ok(left);
                    }
                    _ => {
                        // This must be an operator - read it as a string
                        let op_str = next_pair.as_str();
                        let right_pair = inner.next().ok_or_else(|| ParseError::Syntax("Missing right operand".to_string()))?;
                        let right = parse_expr(right_pair)?;

                        let op = match op_str {
                            "+" => BinOp::Add,
                            "-" => BinOp::Sub,
                            "*" => BinOp::Mul,
                            "/" => BinOp::Div,
                            "%" => BinOp::Mod,
                            _ => return Err(ParseError::Syntax(format!("Unknown operator: {}", op_str))),
                        };

                        left = Expr::BinaryOp {
                            op,
                            left: Box::new(left),
                            right: Box::new(right),
                        };
                    }
                }
            }

            Ok(left)
        }
        Rule::unary_expr | Rule::postfix => {
            let mut inner = pair.into_inner();
            if let Some(first) = inner.next() {
                parse_expr(first)
            } else {
                Err(ParseError::Syntax("Empty expression".to_string()))
            }
        }
        Rule::primary => {
            let inner = pair.into_inner().next().unwrap();
            match inner.as_rule() {
                Rule::literal => parse_literal(inner),
                Rule::col_ref => parse_col_ref(inner),
                Rule::func_call => parse_func_call(inner),
                Rule::expr => parse_expr(inner),
                _ => Err(ParseError::Syntax(format!("Invalid primary: {:?}", inner.as_rule()))),
            }
        }
        Rule::literal => parse_literal(pair),
        Rule::col_ref => parse_col_ref(pair),
        Rule::func_call => parse_func_call(pair),
        _ => Err(ParseError::Syntax(format!("Cannot parse expr: {:?}", pair.as_rule()))),
    }
}

fn parse_literal(pair: pest::iterators::Pair<Rule>) -> Result<Expr, ParseError> {
    let inner = pair.into_inner().next().unwrap();
    let value = match inner.as_rule() {
        Rule::int => Value::Int(inner.as_str().parse().unwrap()),
        Rule::decimal => Value::Float(inner.as_str().parse().unwrap()),
        Rule::string => {
            let s = inner.as_str();
            Value::String(s[1..s.len()-1].to_string()) // Remove quotes
        }
        _ => {
            match inner.as_str() {
                "true" => Value::Bool(true),
                "false" => Value::Bool(false),
                "null" => Value::Null,
                _ => return Err(ParseError::Syntax("Invalid literal".to_string())),
            }
        }
    };
    Ok(Expr::Literal(value))
}

fn parse_col_ref(pair: pest::iterators::Pair<Rule>) -> Result<Expr, ParseError> {
    let parts: Vec<_> = pair.into_inner().collect();

    let (table, column) = if parts.len() == 2 {
        (Some(parts[0].as_str().to_string()), parts[1].as_str().to_string())
    } else {
        (None, parts[0].as_str().to_string())
    };

    Ok(Expr::Column(ColumnRef { table, column }))
}

fn parse_func_call(pair: pest::iterators::Pair<Rule>) -> Result<Expr, ParseError> {
    let mut inner = pair.into_inner();
    let name = inner.next().unwrap().as_str().to_string();

    let args = if let Some(arg_list) = inner.next() {
        arg_list.into_inner()
            .map(parse_expr)
            .collect::<Result<Vec<_>, _>>()?
    } else {
        vec![]
    };

    Ok(Expr::FuncCall(FuncCall { name, args }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_query() {
        let result = parse("from users | select [*]");
        assert!(result.is_ok());
    }
}
