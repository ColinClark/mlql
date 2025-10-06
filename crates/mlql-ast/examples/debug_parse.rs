use pest::Parser;
use mlql_ast::parser::{MlqlParser, Rule};

fn main() {
    let input = "age * 2";
    match MlqlParser::parse(Rule::expr, input) {
        Ok(pairs) => {
            for pair in pairs {
                print_pair(&pair, 0);
            }
        }
        Err(e) => println!("Error: {:?}", e),
    }
}

fn print_pair(pair: &pest::iterators::Pair<Rule>, indent: usize) {
    let indent_str = "  ".repeat(indent);
    println!("{}Rule::{:?} = {:?}", indent_str, pair.as_rule(), pair.as_str());
    for inner in pair.clone().into_inner() {
        print_pair(&inner, indent + 1);
    }
}
