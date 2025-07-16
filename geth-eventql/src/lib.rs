use crate::tokenizer::{Lexer, Pos};

mod parser;
mod sym;
mod tokenizer;

#[cfg(test)]
mod tests;

pub use parser::{Expr, From, Limit, LimitKind, Order, Query, Sort, Value, Where};

pub fn parse(query: &str) -> eyre::Result<Query<Pos>> {
    let lexer = Lexer::new(query);
    parser::parse(lexer)
}
