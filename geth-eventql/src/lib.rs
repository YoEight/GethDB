use crate::tokenizer::Lexer;

mod parser;
mod rename;
mod sym;
mod tokenizer;

#[cfg(test)]
mod tests;

pub use parser::{Expr, From, Limit, LimitKind, Order, Query, Sort, Value, Where};
pub use sym::{Literal, Operation};
pub use tokenizer::Pos;

pub fn parse(query: &str) -> eyre::Result<Query<Pos>> {
    let lexer = Lexer::new(query);
    parser::parse(lexer)
}

pub use rename::rename;
