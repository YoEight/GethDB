use crate::tokenizer::Lexer;

mod parser;
mod sym;
mod tokenizer;

#[cfg(test)]
mod tests;

pub use parser::{Expr, From, Limit, LimitKind, Order, Query, Sort, Value, Where};
pub use tokenizer::Pos;
pub use sym::{Operation, Literal};

pub fn parse(query: &str) -> eyre::Result<Query<Pos>> {
    let lexer = Lexer::new(query);
    parser::parse(lexer)
}
