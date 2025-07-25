use crate::tokenizer::Lexer;

mod infer;
mod parser;
mod rename;
mod sym;
mod tokenizer;

#[cfg(test)]
mod tests;

pub use parser::{Expr, From, Limit, LimitKind, Order, Query, Sort, Value, Var, Where};
pub use sym::{Literal, Operation};
pub use tokenizer::Pos;

pub fn parse(query: &str) -> eyre::Result<Query<Pos>> {
    let lexer = Lexer::new(query);
    parser::parse(lexer)
}

pub use infer::infer;
pub use infer::{Infer, Infered, Type};
pub use rename::rename;
pub use rename::{Lexical, Properties, Renamed, Scope, Scopes};
