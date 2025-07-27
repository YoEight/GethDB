#[macro_use]
mod macros;

use crate::tokenizer::Lexer;

mod error;
mod infer;
mod parser;
mod rename;
mod sym;
mod tokenizer;

#[cfg(test)]
mod tests;

pub mod private {
    pub use std::result::Result::Err;
}

pub use parser::{
    Expr, From, Limit, LimitKind, Order, Query, Sort, Source, SourceType, Value, Var, Where,
};
pub use sym::{Literal, Operation};
pub use tokenizer::Pos;

pub type Result<A> = std::result::Result<A, crate::error::Error>;

pub fn parse(query: &str) -> crate::Result<Query<Pos>> {
    let lexer = Lexer::new(query);
    parser::parse(lexer)
}

pub fn parse_rename_and_infer(query: &str) -> crate::Result<InferedQuery> {
    infer(rename(parse(query)?)?)
}

pub use infer::infer;
pub use infer::{Infer, InferedQuery, Type};
pub use rename::rename;
pub use rename::{Lexical, Properties, Renamed, Scope, Scopes};
