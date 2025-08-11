#[macro_use]
mod macros;

use crate::tokenizer::Lexer;

mod codegen;
mod error;
mod eval;
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
    Expr, FromSource, Limit, LimitKind, Order, Query, Sort, Source, SourceType, Subject, Value,
    Var, Where,
};
pub use sym::{Literal, Operation};
pub use tokenizer::Pos;

pub type Result<A> = std::result::Result<A, crate::error::Error>;

pub fn parse(query: &str) -> crate::Result<Query> {
    let lexer = Lexer::new(query);
    parser::parse(lexer)
}

pub fn parse_rename_and_infer(query: &str) -> crate::Result<InferedQuery> {
    let mut query = parse(query)?;
    let scopes = rename(&mut query)?;

    infer(scopes, query)
}

pub use codegen::{Instr, codegen};
pub use eval::{Dictionary, Entry, EvalError, eval};
pub use infer::infer;
pub use infer::{Infer, InferedQuery, Type};
pub use rename::rename;
pub use rename::{Properties, Scope, Scopes};
