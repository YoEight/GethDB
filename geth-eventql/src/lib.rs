use crate::{
    parser::Query,
    tokenizer::{Lexer, Pos},
};

mod parser;
mod sym;
mod tokenizer;

pub fn parse(query: &str) -> eyre::Result<Query<Pos>> {
    let lexer = Lexer::new(query);
    parser::parse(lexer)
}
