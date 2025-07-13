use crate::{
    sym::{Keyword, Sym},
    tokenizer::{Lexer, Pos},
};

mod ast;
mod state;

pub use ast::*;
use state::ParserState;

pub fn parse(lexer: Lexer<'_>) -> eyre::Result<Ast<Pos>> {
    let init_pos = lexer.pos();
    let mut state = ParserState::new(lexer);

    // let mut sources = Vec::new();

    // sources.push(parse_source(lexer)?);

    todo!()
}

fn parse_source(state: &mut ParserState<'_>) -> eyre::Result<From<Pos>> {
    let pos = state.pos();
    let sym = state.shift_or_bail()?;

    if sym != Sym::Keyword(Keyword::From) {
        eyre::bail!("{}: expected 'FROM' keyword but got '{sym}' instead", pos);
    }

    state.skip_whitespace()?;
    let ident = parse_ident(state)?;
    state.skip_whitespace()?;

    todo!()
}

fn parse_ident(state: &mut ParserState<'_>) -> eyre::Result<()> {
    eyre::bail!("todo")
}
