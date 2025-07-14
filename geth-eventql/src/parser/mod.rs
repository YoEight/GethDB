use crate::{
    sym::{Keyword, Literal, Sym},
    tokenizer::{Lexer, Pos},
};

mod ast;
mod state;

pub use ast::*;
use state::ParserState;

pub fn parse(lexer: Lexer<'_>) -> eyre::Result<Query<Pos>> {
    let mut state = ParserState::new(lexer);

    parse_query(&mut state)
}

fn parse_query(state: &mut ParserState<'_>) -> eyre::Result<Query<Pos>> {
    todo!()
}

fn parse_from_statement(state: &mut ParserState<'_>) -> eyre::Result<From<Pos>> {
    state.skip_whitespace()?;

    let pos = state.pos();
    let sym = state.shift_or_bail()?;

    state.expect(Sym::Keyword(Keyword::From))?;
    state.skip_whitespace()?;
    let ident = parse_ident(state)?;
    state.skip_whitespace()?;
    state.expect(Sym::Keyword(Keyword::In))?;
    state.skip_whitespace()?;

    let source = parse_source(state)?;

    Ok(From { tag: pos, ident, source })
}

fn parse_ident(state: &mut ParserState<'_>) -> eyre::Result<String> {
    let sym = state.shift_or_bail()?;

    match state.shift_or_bail()? {
        Sym::Id(id) => Ok(id),
        x => eyre::bail!("{}: expected an ident but got '{x}' instead", state.pos()),
    }
}

fn parse_source(state: &mut ParserState<'_>) -> eyre::Result<Source<Pos>> {
    let pos = state.pos();
    match state.shift_or_bail()? {
        Sym::Id(id) if id.to_lowercase() == "events" => Ok(Source {
            tag: pos,
            inner: SourceType::Events,
        }),

        Sym::Literal(Literal::String(sub)) => Ok(Source {
            tag: pos,
            inner: SourceType::Subject(sub),
        }),

        Sym::LParens => {
            state.skip_whitespace()?;
            let query = parse_query(state)?;
            state.skip_whitespace()?;
            state.expect(Sym::RParens)?;

            Ok(Source {
                tag: pos,
                inner: SourceType::Subquery(Box::new(query)),
            })
        }

        x => eyre::bail!("{}: expected a source but got {x} instead", state.pos()),
    }
}
