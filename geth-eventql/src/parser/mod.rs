use std::collections::HashMap;

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
    let mut from_stmts = Vec::new();

    from_stmts.push(parse_from_statement(state)?);
    state.skip_whitespace()?;

    while let Some(Sym::Keyword(Keyword::From)) = state.look_ahead()? {
        from_stmts.push(parse_from_statement(state)?);
        state.skip_whitespace()?;
    }

    let predicate = parse_where_clause(state)?;

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

    Ok(From {
        tag: pos,
        ident,
        source,
    })
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

fn parse_where_clause(state: &mut ParserState<'_>) -> eyre::Result<Where<Pos>> {
    state.skip_whitespace()?;
    let pos = state.pos();
    state.expect(Sym::Keyword(Keyword::Where))?;
    state.skip_whitespace()?;
    let expr = parse_expr(state)?;

    Ok(Where { tag: pos, expr })
}

// TODO - move the parsing to from the stack to the heap so we could never have stack overflow
// errors.
fn parse_expr(state: &mut ParserState<'_>) -> eyre::Result<Expr<Pos>> {
    let pos = state.pos();

    match state.shift_or_bail()? {
        Sym::Literal(l) => Ok(Expr {
            tag: pos,
            value: Value::Literal(l),
        }),

        Sym::Id(id) => {
            let mut path = vec![id];

            while let Some(Sym::Dot) = state.look_ahead()? {
                state.shift()?;
                path.push(parse_ident(state)?);
            }

            Ok(Expr {
                tag: pos,
                value: Value::Path(path),
            })
        }

        Sym::LBrace => {
            state.skip_whitespace()?;

            let mut fields = HashMap::new();

            while let Some(Sym::Id(id)) = state.look_ahead()? {
                let id = id.clone();
                state.shift()?;
                state.skip_whitespace()?;
                state.expect(Sym::Colon)?;
                state.skip_whitespace()?;
                fields.insert(id, parse_expr(state)?);
                state.skip_whitespace()?;

                if let Some(Sym::Comma) = state.look_ahead()? {
                    state.shift()?;
                    state.skip_whitespace()?;
                } else {
                    break;
                }
            }

            state.skip_whitespace()?;
            state.expect(Sym::RBrace)?;

            Ok(Expr {
                tag: pos,
                value: Value::Record(Record { fields }),
            })
        }

        x => eyre::bail!(
            "{}: expected an expression but got {x} instead",
            state.pos()
        ),
    }
}

fn parse_record(state: &mut ParserState<'_>) -> eyre::Result<Record<Pos>> {
    state.skip_whitespace()?;
    todo!()
}
