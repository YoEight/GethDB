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
    let pos = state.pos();
    let mut from_stmts = Vec::new();

    from_stmts.push(parse_from_statement(state)?);
    state.skip_whitespace()?;

    while let Some(Sym::Keyword(Keyword::From)) = state.look_ahead()? {
        from_stmts.push(parse_from_statement(state)?);
        state.skip_whitespace()?;
    }

    state.skip_whitespace()?;
    let predicate = parse_where_clause(state)?;
    state.skip_whitespace()?;
    let group_by = parse_group_by(state)?;
    state.skip_whitespace()?;
    let order_by = parse_order_by(state)?;
    state.skip_whitespace()?;
    let limit = parse_limit(state)?;
    state.skip_whitespace()?;

    state.expect(Sym::Keyword(Keyword::Project))?;
    state.skip_whitespace()?;
    state.expect(Sym::Keyword(Keyword::Into))?;
    state.skip_whitespace()?;

    let projection = parse_expr_single(state)?;

    check_projection(&projection)?;

    Ok(Query {
        tag: pos,
        from_stmts,
        predicate,
        group_by,
        projection,
        order_by,
        limit,
    })
}

fn check_projection(proj: &Expr<Pos>) -> eyre::Result<()> {
    match &proj.value {
        Value::Binary { .. } | Value::Unary { .. } => eyre::bail!(
            "{}: binary or unary operations are not allowed when projecting a result",
            proj.tag
        ),
        _ => Ok(()),
    }
}

fn parse_from_statement(state: &mut ParserState<'_>) -> eyre::Result<From<Pos>> {
    state.skip_whitespace()?;

    let pos = state.pos();
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

fn parse_group_by(state: &mut ParserState<'_>) -> eyre::Result<Option<Expr<Pos>>> {
    state.skip_whitespace()?;
    if let Some(sym) = state.look_ahead()?
        && sym != &Sym::Keyword(Keyword::Group)
    {
        return Ok(None);
    }

    state.shift()?;
    state.skip_whitespace()?;
    state.expect(Sym::Keyword(Keyword::By))?;
    state.skip_whitespace()?;

    Ok(Some(parse_expr_single(state)?))
}

fn parse_order_by(state: &mut ParserState<'_>) -> eyre::Result<Option<Sort<Pos>>> {
    state.skip_whitespace()?;
    if let Some(sym) = state.look_ahead()?
        && sym != &Sym::Keyword(Keyword::Order)
    {
        return Ok(None);
    }

    state.shift()?;
    state.skip_whitespace()?;
    state.expect(Sym::Keyword(Keyword::By))?;
    state.skip_whitespace()?;
    let expr = parse_expr_single(state)?;
    state.skip_whitespace()?;
    let pos = state.pos();
    let sym = state.shift_or_bail()?;
    let order = match sym {
        Sym::Keyword(Keyword::Desc) => Order::Desc,
        Sym::Keyword(Keyword::Asc) => Order::Asc,
        x => eyre::bail!("{pos}: expected either 'ASC' or 'DESC' but found '{x}' instead"),
    };

    Ok(Some(Sort { expr, order }))
}

fn parse_limit(state: &mut ParserState<'_>) -> eyre::Result<Option<Limit>> {
    state.skip_whitespace()?;

    if let Some(sym) = state.look_ahead()?
        && sym != &Sym::Keyword(Keyword::Skip)
        && sym != &Sym::Keyword(Keyword::Top)
    {
        return Ok(None);
    }

    let pos = state.pos();
    let sym = state.shift_or_bail()?;
    let kind = match sym {
        Sym::Keyword(Keyword::Skip) => LimitKind::Skip,
        Sym::Keyword(Keyword::Top) => LimitKind::Top,
        x => eyre::bail!("{pos}: expected either 'SKIP' or 'TOP' but found '{x}' instead"),
    };

    state.skip_whitespace()?;
    let pos = state.pos();
    let value = match state.shift_or_bail()? {
        Sym::Literal(Literal::Integral(n)) if n >= 0 => n as u64,
        x => eyre::bail!("{pos}: expected a greater or equal to 0 integer but found '{x}' instead"),
    };

    Ok(Some(Limit { kind, value }))
}

fn parse_ident(state: &mut ParserState<'_>) -> eyre::Result<String> {
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

fn parse_where_clause(state: &mut ParserState<'_>) -> eyre::Result<Option<Where<Pos>>> {
    state.skip_whitespace()?;

    if let Some(sym) = state.look_ahead()?
        && sym != &Sym::Keyword(Keyword::Where)
    {
        return Ok(None);
    }

    let pos = state.pos();
    state.shift()?;
    state.skip_whitespace()?;
    let expr = parse_expr(state)?;

    Ok(Some(Where { tag: pos, expr }))
}

// TODO - move the parsing from the stack to the heap so we could never have stack overflow
// errors.
fn parse_expr(state: &mut ParserState<'_>) -> eyre::Result<Expr<Pos>> {
    state.skip_whitespace()?;
    let pos = state.pos();

    let lhs = parse_expr_single(state)?;
    state.skip_whitespace()?;

    if let Some(Sym::Operation(op)) = state.look_ahead()? {
        let op = *op;
        state.shift()?;
        state.skip_whitespace()?;
        let mut rhs = parse_expr_single(state)?;
        state.skip_whitespace()?;

        while let Some(Sym::Operation(op)) = state.look_ahead()? {
            let op = *op;
            state.shift()?;
            state.skip_whitespace()?;
            let tmp_pos = rhs.tag;
            let tmp = parse_expr_single(state)?;
            state.skip_whitespace()?;

            rhs = Expr {
                tag: tmp_pos,
                value: Value::Binary {
                    lhs: Box::new(rhs),
                    op,
                    rhs: Box::new(tmp),
                },
            };
        }

        return Ok(Expr {
            tag: pos,
            value: Value::Binary {
                lhs: Box::new(lhs),
                op,
                rhs: Box::new(rhs),
            },
        });
    }

    Ok(lhs)
}

// TODO - move the parsing from the stack to the heap so we could never have stack overflow
// errors.
fn parse_expr_single(state: &mut ParserState<'_>) -> eyre::Result<Expr<Pos>> {
    // because parse_expr can be call recursively, it's possible in some situations that we could
    // have dangling whitespaces. To prevent the parser from rejecting the query in that case, it's
    // better to skip the whitespace before doing anything.
    state.skip_whitespace()?;
    let pos = state.pos();

    match state.shift_or_bail()? {
        Sym::LParens => {
            state.skip_whitespace()?;
            let mut expr = parse_expr(state)?;
            expr.tag = pos;
            state.skip_whitespace()?;
            state.expect(Sym::RParens)?;

            Ok(expr)
        }

        Sym::Literal(l) => Ok(Expr {
            tag: pos,
            value: Value::Literal(l),
        }),

        Sym::Id(id) => {
            if let Some(Sym::LParens) = state.look_ahead()? {
                state.shift()?;
                state.skip_whitespace()?;

                let mut params = Vec::new();

                if let Some(sym) = state.look_ahead()?
                    && sym != &Sym::RParens
                {
                    params.push(parse_expr_single(state)?);
                    state.skip_whitespace()?;

                    while let Some(Sym::Comma) = state.look_ahead()? {
                        state.shift()?;
                        state.skip_whitespace()?;
                        params.push(parse_expr_single(state)?);
                        state.skip_whitespace()?;
                    }
                }

                state.skip_whitespace()?;
                state.expect(Sym::RParens)?;

                return Ok(Expr {
                    tag: pos,
                    value: Value::App { fun: id, params },
                });
            }

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

        Sym::LBracket => {
            let mut values = Vec::new();
            state.skip_whitespace()?;

            if let Some(sym) = state.look_ahead()?
                && sym == &Sym::RBracket
            {
                state.shift()?;

                return Ok(Expr {
                    tag: pos,
                    value: Value::Array(values),
                });
            }

            values.push(parse_expr_single(state)?);
            state.skip_whitespace()?;

            while let Some(Sym::Comma) = state.look_ahead()? {
                state.shift()?;
                state.skip_whitespace()?;
                values.push(parse_expr_single(state)?);
                state.skip_whitespace()?;
            }

            state.expect(Sym::RBracket)?;

            Ok(Expr {
                tag: pos,
                value: Value::Array(values),
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
                fields.insert(id, parse_expr_single(state)?);
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

        Sym::Operation(op) => {
            state.skip_whitespace()?;
            let expr = parse_expr_single(state)?;

            Ok(Expr {
                tag: pos,
                value: Value::Unary {
                    op,
                    expr: Box::new(expr),
                },
            })
        }

        x => eyre::bail!(
            "{}: expected an expression but got {x} instead",
            state.pos()
        ),
    }
}
