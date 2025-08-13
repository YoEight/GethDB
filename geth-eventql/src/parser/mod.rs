use crate::{
    error::ParserError,
    sym::{Keyword, Literal, Sym},
    tokenizer::{Lexer, Pos},
};
use std::collections::BTreeMap;

mod ast;
mod state;

pub use ast::*;
use state::ParserState;

pub fn parse(lexer: Lexer<'_>) -> crate::Result<Query> {
    let mut state = ParserState::new(lexer);

    parse_query(&mut state)
}

fn parse_query(state: &mut ParserState<'_>) -> crate::Result<Query> {
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
        attrs: NodeAttributes::new(pos),
        from_stmts,
        predicate,
        group_by,
        projection,
        order_by,
        limit,
    })
}

fn check_projection(proj: &Expr) -> crate::Result<()> {
    match &proj.value {
        Value::Binary { .. } | Value::Unary { .. } => bail!(
            proj.attrs.pos,
            ParserError::BinaryUnaryOperationUnallowedInProjection
        ),
        _ => Ok(()),
    }
}

fn parse_from_statement(state: &mut ParserState<'_>) -> crate::Result<FromSource> {
    state.skip_whitespace()?;

    let pos = state.pos();
    state.expect(Sym::Keyword(Keyword::From))?;
    state.skip_whitespace()?;
    let ident = parse_ident(state)?;
    state.skip_whitespace()?;
    state.expect(Sym::Keyword(Keyword::In))?;
    state.skip_whitespace()?;

    let source = parse_source(state)?;

    Ok(FromSource {
        attrs: NodeAttributes::new(pos),
        ident,
        source,
    })
}

fn parse_group_by(state: &mut ParserState<'_>) -> crate::Result<Option<Expr>> {
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

fn parse_order_by(state: &mut ParserState<'_>) -> crate::Result<Option<Sort>> {
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
        x => bail!(
            pos,
            ParserError::UnexpectedSymbolWithAlternatives(
                x,
                &[Sym::Keyword(Keyword::Asc), Sym::Keyword(Keyword::Desc)]
            )
        ),
    };

    Ok(Some(Sort { expr, order }))
}

fn parse_limit(state: &mut ParserState<'_>) -> crate::Result<Option<Limit>> {
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
        x => bail!(
            pos,
            ParserError::UnexpectedSymbolWithAlternatives(
                x,
                &[Sym::Keyword(Keyword::Skip), Sym::Keyword(Keyword::Top)]
            )
        ),
    };

    state.skip_whitespace()?;
    let pos = state.pos();
    let value = match state.shift_or_bail()? {
        Sym::Literal(Literal::Integral(n)) if n >= 0 => n as u64,
        x => bail!(pos, ParserError::ExpectedGreaterOrEqualToZero(x)),
    };

    Ok(Some(Limit { kind, value }))
}

fn parse_ident(state: &mut ParserState<'_>) -> crate::Result<String> {
    match state.shift_or_bail()? {
        Sym::Id(id) => Ok(id),
        x => bail!(state.pos(), ParserError::ExpectedIdent(x)),
    }
}

fn parse_source(state: &mut ParserState<'_>) -> crate::Result<Source> {
    let pos = state.pos();
    match state.shift_or_bail()? {
        Sym::Id(id) if id.to_lowercase() == "events" => Ok(Source {
            attrs: NodeAttributes::new(pos),
            inner: SourceType::Events,
        }),

        Sym::Literal(Literal::String(sub)) => Ok(Source {
            attrs: NodeAttributes::new(pos),
            inner: SourceType::Subject(parse_subject(pos, &sub)?),
        }),

        Sym::LParens => {
            state.skip_whitespace()?;
            let query = parse_query(state)?;
            state.skip_whitespace()?;
            state.expect(Sym::RParens)?;

            Ok(Source {
                attrs: NodeAttributes::new(pos),
                inner: SourceType::Subquery(Box::new(query)),
            })
        }

        x => bail!(state.pos(), ParserError::ExpectedSource(x)),
    }
}

pub(crate) fn parse_subject(pos: Pos, subject: &str) -> crate::Result<Subject> {
    if !subject.starts_with('/') {
        bail!(pos, ParserError::SubjectDoesNotStartWithSlash);
    }

    let mut inner = Vec::new();
    for segment in subject.split_terminator('/').skip(1) {
        if segment.trim_ascii().is_empty() {
            bail!(pos, ParserError::SubjectInvalidFormat);
        }

        inner.push(segment.to_string());
    }

    Ok(Subject { inner })
}

fn parse_where_clause(state: &mut ParserState<'_>) -> crate::Result<Option<Where>> {
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

    Ok(Some(Where {
        attrs: NodeAttributes::new(pos),
        expr,
    }))
}

// TODO - move the parsing from the stack to the heap so we could never have stack overflow
// errors.
fn parse_expr(state: &mut ParserState<'_>) -> crate::Result<Expr> {
    state.skip_whitespace()?;

    let expr = parse_expr_single(state)?;
    state.skip_whitespace()?;

    // binary operation
    if let Some(Sym::Operation(op)) = state.look_ahead()? {
        let op = *op;
        let mut expr_stack = Vec::new();
        let mut op_stack = Vec::new();

        state.shift()?;
        state.skip_whitespace()?;

        expr_stack.push(Expr {
            attrs: expr.attrs,
            value: Value::Binary {
                lhs: Box::new(expr),
                op,
                rhs: Box::new(parse_expr_single(state)?),
            },
        });

        state.skip_whitespace()?;

        while let Some(Sym::Operation(op)) = state.look_ahead()? {
            op_stack.push(*op);
            state.shift()?;
            state.skip_whitespace()?;
            expr_stack.push(parse_expr_single(state)?);
            state.skip_whitespace()?;
        }

        while let Some(op) = op_stack.pop() {
            let rhs = expr_stack.pop().expect("to be always defined");
            let lhs = expr_stack.pop().expect("to be always defined");

            expr_stack.push(Expr {
                attrs: lhs.attrs,
                value: Value::Binary {
                    lhs: Box::new(lhs),
                    op,
                    rhs: Box::new(rhs),
                },
            });
        }

        return Ok(expr_stack.pop().expect("to be always defined"));
    }

    Ok(expr)
}

// TODO - move the parsing from the stack to the heap so we could never have stack overflow
// errors.
fn parse_expr_single(state: &mut ParserState<'_>) -> crate::Result<Expr> {
    // because parse_expr can be call recursively, it's possible in some situations that we could
    // have dangling whitespaces. To prevent the parser from rejecting the query in that case, it's
    // better to skip the whitespace before doing anything.
    state.skip_whitespace()?;
    let pos = state.pos();

    match state.shift_or_bail()? {
        Sym::LParens => {
            state.skip_whitespace()?;
            let mut expr = parse_expr(state)?;
            expr.attrs = NodeAttributes::new(pos);
            state.skip_whitespace()?;
            state.expect(Sym::RParens)?;

            Ok(expr)
        }

        Sym::Literal(l) => Ok(Expr {
            attrs: NodeAttributes::new(pos),
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
                    attrs: NodeAttributes::new(pos),
                    value: Value::App { fun: id, params },
                });
            }

            let mut var = Var {
                name: id,
                path: vec![],
            };

            while let Some(Sym::Dot) = state.look_ahead()? {
                state.shift()?;
                var.path.push(parse_ident(state)?);
            }

            Ok(Expr {
                attrs: NodeAttributes::new(pos),
                value: Value::Var(var),
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
                    attrs: NodeAttributes::new(pos),
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
                attrs: NodeAttributes::new(pos),
                value: Value::Array(values),
            })
        }

        Sym::LBrace => {
            state.skip_whitespace()?;

            let mut fields = BTreeMap::new();

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
                attrs: NodeAttributes::new(pos),
                value: Value::Record(Record { fields }),
            })
        }

        Sym::Operation(op) => {
            state.skip_whitespace()?;
            let expr = parse_expr_single(state)?;

            Ok(Expr {
                attrs: NodeAttributes::new(pos),
                value: Value::Unary {
                    op,
                    expr: Box::new(expr),
                },
            })
        }

        x => bail!(state.pos(), ParserError::ExpectedExpr(x)),
    }
}
