use std::{collections::HashMap, fmt::Display};

use crate::{
    Expr, From, Lexical, Literal, Operation, Query, Renamed, Scopes, Value, Where,
    parser::{Record, Source, SourceType},
};

pub struct Infered {}

pub struct Infer {}

#[derive(Copy, Clone, PartialEq, Eq)]
enum Type {
    Unspecified,
    Integer,
    Float,
    String,
    Bool,
    Array,
    Record,
}

impl Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Type::Unspecified => write!(f, "<unspecified>"),
            Type::Integer => write!(f, "Integer"),
            Type::Float => write!(f, "Float"),
            Type::String => write!(f, "String"),
            Type::Bool => write!(f, "Bool"),
            Type::Array => write!(f, "Array"),
            Type::Record => write!(f, "Record"),
        }
    }
}

impl Type {
    fn matches(&self, lit: &Literal) -> bool {
        matches!(
            (self, lit),
            (Type::Unspecified, _)
                | (Type::Integer, Literal::Integral(_))
                | (Type::Float, Literal::Float(_))
                | (Type::String, Literal::String(_))
                | (Type::Bool, Literal::Bool(_))
        )
    }

    fn project(lit: &Literal) -> Self {
        match lit {
            Literal::String(_) => Type::String,
            Literal::Integral(_) => Type::Integer,
            Literal::Float(_) => Type::Float,
            Literal::Bool(_) => Type::Bool,
        }
    }
}

#[derive(Default)]
pub struct Assumptions {
    type_id_gen: u64,
    inner: HashMap<u64, Type>,
}

pub fn infer(renamed: Renamed) -> eyre::Result<Infered> {
    todo!()
}

#[derive(Clone, Copy)]
struct Typecheck<'a> {
    assumptions: &'a Assumptions,
    scopes: &'a Scopes,
}

fn infer_query(type_check: Typecheck<'_>, query: Query<Lexical>) -> eyre::Result<Query<Infer>> {
    let mut from_stmts = Vec::new();

    for stmt in query.from_stmts {
        from_stmts.push(infer_from(type_check, stmt)?);
    }

    let predicate = if let Some(predicate) = query.predicate {
        Some(infer_where(type_check, predicate)?)
    } else {
        None
    };

    Ok(Query {
        tag: Infer {},
        from_stmts,
        predicate,
        group_by: todo!(),
        order_by: todo!(),
        limit: todo!(),
        projection: todo!(),
    })
}

fn infer_from(type_check: Typecheck<'_>, stmt: From<Lexical>) -> eyre::Result<From<Infer>> {
    let inner = match stmt.source.inner {
        SourceType::Events => SourceType::Events,
        SourceType::Subject(_) => todo!(),
        SourceType::Subquery(query) => {
            SourceType::Subquery(Box::new(infer_query(type_check, *query)?))
        }
    };

    Ok(From {
        tag: Infer {},
        ident: stmt.ident,
        source: Source {
            tag: Infer {},
            inner,
        },
    })
}

fn infer_where(type_check: Typecheck<'_>, predicate: Where<Lexical>) -> eyre::Result<Where<Infer>> {
    Ok(Where {
        tag: Infer {},
        expr: infer_expr_simple(type_check, Type::Bool, predicate.expr)?,
    })
}

fn infer_expr_simple(
    type_check: Typecheck<'_>,
    assumption: Type,
    expr: Expr<Lexical>,
) -> eyre::Result<Expr<Infer>> {
    let (_, expr) = infer_expr(type_check, assumption, expr)?;
    Ok(expr)
}

fn infer_expr(
    type_check: Typecheck<'_>,
    assumption: Type,
    expr: Expr<Lexical>,
) -> eyre::Result<(Type, Expr<Infer>)> {
    let (typ, value) = match expr.value {
        Value::Literal(lit) => {
            let type_proj = Type::project(&lit);
            let new_lit = Value::Literal(lit);

            if assumption == Type::Unspecified {
                (assumption, new_lit)
            } else {
                if assumption != type_proj {
                    eyre::bail!(
                        "{}: expected type '{}' but got '{}'",
                        expr.tag.pos,
                        assumption,
                        type_proj,
                    );
                }

                (type_proj, new_lit)
            }
        }

        Value::Path(items) => todo!(),

        Value::Record(record) => {
            if assumption != Type::Unspecified && assumption != Type::Record {
                eyre::bail!(
                    "{}: expected a '{}' but got '{}' instead",
                    expr.tag.pos,
                    assumption,
                    Type::Record,
                );
            }

            let mut fields = HashMap::new();

            for (key, value) in record.fields {
                fields.insert(
                    key,
                    infer_expr_simple(type_check, Type::Unspecified, value)?,
                );
            }

            (Type::Record, Value::Record(Record { fields }))
        }

        Value::Array(exprs) => {
            if assumption != Type::Unspecified && assumption != Type::Array {
                eyre::bail!(
                    "{}: expected {} but got an array instead",
                    expr.tag.pos,
                    assumption
                );
            }

            let mut new_exprs = Vec::new();

            for value in exprs {
                new_exprs.push(infer_expr_simple(type_check, Type::Unspecified, value)?);
            }

            (Type::Array, Value::Array(new_exprs))
        }

        Value::App { fun, params } => {
            // TODO - we can make a lot of assumption when it comes to the return type of the
            // function call. Right now we are just going to ignore and forward the assumption
            // back.

            let mut new_params = Vec::new();

            // TODO - based on the function we cold also make assumption about the type of its
            // parameters. Right now we are just going to ignore it.
            for value in params {
                new_params.push(infer_expr_simple(type_check, Type::Unspecified, value)?);
            }

            (
                assumption,
                Value::App {
                    fun,
                    params: new_params,
                },
            )
        }

        Value::Binary { lhs, op, rhs } => {
            let result_type = match op {
                Operation::And
                | Operation::Or
                | Operation::Xor
                | Operation::Contains
                | Operation::Equal
                | Operation::NotEqual
                | Operation::LessThan
                | Operation::GreaterThan
                | Operation::LessThanOrEqual
                | Operation::GreaterThanOrEqual => Type::Bool,

                Operation::Not => {
                    eyre::bail!(
                        "{}: 'NOT' is not supported for binary operations",
                        expr.tag.pos
                    );
                }
            };

            if assumption != Type::Unspecified && assumption != result_type {
                eyre::bail!(
                    "{}: expected '{assumption}' but got '{result_type}'",
                    expr.tag.pos
                );
            }

            let lhs_assumption = match op {
                Operation::And | Operation::Or | Operation::Xor => Type::Bool,
                _ => Type::Unspecified,
            };

            let (lhs_type, lhs) = infer_expr(type_check, lhs_assumption, *lhs)?;

            let rhs_assumption = match op {
                Operation::Contains => Type::Array,
                _ => lhs_type,
            };

            let rhs = infer_expr_simple(type_check, rhs_assumption, *rhs)?;

            (
                result_type,
                Value::Binary {
                    lhs: Box::new(lhs),
                    op,
                    rhs: Box::new(rhs),
                },
            )
        }

        Value::Unary { op, expr } => {
            let result_type = if op == Operation::Not {
                Type::Bool
            } else {
                eyre::bail!(
                    "{}: expected '{assumption}' but got '{}' instead",
                    expr.tag.pos,
                    Type::Bool
                );
            };

            if assumption != Type::Unspecified && assumption != result_type {
                eyre::bail!(
                    "{}: expected '{assumption}' but got '{result_type}' instead",
                    expr.tag.pos
                );
            }

            todo!()
        }
    };

    Ok((
        typ,
        Expr {
            tag: Infer {},
            value,
        },
    ))
}
