use std::{collections::HashMap, fmt::Display};

use crate::{
    Expr, From, Lexical, Literal, Operation, Query, Renamed, Scopes, Sort, Value, Var, Where,
    parser::{Record, Source, SourceType},
};

pub struct Infered {
    assumptions: Assumptions,
    scopes: Scopes,
    query: Query<Infer>,
}

impl Infered {
    pub fn assumptions(&self) -> &Assumptions {
        &self.assumptions
    }

    pub fn scopes(&self) -> &Scopes {
        &self.scopes
    }

    pub fn query(&self) -> &Query<Infer> {
        &self.query
    }
}

pub struct Infer {}

#[derive(Copy, Clone, PartialEq, Eq)]
pub enum Type {
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
    inner: HashMap<String, Type>,
}

impl Assumptions {
    pub fn lookup_type_info(&self, scope: u64, var: &Var) -> Type {
        let key = urn(scope, &var.name, &var.path);
        if let Some(tpe) = self.inner.get(&key) {
            return *tpe;
        }

        Type::Unspecified
    }
}

pub fn infer(renamed: Renamed) -> eyre::Result<Infered> {
    let mut inner = HashMap::new();

    for scope in renamed.scopes.iter() {
        for (name, props) in scope.vars() {
            inner.insert(name.clone(), Type::Record);
            inner.insert(format!("{}:{name}:specversion", scope.id()), Type::String);
            inner.insert(format!("{}:{name}:id", scope.id()), Type::String);
            inner.insert(format!("{}:{name}:time", scope.id()), Type::String);
            inner.insert(format!("{}:{name}:source", scope.id()), Type::String);
            inner.insert(format!("{}:{name}:subject", scope.id()), Type::String);
            inner.insert(format!("{}:{name}:type", scope.id()), Type::String);
            inner.insert(
                format!("{}:{name}:datacontenttype", scope.id()),
                Type::String,
            );
            inner.insert(format!("{}:{name}:data", scope.id()), Type::Record);
            inner.insert(
                format!("{}:{name}:predecessorhash", scope.id()),
                Type::Integer,
            );
            inner.insert(format!("{}:{name}:hash", scope.id()), Type::Integer);

            for prop in props.iter() {
                inner.insert(
                    format!("{}:{name}:data:{prop}", scope.id()),
                    Type::Unspecified,
                );
            }
        }
    }

    let mut type_check = Typecheck {
        assumptions: inner,
        scopes: renamed.scopes,
    };

    let query = infer_query(&mut type_check, renamed.query)?;

    Ok(Infered {
        assumptions: Assumptions {
            inner: type_check.assumptions,
        },
        scopes: type_check.scopes,
        query,
    })
}

struct Typecheck {
    assumptions: HashMap<String, Type>,
    scopes: Scopes,
}

fn urn(scope: u64, name: &String, path: &Vec<String>) -> String {
    let mut agg = format!("{scope}:{name}");

    for prop in path {
        agg.push(':');
        agg.push_str(prop);
    }

    agg
}

impl Typecheck {
    fn lookup_type_info(&self, scope: u64, var: &Var) -> Type {
        let key = urn(scope, &var.name, &var.path);
        if let Some(tpe) = self.assumptions.get(&key) {
            return *tpe;
        }

        Type::Unspecified
    }

    fn set_type_info(&mut self, scope: u64, var: &Var, assumption: Type) {
        let key = urn(scope, &var.name, &var.path);
        self.assumptions.insert(key, assumption);
    }
}

fn infer_query(type_check: &mut Typecheck, query: Query<Lexical>) -> eyre::Result<Query<Infer>> {
    let mut from_stmts = Vec::new();

    for stmt in query.from_stmts {
        from_stmts.push(infer_from(type_check, stmt)?);
    }

    let predicate = if let Some(predicate) = query.predicate {
        Some(infer_where(type_check, predicate)?)
    } else {
        None
    };

    let group_by = if let Some(expr) = query.group_by {
        Some(infer_expr_simple(type_check, Type::Unspecified, expr)?)
    } else {
        None
    };

    let order_by = if let Some(sort) = query.order_by {
        Some(Sort {
            expr: infer_expr_simple(type_check, Type::Unspecified, sort.expr)?,
            order: sort.order,
        })
    } else {
        None
    };

    Ok(Query {
        tag: Infer {},
        from_stmts,
        predicate,
        group_by,
        order_by,
        limit: query.limit,
        projection: infer_expr_simple(type_check, Type::Unspecified, query.projection)?,
    })
}

fn infer_from(type_check: &mut Typecheck, stmt: From<Lexical>) -> eyre::Result<From<Infer>> {
    let inner = match stmt.source.inner {
        SourceType::Events => SourceType::Events,
        SourceType::Subject(s) => SourceType::Subject(s),
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

fn infer_where(
    type_check: &mut Typecheck,
    predicate: Where<Lexical>,
) -> eyre::Result<Where<Infer>> {
    Ok(Where {
        tag: Infer {},
        expr: infer_expr_simple(type_check, Type::Bool, predicate.expr)?,
    })
}

fn infer_expr_simple(
    type_check: &mut Typecheck,
    assumption: Type,
    expr: Expr<Lexical>,
) -> eyre::Result<Expr<Infer>> {
    let (_, expr) = infer_expr(type_check, assumption, expr)?;
    Ok(expr)
}

fn infer_expr(
    type_check: &mut Typecheck,
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

        Value::Var(var) => {
            let register_assumption = type_check.lookup_type_info(expr.tag.scope, &var);

            if assumption == Type::Unspecified && register_assumption == assumption {
                (Type::Unspecified, Value::Var(var))
            } else if register_assumption == Type::Unspecified {
                type_check.set_type_info(expr.tag.scope, &var, assumption);
                (assumption, Value::Var(var))
            } else if assumption == Type::Unspecified {
                (register_assumption, Value::Var(var))
            } else if assumption != register_assumption {
                eyre::bail!(
                    "{}: expected {assumption} but got {register_assumption} instead",
                    expr.tag.pos,
                );
            } else {
                (assumption, Value::Var(var))
            }
        }

        Value::Record(record) => {
            if assumption != Type::Unspecified && assumption != Type::Record {
                eyre::bail!(
                    "{}: expected a '{assumption}' but got '{}' instead",
                    expr.tag.pos,
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

            let new_assumption = if op == Operation::Not {
                Type::Bool
            } else {
                assumption
            };

            let expr = infer_expr_simple(type_check, new_assumption, *expr)?;

            (
                result_type,
                Value::Unary {
                    op,
                    expr: Box::new(expr),
                },
            )
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
