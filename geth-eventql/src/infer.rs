use std::{collections::HashMap, fmt::Display};

use crate::{
    Expr, From, Lexical, Literal, Operation, Pos, Query, Renamed, Scopes, Sort, Value, Var, Where,
    error::InferError,
    parser::{Record, Source, SourceType},
};

pub struct InferedQuery {
    assumptions: Assumptions,
    scopes: Scopes,
    query: Query<Infer>,
}

impl InferedQuery {
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

pub struct Infer {
    pos: Pos,
    scope: u64,
}

impl Infer {
    pub fn pos(&self) -> Pos {
        self.pos
    }

    pub fn scope(&self) -> u64 {
        self.scope
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Type {
    Unspecified,
    Integer,
    Float,
    String,
    Bool,
    Array,
    Record,
    Subject,
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
            Type::Subject => write!(f, "Subject"),
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
            Literal::Subject(_) => Type::Subject,
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

pub fn infer(renamed: Renamed) -> crate::Result<InferedQuery> {
    let mut inner = HashMap::new();

    for scope in renamed.scopes.iter() {
        for (name, props) in scope.vars() {
            inner.insert(format!("{}:{name}", scope.id()), Type::Record);
            inner.insert(format!("{}:{name}:specversion", scope.id()), Type::String);
            inner.insert(format!("{}:{name}:id", scope.id()), Type::String);
            inner.insert(format!("{}:{name}:time", scope.id()), Type::String);
            inner.insert(format!("{}:{name}:source", scope.id()), Type::String);
            inner.insert(format!("{}:{name}:subject", scope.id()), Type::Subject);
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

    Ok(InferedQuery {
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

fn infer_query(type_check: &mut Typecheck, query: Query<Lexical>) -> crate::Result<Query<Infer>> {
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
        tag: Infer {
            pos: query.tag.pos,
            scope: query.tag.scope,
        },

        from_stmts,
        predicate,
        group_by,
        order_by,
        limit: query.limit,
        projection: infer_expr_simple(type_check, Type::Unspecified, query.projection)?,
    })
}

fn infer_from(type_check: &mut Typecheck, stmt: From<Lexical>) -> crate::Result<From<Infer>> {
    let inner = match stmt.source.inner {
        SourceType::Events => SourceType::Events,
        SourceType::Subject(s) => SourceType::Subject(s),
        SourceType::Subquery(query) => {
            SourceType::Subquery(Box::new(infer_query(type_check, *query)?))
        }
    };

    Ok(From {
        tag: Infer {
            pos: stmt.tag.pos,
            scope: stmt.tag.scope,
        },

        ident: stmt.ident,
        source: Source {
            tag: Infer {
                pos: stmt.source.tag.pos,
                scope: stmt.source.tag.scope,
            },
            inner,
        },
    })
}

fn infer_where(
    type_check: &mut Typecheck,
    predicate: Where<Lexical>,
) -> crate::Result<Where<Infer>> {
    Ok(Where {
        tag: Infer {
            pos: predicate.tag.pos,
            scope: predicate.tag.scope,
        },
        expr: infer_expr_simple(type_check, Type::Bool, predicate.expr)?,
    })
}

fn infer_expr_simple(
    type_check: &mut Typecheck,
    assumption: Type,
    expr: Expr<Lexical>,
) -> crate::Result<Expr<Infer>> {
    let (_, expr) = infer_expr(type_check, assumption, expr)?;
    Ok(expr)
}

fn infer_expr(
    type_check: &mut Typecheck,
    assumption: Type,
    expr: Expr<Lexical>,
) -> crate::Result<(Type, Expr<Infer>)> {
    let (typ, value) = match expr.value {
        Value::Literal(lit) => {
            let type_proj = Type::project(&lit);
            let new_lit = Value::Literal(lit);

            if assumption != Type::Unspecified && assumption != type_proj {
                bail!(
                    expr.tag.pos,
                    InferError::TypeMismatch(assumption, type_proj)
                );
            }

            (type_proj, new_lit)
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
                bail!(
                    expr.tag.pos,
                    InferError::VarTypeMismatch(var, assumption, register_assumption)
                );
            } else {
                (assumption, Value::Var(var))
            }
        }

        Value::Record(record) => {
            if assumption != Type::Unspecified && assumption != Type::Record {
                bail!(
                    expr.tag.pos,
                    InferError::TypeMismatch(assumption, Type::Record)
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
                bail!(
                    expr.tag.pos,
                    InferError::TypeMismatch(assumption, Type::Array)
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
            let scope = expr.tag.scope;
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
                    bail!(expr.tag.pos, InferError::UnsupportedBinaryOperation(op));
                }
            };

            if assumption != Type::Unspecified && assumption != result_type {
                bail!(
                    expr.tag.pos,
                    InferError::TypeMismatch(assumption, result_type)
                );
            }

            let (mut lhs_type, lhs) = infer_expr(type_check, Type::Unspecified, *lhs)?;

            let rhs_assumption = match op {
                Operation::Contains => Type::Array,
                x if operation_requires_same_type(x) => lhs_type,
                _ => Type::Unspecified,
            };

            let (mut rhs_type, rhs) = infer_expr(type_check, rhs_assumption, *rhs)?;

            if lhs_type == Type::Unspecified
                && rhs_type != Type::Unspecified
                && operation_requires_same_type(op)
            {
                lhs_type = rhs_type;

                if let Some(var) = lhs.as_var() {
                    type_check.set_type_info(scope, var, rhs_type);
                }
            } else if rhs_type == Type::Unspecified
                && lhs_type != Type::Unspecified
                && operation_requires_same_type(op)
            {
                rhs_type = lhs_type;

                if let Some(var) = rhs.as_var() {
                    type_check.set_type_info(scope, var, lhs_type);
                }
            }

            if operation_requires_same_type(op) && lhs_type != rhs_type {
                bail!(expr.tag.pos, InferError::TypeMismatch(lhs_type, rhs_type));
            }

            if op == Operation::Contains && rhs_type != Type::Array {
                bail!(
                    expr.tag.pos,
                    InferError::TypeMismatch(rhs_type, Type::Array)
                );
            }

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
                bail!(
                    expr.tag.pos,
                    InferError::TypeMismatch(assumption, Type::Bool)
                );
            };

            if assumption != Type::Unspecified && assumption != result_type {
                bail!(
                    expr.tag.pos,
                    InferError::TypeMismatch(assumption, result_type)
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
            tag: Infer {
                pos: expr.tag.pos,
                scope: expr.tag.scope,
            },
            value,
        },
    ))
}

fn operation_requires_same_type(op: Operation) -> bool {
    !matches!(op, Operation::Contains)
}
