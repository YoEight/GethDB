use std::{collections::HashMap, fmt::Display};

use crate::{
    Expr, FromSource, Literal, Operation, Pos, Query, Scopes, Value, Var, Where, error::InferError,
    parser::SourceType,
};

pub struct InferedQuery {
    assumptions: Assumptions,
    scopes: Scopes,
    query: Query,
}

impl InferedQuery {
    pub fn assumptions(&self) -> &Assumptions {
        &self.assumptions
    }

    pub fn scopes(&self) -> &Scopes {
        &self.scopes
    }

    pub fn query(&self) -> &Query {
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

pub fn infer(scopes: Scopes, mut query: Query) -> crate::Result<InferedQuery> {
    let mut inner = HashMap::new();

    for scope in scopes.iter() {
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
        scopes,
    };

    infer_query(&mut type_check, &mut query)?;

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

fn infer_query(type_check: &mut Typecheck, query: &mut Query) -> crate::Result<()> {
    for stmt in query.from_stmts.iter_mut() {
        infer_from(type_check, stmt)?;
    }

    if let Some(predicate) = query.predicate.as_mut() {
        Some(infer_where(type_check, predicate)?);
    }

    if let Some(expr) = query.group_by.as_mut() {
        infer_expr_simple(type_check, Type::Unspecified, expr)?;
    }

    if let Some(sort) = query.order_by.as_mut() {
        infer_expr_simple(type_check, Type::Unspecified, &mut sort.expr)?;
    }

    infer_expr_simple(type_check, Type::Unspecified, &mut query.projection)?;

    Ok(())
}

fn infer_from(type_check: &mut Typecheck, stmt: &mut FromSource) -> crate::Result<()> {
    if let SourceType::Subquery(query) = &mut stmt.source.inner {
        infer_query(type_check, query.as_mut())?;
    }

    Ok(())
}

fn infer_where(type_check: &mut Typecheck, predicate: &mut Where) -> crate::Result<()> {
    infer_expr_simple(type_check, Type::Bool, &mut predicate.expr)?;
    Ok(())
}

fn infer_expr_simple(
    type_check: &mut Typecheck,
    assumption: Type,
    expr: &mut Expr,
) -> crate::Result<()> {
    infer_expr(type_check, assumption, expr)
}

fn infer_expr(type_check: &mut Typecheck, assumption: Type, expr: &mut Expr) -> crate::Result<()> {
    match &mut expr.value {
        Value::Literal(lit) => {
            let type_proj = Type::project(&lit);

            if assumption != Type::Unspecified && assumption != type_proj {
                bail!(
                    expr.attrs.pos,
                    InferError::TypeMismatch(assumption, type_proj)
                );
            }

            expr.attrs.tpe = type_proj;
        }

        Value::Var(var) => {
            let register_assumption = type_check.lookup_type_info(expr.attrs.scope, &var);

            if assumption == Type::Unspecified && register_assumption == assumption {
                expr.attrs.tpe = Type::Unspecified;
            } else if register_assumption == Type::Unspecified {
                type_check.set_type_info(expr.attrs.scope, &var, assumption);
                expr.attrs.tpe = assumption;
            } else if assumption == Type::Unspecified {
                expr.attrs.tpe = register_assumption;
            } else if assumption != register_assumption {
                bail!(
                    expr.attrs.pos,
                    InferError::VarTypeMismatch(var.clone(), assumption, register_assumption)
                );
            } else {
                expr.attrs.tpe = assumption;
            }
        }

        Value::Record(record) => {
            if assumption != Type::Unspecified && assumption != Type::Record {
                bail!(
                    expr.attrs.pos,
                    InferError::TypeMismatch(assumption, Type::Record)
                );
            }

            for value in record.fields.values_mut() {
                infer_expr_simple(type_check, Type::Unspecified, value)?;
            }

            expr.attrs.tpe = Type::Record;
        }

        Value::Array(exprs) => {
            if assumption != Type::Unspecified && assumption != Type::Array {
                bail!(
                    expr.attrs.pos,
                    InferError::TypeMismatch(assumption, Type::Array)
                );
            }

            for value in exprs.iter_mut() {
                infer_expr_simple(type_check, Type::Unspecified, value)?;
            }

            expr.attrs.tpe = Type::Array;
        }

        Value::App { params, .. } => {
            // TODO - we can make a lot of assumption when it comes to the return type of the
            // function call. Right now we are just going to ignore and forward the assumption
            // back.

            // TODO - based on the function we cold also make assumption about the type of its
            // parameters. Right now we are just going to ignore it.
            for value in params.iter_mut() {
                infer_expr_simple(type_check, Type::Unspecified, value)?;
            }

            expr.attrs.tpe = assumption;
        }

        Value::Binary { lhs, op, rhs } => {
            let scope = expr.attrs.scope;
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
                    bail!(expr.attrs.pos, InferError::UnsupportedBinaryOperation(*op));
                }
            };

            if assumption != Type::Unspecified && assumption != result_type {
                bail!(
                    expr.attrs.pos,
                    InferError::TypeMismatch(assumption, result_type)
                );
            }

            infer_expr(type_check, Type::Unspecified, lhs.as_mut())?;

            let rhs_assumption = match &op {
                Operation::Contains => Type::Array,
                x if operation_requires_same_type(x) => lhs.attrs.tpe,
                _ => Type::Unspecified,
            };

            infer_expr(type_check, rhs_assumption, rhs.as_mut())?;

            if lhs.attrs.tpe == Type::Unspecified
                && rhs.attrs.tpe != Type::Unspecified
                && operation_requires_same_type(op)
            {
                lhs.attrs.tpe = rhs.attrs.tpe;

                if let Some(var) = lhs.as_var() {
                    type_check.set_type_info(scope, var, rhs.attrs.tpe);
                }
            } else if rhs.attrs.tpe == Type::Unspecified
                && lhs.attrs.tpe != Type::Unspecified
                && operation_requires_same_type(op)
            {
                rhs.attrs.tpe = lhs.attrs.tpe;

                if let Some(var) = rhs.as_var() {
                    type_check.set_type_info(scope, var, lhs.attrs.tpe);
                }
            }

            if operation_requires_same_type(op) && lhs.attrs.tpe != rhs.attrs.tpe {
                bail!(
                    expr.attrs.pos,
                    InferError::TypeMismatch(lhs.attrs.tpe, rhs.attrs.tpe)
                );
            }

            if op == &Operation::Contains && rhs.attrs.tpe != Type::Array {
                bail!(
                    expr.attrs.pos,
                    InferError::TypeMismatch(rhs.attrs.tpe, Type::Array)
                );
            }
        }

        Value::Unary { op, expr } => {
            let result_type = if op == &Operation::Not {
                Type::Bool
            } else {
                bail!(
                    expr.attrs.pos,
                    InferError::TypeMismatch(assumption, Type::Bool)
                );
            };

            if assumption != Type::Unspecified && assumption != result_type {
                bail!(
                    expr.attrs.pos,
                    InferError::TypeMismatch(assumption, result_type)
                );
            }

            let new_assumption = if op == &Operation::Not {
                Type::Bool
            } else {
                assumption
            };

            infer_expr_simple(type_check, new_assumption, expr.as_mut())?;
        }
    }

    Ok(())
}

fn operation_requires_same_type(op: &Operation) -> bool {
    !matches!(op, Operation::Contains)
}
