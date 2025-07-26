use std::{collections::HashMap, fmt::Display};

use crate::sym::{Literal, Operation};

pub struct Query<A> {
    pub tag: A,
    pub from_stmts: Vec<From<A>>,
    pub predicate: Option<Where<A>>,
    pub group_by: Option<Expr<A>>,
    pub order_by: Option<Sort<A>>,
    pub limit: Option<Limit>,
    pub projection: Expr<A>,
}

pub enum SourceType<A> {
    Events,
    Subject(String),
    Subquery(Box<Query<A>>),
}

impl<A> SourceType<A> {
    pub fn as_subject(&self) -> Option<&String> {
        if let Self::Subject(sub) = self {
            return Some(sub);
        }

        None
    }

    pub fn targets_events(&self) -> bool {
        if let Self::Events = self {
            return true;
        }

        false
    }
}

pub struct Source<A> {
    pub tag: A,
    pub inner: SourceType<A>,
}

impl<A> Source<A> {
    pub fn as_subquery(&self) -> Option<&Query<A>> {
        if let SourceType::Subquery(q) = &self.inner {
            return Some(q);
        }

        None
    }
}

pub struct From<A> {
    pub tag: A,
    pub ident: String,
    pub source: Source<A>,
}

pub struct Where<A> {
    pub tag: A,
    pub expr: Expr<A>,
}

pub struct Expr<A> {
    pub tag: A,
    pub value: Value<A>,
}

impl<A> Expr<A> {
    pub fn as_var(&self) -> Option<&Var> {
        if let Value::Var(p) = &self.value {
            return Some(p);
        }

        None
    }

    pub fn as_binary_op(&self) -> Option<BinaryOp<'_, A>> {
        if let Value::Binary { lhs, op, rhs } = &self.value {
            return Some(BinaryOp {
                lhs: lhs.as_ref(),
                op: *op,
                rhs: rhs.as_ref(),
            });
        }

        None
    }

    pub fn as_unary_op(&self) -> Option<UnaryOp<'_, A>> {
        if let Value::Unary { op, expr } = &self.value {
            return Some(UnaryOp { op: *op, expr });
        }

        None
    }

    pub fn as_string_literal(&self) -> Option<&String> {
        if let Value::Literal(Literal::String(s)) = &self.value {
            return Some(s);
        }

        None
    }

    pub fn as_i64_literal(&self) -> Option<i64> {
        if let Value::Literal(Literal::Integral(i)) = &self.value {
            return Some(*i);
        }

        None
    }

    pub fn as_record(&self) -> Option<&Record<A>> {
        if let Value::Record(r) = &self.value {
            return Some(r);
        }

        None
    }

    pub fn as_apply_fun(&self) -> Option<ApplyFun<'_, A>> {
        if let Value::App { fun, params } = &self.value {
            return Some(ApplyFun { name: fun, params });
        }

        None
    }
}

pub struct BinaryOp<'a, A> {
    pub lhs: &'a Expr<A>,
    pub op: Operation,
    pub rhs: &'a Expr<A>,
}

pub struct UnaryOp<'a, A> {
    pub op: Operation,
    pub expr: &'a Expr<A>,
}

pub struct ApplyFun<'a, A> {
    pub name: &'a String,
    pub params: &'a Vec<Expr<A>>,
}

#[derive(Debug, Clone)]
pub struct Var {
    pub name: String,
    pub path: Vec<String>,
}

impl Display for Var {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)?;

        for p in &self.path {
            write!(f, ".{p}")?;
        }

        Ok(())
    }
}

pub enum Value<A> {
    Literal(Literal),
    Var(Var),
    Record(Record<A>),
    Array(Vec<Expr<A>>),
    App {
        fun: String,
        params: Vec<Expr<A>>,
    },
    Binary {
        lhs: Box<Expr<A>>,
        op: Operation,
        rhs: Box<Expr<A>>,
    },
    Unary {
        op: Operation,
        expr: Box<Expr<A>>,
    },
}

pub struct Record<A> {
    pub fields: HashMap<String, Expr<A>>,
}

impl<A> Record<A> {
    pub fn get(&self, id: &str) -> Option<&Expr<A>> {
        self.fields.get(id)
    }
}

pub struct Sort<A> {
    pub expr: Expr<A>,
    pub order: Order,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitKind {
    Skip,
    Top,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limit {
    pub kind: LimitKind,
    pub value: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    Asc,
    Desc,
}
