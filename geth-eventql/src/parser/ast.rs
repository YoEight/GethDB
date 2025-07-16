use std::collections::HashMap;

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
    pub fn as_path(&self) -> Option<&Vec<String>> {
        if let Value::Path(p) = &self.value {
            return Some(p);
        }

        None
    }
}

pub enum Value<A> {
    Literal(Literal),
    Path(Vec<String>),
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
