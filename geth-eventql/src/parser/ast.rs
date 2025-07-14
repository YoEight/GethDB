use std::collections::HashMap;

use crate::sym::{Literal, Operation};

pub struct Query<A> {
    pub tag: A,
    pub sources: Vec<From<A>>,
    pub predicate: Option<Where<A>>,
    pub limit: Option<u64>,
    pub projection: Expr<A>,
}

pub enum SourceType<A> {
    Events,
    Subject(String),
    Subquery(Box<Query<A>>),
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

pub enum Value<A> {
    Literal(Literal),
    Path(Vec<String>),
    Record(Record<A>),
    App {
        fun: String,
        params: Vec<Expr<A>>,
    },
    BinaryOp {
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
