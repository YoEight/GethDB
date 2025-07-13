use std::collections::HashMap;

use crate::sym::{Literal, Operation};

pub struct Ast<A> {
    tag: A,
    from: Vec<From<A>>,
    predicate: Option<Where<A>>,
    limit: Option<u64>,
    projection: Expr<A>,
}

pub enum SourceType<A> {
    Events,
    Subject(String),
    Subquery(Box<Ast<A>>),
}

pub struct Source<A> {
    tag: A,
    inner: SourceType<A>,
}

pub struct From<A> {
    tag: A,
    ident: String,
    source: Source<A>,
}

pub struct Where<A> {
    tag: A,
    expr: Expr<A>,
}

pub struct Expr<A> {
    tag: A,
    value: Value<A>,
}

pub enum Value<A> {
    Literal(Literal),
    Path(Vec<String>),
    Record(Record<A>),
    App {
        fun: Box<Expr<A>>,
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
    fields: HashMap<String, Expr<A>>,
}
