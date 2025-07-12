use crate::sym::Logical;

pub struct Ast<A> {
    tag: A,
    from: Vec<From<A>>,
}

pub enum SourceType<A> {
    Events,
    Subject(String),
    Subquery(Box<Ast<A>>)
}

pub struct Source<A> {
    tag: A,
    inner: SourceType<A>
}

pub struct From<A> {
    tag: A,
    ident: String,
    source: Source<A>,
}

pub struct Where<A> {
    tag: A,
    expr: WhereExpr<A>,
}

pub enum WhereExprType<A> {
    Unary { op: Logical, expr: Box<WhereExpr<A>> },
    Binary { lhs: Box<WhereExpr<A>>, op: Logical, rhs: Box<WhereExprType<A>> },
}

pub struct WhereExpr<A> {
    tag: A,
    r#type:  WhereExprType<A>,
}
