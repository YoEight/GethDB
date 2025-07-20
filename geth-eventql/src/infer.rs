use crate::{Lexical, Query, Renamed, Scopes};

pub struct Infered {}

pub struct Infer {}

pub struct Assumptions {}

pub fn infer(renamed: Renamed) -> eyre::Result<Infered> {
    todo!()
}

#[derive(Clone, Copy)]
struct Typecheck<'a> {
    assumptions: &'a Assumptions,
    scopes: &'a Scopes,
}

fn infer_query(
    type_check: Typecheck<'_>,
    query: Query<Lexical>,
) -> eyre::Result<Query<Infer>> {
    todo!()
}

fn infer_from(type_check: Typecheck<'_>, query: Query<Lexical>) -> eyre::Result<Query<Infer>> {
    
}
