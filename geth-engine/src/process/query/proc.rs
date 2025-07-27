use std::collections::{HashMap, HashSet};

use geth_eventql::{
    Expr, Infer, InferedQuery, Literal, Operation, Pos, Query, SourceType, Value, Var, Where,
};

use crate::process::{
    messages::{QueryRequests, QueryResponses},
    Item, Managed, ProcessEnv,
};

#[tracing::instrument(skip_all, fields(proc_id = env.client.id(), proc = ?env.proc))]
pub async fn run(mut env: ProcessEnv<Managed>) -> eyre::Result<()> {
    while let Some(item) = env.recv().await {
        if let Item::Stream(stream) = item {
            if let Ok(QueryRequests::Query { query }) = stream.payload.try_into() {
                let query = match geth_eventql::parse_rename_and_infer(&query) {
                    Ok(q) => q,
                    Err(e) => {
                        let _ = stream.sender.send(QueryResponses::Error(e.into()).into());
                        continue;
                    }
                };

                let requirements = collect_requirements(&query);
            }
        }
    }

    Ok(())
}

#[derive(Hash, PartialEq, Eq)]
struct Binding {
    scope: u64,
    ident: String,
}

#[derive(Default)]
struct Requirements {
    subjects: HashMap<Binding, HashSet<String>>,
}

enum Stack<'a> {
    Operation(Operation),
    Expr(&'a Expr<Pos>),
}

enum Eval<'a> {
    Literal(&'a Literal),
    Var(&'a Var),
}

fn collect_requirements(query: &InferedQuery) -> Requirements {
    let mut reqs = Requirements::default();

    collect_subjects_from_query(&mut reqs, query.query());

    reqs
}

fn collect_subjects_from_query(reqs: &mut Requirements, query: &Query<Infer>) {
    for from_stmt in &query.from_stmts {
        collect_subjects_from_decl(reqs, from_stmt);
    }

    if let Some(clause) = &query.predicate {
        collect_subjects_from_where_clause(reqs, clause);
    }
}

fn collect_subjects_from_decl(reqs: &mut Requirements, from_stmt: &geth_eventql::From<Infer>) {
    match &from_stmt.source.inner {
        SourceType::Events => {}

        SourceType::Subject(sub) => {
            let binding = Binding {
                scope: from_stmt.source.tag.scope(),
                ident: from_stmt.ident.clone(),
            };

            reqs.subjects
                .entry(binding)
                .or_default()
                .insert(sub.clone());
        }

        SourceType::Subquery(query) => collect_subjects_from_query(reqs, query),
    }
}

fn collect_subjects_from_where_clause(reqs: &mut Requirements, clause: &Where<Infer>) {
    collect_subjects_from_expr(reqs, &clause.expr);
}

fn collect_subjects_from_expr(reqs: &mut Requirements, expr: &Expr<Infer>) {
    match &expr.value {
        Value::Binary { lhs, op, rhs } => match (&lhs.value, &rhs.value) {
            (Value::Var(_), Value::Var(_)) => {}

            (Value::Var(x), Value::Literal(Literal::String(s)))
            | (Value::Literal(Literal::String(s)), Value::Var(x)) => {
                // TODO - we check if the predicate is checking for a subject of a specific value
            }

            _ => {
                collect_subjects_from_expr(reqs, lhs);
                collect_subjects_from_expr(reqs, rhs);
            }
        },

        Value::Unary { op, expr } => todo!(),
        _ => {}
    }
}
