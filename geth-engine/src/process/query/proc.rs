use std::collections::{HashMap, HashSet};

use geth_common::{Direction, Revision};
use geth_eventql::{
    Expr, FromSource, InferedQuery, Literal, Operation, Query, SourceType, Subject, Value, Where,
};

use crate::{
    RequestContext,
    process::{
        Item, Managed, ProcessEnv,
        messages::{QueryRequests, QueryResponses},
    },
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

                let reqs = collect_requirements(&query);
                let reader = env.client.new_reader_client().await?;
                // let mut sources = HashMap::with_capacity(reqs.subjects.len());
                let ctx = RequestContext::new();

                for (_, subjects) in reqs.subjects.iter() {
                    for subject in subjects.iter() {
                        // TODO - need to support true subject instead of relaying on stream name.
                        let stream_name = subject.to_string();
                        let _stream = reader
                            .read(
                                ctx,
                                &stream_name,
                                Revision::Start,
                                Direction::Forward,
                                usize::MAX,
                            )
                            .await?;

                        // TODO need to load them stream readers in a better way for all the sources.
                    }
                }
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
    subjects: HashMap<Binding, HashSet<Subject>>,
}

fn collect_requirements(query: &InferedQuery) -> Requirements {
    let mut reqs = Requirements::default();

    collect_subjects_from_query(&mut reqs, query.query());

    reqs
}

fn collect_subjects_from_query(reqs: &mut Requirements, query: &Query) {
    for from_stmt in &query.from_stmts {
        collect_subjects_from_decl(reqs, from_stmt);
    }

    if let Some(clause) = &query.predicate {
        collect_subjects_from_where_clause(reqs, clause);
    }
}

fn collect_subjects_from_decl(reqs: &mut Requirements, from_stmt: &FromSource) {
    match &from_stmt.source.inner {
        SourceType::Events => {}

        SourceType::Subject(sub) => {
            let binding = Binding {
                scope: from_stmt.source.attrs.scope,
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

fn collect_subjects_from_where_clause(reqs: &mut Requirements, clause: &Where) {
    collect_subjects_from_where_expr(reqs, &clause.expr);
}

fn collect_subjects_from_where_expr(reqs: &mut Requirements, expr: &Expr) {
    if let Value::Binary { lhs, op, rhs } = &expr.value {
        match (&lhs.value, &rhs.value) {
            (Value::Var(_), Value::Var(_)) => {
                // TODO - a possible optimization could be to check if two variables are looking at the same subject.
                // if they do then we can merge the request into a singular read operation. Current thought of the matter: the way we deal
                // with binding to subjects needs to be extended to either we have a direct binding
                //  (a.k.a an explict x.subject = "/foo/bar" or FROM x IN "/foo/bar") and an indirect where
                // we have something like x.subject ==  y.subject. By doing that we can simplify a convuloted requests like
                // those to a much direct one.
            }

            (Value::Var(x), Value::Literal(Literal::Subject(sub)))
            | (Value::Literal(Literal::Subject(sub)), Value::Var(x)) => {
                if x.path.as_slice() == ["subject"] && op == &Operation::Equal {
                    let binding = Binding {
                        scope: expr.attrs.scope,
                        ident: x.name.clone(),
                    };

                    reqs.subjects
                        .entry(binding)
                        .or_default()
                        .insert(sub.clone());
                }
            }

            _ => {
                collect_subjects_from_where_expr(reqs, lhs);
                collect_subjects_from_where_expr(reqs, rhs);
            }
        }
    }
}
