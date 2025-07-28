use std::collections::{HashMap, HashSet};

use geth_common::Record;
use geth_eventql::{
    Expr, Infer, InferedQuery, Literal, Operation, Query, SourceType, Subject, Value, Where,
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

                let _ = collect_requirements(&query);
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
    collect_subjects_from_where_expr(reqs, &clause.expr);
}

fn collect_subjects_from_where_expr(reqs: &mut Requirements, expr: &Expr<Infer>) {
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
                        scope: expr.tag.scope(),
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

enum Instr<'a> {
    Expr(&'a Expr<Infer>),
    Operation(Operation),
    Array(usize),
    App(usize),
}

struct Table {
    inner: HashMap<String, Literal>,
}

impl Table {
    fn property(&self, key: &str) -> Option<&Literal> {
        self.inner.get(key)
    }

    fn payload(&self, key: &str) -> Option<&Literal> {
        todo!()
    }
}

fn evaluate_predicate(env: &HashMap<String, Table>, expr: &Expr<Infer>) -> bool {
    let mut stack = vec![Instr::Expr(expr)];
    let mut params = Vec::<&Literal>::new();

    while let Some(instr) = stack.pop() {
        match instr {
            Instr::Expr(expr) => match &expr.value {
                Value::Literal(literal) => params.push(literal),

                Value::Var(var) => {
                    // technically, it's not possible for a var to not be here but we give us some leeway
                    // by considering that situation to support some kind of RIGHT JOIN feature.
                    let table = if let Some(t) = env.get(&var.name) {
                        t
                    } else {
                        return false;
                    };

                    match var.path.as_slice() {
                        [key] => {
                            if let Some(lit) = table.property(key) {
                                params.push(lit);
                            }
                        }

                        [prop, key] if prop == "data" => {
                            if let Some(lit) = table.payload(key) {
                                params.push(lit);
                            }
                        }

                        _ => {}
                    }

                    // if path == ["id"] {
                    //     params.push(&Literal::String(record.id.to_string()));
                    // }

                    // match path {
                    //     ["specversion"] => params.push(&Literal::String("1.0.0".to_string())),
                    //     // TODO - put that todo notice here in case I forgot about that wildcard pattern
                    //     _ => return false,
                    // }
                }

                Value::Record(record) => todo!(),
                Value::Array(exprs) => todo!(),
                Value::App { fun, params } => todo!(),
                Value::Binary { lhs, op, rhs } => todo!(),
                Value::Unary { op, expr } => todo!(),
            },

            Instr::Operation(operation) => todo!(),

            Instr::Array(_) => todo!(),

            Instr::App(_) => todo!(),
        }
    }

    todo!()
}
