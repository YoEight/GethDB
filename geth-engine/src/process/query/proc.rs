use std::collections::{HashMap, HashSet};

use geth_common::{Direction, Revision};
use geth_eventql::{
    ContextFrame, Expr, ExprVisitor, Literal, NodeAttributes, Operation, Query, QueryVisitor,
    Subject, Value,
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
        if let Item::Stream(stream) = item
            && let Ok(QueryRequests::Query { query }) = stream.payload.try_into()
        {
            let infered = match geth_eventql::parse_rename_and_infer(&query) {
                Ok(q) => q,
                Err(e) => {
                    let _ = stream.sender.send(QueryResponses::Error(e.into()).into());
                    continue;
                }
            };

            let reqs = collect_requirements(infered.query());
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

    Ok(())
}

struct Requirements {
    subjects: HashMap<Binding, HashSet<Subject>>,
}

fn collect_requirements(query: &Query) -> Requirements {
    let mut collect_reqs = CollectRequirements::default();
    query.dfs_post_order(&mut collect_reqs);

    Requirements {
        subjects: collect_reqs.subjects,
    }
}

#[derive(Hash, PartialEq, Eq)]
struct Binding {
    scope: u64,
    ident: String,
}

#[derive(Default)]
struct CollectRequirements {
    context: ContextFrame,
    subjects: HashMap<Binding, HashSet<Subject>>,
}

impl QueryVisitor for CollectRequirements {
    type Inner<'a> = CollectRequirementsFromExpr<'a>;

    fn enter_where_clause(&mut self, _attrs: &NodeAttributes, _expr: &Expr) {
        self.context = ContextFrame::Where;
    }

    fn exit_where_clause(&mut self, _attrs: &NodeAttributes, _expr: &Expr) {
        self.context = ContextFrame::Unspecified;
    }

    fn on_source_subject(&mut self, attrs: &NodeAttributes, ident: &str, subject: &Subject) {
        let binding = Binding {
            scope: attrs.scope,
            ident: ident.to_string(),
        };

        self.subjects
            .entry(binding)
            .or_default()
            .insert(subject.clone());
    }

    fn expr_visitor<'a>(&'a mut self) -> Self::Inner<'a> {
        CollectRequirementsFromExpr { inner: self }
    }
}

struct CollectRequirementsFromExpr<'a> {
    inner: &'a mut CollectRequirements,
}

impl ExprVisitor for CollectRequirementsFromExpr<'_> {
    fn exit_binary_op(&mut self, attrs: &NodeAttributes, op: &Operation, lhs: &Expr, rhs: &Expr) {
        if self.inner.context != ContextFrame::Where {
            return;
        }

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
                        scope: attrs.scope,
                        ident: x.name.clone(),
                    };

                    self.inner
                        .subjects
                        .entry(binding)
                        .or_default()
                        .insert(sub.clone());
                }
            }

            _ => {}
        }
    }
}
