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
            // // let mut sources = HashMap::with_capacity(reqs.subjects.len());
            // let ctx = RequestContext::new();

            // for (binding, subjects) in reqs.subjects.iter() {
            //     for subject in subjects.iter() {
            //         // TODO - need to support true subject instead of relaying on stream name.
            //         let stream_name = subject.to_string();
            //         let _stream = reader
            //             .read(
            //                 ctx,
            //                 &stream_name,
            //                 Revision::Start,
            //                 Direction::Forward,
            //                 usize::MAX,
            //             )
            //             .await?;

            //         // TODO need to load them stream readers in a better way for all the sources.
            //     }
            // }
        }
    }

    Ok(())
}

struct Requirements {
    parent_queries: HashMap<u64, ParentQuery>,
    scoped_reqs: HashMap<u64, HashMap<String, ScopedRequirements>>,
}

fn collect_requirements(query: &Query) -> Requirements {
    let mut collect_reqs = CollectRequirements::default();
    query.dfs_pre_order(&mut collect_reqs);

    Requirements {
        parent_queries: collect_reqs.parent_queries,
        scoped_reqs: collect_reqs.scoped_reqs,
    }
}

#[derive(Hash, PartialEq, Eq)]
struct Binding {
    scope: u64,
    ident: String,
}

struct ParentQuery {
    scope: u64,
    ident: String,
}

#[derive(Default)]
struct ScopedRequirements {
    needed_props: HashSet<Vec<String>>,
    pushable_preds: HashMap<Vec<String>, Vec<Expr>>,
    subjects: HashSet<Subject>,
}

#[derive(Default)]
struct CollectRequirements {
    context: ContextFrame,
    parent_queries: HashMap<u64, ParentQuery>,
    scoped_reqs: HashMap<u64, HashMap<String, ScopedRequirements>>,
}

impl QueryVisitor for CollectRequirements {
    type Inner<'a> = CollectRequirementsFromExpr<'a>;

    fn enter_where_clause(&mut self, _attrs: &NodeAttributes, _expr: &Expr) {
        self.context = ContextFrame::Where;
    }

    fn exit_where_clause(&mut self, _attrs: &NodeAttributes, expr: &Expr) {
        self.context = ContextFrame::Unspecified;
    }

    fn on_source_subject(&mut self, attrs: &NodeAttributes, ident: &str, subject: &Subject) {
        self.scoped_reqs
            .entry(attrs.scope)
            .or_default()
            .entry(ident.to_string())
            .or_default()
            .subjects
            .insert(subject.clone());
    }

    fn on_source_subquery(&mut self, attrs: &NodeAttributes, ident: &str, query: &Query) -> bool {
        self.parent_queries.insert(
            query.attrs.scope,
            ParentQuery {
                scope: attrs.scope,
                ident: ident.to_string(),
            },
        );

        true
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

            (Value::Var(x), Value::Literal(lit)) | (Value::Literal(lit), Value::Var(x)) => {
                let reqs = self
                    .inner
                    .scoped_reqs
                    .entry(attrs.scope)
                    .or_default()
                    .entry(x.name.clone())
                    .or_default();

                reqs.pushable_preds
                    .entry(x.path.clone())
                    .or_default()
                    .push(Expr {
                        attrs: *attrs,
                        value: Value::Binary {
                            lhs: Box::new(lhs.clone()),
                            op: *op,
                            rhs: Box::new(rhs.clone()),
                        },
                    });

                if let Literal::Subject(sub) = lit
                    && x.path.as_slice() == ["subject"]
                    && op == &Operation::Equal
                {
                    reqs.subjects.insert(sub.clone());
                }
            }

            _ => {}
        }
    }
}
