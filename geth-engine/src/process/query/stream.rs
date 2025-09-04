use crate::process::query::requirements::{
    ParentQuery, Requirements, ScopedRequirements, SourceType,
};
use crate::reading::Streaming;
use crate::{ReaderClient, RequestContext};
use geth_common::{Direction, ReadStreamCompleted, Record, Revision};
use geth_eventql::Query;
use std::collections::HashMap;

struct N<A> {
    node: A,
    visited: bool,
}

impl<A> N<A> {
    fn new(node: A) -> Self {
        Self {
            node,
            visited: false,
        }
    }
}

struct Source {
    parent_query: Option<ParentQuery>,
    requirements: ScopedRequirements,
    streaming: Streaming,
}

enum State {
    Init,
}

#[derive(Hash, PartialEq, Eq)]
struct Binding {
    scope: u64,
    ident: String,
}

#[derive(Default)]
struct Sources {
    inner: HashMap<Binding, Source>,
}

pub struct QueryStream {
    ctx: RequestContext,
    query: Query,
    state: State,
    requirements: Requirements,
    client: ReaderClient,
}

impl QueryStream {
    pub async fn next(&mut self) -> eyre::Result<Option<Record>> {
        loop {
            match self.state {
                State::Init => {
                    self.init_sources().await;
                }
            }
        }
    }

    async fn init_sources(&mut self) -> eyre::Result<()> {
        let mut stack = vec![N::new(&self.query)];
        let mut sources = Sources::default();

        while let Some(mut item) = stack.pop() {
            if item.visited {
                continue;
            }

            let stmts = item.node.from_stmts.iter();
            let query_scope = item.node.attrs.scope;
            item.visited = true;
            stack.push(item);

            for stmt in stmts {
                if let Some(idents) = self.requirements.scoped_reqs.get(&stmt.attrs.scope) {
                    if let Some(reqs) = idents.get(&stmt.ident) {
                        match &reqs.source_type {
                            SourceType::Subject(sub) => {
                                let resp = self
                                    .client
                                    .read(
                                        self.ctx.clone(),
                                        &sub.to_string(),
                                        Revision::Start,
                                        Direction::Forward,
                                        usize::MAX,
                                    )
                                    .await?;

                                let streaming = match resp {
                                    ReadStreamCompleted::StreamDeleted => {
                                        eyre::bail!("stream '{}' was deleted", sub);
                                    }

                                    ReadStreamCompleted::Success(s) => s,
                                };

                                sources.inner.insert(
                                    Binding {
                                        scope: stmt.attrs.scope,
                                        ident: stmt.ident.to_string(),
                                    },
                                    Source {
                                        parent_query: self
                                            .requirements
                                            .parent_queries
                                            .get(&query_scope)
                                            .cloned(),

                                        requirements: self
                                            .requirements
                                            .scoped_reqs
                                            .get(&stmt.attrs.scope)
                                            .unwrap()
                                            .get(&stmt.ident)
                                            .cloned()
                                            .unwrap(),
                                        streaming,
                                    },
                                );
                            }

                            SourceType::Events => {
                                let streaming = self
                                    .client
                                    .read(
                                        self.ctx.clone(),
                                        "$all",
                                        Revision::Start,
                                        Direction::Forward,
                                        usize::MAX,
                                    )
                                    .await?.success()?;

                                sources.inner.insert(
                                    Binding {
                                        scope: stmt.attrs.scope,
                                        ident: stmt.ident.to_string(),
                                    },
                                    Source {
                                        parent_query: self
                                            .requirements
                                            .parent_queries
                                            .get(&query_scope)
                                            .cloned(),

                                        requirements: self
                                            .requirements
                                            .scoped_reqs
                                            .get(&stmt.attrs.scope)
                                            .unwrap()
                                            .get(&stmt.ident)
                                            .cloned()
                                            .unwrap(),
                                        streaming,
                                    },
                                );
                            }

                            SourceType::Subquery(query) => {}
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
