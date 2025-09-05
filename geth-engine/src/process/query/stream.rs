use crate::process::query::requirements::{ParentQuery, Requirements, ScopedRequirements};
use crate::reading::Streaming;
use crate::{ReaderClient, RequestContext};
use geth_common::{Direction, ReadStreamCompleted, Record, Revision};
use geth_eventql::{Query, SourceType, Subject};
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

struct DataStream {
    sub_query_scope: u64,
    streaming: Option<Streaming>,
}

impl DataStream {
    fn from_streaming(streaming: Streaming) -> Self {
        Self {
            sub_query_scope: 0,
            streaming: Some(streaming),
        }
    }

    fn from_sub_query(scope: u64) -> Self {
        Self {
            sub_query_scope: scope,
            streaming: None,
        }
    }
}

struct DataProvider {
    parent_query: Option<ParentQuery>,
    requirements: ScopedRequirements,
    data_stream: DataStream,
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
    inner: HashMap<u64, HashMap<Binding, DataProvider>>,
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
                    self.init_sources().await?;
                }
            }
        }
    }

    async fn init_sources(&mut self) -> eyre::Result<()> {
        let mut stack = vec![&self.query];
        let mut sources = Sources::default();

        while let Some(query) = stack.pop() {
            let stmts = query.from_stmts.iter();
            let query_scope = query.attrs.scope;
            stack.push(query);

            for stmt in stmts {
                if let Some(idents) = self.requirements.scoped_reqs.get(&stmt.attrs.scope) {
                    if let Some(reqs) = idents.get(&stmt.ident) {
                        let data_stream = match &stmt.source.inner {
                            SourceType::Subject(sub) => {
                                let stream_name = if reqs.subjects.is_empty() {
                                    sub.to_string()
                                } else {
                                    sub.common_subject(reqs.subjects.iter().collect())
                                        .to_string()
                                };

                                let resp = self
                                    .client
                                    .read(
                                        self.ctx.clone(),
                                        &stream_name,
                                        Revision::Start,
                                        Direction::Forward,
                                        usize::MAX,
                                    )
                                    .await?;

                                match resp {
                                    ReadStreamCompleted::StreamDeleted => {
                                        eyre::bail!("stream '{}' was deleted", sub);
                                    }

                                    ReadStreamCompleted::Success(s) => {
                                        DataStream::from_streaming(s)
                                    }
                                }
                            }

                            SourceType::Events => {
                                let stream_name = if reqs.subjects.is_empty() {
                                    "$all".to_string()
                                } else {
                                    Subject::from_subjects(reqs.subjects.iter().collect())
                                        .to_string()
                                };

                                let s = self
                                    .client
                                    .read(
                                        self.ctx.clone(),
                                        &stream_name,
                                        Revision::Start,
                                        Direction::Forward,
                                        usize::MAX,
                                    )
                                    .await?
                                    .success()?;

                                DataStream::from_streaming(s)
                            }

                            SourceType::Subquery(sub_query) => {
                                let sub_query_scope = sub_query.attrs.scope;
                                stack.push(sub_query);
                                DataStream::from_sub_query(sub_query_scope)
                            }
                        };

                        sources.inner.entry(query_scope).or_default().insert(
                            Binding {
                                scope: stmt.attrs.scope,
                                ident: stmt.ident.to_string(),
                            },
                            DataProvider {
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
                                data_stream,
                            },
                        );
                    }
                }
            }
        }

        Ok(())
    }
}
