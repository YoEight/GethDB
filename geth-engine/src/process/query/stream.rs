use crate::process::query::requirements::{ParentQuery, Requirements, ScopedRequirements};
use crate::reading::Streaming;
use crate::{ReaderClient, RequestContext};
use geth_common::{Direction, ReadStreamCompleted, Record, Revision};
use geth_eventql::{IntoLiteral, Query, SourceType, Subject};
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
    binding: Binding,
    parent_query: Option<ParentQuery>,
    requirements: ScopedRequirements,
    data_stream: DataStream,
}

enum State {
    Init,
    Live,
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

struct RecordProperties {
    values: HashMap<String, geth_eventql::Literal>,
    record: Record,
}

#[derive(Default)]
struct Cache {
    inner: HashMap<Binding, RecordProperties>,
}

#[derive(Default)]
struct DataProviders {
    inner: Vec<DataProvider>,
}

impl DataProviders {
    fn push(&mut self, provider: DataProvider) {
        self.inner.push(provider);
    }

    async fn move_next(
        &mut self,
        reqs_map: &Requirements,
        cache: &mut Cache,
    ) -> eyre::Result<bool> {
        for provider in &mut self.inner {
            let mut properties = HashMap::<String, geth_eventql::Literal>::default();
            if let Some(streaming) = provider.data_stream.streaming.as_mut() {
                let record = if let Some(r) = streaming.next().await? {
                    r
                } else {
                    return Ok(false);
                };

                let binding = &provider.binding;
                let reqs = reqs_map.get_requirements(binding.scope, &binding.ident);

                for needed in reqs.needed_props.iter() {
                    let mut in_payload = false;

                    for path in needed {
                        if in_payload {
                            continue;
                        }

                        match path.as_str() {
                            "data" => {
                                in_payload = true;
                            }

                            "specversion" => {
                                properties.insert(path.clone(), "0.1".into_literal());
                            }

                            "id" => {
                                properties
                                    .insert(path.clone(), record.id.to_string().into_literal());
                            }

                            "time" => {
                                properties.insert(path.clone(), "unsupported".into_literal());
                            }

                            "source" => {
                                properties.insert(path.clone(), "unsupported".into_literal());
                            }

                            "subject" => {
                                properties.insert(
                                    path.clone(),
                                    Subject::from_path(record.stream_name.as_str()).into_literal(),
                                );
                            }

                            _ => return Ok(false),
                        }
                    }
                }

                for (path, preds) in reqs.pushable_preds.iter() {
                    let path = path.as_slice();
                }
            } else {
                // TODO - implement subquery part
            }
        }

        Ok(true)
    }
}

pub struct QueryStream {
    ctx: RequestContext,
    query: Query,
    state: State,
    requirements: Requirements,
    client: ReaderClient,
    cache: Cache,
    providers: DataProviders,
}

impl QueryStream {
    pub async fn next(&mut self) -> eyre::Result<Option<Record>> {
        loop {
            match self.state {
                State::Init => {
                    self.init_sources().await?;
                    self.state = State::Live;
                }

                State::Live => {}
            }
        }
    }

    async fn init_sources(&mut self) -> eyre::Result<()> {
        let mut stack = vec![N::new(&self.query)];
        let mut sources = Sources::default();

        while let Some(mut item) = stack.pop() {
            let stmts = item.node.from_stmts.iter();
            let query_scope = item.node.attrs.scope;

            if item.visited {
                for stmt in stmts {
                    if let Some(idents) = self.requirements.scoped_reqs.get(&stmt.attrs.scope) {
                        if let Some(reqs) = idents.get(&stmt.ident) {
                            let binding = Binding {
                                scope: stmt.attrs.scope,
                                ident: stmt.ident.to_string(),
                            };

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
                                    DataStream::from_sub_query(sub_query_scope)
                                }
                            };

                            self.providers.push(DataProvider {
                                binding,
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
                            });
                        }
                    }
                }

                continue;
            }

            item.visited = true;
            stack.push(item);

            for stmt in stmts {
                if let SourceType::Subquery(sub_query) = &stmt.source.inner {
                    stack.push(N::new(sub_query.as_ref()));
                }
            }
        }

        Ok(())
    }
}
