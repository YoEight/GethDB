use crate::bus::SubscribeMsg;
use crate::messages::{StreamTarget, SubscribeTo, SubscriptionTarget};
use crate::process::subscriptions::SubscriptionsClient;
use bytes::Bytes;
use chrono::Utc;
use geth_common::{Position, Record, Revision};
use geth_mikoshi::{Entry, MikoshiStream};
use pyro_core::ast::{Prop, Type};
use pyro_core::sym::Literal;
use pyro_runtime::{Channel, Engine, PyroLiteral, RuntimeValue, Symbol};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::{mpsc, oneshot, Mutex};
use uuid::Uuid;

struct SubServer {
    recv: Arc<Mutex<mpsc::UnboundedReceiver<RuntimeValue>>>,
}

impl SubServer {
    fn new(inner: mpsc::UnboundedReceiver<RuntimeValue>) -> Self {
        Self {
            recv: Arc::new(Mutex::new(inner)),
        }
    }
}

struct Blob(Arc<RuntimeValue>);

impl PyroLiteral for Blob {
    fn try_from_value(value: Arc<RuntimeValue>) -> eyre::Result<Self>
    where
        Self: Sized,
    {
        Ok(Blob(value))
    }

    fn r#type() -> Type {
        Type::generic("a")
    }

    fn value(self) -> RuntimeValue {
        self.0.as_ref().clone()
    }
}

fn event_type() -> Type {
    let mut props = Vec::new();

    props.push(Prop {
        label: Some("id".to_string()),
        val: Type::string(),
    });

    props.push(Prop {
        label: Some("stream_name".to_string()),
        val: Type::string(),
    });

    props.push(Prop {
        label: Some("event_type".to_string()),
        val: Type::string(),
    });

    props.push(Prop {
        label: Some("event_revision".to_string()),
        val: Type::integer(),
    });

    props.push(Prop {
        label: Some("position".to_string()),
        val: Type::integer(),
    });

    Type::Record(pyro_core::ast::Record { props })
}

fn event_record_type() -> Type {
    let mut props = Vec::new();

    props.push(Prop {
        label: Some("id".to_string()),
        val: Type::string(),
    });

    props.push(Prop {
        label: Some("stream_name".to_string()),
        val: Type::string(),
    });

    props.push(Prop {
        label: Some("event_type".to_string()),
        val: Type::string(),
    });

    props.push(Prop {
        label: Some("event_revision".to_string()),
        val: Type::integer(),
    });

    props.push(Prop {
        label: Some("position".to_string()),
        val: Type::integer(),
    });

    props.push(Prop {
        label: Some("payload".to_string()),
        val: entry_type(),
    });

    Type::Record(pyro_core::ast::Record { props })
}

fn event_record_runtime_value(record: Record) -> eyre::Result<RuntimeValue> {
    let mut props = Vec::new();

    props.push(Prop {
        label: Some("id".to_string()),
        val: RuntimeValue::string(record.id.to_string()),
    });

    props.push(Prop {
        label: Some("stream_name".to_string()),
        val: RuntimeValue::string(record.stream_name),
    });

    props.push(Prop {
        label: Some("event_type".to_string()),
        val: RuntimeValue::string(record.r#type),
    });

    props.push(Prop {
        label: Some("event_revision".to_string()),
        val: RuntimeValue::Literal(Literal::Integer(record.revision)),
    });

    props.push(Prop {
        label: Some("position".to_string()),
        val: RuntimeValue::Literal(Literal::Integer(record.position.raw())),
    });

    let value = serde_json::from_slice::<Value>(record.data.as_ref())?;

    props.push(Prop {
        label: Some("payload".to_string()),
        val: from_json_to_pyro_runtime_value(value)?,
    });

    Ok(RuntimeValue::Record(pyro_core::ast::Record { props }))
}

fn entry_type() -> Type {
    Type::Name {
        parent: vec![Type::show()],
        name: "Entry".to_string(),
        kind: 0,
        generic: false,
    }
}

fn from_json_to_pyro_runtime_value(value: Value) -> eyre::Result<RuntimeValue> {
    match value {
        Value::Null => eyre::bail!("NULL is not supported"),
        Value::Bool(b) => Ok(RuntimeValue::Literal(Literal::Bool(b))),
        Value::Number(n) => match n.as_u64() {
            None => eyre::bail!("We only support u64 so far"),
            Some(n) => Ok(RuntimeValue::Literal(Literal::Integer(n))),
        },
        Value::String(s) => Ok(RuntimeValue::Literal(Literal::String(s))),
        Value::Array(xs) => {
            let mut props = Vec::new();

            for x in xs {
                props.push(Prop {
                    label: None,
                    val: from_json_to_pyro_runtime_value(x)?,
                });
            }

            Ok(RuntimeValue::Record(pyro_core::ast::Record { props }))
        }
        Value::Object(xs) => {
            let mut props = Vec::new();

            for (name, x) in xs {
                props.push(Prop {
                    label: Some(name),
                    val: from_json_to_pyro_runtime_value(x)?,
                });
            }

            Ok(RuntimeValue::Record(pyro_core::ast::Record { props }))
        }
    }
}

fn from_runtime_value_to_json(value: RuntimeValue) -> eyre::Result<Value> {
    match value {
        RuntimeValue::Channel(_) => eyre::bail!("Pyro channels can't be converted to JSON"),
        RuntimeValue::Abs(_) => eyre::bail!("Pyro anonymous clients can't be converted to JSON"),
        RuntimeValue::Fun(_) => eyre::bail!("Pyro functions can't be converted to JSON"),
        RuntimeValue::Literal(l) => Ok(match l {
            Literal::Integer(n) => serde_json::to_value(n)?,
            Literal::String(s) => Value::String(s),
            Literal::Char(c) => serde_json::to_value(c)?,
            Literal::Bool(b) => Value::Bool(b),
        }),

        RuntimeValue::Record(rec) => {
            if rec.is_array() {
                let mut values = Vec::new();

                for prop in rec.props {
                    values.push(from_runtime_value_to_json(prop.val)?);
                }

                Ok(serde_json::to_value(values)?)
            } else {
                let mut props = HashMap::new();
                let mut anon_name = -1;

                for prop in rec.props {
                    let prop_name = if let Some(name) = prop.label {
                        name
                    } else {
                        anon_name += 1;
                        anon_name.to_string()
                    };

                    props.insert(prop_name, from_runtime_value_to_json(prop.val)?);
                }

                Ok(serde_json::to_value(props)?)
            }
        }
    }
}

impl PyroLiteral for SubServer {
    fn try_from_value(value: Arc<RuntimeValue>) -> eyre::Result<Self>
    where
        Self: Sized,
    {
        if let RuntimeValue::Channel(Channel::Server(recv)) = value.as_ref() {
            return Ok(SubServer { recv: recv.clone() });
        }

        eyre::bail!("Expected a server")
    }

    fn r#type() -> Type {
        Type::server(event_record_type())
    }

    fn value(self) -> RuntimeValue {
        RuntimeValue::Channel(Channel::Server(self.recv))
    }
}

pub fn spawn(client: SubscriptionsClient, name: String, source_code: String) -> MikoshiStream {
    let (send_output, mut recv_output) = unbounded_channel();
    let local_name = name.clone();
    let engine = Engine::builder()
        .add_type("EventRecord", event_record_type())
        .add_type("Entry", entry_type())
        .add_symbol(Symbol::client("output", Type::generic("a"), send_output))
        .add_symbol(Symbol::func("subscribe", move |stream_name: String| {
            let (input, recv) = mpsc::unbounded_channel();
            let (mailbox, confirmed) = oneshot::channel();
            let _ = client.subscribe(SubscribeMsg {
                payload: SubscribeTo {
                    correlation: Uuid::new_v4(),
                    target: SubscriptionTarget::Stream(StreamTarget {
                        stream_name,
                        starting: Revision::Start,
                    }),
                },
                mail: mailbox,
            });

            tokio::spawn(async move {
                let mut confirmed = if let Ok(c) = confirmed.await {
                    c
                } else {
                    eyre::bail!("Subscription failed in programmable subscription");
                };

                while let Some(record) = confirmed.reader.next().await? {
                    if !input.send(event_record_runtime_value(record)?).is_err() {
                        break;
                    }
                }

                Ok(())
            });

            SubServer::new(recv)
        }))
        .build();

    let (send_mikoshi, recv_mikoshi) = mpsc::channel(500);

    tokio::spawn(async move {
        let mut revision = 0;
        while let Some(value) = recv_output.recv().await {
            let value = from_runtime_value_to_json(value)?;
            if send_mikoshi
                .send(Entry {
                    id: Uuid::new_v4(),
                    r#type: "subscription-sent".to_string(),
                    // TODO - Find a better name for those programmable subscriptions.
                    stream_name: name.clone(),
                    revision,
                    data: Bytes::from(serde_json::to_vec(&value)?),
                    position: Position::end(),
                    created: Utc::now(),
                })
                .await
                .is_err()
            {
                tracing::info!(
                    "Programmable subscription '{}' was cancelled by the user",
                    name
                );
                break;
            }

            revision += 1;
        }

        Ok::<_, eyre::Report>(())
    });

    tokio::spawn(async move {
        if let Err(e) = engine.run(source_code.as_str()).await {
            tracing::error!(
                "Programmable subscription '{}' exited with an error: {}",
                local_name,
                e
            );
        }
    });

    MikoshiStream::new(recv_mikoshi)
}
