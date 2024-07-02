use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use pyro_core::ast::Prop;
use pyro_core::sym::Literal;
use pyro_runtime::{Channel, Engine, Env, PyroType, PyroValue, RuntimeValue};
use pyro_runtime::helpers::{Declared, TypeBuilder};
use serde_json::Value;
use tokio::sync::{mpsc, Mutex, oneshot};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::task::JoinHandle;
use uuid::Uuid;

use geth_common::{Position, Record};
use geth_mikoshi::MikoshiStream;

use crate::bus::SubscribeMsg;
use crate::messages::{StreamTarget, SubscribeTo, SubscriptionRequestOutcome, SubscriptionTarget};
use crate::process::subscriptions::SubscriptionsClient;

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

#[derive(Clone)]
struct ProgramOutput(UnboundedSender<RuntimeValue>);

impl PyroType for ProgramOutput {
    fn r#type(builder: TypeBuilder) -> Declared {
        builder.constr("Client").for_all_type().done()
    }
}

impl PyroValue for ProgramOutput {
    fn deserialize(_value: Arc<RuntimeValue>) -> eyre::Result<Self>
    where
        Self: Sized,
    {
        eyre::bail!("You can't deserialize a 'ProgramOutput' client")
    }

    fn serialize(self) -> eyre::Result<RuntimeValue> {
        Ok(RuntimeValue::Channel(Channel::Client(self.0)))
    }
}

// struct Blob(Arc<RuntimeValue>);
//
// impl PyroLiteral for Blob {
//     fn try_from_value(value: Arc<RuntimeValue>) -> eyre::Result<Self>
//     where
//         Self: Sized,
//     {
//         Ok(Blob(value))
//     }
//
//     fn r#type() -> Type {
//         Type::generic("a")
//     }
//
//     fn value(self) -> RuntimeValue {
//         self.0.as_ref().clone()
//     }
// }

struct EventRecord(Record);

impl PyroType for EventRecord {
    fn r#type(builder: TypeBuilder) -> Declared {
        builder
            .rec()
            .prop::<String>("id")
            .prop::<String>("stream_name")
            .prop::<String>("event_type")
            .prop::<i64>("event_revision")
            .prop::<i64>("position")
            .prop::<EventEntry>("payload")
            .done()
    }
}

impl PyroValue for EventRecord {
    fn deserialize(_value: Arc<RuntimeValue>) -> eyre::Result<Self> {
        eyre::bail!("You can't deserialize an EventRecord from the Pyro runtime")
    }

    fn serialize(self) -> eyre::Result<RuntimeValue> {
        let record = self.0;
        let props = vec![
            Prop {
                label: Some("id".to_string()),
                val: RuntimeValue::string(record.id.to_string()),
            },
            Prop {
                label: Some("stream_name".to_string()),
                val: RuntimeValue::string(record.stream_name),
            },
            Prop {
                label: Some("event_type".to_string()),
                val: RuntimeValue::string(record.r#type),
            },
            Prop {
                label: Some("event_revision".to_string()),
                val: RuntimeValue::Literal(Literal::Integer(record.revision as i64)),
            },
            Prop {
                label: Some("position".to_string()),
                val: RuntimeValue::Literal(Literal::Integer(record.position.raw() as i64)),
            },
            Prop {
                label: Some("payload".to_string()),
                val: from_json_to_pyro_runtime_value(serde_json::from_slice::<Value>(
                    record.data.as_ref(),
                )?)?,
            },
        ];

        Ok(RuntimeValue::Record(pyro_core::ast::Record { props }))
    }
}

struct EventEntry;

impl PyroType for EventEntry {
    fn r#type(builder: TypeBuilder) -> Declared {
        builder.create("Entry").with_constraint("Show").build()
    }
}

fn from_json_to_pyro_runtime_value(value: Value) -> eyre::Result<RuntimeValue> {
    match value {
        Value::Null => eyre::bail!("NULL is not supported"),
        Value::Bool(b) => Ok(RuntimeValue::Literal(Literal::Bool(b))),
        Value::Number(n) => match n.as_i64() {
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

impl PyroType for SubServer {
    fn r#type(builder: TypeBuilder) -> Declared {
        builder.constr("Server").of::<EventRecord>()
    }
}

impl PyroValue for SubServer {
    fn deserialize(value: Arc<RuntimeValue>) -> eyre::Result<Self>
    where
        Self: Sized,
    {
        if let RuntimeValue::Channel(Channel::Server(recv)) = value.as_ref() {
            return Ok(SubServer { recv: recv.clone() });
        }

        eyre::bail!("Expected a server")
    }

    fn serialize(self) -> eyre::Result<RuntimeValue> {
        Ok(RuntimeValue::Channel(Channel::Server(self.recv)))
    }
}

pub struct Programmable {
    pub handle: JoinHandle<()>,
    pub stream: MikoshiStream,
}

pub fn spawn(
    client: SubscriptionsClient,
    id: Uuid,
    name: String,
    source_code: String,
) -> eyre::Result<Programmable> {
    let (stdout_handle, mut stdout_recv) = unbounded_channel();
    let env = Env { stdout_handle };
    let prog_name = name.clone();
    tokio::spawn(async move {
        while let Some(value) = stdout_recv.recv().await {
            tracing::info!(
                target = "programmable_subscriptions",
                name = prog_name.as_str(),
                message = value.to_string(),
            );
        }
    });

    let (send_output, mut recv_output) = unbounded_channel();
    let local_name = name.clone();
    let inner_name = name.clone();
    let engine = Engine::with_nominal_typing()
        .stdlib(env)
        .register_type::<EventEntry>("Entry")
        .register_type::<EventRecord>("EventRecord")
        .register_value("output", ProgramOutput(send_output))
        .register_function("subscribe", move |stream_name: String| {
            let (input, recv) = mpsc::unbounded_channel();
            let (mailbox, confirmed) = oneshot::channel();
            let _ = client.subscribe(SubscribeMsg {
                payload: SubscribeTo {
                    target: SubscriptionTarget::Stream(StreamTarget {
                        parent: Some(id),
                        stream_name: stream_name.clone(),
                    }),
                },
                mail: mailbox,
            });

            let local_name_2 = inner_name.clone();
            tokio::spawn(async move {
                let mut reader = if let Ok(c) = confirmed.await {
                    match c.outcome {
                        SubscriptionRequestOutcome::Success(r) => r,
                        SubscriptionRequestOutcome::Failure(e) => {
                            tracing::error!("Programmable subscription '{}' errored when subscribing to '{}' stream: {}", local_name_2, stream_name, e);
                            return Err(e);
                        }
                    }
                } else {
                    eyre::bail!("Subscription failed in programmable subscription");
                };

                while let Some(record) = reader.next().await? {
                    if input.send(EventRecord(record).serialize()?).is_err() {
                        tracing::warn!(
                            "Programmable subscription '{}' stopped to care about incoming message",
                            local_name_2
                        );
                        break;
                    }
                }

                tracing::info!("Programmable subscription '{}' completed", local_name_2);
                Ok(())
            });

            SubServer::new(recv)
        })
        .build()?;

    let (send_mikoshi, recv_mikoshi) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        let mut revision = 0;
        while let Some(value) = recv_output.recv().await {
            let value = from_runtime_value_to_json(value)?;
            if send_mikoshi
                .send(Record {
                    id: Uuid::new_v4(),
                    r#type: "subscription-sent".to_string(),
                    // TODO - Find a better name for those programmable subscriptions.
                    stream_name: name.clone(),
                    revision,
                    data: Bytes::from(serde_json::to_vec(&value)?),
                    position: Position::end(),
                })
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

    let process = engine.compile(source_code.as_str())?;
    let handle = tokio::spawn(async move {
        if let Err(e) = process.run().await {
            tracing::error!(
                "Programmable subscription '{}' exited with an error: {}",
                local_name,
                e
            );
        }
    });

    Ok(Programmable {
        handle,
        stream: MikoshiStream::new(recv_mikoshi),
    })
}
