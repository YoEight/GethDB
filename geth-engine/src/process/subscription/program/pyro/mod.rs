use std::{collections::HashMap, sync::Arc};

use base64::Engine as _;
use chrono::{DateTime, Utc};
use geth_common::{ContentType, Record, Revision, SubscriptionConfirmation, SubscriptionEvent};
use pyro_core::{NominalTyping, ast::Prop, sym::Literal};
use pyro_runtime::{
    Channel, Engine, Env, PyroProcess, PyroType, PyroValue, RuntimeValue,
    helpers::{Declared, TypeBuilder},
};
use serde_json::Value;
use tokio::{
    select,
    sync::{
        Mutex,
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    },
};

use crate::{
    ManagerClient,
    process::{
        ProcId, RequestContext,
        consumer::{ConsumerResult, start_consumer},
    },
};

pub mod worker;

struct EventEntry;

impl PyroType for EventEntry {
    fn r#type(builder: TypeBuilder) -> Declared {
        builder.create("Entry").with_constraint("Show").build()
    }
}

struct EventRecord(Record);

impl PyroType for EventRecord {
    fn r#type(builder: TypeBuilder) -> Declared {
        builder
            .rec()
            .prop::<String>("id")
            .prop::<String>("stream_name")
            .prop::<String>("class")
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

        let payload = if record.data.is_empty() {
            serde_json::Value::Array(vec![])
        } else if record.content_type != ContentType::Json {
            let encoded = base64::engine::general_purpose::STANDARD.encode(&record.data);
            serde_json::Value::String(encoded)
        } else {
            serde_json::from_slice::<Value>(record.data.as_ref())?
        };

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
                label: Some("class".to_string()),
                val: RuntimeValue::string(record.class),
            },
            Prop {
                label: Some("event_revision".to_string()),
                val: RuntimeValue::Literal(Literal::Integer(record.revision as i64)),
            },
            Prop {
                label: Some("position".to_string()),
                val: RuntimeValue::Literal(Literal::Integer(record.position as i64)),
            },
            Prop {
                label: Some("payload".to_string()),
                val: from_json_to_pyro_runtime_value(payload)?,
            },
        ];

        Ok(RuntimeValue::Record(pyro_core::ast::Record { props }))
    }
}

fn from_json_to_pyro_runtime_value(value: Value) -> eyre::Result<RuntimeValue> {
    match value {
        Value::Null => eyre::bail!("NULL is not supported"),
        Value::Bool(b) => Ok(RuntimeValue::Literal(Literal::Bool(b))),
        Value::Number(n) => match n.as_i64() {
            None => match n.as_f64() {
                None => eyre::bail!("We only support i64 or f64 so far"),
                Some(n) => Ok(RuntimeValue::Literal(Literal::Integer(
                    i64::try_from(n as i128).unwrap_or(i64::MAX),
                ))),
            },
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

pub fn from_runtime_value_to_json(value: RuntimeValue) -> eyre::Result<Value> {
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

struct SubServer {
    recv: Arc<Mutex<UnboundedReceiver<RuntimeValue>>>,
}

impl SubServer {
    fn new(inner: UnboundedReceiver<RuntimeValue>) -> Self {
        Self {
            recv: Arc::new(Mutex::new(inner)),
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

pub enum PyroRuntimeNotification {
    SubscribedToStream(String),
    UnsubscribedToStream(String),
}

#[allow(clippy::large_enum_variant)]
pub enum PyroEvent {
    Value(RuntimeValue),
    Notification(PyroRuntimeNotification),
}

pub struct PyroRuntime {
    engine: Engine<NominalTyping>,
    output: UnboundedReceiver<RuntimeValue>,
    notifications: UnboundedReceiver<PyroRuntimeNotification>,
    started: DateTime<Utc>,
}

impl PyroRuntime {
    pub fn compile(&self, code: &str) -> eyre::Result<PyroProcess> {
        self.engine.compile(code)
    }

    pub async fn recv(&mut self) -> Option<PyroEvent> {
        select! {
            Some(value) = self.output.recv() => Some(PyroEvent::Value(value)),
            Some(notif) = self.notifications.recv() => Some(PyroEvent::Notification(notif)),
            else => None
        }
    }

    pub fn started(&self) -> DateTime<Utc> {
        self.started
    }
}

pub fn create_pyro_runtime(
    context: RequestContext,
    client: ManagerClient,
    proc_id: ProcId,
    name: &str,
) -> eyre::Result<PyroRuntime> {
    let (stdout_handle, mut stdout_recv) = unbounded_channel();
    let env = Env { stdout_handle };
    let name_stdout = name.to_string();
    tokio::spawn(async move {
        while let Some(value) = stdout_recv.recv().await {
            tracing::info!(
                kind = "pyro",
                process = name_stdout,
                message = value.to_string(),
            );
        }
    });

    let (send_output, recv_output) = unbounded_channel();
    let (send_notification, recv_notification) = unbounded_channel();
    let name_subscribe = name.to_string();
    let engine = Engine::with_nominal_typing()
        .stdlib(env)
        .register_type::<EventEntry>("Entry")
        .register_type::<EventRecord>("EventRecord")
        .register_value("output", ProgramOutput(send_output))
        .register_function("subscribe", move |stream_name: String| {
            tracing::debug!(
                name = name_subscribe,
                proc_id = proc_id,
                stream_name = stream_name,
                "program emitted a subscription request"
            );

            let (input, recv) = unbounded_channel();
            let name_subscribe_local = name_subscribe.clone();
            let manager_client = client.clone();
            let local_send_notification = send_notification.clone();
            tokio::spawn(async move {
                let mut consumer =
                    match start_consumer(context, stream_name.clone(), Revision::Start, manager_client)
                        .await
                    {
                        Err(error) => {
                            tracing::error!(%error, stream_name, "unexpected error when starting a new consumer");
                            return Ok(());
                        }

                        Ok(result) => match result {
                            ConsumerResult::Success(c) => c,
                            ConsumerResult::StreamDeleted => {
                                tracing::error!(reason = "stream deleted", stream_name, "cannot start a new consumer");
                                return Ok(());
                            }
                        }
                    };

                loop {
                    match consumer.next().await {
                        Err(error) => {
                            tracing::error!(
                                name = name_subscribe_local,
                                target = "subscription",
                                proc_id,
                                kind = "pyro",
                                stream_name,
                                reason = "error",
                                %error,
                                "unexpected subscription error"
                            );

                            let _ = local_send_notification.send(PyroRuntimeNotification::UnsubscribedToStream(name_subscribe_local.clone()));

                            break;
                        }

                        Ok(outcome) => {
                            if let Some(event) = outcome {
                                match event {
                                    SubscriptionEvent::CaughtUp => {}

                                    SubscriptionEvent::Confirmed(conf) => {
                                        if let SubscriptionConfirmation::StreamName(stream_name) =
                                            conf
                                        {
                                            tracing::debug!(
                                                name = name_subscribe_local,
                                                proc_id = proc_id,
                                                target = "subscription",
                                                kind = "pyro",
                                                stream_name = stream_name,
                                                "subscription is confirmed"
                                            );

                                            let _ = local_send_notification.send(PyroRuntimeNotification::SubscribedToStream(stream_name));
                                        }
                                    }

                                    SubscriptionEvent::Unsubscribed(reason) => {
                                        tracing::warn!(
                                            name = name_subscribe_local,
                                            target = "subscription",
                                            proc_id = proc_id,
                                            kind = "pyro",
                                            stream_name = stream_name,
                                            reason = ?reason,
                                            "subscription was dropped"
                                        );

                                        let _ = local_send_notification.send(PyroRuntimeNotification::UnsubscribedToStream(name_subscribe_local.clone()));

                                        break;
                                    }

                                    SubscriptionEvent::EventAppeared(record) => {
                                        let serialized = EventRecord(record)
                                            .serialize()
                                            .inspect_err(|error| {
                                                tracing::error!(
                                                    %error,
                                                    proc_id,
                                                    stream_name,
                                                    name = name_subscribe_local,
                                                    "serialization error"
                                                );
                                            })?;

                                        if input.send(serialized).is_err() {
                                            tracing::debug!(
                                                name = name_subscribe_local,
                                                target = "subscription",
                                                proc_id,
                                                kind = "pyro",
                                                stream_name,
                                                reason = "User",
                                                "subscription was dropped"
                                            );

                                            break;
                                        }
                                    }

                                    SubscriptionEvent::Notification(_) => {}
                                }
                            } else {
                                break;
                            }
                        }
                    }
                }

                tracing::debug!(
                    process = name_subscribe_local,
                    target = "subscription",
                    kind = "pyro",
                    proc_id = proc_id,
                    stream_name,
                    "subscription has completed"
                );

                let _ = local_send_notification.send(PyroRuntimeNotification::UnsubscribedToStream(name_subscribe_local));

                Ok::<_, eyre::Report>(())
            });

            SubServer::new(recv)
        })
        .build()?;

    Ok(PyroRuntime {
        engine,
        output: recv_output,
        notifications: recv_notification,
        started: Utc::now(),
    })
}
