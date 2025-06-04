use std::{
    collections::HashMap,
    sync::{
        atomic::{self, AtomicUsize},
        Arc,
    },
};

use chrono::{DateTime, Utc};
use geth_common::Record;
use pyro_core::{ast::Prop, sym::Literal, NominalTyping};
use pyro_runtime::{
    helpers::{Declared, TypeBuilder},
    Channel, Engine, Env, PyroProcess, PyroType, PyroValue, RuntimeValue,
};
use serde_json::Value;
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    Mutex, RwLock,
};

use crate::process::{subscription::SubscriptionClient, ProcId};

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
                val: from_json_to_pyro_runtime_value(serde_json::from_slice::<Value>(
                    record.data.as_ref(),
                )?)?,
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

pub struct PyroRuntime {
    engine: Engine<NominalTyping>,
    output: UnboundedReceiver<RuntimeValue>,
    subs: Arc<RwLock<Vec<String>>>,
    pushed_events: Arc<AtomicUsize>,
    started: DateTime<Utc>,
}

impl PyroRuntime {
    pub fn compile(&self, code: &str) -> eyre::Result<PyroProcess> {
        self.engine.compile(code)
    }

    pub async fn recv(&mut self) -> Option<RuntimeValue> {
        self.output.recv().await
    }

    pub async fn subs(&self) -> Vec<String> {
        self.subs.read().await.clone()
    }

    pub fn pushed_events(&self) -> usize {
        self.pushed_events.load(atomic::Ordering::SeqCst)
    }

    pub fn started(&self) -> DateTime<Utc> {
        self.started
    }
}

pub fn create_pyro_runtime(
    client: SubscriptionClient,
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
    let name_subscribe = name.to_string();
    let subs = Arc::new(RwLock::new(Vec::new()));
    let pushed_events = Arc::new(AtomicUsize::new(0));
    let subs_subscribe = subs.clone();
    let pushed_events_subscribe = pushed_events.clone();
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

            let local_client = client.clone();
            let name_subscribe_local = name_subscribe.clone();
            let subs_subscribe_local = subs_subscribe.clone();
            let pushed_events_subscribe_local = pushed_events_subscribe.clone();
            tokio::spawn(async move {
                let mut streaming = local_client
                    .clone()
                    .subscribe_to_stream(stream_name.as_str())
                    .await
                    .inspect_err(|e| {
                        tracing::error!(
                            name = name_subscribe_local,
                            proc_id = proc_id,
                            target = "subscription",
                            kind = "pyro",
                            error = %e,
                            stream_name = stream_name,
                            "error when subscribing to stream",
                        );
                    })?;

                tracing::debug!(
                    name = name_subscribe_local,
                    proc_id = proc_id,
                    target = "subscription",
                    kind = "pyro",
                    stream_name = stream_name,
                    "subscription is confirmed"
                );

                {
                    let mut streams = subs_subscribe_local.write().await;
                    streams.push(stream_name.clone());
                }

                loop {
                    match streaming.next().await {
                        Err(e) => {
                            tracing::error!(
                                name = name_subscribe_local,
                                target = "subscription",
                                proc_id = proc_id,
                                kind = "pyro",
                                stream_name = stream_name,
                                reason = "error",
                                error = %e,
                                "unexpected subscription error"
                            );

                            break;
                        }

                        Ok(outcome) => {
                            if let Some(record) = outcome {
                                if input.send(EventRecord(record).serialize()?).is_err() {
                                    tracing::debug!(
                                        name = name_subscribe_local,
                                        target = "subscription",
                                        proc_id = proc_id,
                                        kind = "pyro",
                                        stream_name = stream_name,
                                        reason = "unsubscribe",
                                        "subscription was dropped"
                                    );

                                    break;
                                }

                                pushed_events_subscribe_local
                                    .fetch_add(1, atomic::Ordering::SeqCst);
                            } else {
                                tracing::error!("WTF!!!");
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
                    stream_name = stream_name,
                    "subscription has completed"
                );

                Ok::<_, eyre::Report>(())
            });

            SubServer::new(recv)
        })
        .build()?;

    Ok(PyroRuntime {
        engine,
        output: recv_output,
        subs,
        pushed_events,
        started: Utc::now(),
    })
}
