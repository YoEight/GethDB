use std::marker::PhantomData;

use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tonic::codegen::tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Uri;
use uuid::Uuid;

use geth_common::{
    AppendStream, AppendStreamCompleted, DeleteStream, DeleteStreamCompleted, GetProgram,
    KillProgram, ListPrograms, ProgramKilled, ProgramListed, ProgramObtained, ProgramSummary,
    ReadStream, StreamRead, Subscribe, SubscriptionEventIR,
};
use geth_common::generated::next;
use geth_common::generated::next::protocol;
use geth_common::generated::next::protocol::{operation_in, operation_out};
use geth_common::generated::next::protocol::protocol_client::ProtocolClient;

use crate::next::driver::Driver;

mod driver;
pub mod grpc;

pub enum Msg {
    Command(Command),
    Event(Event),
}

pub type Callback = UnboundedReceiver<Event>;
pub type Package = (Callback, Msg);

#[derive(Clone)]
pub struct Command {
    pub operation_in: OperationIn,
    pub resp: UnboundedSender<Event>,
}

#[derive(Clone)]
pub struct OperationIn {
    pub correlation: Uuid,
    pub operation: Operation,
}

impl From<protocol::OperationIn> for OperationIn {
    fn from(operation: protocol::OperationIn) -> Self {
        let correlation = operation.correlation.unwrap().into();
        let operation = match operation.operation.unwrap() {
            operation_in::Operation::AppendStream(req) => Operation::AppendStream(req.into()),
            operation_in::Operation::DeleteStream(req) => Operation::DeleteStream(req.into()),
            operation_in::Operation::ReadStream(req) => Operation::ReadStream(req.into()),
            operation_in::Operation::Subscribe(req) => Operation::Subscribe(req.into()),
            operation_in::Operation::ListPrograms(req) => Operation::ListPrograms(req.into()),
            operation_in::Operation::GetProgram(req) => Operation::GetProgram(req.into()),
            operation_in::Operation::KillProgram(req) => Operation::KillProgram(req.into()),
        };

        Self {
            correlation,
            operation,
        }
    }
}

impl From<OperationIn> for protocol::OperationIn {
    fn from(operation: OperationIn) -> Self {
        let correlation = Some(operation.correlation.into());
        let operation = Some(operation.operation.into());

        Self {
            correlation,
            operation,
        }
    }
}

#[derive(Clone)]
pub enum Operation {
    AppendStream(AppendStream),
    DeleteStream(DeleteStream),
    ReadStream(ReadStream),
    Subscribe(Subscribe),
    ListPrograms(ListPrograms),
    GetProgram(GetProgram),
    KillProgram(KillProgram),
}

impl From<Operation> for operation_in::Operation {
    fn from(operation: Operation) -> Self {
        match operation {
            Operation::AppendStream(req) => operation_in::Operation::AppendStream(req.into()),
            Operation::DeleteStream(req) => operation_in::Operation::DeleteStream(req.into()),
            Operation::ReadStream(req) => operation_in::Operation::ReadStream(req.into()),
            Operation::Subscribe(req) => operation_in::Operation::Subscribe(req.into()),
            Operation::ListPrograms(req) => operation_in::Operation::ListPrograms(req.into()),
            Operation::GetProgram(req) => operation_in::Operation::GetProgram(req.into()),
            Operation::KillProgram(req) => operation_in::Operation::KillProgram(req.into()),
        }
    }
}

pub struct Event {
    pub correlation: Uuid,
    pub reply: Reply,
}

impl Event {
    pub fn is_subscription_related(&self) -> bool {
        match &self.reply {
            Reply::Success(OperationOut::SubscriptionEvent(event)) => match event {
                SubscriptionEventIR::Error(_) => false,
                _ => true,
            },

            _ => false,
        }
    }
}

pub enum OperationOut {
    AppendStreamCompleted(AppendStreamCompleted),
    StreamRead(StreamRead),
    SubscriptionEvent(SubscriptionEventIR),
    DeleteStreamCompleted(DeleteStreamCompleted),
    ProgramsListed(ProgramListed),
    ProgramKilled(ProgramKilled),
    ProgramObtained(ProgramObtained),
}

pub enum Reply {
    Success(OperationOut),
    Errored,
}

type Connection = UnboundedSender<protocol::OperationIn>;
type Mailbox = UnboundedSender<Msg>;

pub(crate) async fn connect_to_node(uri: Uri, mailbox: Mailbox) -> eyre::Result<Connection> {
    let mut client = ProtocolClient::connect(uri).await?;
    let (connection, stream_request) = mpsc::unbounded_channel();

    let mut stream_response = client
        .multiplex(UnboundedReceiverStream::new(stream_request))
        .await?
        .into_inner();

    tokio::spawn(async move {
        while let Some(reply) = stream_response.next().await {
            match reply {
                Err(e) => {
                    tracing::error!("Error receiving response: {:?}", e);
                    break;
                }

                Ok(reply) => {
                    let correlation = reply.correlation.unwrap().into();
                    let reply = match reply.operation.unwrap() {
                        operation_out::Operation::AppendCompleted(resp) => {
                            Reply::Success(OperationOut::AppendStreamCompleted(resp.into()))
                        }
                        operation_out::Operation::StreamRead(resp) => {
                            Reply::Success(OperationOut::StreamRead(resp.into()))
                        }
                        operation_out::Operation::SubscriptionEvent(resp) => {
                            Reply::Success(OperationOut::SubscriptionEvent(resp.into()))
                        }
                        operation_out::Operation::DeleteCompleted(resp) => {
                            Reply::Success(OperationOut::DeleteStreamCompleted(resp.into()))
                        }
                        operation_out::Operation::ProgramsListed(resp) => {
                            Reply::Success(OperationOut::ProgramsListed(resp.into()))
                        }

                        operation_out::Operation::ProgramKilled(resp) => {
                            Reply::Success(OperationOut::ProgramKilled(resp.into()))
                        }

                        operation_out::Operation::ProgramGot(resp) => {
                            Reply::Success(OperationOut::ProgramObtained(resp.into()))
                        }
                    };

                    let response = Event { correlation, reply };

                    if mailbox.send(Msg::Event(response)).is_err() {
                        tracing::warn!("seems main connection is closed");
                        break;
                    }
                }
            }
        }
    });

    Ok(connection)
}

pub(crate) async fn multiplex_loop(mut driver: Driver, mut receiver: UnboundedReceiver<Msg>) {
    while let Some(msg) = receiver.recv().await {
        match msg {
            Msg::Command(cmd) => {
                if let Err(e) = driver.handle_command(cmd).await {
                    tracing::error!("expected error when dealing command: {:?}", e);
                }
            }

            Msg::Event(event) => driver.handle_event(event),
        }
    }
}
