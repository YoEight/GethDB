use futures_util::StreamExt;
use tokio::sync::{mpsc, oneshot};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tonic::codegen::tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Uri;
use uuid::Uuid;

use geth_common::{
    AppendStream, AppendStreamCompleted, DeleteStream, DeleteStreamCompleted, EndPoint, ReadStream,
    StreamRead, Subscribe, SubscriptionEvent,
};
use geth_common::generated::next::protocol::{operation_in, operation_out, OperationIn};
use geth_common::generated::next::protocol::protocol_client::ProtocolClient;

use crate::next::driver::Driver;

mod driver;

enum Msg {
    Command(Command),
    Event(Event),
}

#[derive(Clone)]
pub struct Command {
    pub correlation: Uuid,
    pub operation: Operation,
    pub resp: UnboundedSender<Event>,
}

#[derive(Clone)]
enum Operation {
    AppendStream(AppendStream),
    DeleteStream(DeleteStream),
    ReadStream(ReadStream),
    Subscribe(Subscribe),
}

impl From<Operation> for operation_in::Operation {
    fn from(operation: Operation) -> Self {
        match operation {
            Operation::AppendStream(req) => operation_in::Operation::AppendStream(req.into()),
            Operation::DeleteStream(req) => operation_in::Operation::DeleteStream(req.into()),
            Operation::ReadStream(req) => operation_in::Operation::ReadStream(req.into()),
            Operation::Subscribe(req) => operation_in::Operation::Subscribe(req.into()),
        }
    }
}

struct Event {
    correlation: Uuid,
    response: Reply,
}

impl Event {
    pub fn is_subscription_related(&self) -> bool {
        match &self.response {
            Reply::SubscriptionEvent(event) => match event {
                SubscriptionEvent::Error(_) => false,
                _ => true,
            },

            _ => false,
        }
    }
}

enum Reply {
    AppendStreamCompleted(AppendStreamCompleted),
    StreamRead(StreamRead),
    SubscriptionEvent(SubscriptionEvent),
    DeleteStreamCompleted(DeleteStreamCompleted),
    Errored,
}

type Connection = UnboundedSender<OperationIn>;
type Mailbox = UnboundedSender<Msg>;

async fn connect_to_node(uri: Uri, mailbox: Mailbox) -> eyre::Result<Connection> {
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
                            Reply::AppendStreamCompleted(resp.into())
                        }
                        operation_out::Operation::StreamRead(resp) => {
                            Reply::StreamRead(resp.into())
                        }
                        operation_out::Operation::SubscriptionEvent(resp) => {
                            Reply::SubscriptionEvent(resp.into())
                        }
                        operation_out::Operation::DeleteCompleted(resp) => {
                            Reply::DeleteStreamCompleted(resp.into())
                        }
                    };

                    let response = Event {
                        correlation,
                        response: reply,
                    };

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

async fn multiplex_loop(mut driver: Driver, mut receiver: UnboundedReceiver<Msg>) {
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
