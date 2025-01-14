use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tonic::codegen::tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::transport::Uri;

use geth_common::generated::next::protocol;
use geth_common::generated::next::protocol::protocol_client::ProtocolClient;
use geth_common::{OperationIn, OperationOut};

use crate::next::driver::Driver;

mod driver;
pub mod grpc;

pub enum Msg {
    Command(Command),
    Event(OperationOut),
    Disconnected,
}

#[derive(Clone)]
pub struct Command {
    pub operation_in: OperationIn,
    pub resp: UnboundedSender<OperationOut>,
}

type Connection = UnboundedSender<protocol::OperationIn>;
type Mailbox = UnboundedSender<Msg>;

pub enum ConnErr {
    Transport(tonic::transport::Error),
    Status(tonic::Status),
}

pub(crate) async fn connect_to_node(uri: &Uri, mailbox: Mailbox) -> Result<Connection, ConnErr> {
    let mut client = ProtocolClient::connect(uri.clone())
        .await
        .map_err(ConnErr::Transport)?;
    let (connection, stream_request) = mpsc::unbounded_channel();

    let mut stream_response = client
        .multiplex(UnboundedReceiverStream::new(stream_request))
        .await
        .map_err(ConnErr::Status)?
        .into_inner();

    tokio::spawn(async move {
        while let Some(reply) = stream_response.next().await {
            match reply {
                Err(e) => {
                    tracing::error!("error receiving response: {:?}", e);
                    // TODO - Needs find a way to handle unexpected errors so we avoid
                    // stalled operations.
                }

                Ok(out) => {
                    if mailbox.send(Msg::Event(out.into())).is_err() {
                        tracing::warn!("seems main connection is closed");
                        return;
                    }
                }
            }
        }

        let _ = mailbox.send(Msg::Disconnected);
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

            Msg::Disconnected => {
                driver.handle_disconnect();
            }
        }
    }
}
