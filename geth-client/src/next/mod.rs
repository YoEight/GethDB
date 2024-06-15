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
use geth_common::generated::next::protocol::{operation_out, OperationIn};
use geth_common::generated::next::protocol::protocol_client::ProtocolClient;

enum Msg {
    Request(Request),
    Response(Response),
}

pub struct Request {
    pub correlation: Uuid,
    pub operation: Operation,
    pub resp: oneshot::Sender<Response>,
}

enum Operation {
    AppendStream(AppendStream),
    DeleteStream(DeleteStream),
    ReadStream(ReadStream),
    Subscribe(Subscribe),
}

struct Response {
    correlation: Uuid,
    response: Reply,
}

enum Reply {
    AppendStreamCompleted(AppendStreamCompleted),
    StreamRead(StreamRead),
    SubscriptionEvent(SubscriptionEvent),
    DeleteStreamCompleted(DeleteStreamCompleted),
}

type Connection = UnboundedSender<OperationIn>;
type Mailbox = UnboundedSender<Msg>;

async fn set_up_connection(uri: Uri, mailbox: Mailbox) -> eyre::Result<Connection> {
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

                Ok(reply) => match reply.operation.unwrap() {
                    operation_out::Operation::AppendCompleted(resp) => {
                        // mailbox.send(Msg::Response(Response {
                        //     correlation: resp.correlation,
                        //     response: Reply::AppendStreamCompleted(resp),
                        // }));
                    }
                    operation_out::Operation::StreamRead(resp) => {
                        // mailbox.send(Msg::Response(Response {
                        //     correlation: resp.correlation,
                        //     response: Reply::StreamRead(resp),
                        // }));
                    }
                    operation_out::Operation::SubscriptionEvent(resp) => {
                        // mailbox.send(Msg::Response(Response {
                        //     correlation: resp.correlation,
                        //     response: Reply::SubscriptionEvent(resp),
                        // }));
                    }
                    operation_out::Operation::DeleteCompleted(resp) => {
                        // mailbox.send(Msg::Response(Response {
                        //     correlation: resp.correlation,
                        //     response: Reply::DeleteStreamCompleted(resp),
                        // }));
                    }
                },
            }
        }
    });

    Ok(connection)
}

async fn multiplex_loop(mut endpoint: EndPoint, mut receiver: UnboundedReceiver<Msg>) {
    let uri = format!("http://{}:{}", endpoint.host, endpoint.port)
        .parse::<Uri>()
        .unwrap();
    let mut client = ProtocolClient::connect(uri).await.unwrap();
    let (mailbox, stream_request) = mpsc::unbounded_channel();

    let stream_response = client
        .multiplex(UnboundedReceiverStream::new(stream_request))
        .await
        .unwrap()
        .into_inner();

    while let Some(msg) = receiver.recv().await {
        match msg {
            Msg::Request(req) => {
                match req.operation {
                    Operation::AppendStream(req) => {
                        // handle append stream request
                    }
                    Operation::DeleteStream(req) => {
                        // handle delete stream request
                    }
                    Operation::ReadStream(req) => {
                        // handle read stream request
                    }
                    Operation::Subscribe(req) => {
                        // handle subscribe request
                    }
                }
            }
            Msg::Response(_) => {}
        }
    }
}
