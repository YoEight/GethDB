use tokio::sync::mpsc::UnboundedReceiver;
use uuid::Uuid;

use geth_common::{AppendStream, AppendStreamCompleted, DeleteStream, DeleteStreamCompleted, EndPoint, ReadStream, StreamRead, Subscribe, SubscriptionEvent};

enum Msg {
    Request(Request),
    Response(Response),
}

pub struct Request {
    pub correlation: Uuid,
    pub operation: Operation,
}

enum Operation {
    AppendStream(AppendStream),
    DeleteStream(DeleteStream),
    ReadStream(ReadStream),
    Subscribe(Subscribe),
}

enum Response {
    AppendStreamCompleted(AppendStreamCompleted),
    StreamRead(StreamRead),
    SubscriptionEvent(SubscriptionEvent),
    DeleteStreamCompleted(DeleteStreamCompleted),
}

async fn multiplex_loop(mut endpoint: EndPoint, mut receiver: UnboundedReceiver<Msg>) {
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
